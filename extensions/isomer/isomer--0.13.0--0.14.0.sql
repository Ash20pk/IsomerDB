-- IsomerDB 0.14.0: usage metering for billing (PRD FR-CL2).
--
--   * isomer.usage holds one row per hour, metric and dimension. Gauges (storage) keep the hour's
--     peak; counters (node-hours, documents written, embedding tokens and cost, SQL statements,
--     HTTP API requests) accumulate.
--   * isomer.meter() samples every minute (pg_cron). Counters are taken as differences from the
--     last sample, so a restart or a reset never counts anything twice.
--   * isomer.record_usage(metric, dimension, amount) adds to a counter: the HTTP API reports its
--     requests that way.
--   * isomer.usage_report(from, to, granularity) rolls the hours up by hour, day or month, for a
--     billing system to pull (the admin API serves it as JSON or CSV).

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.14.0'" to load this file. \quit

CREATE TABLE isomer.usage (
    hour      timestamptz NOT NULL,   -- start of the hour, UTC
    metric    text NOT NULL,
    dimension text NOT NULL DEFAULT '',
    kind      text NOT NULL CHECK (kind IN ('gauge', 'counter')),
    value     numeric NOT NULL,
    PRIMARY KEY (hour, metric, dimension)
);
SELECT pg_catalog.pg_extension_config_dump('isomer.usage', '');

-- What each metric means, its unit, and whether it is a gauge or a counter.
CREATE TABLE isomer.usage_metrics (
    metric      text PRIMARY KEY,
    kind        text NOT NULL CHECK (kind IN ('gauge', 'counter')),
    unit        text NOT NULL,
    description text NOT NULL
);
INSERT INTO isomer.usage_metrics VALUES
    ('storage_bytes',      'gauge',   'bytes',      'Data stored across every node, peak in the period'),
    ('node_hours',         'counter', 'node-hours', 'Database nodes (coordinator and workers) running, by role'),
    ('documents_written',  'counter', 'documents',  'Documents and cache entries written by projection maintenance, by projection'),
    ('embedding_tokens',   'counter', 'tokens',     'Tokens embedded, by model'),
    ('embedding_cost_usd', 'counter', 'USD',        'Embedding provider cost, by model'),
    ('sql_statements',     'counter', 'statements', 'SQL statements run on the coordinator'),
    ('api_requests',       'counter', 'requests',   'HTTP API requests, by API key');

-- Last values seen for cumulative sources, to turn them into increments.
CREATE TABLE isomer.usage_cursor (
    source text PRIMARY KEY,
    value  numeric NOT NULL,
    at     timestamptz NOT NULL
);

CREATE FUNCTION isomer._usage_add(p_metric text, p_dimension text, p_amount numeric, p_at timestamptz DEFAULT clock_timestamp())
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    INSERT INTO isomer.usage AS u (hour, metric, dimension, kind, value)
    SELECT date_trunc('hour', p_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC', p_metric, coalesce(p_dimension, ''), m.kind, p_amount
    FROM isomer.usage_metrics m WHERE m.metric = p_metric AND p_amount <> 0
    ON CONFLICT (hour, metric, dimension) DO UPDATE
        SET value = CASE WHEN u.kind = 'gauge' THEN greatest(u.value, EXCLUDED.value) ELSE u.value + EXCLUDED.value END
$$;

-- The increment of a cumulative source since the last sample. A source seen for the first time
-- counts in full (it appeared since the last sample: a new projection, model or month), except on
-- the meter's very first sample, which only takes a baseline. A source that went backwards was
-- reset, and counts what it has gathered since.
CREATE FUNCTION isomer._usage_delta(p_source text, p_now numeric, p_baseline boolean)
RETURNS numeric LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_prev numeric;
BEGIN
    SELECT value INTO v_prev FROM isomer.usage_cursor WHERE source = p_source;
    INSERT INTO isomer.usage_cursor (source, value, at) VALUES (p_source, p_now, clock_timestamp())
    ON CONFLICT (source) DO UPDATE SET value = EXCLUDED.value, at = EXCLUDED.at;
    IF v_prev IS NULL THEN
        RETURN CASE WHEN p_baseline THEN 0 ELSE p_now END;
    END IF;
    IF p_now < v_prev THEN
        RETURN p_now;
    END IF;
    RETURN p_now - v_prev;
END $$;

-- Add to a counter. For services that count their own work (the HTTP API).
CREATE FUNCTION isomer.record_usage(metric text, dimension text, amount numeric)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM isomer.usage_metrics m WHERE m.metric = record_usage.metric AND m.kind = 'counter') THEN
        RAISE EXCEPTION 'isomer: % is not a usage counter (see isomer.usage_metrics)', metric;
    END IF;
    PERFORM isomer._usage_add(metric, dimension, amount);
END $$;

-- Take one sample of every metric. Runs every minute.
CREATE FUNCTION isomer.meter()
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE
    v_last  timestamptz;
    v_now   timestamptz := clock_timestamp();
    v_hours numeric;
    r       record;
    v_bytes numeric;
BEGIN
    -- Serialise samplers: two at once would count the same interval twice.
    PERFORM pg_advisory_xact_lock(hashtext('isomer'), hashtext('meter'));
    SELECT at INTO v_last FROM isomer.usage_cursor WHERE source = 'meter';
    INSERT INTO isomer.usage_cursor (source, value, at) VALUES ('meter', 0, v_now)
    ON CONFLICT (source) DO UPDATE SET at = EXCLUDED.at;
    -- Node-hours for the time since the last sample (at most two minutes: longer gaps mean the
    -- sampler was not running, and are not billed).
    v_hours := CASE WHEN v_last IS NULL THEN 0
                    ELSE least(extract(epoch FROM v_now - v_last), 120) / 3600 END;
    IF v_hours > 0 THEN
        FOR r IN SELECT CASE WHEN groupid = 0 THEN 'coordinator' ELSE 'worker' END AS role, count(*) AS n
                 FROM pg_dist_node WHERE isactive AND noderole = 'primary' GROUP BY 1 LOOP
            PERFORM isomer._usage_add('node_hours', r.role, r.n * v_hours, v_now);
        END LOOP;
    END IF;

    -- Storage: this database on every node.
    v_bytes := pg_database_size(current_database());
    BEGIN
        SELECT v_bytes + coalesce(sum(result::numeric), 0) INTO v_bytes
        FROM run_command_on_workers('SELECT pg_database_size(current_database())') WHERE success;
    EXCEPTION WHEN OTHERS THEN
        NULL;   -- single node, or a worker unreachable: count what can be seen
    END;
    PERFORM isomer._usage_add('storage_bytes', '', v_bytes, v_now);

    -- Projection writes.
    FOR r IN SELECT projection, documents_written + documents_deleted AS n FROM isomer.projection_stats LOOP
        PERFORM isomer._usage_add('documents_written', r.projection,
                                  isomer._usage_delta('documents:' || r.projection, r.n, v_last IS NULL), v_now);
    END LOOP;

    -- Embeddings (embedding_usage keeps monthly totals).
    FOR r IN SELECT model, month, sum(tokens) AS tokens, sum(cost_usd) AS cost
             FROM isomer.embedding_usage WHERE month >= date_trunc('month', v_now - interval '1 day')::date
             GROUP BY model, month LOOP
        PERFORM isomer._usage_add('embedding_tokens', r.model,
                                  isomer._usage_delta(format('tokens:%s:%s', r.model, r.month), r.tokens, v_last IS NULL), v_now);
        PERFORM isomer._usage_add('embedding_cost_usd', r.model,
                                  isomer._usage_delta(format('cost:%s:%s', r.model, r.month), r.cost, v_last IS NULL), v_now);
    END LOOP;

    -- SQL statements on the coordinator.
    IF to_regclass('public.pg_stat_statements') IS NOT NULL THEN
        PERFORM isomer._usage_add('sql_statements', '',
            isomer._usage_delta('statements', (SELECT coalesce(sum(calls), 0) FROM public.pg_stat_statements), v_last IS NULL), v_now);
    END IF;

    -- Keep 400 days.
    DELETE FROM isomer.usage WHERE hour < v_now - interval '400 days';
END $$;

-- Usage rolled up by hour, day or month, from (inclusive) to (exclusive).
CREATE FUNCTION isomer.usage_report(p_from timestamptz, p_to timestamptz DEFAULT now(), p_granularity text DEFAULT 'day')
RETURNS TABLE (period timestamptz, metric text, dimension text, unit text, value numeric)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
BEGIN
    IF p_granularity NOT IN ('hour', 'day', 'month') THEN
        RAISE EXCEPTION 'isomer: granularity must be hour, day or month, not %', p_granularity USING ERRCODE = '22023';
    END IF;
    RETURN QUERY
    SELECT date_trunc(p_granularity, u.hour AT TIME ZONE 'UTC') AT TIME ZONE 'UTC', u.metric, u.dimension, m.unit,
           CASE WHEN u.kind = 'gauge' THEN max(u.value) ELSE sum(u.value) END
    FROM isomer.usage u JOIN isomer.usage_metrics m USING (metric)
    WHERE u.hour >= date_trunc('hour', p_from AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AND u.hour < p_to
    GROUP BY 1, u.metric, u.dimension, m.unit, u.kind
    ORDER BY 1, u.metric, u.dimension;
END $$;

REVOKE EXECUTE ON FUNCTION isomer.meter(), isomer.usage_report(timestamptz, timestamptz, text),
                           isomer.record_usage(text, text, numeric) FROM PUBLIC;

SELECT cron.schedule('isomer-meter', '* * * * *', 'SELECT isomer.meter()') WHERE pg_catalog.citus_is_coordinator();
