-- IsomerDB 0.18.0: embedding backfills through provider batch APIs (PRD FR-V1a), and bundled
-- models billed at list price plus a markup, after a one-time free allowance (FR-V1b).
--
--   * A model registered with batch_api => true sends a large backfill (at least batch_min_items
--     jobs ready) to the provider's Batch API (OpenAI's /v1/batches, or any compatible endpoint)
--     instead of synchronous requests: half the price at OpenAI, and none of the rate limit. The
--     embedder uploads the inputs, polls, and hands each result back through embed_complete, which
--     writes it only if the row's text still has the same hash. Items a batch doesn't return go
--     back to the ordinary path. Steady-state work never waits for a batch.
--   * A bundled model (bundled => true) runs on IsomerDB's own provider key. Its usage is billed at
--     the provider's list price times (1 + markup); the first bundled.free_tokens tokens of the
--     cluster are free. embedding_usage.billed_usd is what the customer owes; cost_usd stays what
--     the provider charges. Monthly caps count what the customer pays: billed_usd for bundled
--     models, cost_usd for their own keys. Metered hourly as embedding_billed_usd.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.18.0'" to load this file. \quit

ALTER TABLE isomer.embedding_models
    ADD COLUMN batch_api       boolean NOT NULL DEFAULT false,   -- the provider has an OpenAI-style Batch API
    ADD COLUMN batch_min_items integer NOT NULL DEFAULT 1000 CHECK (batch_min_items >= 1),
    ADD COLUMN batch_discount  numeric NOT NULL DEFAULT 0.5 CHECK (batch_discount > 0 AND batch_discount <= 1),
    ADD COLUMN bundled         boolean NOT NULL DEFAULT false,   -- IsomerDB's key, billed to the customer
    ADD COLUMN markup          numeric NOT NULL DEFAULT 0 CHECK (markup >= 0);

ALTER TABLE isomer.embedding_usage ADD COLUMN billed_usd numeric NOT NULL DEFAULT 0;
INSERT INTO isomer.usage_metrics VALUES
    ('embedding_billed_usd', 'counter', 'USD', 'What bundled embedding models bill the customer (list price plus markup, past the free allowance), by model');

-- One provider batch: the backfill jobs it carries point at it until it finishes.
CREATE TABLE isomer.embed_batches (
    id                bigserial PRIMARY KEY,
    model             text NOT NULL REFERENCES isomer.embedding_models (name),
    status            text NOT NULL DEFAULT 'preparing'
                      CHECK (status IN ('preparing', 'submitted', 'completed', 'failed', 'expired', 'cancelled')),
    provider_batch_id text,
    input_file_id     text,
    items             integer NOT NULL DEFAULT 0,
    tokens_est        bigint NOT NULL DEFAULT 0,
    returned          integer NOT NULL DEFAULT 0,   -- results written back
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    submitted_at      timestamptz,
    last_checked_at   timestamptz,
    finished_at       timestamptz,
    error             text
);
REVOKE ALL ON isomer.embed_batches FROM PUBLIC;

ALTER TABLE isomer.embed_jobs
    ADD COLUMN batch_id   bigint REFERENCES isomer.embed_batches (id) ON DELETE SET NULL,
    ADD COLUMN batch_item integer,
    ADD COLUMN batch_hash text;   -- the text's hash when it was batched
CREATE INDEX embed_jobs_batch ON isomer.embed_jobs (batch_id) WHERE batch_id IS NOT NULL;

-- The cluster's bundled-model free allowance, used once.
CREATE TABLE isomer.bundled_allowance (
    singleton   boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    used_tokens bigint NOT NULL DEFAULT 0
);
INSERT INTO isomer.bundled_allowance VALUES (true, 0);
REVOKE ALL ON isomer.bundled_allowance FROM PUBLIC;
INSERT INTO isomer.settings VALUES ('bundled.free_tokens', '1000000') ON CONFLICT (key) DO NOTHING;

DROP FUNCTION isomer.add_embedding_model(text, text, text, integer, text, text, numeric, integer, integer, integer, integer);

CREATE FUNCTION isomer.add_embedding_model(
    name              text,
    provider          text,
    model             text,
    dimensions        integer,
    endpoint          text    DEFAULT NULL,
    api_key_env       text    DEFAULT NULL,
    price_per_million numeric DEFAULT 0,
    max_batch_tokens  integer DEFAULT 8000,
    max_batch_items   integer DEFAULT 96,
    requests_per_min  integer DEFAULT 3000,
    tokens_per_min    integer DEFAULT 1000000,
    batch_api         boolean DEFAULT false,
    batch_min_items   integer DEFAULT 1000,
    batch_discount    numeric DEFAULT 0.5,
    bundled           boolean DEFAULT false,
    markup            numeric DEFAULT 0)
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    INSERT INTO isomer.embedding_models (name, provider, model, dimensions, endpoint, api_key_env, price_per_million,
                                         max_batch_tokens, max_batch_items, requests_per_min, tokens_per_min,
                                         batch_api, batch_min_items, batch_discount, bundled, markup)
    VALUES (name, provider, model, dimensions, endpoint, api_key_env, price_per_million,
            max_batch_tokens, max_batch_items, requests_per_min, tokens_per_min,
            batch_api, batch_min_items, batch_discount, bundled, markup)
$$;

-- Usage, priced: p_price_factor is 1 for ordinary requests and the model's batch_discount for
-- batch work. A bundled model's tokens past the free allowance are billed at list price plus
-- markup.
CREATE FUNCTION isomer.record_embedding_usage(p_projection text, p_model text, p_phase text,
                                              p_items integer, p_tokens bigint, p_price_factor numeric)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    m        isomer.embedding_models;
    v_cost   numeric;
    v_used   bigint;
    v_free   bigint := 0;
    v_billed numeric := 0;
    v_cap    numeric;
    v_spent  numeric;
BEGIN
    SELECT * INTO m FROM isomer.embedding_models WHERE name = p_model;
    v_cost := p_tokens * coalesce(m.price_per_million, 0) * p_price_factor / 1000000;
    IF m.bundled THEN
        SELECT used_tokens INTO v_used FROM isomer.bundled_allowance FOR UPDATE;
        v_free := least(p_tokens, greatest(0, coalesce(isomer._setting('bundled.free_tokens')::bigint, 0) - v_used));
        UPDATE isomer.bundled_allowance SET used_tokens = used_tokens + v_free;
        v_billed := (p_tokens - v_free) * m.price_per_million * p_price_factor * (1 + m.markup) / 1000000;
    END IF;
    INSERT INTO isomer.embedding_usage AS u (projection, model, month, phase, requests, items, tokens, cost_usd, billed_usd)
    VALUES (coalesce(p_projection, ''), p_model, date_trunc('month', now())::date, p_phase, 1, p_items, p_tokens, v_cost, v_billed)
    ON CONFLICT (projection, model, month, phase) DO UPDATE
        SET requests = u.requests + 1, items = u.items + EXCLUDED.items, tokens = u.tokens + EXCLUDED.tokens,
            cost_usd = u.cost_usd + EXCLUDED.cost_usd, billed_usd = u.billed_usd + EXCLUDED.billed_usd;

    IF p_projection IS NOT NULL THEN
        SELECT monthly_cap_usd INTO v_cap FROM isomer.projections WHERE name = p_projection;
        IF v_cap IS NOT NULL THEN
            SELECT sum(CASE WHEN em.bundled THEN u.billed_usd ELSE u.cost_usd END) INTO v_spent
            FROM isomer.embedding_usage u JOIN isomer.embedding_models em ON em.name = u.model
            WHERE u.projection = p_projection AND u.month = date_trunc('month', now())::date;
            IF v_spent >= v_cap THEN
                UPDATE isomer.projections SET status = 'paused_budget' WHERE name = p_projection AND status = 'active';
                IF FOUND THEN
                    PERFORM isomer._log('warning', p_projection, format(
                        'monthly embedding cap $%s reached ($%s spent): paused until next month or a higher cap',
                        v_cap, round(v_spent, 4)));
                END IF;
            END IF;
        END IF;
    END IF;
END $$;

-- The old five-argument form: ordinary requests at full price.
CREATE OR REPLACE FUNCTION isomer.record_embedding_usage(p_projection text, p_model text, p_phase text,
                                                         p_items integer, p_tokens bigint)
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    SELECT isomer.record_embedding_usage(p_projection, p_model, p_phase, p_items, p_tokens, 1)
$$;

-- ── Batch backfills (FR-V1a) ────────────────────────────────────────────────

-- Which projection each returned item belonged to, for billing the batch's tokens.
CREATE TABLE isomer._embed_batch_shares (
    batch_id   bigint NOT NULL,
    projection text NOT NULL,
    tokens     bigint NOT NULL
);
REVOKE ALL ON isomer._embed_batch_shares FROM PUBLIC;


-- Claim up to p_max ready backfill jobs for a model into a new batch, if at least the model's
-- batch_min_items are ready. The jobs stay claimed (not_before well past the provider's 24-hour
-- window) until the batch finishes. Returns the batch's id and its items, numbered.
CREATE FUNCTION isomer.embed_batch_lease(p_model text, p_max integer DEFAULT 50000)
RETURNS TABLE (batch_id bigint, item integer, input text, tokens_est integer)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    m     isomer.embedding_models;
    v_id  bigint;
    v_n   integer;
BEGIN
    SELECT * INTO STRICT m FROM isomer.embedding_models WHERE name = p_model;
    IF NOT m.batch_api THEN
        RETURN;
    END IF;
    SELECT count(*) INTO v_n FROM isomer.embed_jobs j JOIN isomer.projections p ON p.name = j.projection AND p.status = 'active'
    WHERE j.model = p_model AND j.phase = 'backfill' AND j.batch_id IS NULL AND j.not_before <= now();
    IF v_n < m.batch_min_items THEN
        RETURN;
    END IF;
    INSERT INTO isomer.embed_batches (model) VALUES (p_model) RETURNING id INTO v_id;
    -- Lock first, then number: Postgres doesn't take row locks in a query with window functions.
    WITH locked AS (
        SELECT j.projection, j.key, j.model, j.enqueued_at
        FROM isomer.embed_jobs j JOIN isomer.projections p ON p.name = j.projection AND p.status = 'active'
        WHERE j.model = p_model AND j.phase = 'backfill' AND j.batch_id IS NULL AND j.not_before <= now()
        ORDER BY j.enqueued_at, j.key
        LIMIT p_max
        FOR UPDATE OF j SKIP LOCKED),
    chosen AS (
        SELECT l.projection, l.key, l.model, row_number() OVER (ORDER BY l.enqueued_at, l.key) AS n FROM locked l)
    UPDATE isomer.embed_jobs j
    SET batch_id = v_id, batch_item = c.n, batch_hash = j.source_hash, not_before = now() + interval '26 hours'
    FROM chosen c
    WHERE j.projection = c.projection AND j.key = c.key AND j.model = c.model;
    UPDATE isomer.embed_batches b
    SET (items, tokens_est) = (SELECT count(*), coalesce(sum(j.tokens_est), 0) FROM isomer.embed_jobs j WHERE j.batch_id = v_id)
    WHERE b.id = v_id;
    RETURN QUERY SELECT v_id, j.batch_item, j.input, j.tokens_est FROM isomer.embed_jobs j
                 WHERE j.batch_id = v_id ORDER BY j.batch_item;
END $$;

CREATE FUNCTION isomer.embed_batch_submitted(p_batch bigint, p_provider_batch_id text, p_input_file_id text)
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    UPDATE isomer.embed_batches
    SET status = 'submitted', provider_batch_id = p_provider_batch_id, input_file_id = p_input_file_id,
        submitted_at = clock_timestamp(), last_checked_at = clock_timestamp()
    WHERE id = p_batch
$$;

-- An item the provider rejected: back to the ordinary path, counted as an attempt.
CREATE FUNCTION isomer.embed_batch_item_failed(p_batch bigint, p_item integer, p_error text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    j isomer.embed_jobs;
BEGIN
    UPDATE isomer.embed_jobs SET batch_id = NULL, batch_item = NULL, batch_hash = NULL, not_before = now()
    WHERE batch_id = p_batch AND batch_item = p_item
    RETURNING * INTO j;
    IF FOUND THEN
        PERFORM isomer.embed_failed(j.projection, j.key, j.model, j.source_hash, p_error);
    END IF;
END $$;

-- A batch is over (completed, failed, expired or cancelled): whatever it didn't return goes back
-- to the ordinary path at once, and its tokens are billed at the batch price.
CREATE FUNCTION isomer.embed_batch_finished(p_batch bigint, p_status text, p_tokens bigint DEFAULT NULL,
                                            p_error text DEFAULT NULL)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    b      isomer.embed_batches;
    v_left integer;
    r      record;
BEGIN
    SELECT * INTO STRICT b FROM isomer.embed_batches WHERE id = p_batch FOR UPDATE;
    UPDATE isomer.embed_jobs SET batch_id = NULL, batch_item = NULL, batch_hash = NULL, not_before = now() WHERE batch_id = p_batch;
    GET DIAGNOSTICS v_left = ROW_COUNT;
    UPDATE isomer.embed_batches SET status = p_status, finished_at = clock_timestamp(), error = p_error WHERE id = p_batch;
    IF b.returned > 0 AND coalesce(p_tokens, 0) > 0 THEN
        -- The provider reports tokens for the whole batch; attribute them to the projections
        -- whose items came back, in proportion to their estimates.
        FOR r IN SELECT projection, sum(tokens) AS tokens, count(*)::int AS items
                 FROM isomer._embed_batch_shares WHERE batch_id = p_batch GROUP BY projection LOOP
            PERFORM isomer.record_embedding_usage(r.projection, b.model, 'backfill', r.items,
                round(p_tokens * r.tokens / greatest((SELECT sum(tokens) FROM isomer._embed_batch_shares WHERE batch_id = p_batch), 1))::bigint,
                (SELECT batch_discount FROM isomer.embedding_models WHERE name = b.model));
        END LOOP;
    END IF;
    DELETE FROM isomer._embed_batch_shares WHERE batch_id = p_batch;
    IF v_left > 0 THEN
        PERFORM isomer._log(CASE WHEN p_status = 'completed' THEN 'info' ELSE 'warning' END, b.model,
            format('embedding batch %s %s: %s item(s) back to ordinary requests%s', p_batch, p_status, v_left,
                   coalesce(': ' || p_error, '')));
    END IF;
    RETURN v_left;
END $$;

-- One returned item: written if the row's text is unchanged since it was batched (the same
-- check as any embedding), then counted for billing.
CREATE FUNCTION isomer.embed_batch_result(p_batch bigint, p_item integer, p_embedding text)
RETURNS boolean LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    j isomer.embed_jobs;
BEGIN
    SELECT * INTO j FROM isomer.embed_jobs WHERE batch_id = p_batch AND batch_item = p_item;
    IF NOT FOUND THEN
        RETURN false;   -- the row went away, or its new text was embedded meanwhile
    END IF;
    UPDATE isomer.embed_batches SET returned = returned + 1 WHERE id = p_batch;
    INSERT INTO isomer._embed_batch_shares VALUES (p_batch, j.projection, j.tokens_est);
    IF j.source_hash IS DISTINCT FROM j.batch_hash THEN
        -- The row's text changed after it was batched: this embedding is of the old text. The
        -- job was made ready again by the change, and the new text goes by ordinary request.
        RETURN false;
    END IF;
    RETURN isomer.embed_complete(j.projection, j.key, j.model, j.source_hash, p_embedding);
END $$;

-- Batches the embedder is waiting on.
CREATE FUNCTION isomer.embed_batches_open()
RETURNS SETOF isomer.embed_batches LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT * FROM isomer.embed_batches WHERE status IN ('preparing', 'submitted') ORDER BY id
$$;

-- ── Metering: what bundled models bill ──────────────────────────────────────

CREATE OR REPLACE FUNCTION isomer._meter_embedding_billing(v_now timestamptz, v_baseline boolean)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT model, month, sum(billed_usd) AS billed
             FROM isomer.embedding_usage WHERE month >= date_trunc('month', v_now - interval '1 day')::date
             GROUP BY model, month LOOP
        PERFORM isomer._usage_add('embedding_billed_usd', r.model,
                                  isomer._usage_delta(format('billed:%s:%s', r.model, r.month), r.billed, v_baseline), v_now);
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION isomer.meter()
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
    -- What bundled models bill the customer (list price plus markup, past the free allowance).
    PERFORM isomer._meter_embedding_billing(v_now, v_last IS NULL);

    -- SQL statements on the coordinator.
    IF to_regclass('public.pg_stat_statements') IS NOT NULL THEN
        PERFORM isomer._usage_add('sql_statements', '',
            isomer._usage_delta('statements', (SELECT coalesce(sum(calls), 0) FROM public.pg_stat_statements), v_last IS NULL), v_now);
    END IF;

    -- Keep 400 days.
    DELETE FROM isomer.usage WHERE hour < v_now - interval '400 days';
END $$;

REVOKE EXECUTE ON FUNCTION isomer.add_embedding_model(text, text, text, integer, text, text, numeric, integer, integer,
                                                       integer, integer, boolean, integer, numeric, boolean, numeric),
                           isomer.record_embedding_usage(text, text, text, integer, bigint, numeric),
                           isomer.embed_batch_lease(text, integer), isomer.embed_batch_submitted(bigint, text, text),
                           isomer.embed_batch_result(bigint, integer, text), isomer.embed_batch_item_failed(bigint, integer, text),
                           isomer.embed_batch_finished(bigint, text, bigint, text), isomer.embed_batches_open(),
                           isomer._meter_embedding_billing(timestamptz, boolean)
    FROM PUBLIC;
