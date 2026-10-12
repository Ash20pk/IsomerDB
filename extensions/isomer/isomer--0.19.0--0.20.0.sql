-- IsomerDB 0.20.0: event projections and analytics projections (PRD roadmap, after phase 3).
--
--   * An event projection turns row changes into domain events. Each event has a type, a key and
--     a payload, from rules over the old and new row ("order_paid when new.status = 'paid' and
--     old.status is distinct from 'paid'"). Events are captured in the writing transaction, on the
--     node holding the row, reach the coordinator exactly once (the incremental engine's delta
--     pull), and are appended to the projection's feed in commit order. Consumers read them with
--     isomer.changes_since, or over HTTP from /changes and /live. Retention is the projection's
--     history setting.
--   * An analytics projection is a query kept as a columnar table (Citus columnar: compressed,
--     read column by column), for scans and aggregates over wide data. It is rebuilt from the query
--     and swapped in atomically when its sources have changed, at most once per refresh interval.
--     Changes are noticed from the tables' own statistics (rows inserted, updated, deleted, and the
--     file a TRUNCATE replaces), on every shard, so it needs no triggers and follows attached
--     tables too.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.20.0'" to load this file. \quit

ALTER TABLE isomer.projections DROP CONSTRAINT projections_kind_check;
ALTER TABLE isomer.projections ADD CONSTRAINT projections_kind_check
    CHECK (kind IN ('document', 'search', 'vector', 'cache', 'event', 'analytics'));

ALTER TABLE isomer.changes DROP CONSTRAINT changes_op_check;
ALTER TABLE isomer.changes ADD CONSTRAINT changes_op_check CHECK (op IN ('u', 'd', 'e'));   -- upsert, delete, event

-- ── Event projections ───────────────────────────────────────────────────────

-- events: {"<type>": {"when": "<condition over old and new>", "payload": "<jsonb expression>"}}.
-- old and new are the row before and after the change, of the source's row type: old is null for
-- an insert, new for a delete. "when" defaults to true (every change), payload to the new row (the
-- old one for a delete). key is an expression for the event's key.
CREATE FUNCTION isomer.create_event_projection(
    name      text,
    source    regclass,
    events    jsonb,
    key       text DEFAULT NULL,
    retention interval DEFAULT '7 days')
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_src    text := isomer._qualified(source);
    v_pk     text[] := isomer._primary_key(source);
    v_key    text;
    v_union  text;
    v_sql    text;
    v_probe  text;
BEGIN
    IF jsonb_typeof(events) <> 'object' OR events = '{}' THEN
        RAISE EXCEPTION 'isomer: events must be an object of event types, e.g. {"order_paid": {"when": "new.status = ''paid''"}}';
    END IF;
    v_key := coalesce(key, CASE WHEN cardinality(v_pk) = 1 THEN format('coalesce(new.%1$I, old.%1$I)', v_pk[1]) END);
    IF v_key IS NULL THEN
        RAISE EXCEPTION 'isomer: % has no single-column primary key; give key => an expression over old and new', source;
    END IF;
    SELECT string_agg(format('SELECT %L::text AS type, (%s)::jsonb AS payload WHERE %s',
                             e.type,
                             -- A missing row reads as a row of nulls, which IS NULL.
                             coalesce(e.def ->> 'payload', 'to_jsonb(CASE WHEN new IS NULL THEN old ELSE new END)'),
                             coalesce(e.def ->> 'when', 'true')), ' UNION ALL ' ORDER BY e.type)
    INTO v_union FROM jsonb_each(events) e(type, def);
    -- One statement turns a batch of captured changes (old and new images) into events.
    v_sql := format(
        $q$SELECT d.id, d.token, ev.type, (%1$s)::text AS key, ev.payload
           FROM unnest($1::isomer.deltas[]) d
           CROSS JOIN LATERAL jsonb_populate_record(NULL::%2$s, CASE WHEN jsonb_typeof(d.image -> 'old') = 'object' THEN d.image -> 'old' END) AS old
           CROSS JOIN LATERAL jsonb_populate_record(NULL::%2$s, CASE WHEN jsonb_typeof(d.image -> 'new') = 'object' THEN d.image -> 'new' END) AS new
           CROSS JOIN LATERAL (%3$s) ev
           ORDER BY d.id, ev.type$q$,
        v_key, v_src, v_union);
    -- Check it compiles now, not at the first write.
    v_probe := format('SELECT * FROM (%s) s LIMIT 0', replace(v_sql, '$1::isomer.deltas[]', 'ARRAY[]::isomer.deltas[]'));
    BEGIN
        EXECUTE v_probe;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'isomer: the event rules for % do not compile: %', name, SQLERRM
            USING HINT = 'Each "when" is a condition and each "payload" a jsonb expression, over old and new.';
    END;

    INSERT INTO isomer.projections (name, kind, query, key_column, key_type, freshness, lag_budget, history, history_since, strategy, strategy_reason)
    VALUES (name, 'event', v_sql, 'key', 'text', 'bounded', interval '2 seconds', retention, clock_timestamp(),
            'incremental', 'events are derived from each captured change');
    INSERT INTO isomer.projection_sources (projection, source_table, key_column) VALUES (name, v_src, NULL);
    INSERT INTO isomer.projection_stats (projection) VALUES (name) ON CONFLICT DO NOTHING;
    PERFORM isomer._create_source_triggers(name);
    RETURN format('event projection %s: %s event type(s) from %s, kept %s; read with isomer.changes_since(%L, cursor)',
                  name, (SELECT count(*) FROM jsonb_object_keys(events)), v_src, retention, name);
END $$;

-- Capture for event projections records the old and new images together, as one change.
CREATE OR REPLACE FUNCTION isomer._on_source_delta()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    v_projection text := TG_ARGV[0];
    v_source     text := TG_ARGV[1];
    v_columns    text[] := nullif(TG_ARGV[3], '')::text[];
    v_events     boolean := TG_ARGV[4] = 'e';
    v_token      bigint := isomer.current_token();
    v_old        jsonb;
    v_new        jsonb;
BEGIN
    PERFORM set_config('isomer.last_write_token', v_token::text, false);
    IF TG_ARGV[2] = 'r' AND NOT isomer._first_placement(TG_TABLE_NAME) THEN
        RETURN NULL;
    END IF;
    IF TG_OP <> 'TRUNCATE' THEN
        IF TG_OP <> 'INSERT' THEN v_old := to_jsonb(OLD); END IF;
        IF TG_OP <> 'DELETE' THEN v_new := to_jsonb(NEW); END IF;
        IF TG_OP = 'UPDATE' AND v_columns IS NOT NULL AND isomer._pick(v_old, v_columns) = isomer._pick(v_new, v_columns) THEN
            RETURN NULL;
        END IF;
    ELSIF v_events THEN
        RETURN NULL;   -- a truncate is not a row change: no events
    END IF;
    IF EXISTS (SELECT 1 FROM isomer.projections WHERE name = v_projection) THEN
        IF v_events THEN
            INSERT INTO isomer.deltas (projection, source, weight, image, token)
            VALUES (v_projection, v_source, 1, jsonb_build_object('old', v_old, 'new', v_new), v_token);
        ELSE
            INSERT INTO isomer.deltas (projection, source, weight, image, token)
            SELECT v_projection, v_source, d.w, d.image, v_token
            FROM (VALUES (-1, v_old), (1, v_new), (0, NULL)) d(w, image)
            WHERE (TG_OP = 'TRUNCATE') = (d.w = 0) AND (d.w = 0 OR d.image IS NOT NULL);
        END IF;
    ELSE
        IF v_events THEN
            INSERT INTO isomer.node_deltas (projection, source, weight, image, token)
            VALUES (v_projection, v_source, 1, jsonb_build_object('old', v_old, 'new', v_new), v_token);
        ELSE
            INSERT INTO isomer.node_deltas (projection, source, weight, image, token)
            SELECT v_projection, v_source, d.w, d.image, v_token
            FROM (VALUES (-1, v_old), (1, v_new), (0, NULL)) d(w, image)
            WHERE (TG_OP = 'TRUNCATE') = (d.w = 0) AND (d.w = 0 OR d.image IS NOT NULL);
        END IF;
    END IF;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION isomer._create_source_triggers(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_unsafe_triggers = on SET citus.enable_ddl_propagation = on AS $$
DECLARE
    p         isomer.projections;
    s         record;
    v_trigger text := format('isomer_%s', p_projection);
    v_plan    jsonb;
    v_args    text;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    IF p.kind = 'analytics' THEN
        RETURN;   -- noticed from table statistics, no triggers
    END IF;
    SELECT e.plan INTO v_plan FROM isomer.engine_plans e WHERE e.projection = p.name;
    FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger, s.source_table);
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger || '_truncate', s.source_table);
        IF p.kind = 'event' OR p.strategy = 'incremental' THEN
            v_args := format('%L, %L, %L, %L, %L', p.name, s.source_table,
                             CASE WHEN (isomer._distribution(s.source_table::regclass)).method = 'n' THEN 'r' ELSE '' END,
                             CASE WHEN p.kind = 'event' THEN '' ELSE
                                 ARRAY(SELECT jsonb_array_elements_text(v_plan -> 'capture' -> (s.source_table::regclass::oid)::text))::text END,
                             CASE WHEN p.kind = 'event' THEN 'e' ELSE '' END);
            EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW '
                           'EXECUTE FUNCTION isomer._on_source_delta(%s)', v_trigger, s.source_table, v_args);
            EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT '
                           'EXECUTE FUNCTION isomer._on_source_delta(%s)', v_trigger || '_truncate', s.source_table, v_args);
        ELSE
            EXECUTE format(
                'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW '
                'EXECUTE FUNCTION isomer._on_source_change(%L, %L, %L)',
                v_trigger, s.source_table, p.name, coalesce(s.key_column, ''), p.freshness);
            EXECUTE format(
                'CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT '
                'EXECUTE FUNCTION isomer._on_source_change(%L, %L, %L)',
                v_trigger || '_truncate', s.source_table, p.name, coalesce(s.key_column, ''), p.freshness);
        END IF;
        -- Fire for changes applied by logical replication too (attach mode).
        EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %I', s.source_table, v_trigger);
        EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %I', s.source_table, v_trigger || '_truncate');
    END LOOP;
END $$;

-- Turn an event projection's captured changes into events, appended to its feed in commit order
-- (ids taken under the projection's feed lock, as for document history). Returns how many changes
-- were consumed.
CREATE FUNCTION isomer._event_fold(p_projection text, p_batch integer DEFAULT 5000)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p      isomer.projections;
    v_rows isomer.deltas[];
    v_n    integer;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    PERFORM pg_advisory_xact_lock(hashtext('isomer.changes'), hashtext(p.name));
    WITH taken AS (
        DELETE FROM isomer.deltas WHERE id IN (
            SELECT id FROM isomer.deltas WHERE projection = p.name ORDER BY id LIMIT p_batch)
        RETURNING *)
    SELECT array_agg(t ORDER BY t.id) INTO v_rows FROM taken t;
    IF v_rows IS NULL THEN
        RETURN 0;
    END IF;
    EXECUTE format(
        $q$INSERT INTO isomer.changes (id, projection, key, op, doc, token)
           SELECT nextval('isomer.changes_id_seq'), $2, e.key, 'e', jsonb_build_object('type', e.type, 'payload', e.payload), e.token
           FROM (%s) e$q$, p.query)
    USING v_rows, p.name;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n > 0 THEN
        PERFORM pg_notify('isomer_changes', p.name);
    END IF;
    UPDATE isomer.projection_stats
    SET deltas_folded = deltas_folded + cardinality(v_rows), documents_written = documents_written + v_n,
        last_applied_at = clock_timestamp(),
        last_lag_ms = extract(epoch FROM clock_timestamp() - (SELECT min(d.enqueued_at) FROM unnest(v_rows) d)) * 1000
    WHERE projection = p.name;
    RETURN cardinality(v_rows);
END $$;

-- The feed names events as such.
CREATE OR REPLACE FUNCTION isomer.changes_since(p_projection text, p_after bigint DEFAULT 0, p_limit integer DEFAULT 1000)
RETURNS TABLE (id bigint, key text, op text, doc jsonb, token bigint, at timestamptz)
LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
BEGIN
    SELECT * INTO p FROM isomer.projections pr WHERE pr.name = p_projection;
    IF NOT FOUND OR p.history IS NULL THEN
        RAISE EXCEPTION 'isomer: projection % keeps no history', p_projection
            USING HINT = format('SELECT isomer.set_history(%L, ''7 days'')', p_projection);
    END IF;
    IF p_after > 0 AND p_after < p.history_pruned_through THEN
        RAISE EXCEPTION 'isomer: cursor % of % is older than its history (pruned through %)', p_after, p_projection, p.history_pruned_through
            USING ERRCODE = 'IS003', HINT = 'Read a snapshot with isomer.snapshot(), then follow the feed from its cursor.';
    END IF;
    RETURN QUERY
        SELECT c.id, c.key, CASE c.op WHEN 'u' THEN 'upsert' WHEN 'd' THEN 'delete' ELSE 'event' END, c.doc, c.token, c.at
        FROM isomer.changes c
        WHERE c.projection = p_projection AND c.id > p_after
        ORDER BY c.id LIMIT p_limit;
END $$;

-- Events past retention go; a document's history keeps each key's version at the cutoff.
CREATE OR REPLACE FUNCTION isomer.prune_history()
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        record;
    v_cutoff timestamptz;
    v_max    bigint;
    v_n      bigint;
    v_total  bigint := 0;
BEGIN
    FOR p IN SELECT name, kind, history FROM isomer.projections WHERE history IS NOT NULL LOOP
        v_cutoff := now() - p.history;
        WITH gone AS (
            DELETE FROM isomer.changes c
            WHERE c.projection = p.name AND c.at < v_cutoff
              AND (p.kind = 'event' OR c.op = 'd'
                   -- superseded before the cutoff (a key with no version at the cutoff reads as
                   -- absent either way)
                   OR EXISTS (SELECT 1 FROM isomer.changes n
                              WHERE n.projection = c.projection AND n.key = c.key AND n.id > c.id AND n.at < v_cutoff))
            RETURNING c.id)
        SELECT count(*), max(id) INTO v_n, v_max FROM gone;
        IF v_n > 0 THEN
            UPDATE isomer.projections SET history_pruned_through = greatest(history_pruned_through, v_max) WHERE name = p.name;
            v_total := v_total + v_n;
        END IF;
    END LOOP;
    RETURN v_total;
END $$;

-- ── Analytics projections ───────────────────────────────────────────────────

CREATE SCHEMA isomer_analytics;
COMMENT ON SCHEMA isomer_analytics IS 'IsomerDB: columnar tables behind analytics projections';

CREATE TABLE isomer.analytics_builds (
    projection   text PRIMARY KEY REFERENCES isomer.projections (name) ON DELETE CASCADE,
    generation   integer NOT NULL DEFAULT 0,
    fingerprint  text,
    built_at     timestamptz,
    build_ms     double precision,
    rows         bigint,
    bytes        bigint,
    last_error   text
);
REVOKE ALL ON isomer.analytics_builds FROM PUBLIC;

-- A fingerprint of the sources' contents, from their statistics on every shard: rows inserted,
-- updated and deleted, and the file each one is in (a TRUNCATE replaces it).
CREATE FUNCTION isomer._sources_fingerprint(p_projection text)
RETURNS text LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    s     record;
    v_all text := '';
    v_one text;
BEGIN
    FOR s IN SELECT source_table FROM isomer.projection_sources WHERE projection = p_projection ORDER BY source_table LOOP
        IF (isomer._distribution(s.source_table::regclass)).method IS NOT NULL THEN
            SELECT string_agg(result, ',' ORDER BY shardid) INTO v_one
            FROM run_command_on_shards(s.source_table::regclass,
                $q$SELECT coalesce((SELECT n_tup_ins + n_tup_upd + n_tup_del FROM pg_stat_all_tables WHERE relid = '%1$s'::regclass), 0) || ':' || pg_relation_filenode('%1$s')$q$);
        ELSE
            SELECT coalesce((SELECT n_tup_ins + n_tup_upd + n_tup_del FROM pg_stat_all_tables WHERE relid = s.source_table::regclass), 0)
                   || ':' || pg_relation_filenode(s.source_table::regclass)
            INTO v_one;
        END IF;
        v_all := v_all || s.source_table || '=' || coalesce(v_one, '') || ';';
    END LOOP;
    RETURN md5(v_all);
END $$;

CREATE FUNCTION isomer.create_analytics_projection(
    name          text,
    query         text,
    refresh       interval DEFAULT '5 minutes',
    target_schema text DEFAULT 'public')
-- The query resolves with the caller's search path, then is stored schema-qualified.
RETURNS text LANGUAGE plpgsql SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_probe  text := format('pg_temp.isomer_create_%s', pg_backend_pid());
    v_first  text;
    v_result text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_am WHERE amname = 'columnar') THEN
        RAISE EXCEPTION 'isomer: analytics projections need columnar storage'
            USING HINT = 'CREATE EXTENSION citus_columnar; (new clusters have it).';
    END IF;
    EXECUTE format('CREATE OR REPLACE TEMP VIEW %s AS %s', v_probe, query);
    query := isomer._viewdef(v_probe::regclass);
    EXECUTE format('DROP VIEW %s', v_probe);
    SELECT column_name INTO v_first FROM isomer._query_columns(query) LIMIT 1;
    INSERT INTO isomer.projections (name, kind, query, key_column, key_type, freshness, lag_budget, target_schema, strategy, strategy_reason)
    VALUES (name, 'analytics', query, v_first, 'text', 'bounded', refresh, target_schema, 'recompute',
            'rebuilt as a columnar table when its sources change, at most every ' || refresh);
    INSERT INTO isomer.projection_sources (projection, source_table, key_column)
    SELECT name, isomer._qualified(t), NULL FROM isomer._query_tables(query) t;
    INSERT INTO isomer.projection_stats (projection) VALUES (name) ON CONFLICT DO NOTHING;
    INSERT INTO isomer.analytics_builds (projection) VALUES (name);
    v_result := isomer.refresh_analytics(name, force => true);
    RETURN format('analytics projection %s: %s; read it as %I.%I', name, v_result, target_schema, name);
END $$;

-- Build the projection again as a new columnar table and swap it in, if its sources changed (or
-- force). Readers see the old table until the swap commits, then the new one.
CREATE FUNCTION isomer.refresh_analytics(p_projection text, force boolean DEFAULT false)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p       isomer.projections;
    b       isomer.analytics_builds;
    v_fp    text;
    v_table text;
    v_start timestamptz := clock_timestamp();
    v_rows  bigint;
    v_bytes bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    SELECT * INTO STRICT b FROM isomer.analytics_builds WHERE projection = p.name FOR UPDATE;
    v_fp := isomer._sources_fingerprint(p.name);
    IF NOT force AND b.fingerprint = v_fp THEN
        RETURN 'unchanged';
    END IF;
    v_table := format('isomer_analytics.%I', p.name || '_' || (b.generation + 1));
    EXECUTE format('CREATE TABLE %s USING columnar AS %s', v_table, p.query);
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
    EXECUTE format('CREATE VIEW %I.%I AS SELECT * FROM %s', p.target_schema, p.name, v_table);
    IF b.generation > 0 THEN
        EXECUTE format('DROP TABLE IF EXISTS isomer_analytics.%I', p.name || '_' || b.generation);
    END IF;
    v_bytes := pg_total_relation_size(v_table::regclass);
    UPDATE isomer.analytics_builds
    SET generation = b.generation + 1, fingerprint = v_fp, built_at = clock_timestamp(),
        build_ms = extract(epoch FROM clock_timestamp() - v_start) * 1000, rows = v_rows, bytes = v_bytes, last_error = NULL
    WHERE projection = p.name;
    UPDATE isomer.projection_stats
    SET last_applied_at = clock_timestamp(), documents_written = documents_written + v_rows, last_batch_size = v_rows
    WHERE projection = p.name;
    RETURN format('%s rows, %s columnar', v_rows, pg_size_pretty(v_bytes));
END $$;

-- Every analytics projection whose refresh interval has passed and whose sources changed.
CREATE FUNCTION isomer.refresh_analytics_due()
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p   record;
    v_n integer := 0;
BEGIN
    FOR p IN SELECT pr.name FROM isomer.projections pr JOIN isomer.analytics_builds b ON b.projection = pr.name
             WHERE pr.kind = 'analytics' AND pr.status = 'active'
               AND (b.built_at IS NULL OR b.built_at < clock_timestamp() - pr.lag_budget) LOOP
        BEGIN
            IF isomer.refresh_analytics(p.name) <> 'unchanged' THEN
                v_n := v_n + 1;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            UPDATE isomer.analytics_builds SET last_error = SQLERRM WHERE projection = p.name;
            PERFORM isomer._log('error', p.name, format('analytics refresh failed: %s', SQLERRM));
        END;
    END LOOP;
    RETURN v_n;
END $$;

SELECT cron.schedule('isomer-analytics', '30 seconds', 'SELECT isomer.refresh_analytics_due()')
WHERE pg_catalog.citus_is_coordinator();

-- ── Draining, dropping, certifying, verifying the new kinds ─────────────────

CREATE OR REPLACE FUNCTION isomer.drain(p_batch integer DEFAULT 500)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        record;
    v_keys   text[];
    v_phases text[];
    v_oldest timestamptz;
    v_token  bigint;
    v_n      integer;
    v_total  integer := 0;
    v_lag_ms double precision;
    v_until  timestamptz;
BEGIN
    -- Queued keys reach pending for every projection, paused ones too: their work waits there.
    FOR p IN SELECT DISTINCT projection AS name FROM isomer.key_log LOOP
        BEGIN
            PERFORM isomer._merge_key_log(p.name);
        EXCEPTION WHEN OTHERS THEN
            PERFORM isomer._log('error', p.name, format('merging queued keys failed: %s', SQLERRM));
        END;
    END LOOP;
    -- Event projections: captured changes become events.
    FOR p IN SELECT name, lag_budget FROM isomer.projections WHERE kind = 'event' AND status = 'active' LOOP
        v_until := clock_timestamp() + greatest(p.lag_budget / 2, interval '200 milliseconds');
        BEGIN
            LOOP
                v_n := isomer._event_fold(p.name);
                v_total := v_total + v_n;
                EXIT WHEN v_n = 0 OR clock_timestamp() > v_until;
            END LOOP;
        EXCEPTION WHEN OTHERS THEN
            PERFORM isomer._log('error', p.name, format('turning changes into events failed: %s', SQLERRM));
        END;
    END LOOP;
    FOR p IN SELECT name, kind, lag_budget, strategy FROM isomer.projections
             WHERE kind IN ('document', 'vector', 'cache') AND freshness = 'bounded' AND status = 'active'
    LOOP
        IF p.strategy = 'incremental' THEN
            v_until := clock_timestamp() + greatest(p.lag_budget / 2, interval '200 milliseconds');
            BEGIN
                LOOP
                    EXIT WHEN isomer._engine_fold(p.name) = 0 OR clock_timestamp() > v_until;
                END LOOP;
            EXCEPTION WHEN OTHERS THEN
                PERFORM isomer._log('error', p.name, format('folding changes failed: %s', SQLERRM));
            END;
        END IF;
        LOOP
            BEGIN
                WITH batch AS (
                    DELETE FROM isomer.pending
                    WHERE (projection, key) IN (
                        SELECT projection, key FROM isomer.pending
                        WHERE projection = p.name
                        ORDER BY enqueued_at LIMIT p_batch
                        FOR UPDATE SKIP LOCKED)
                    RETURNING key, enqueued_at, phase, token)
                SELECT array_agg(key), array_agg(DISTINCT phase), min(enqueued_at), max(token), count(*)
                INTO v_keys, v_phases, v_oldest, v_token, v_n FROM batch;

                EXIT WHEN v_n = 0;
                IF p.kind IN ('document', 'cache') THEN
                    PERFORM isomer._apply(p.name, v_keys);
                    v_lag_ms := extract(epoch FROM clock_timestamp() - v_oldest) * 1000;
                    INSERT INTO isomer.projection_stats AS s
                        (projection, last_applied_at, last_batch_size, last_lag_ms, max_lag_ms, budget_breaches)
                    VALUES (p.name, clock_timestamp(), v_n, v_lag_ms, v_lag_ms,
                            (v_lag_ms > extract(epoch FROM p.lag_budget) * 1000)::int)
                    ON CONFLICT (projection) DO UPDATE
                        SET last_applied_at = EXCLUDED.last_applied_at,
                            last_batch_size = EXCLUDED.last_batch_size,
                            last_lag_ms     = EXCLUDED.last_lag_ms,
                            max_lag_ms      = greatest(s.max_lag_ms, EXCLUDED.max_lag_ms),
                            budget_breaches = s.budget_breaches + EXCLUDED.budget_breaches;
                    IF v_lag_ms > extract(epoch FROM p.lag_budget) * 1000 THEN
                        PERFORM isomer._log('warning', p.name,
                            format('lag %s ms exceeded budget %s', round(v_lag_ms::numeric), p.lag_budget));
                    END IF;
                ELSE
                    PERFORM isomer._enqueue_embeddings(p.name, v_keys,
                        CASE WHEN 'backfill' = ANY (v_phases) THEN 'backfill' ELSE 'steady' END);
                    UPDATE isomer.embed_jobs SET (enqueued_at, token) = (least(enqueued_at, v_oldest), greatest(embed_jobs.token, v_token))
                    WHERE projection = p.name AND key = ANY (v_keys);
                END IF;
                v_total := v_total + v_n;
            EXCEPTION WHEN OTHERS THEN
                PERFORM isomer._log('error', p.name, format('drain failed: %s', SQLERRM));
                EXIT;
            END;
        END LOOP;
        IF p.kind = 'vector' THEN
            PERFORM isomer._maybe_swap_model(p.name);
        END IF;
    END LOOP;
    RETURN v_total;
END $$;

CREATE OR REPLACE FUNCTION isomer.drop_projection(name text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off SET citus.enable_unsafe_triggers = on AS $$
DECLARE
    p isomer.projections;
BEGIN
    SELECT * INTO p FROM isomer.projections WHERE projections.name = drop_projection.name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', name;
    END IF;
    IF p.kind = 'event' THEN
        PERFORM isomer._drop_source_triggers(p.name);
    ELSIF p.kind = 'analytics' THEN
        EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
        EXECUTE format('DROP TABLE IF EXISTS isomer_analytics.%I',
                       p.name || '_' || (SELECT generation FROM isomer.analytics_builds WHERE projection = p.name));
    ELSIF p.kind IN ('document', 'vector', 'cache') THEN
        PERFORM isomer._drop_source_triggers(p.name);
        IF p.strategy = 'incremental' THEN
            PERFORM isomer._engine_drop_state(p.name);
        END IF;
        IF p.kind = 'document' THEN
            PERFORM documentdb_api.drop_collection(p.mongo_database, p.name);
        ELSIF p.kind = 'vector' THEN
            EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
            EXECUTE format('DROP TABLE IF EXISTS %s', isomer._vector_store(p.name));
        ELSE
            EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
            EXECUTE format('DROP TABLE IF EXISTS %s', isomer._cache_store(p.name));
        END IF;
    ELSE
        PERFORM set_config('isomer.dropping_projection', p.name, true);
        PERFORM pgtrickle.drop_stream_table(format('%I.%I', p.target_schema, p.name), false);
    END IF;
    DELETE FROM isomer.embed_dead_letter WHERE projection = p.name;
    DELETE FROM isomer.audit_suspects WHERE projection = p.name;
    DELETE FROM isomer.projections WHERE projections.name = p.name;
END $$;

CREATE OR REPLACE FUNCTION isomer.audit(p_sample integer DEFAULT 50)
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog SET paradedb.planner_warnings = off AS $$
DECLARE
    p        isomer.projections;
    v_keys   text[];
    r        record;
    v_hash   text;
    v_more   text[];
    v_s      isomer.audit_suspects;
    v_min    interval;
    v_checks integer;
    v_drift  bigint := 0;
BEGIN
    FOR p IN SELECT * FROM isomer.projections WHERE status = 'active' AND kind NOT IN ('event', 'analytics') LOOP
        BEGIN
            -- Keys from the tables...
            EXECUTE format('SELECT array_agg(k) FROM (SELECT q.%I::text AS k FROM (%s) q ORDER BY random() LIMIT %s) s',
                           p.key_column, isomer._truth(p), p_sample) INTO v_keys;
            -- ...and from the projection's storage.
            IF p.kind = 'document' THEN
                EXECUTE format('SELECT array_agg(k) FROM (SELECT isomer._ejson_relax(documentdb_core.bson_to_json_string(c.document)::text::jsonb) ->> ''_id'' AS k '
                               'FROM %s c ORDER BY random() LIMIT %s) s',
                               isomer._collection_table(p.mongo_database, p.name), p_sample / 5 + 1) INTO v_more;
                v_keys := v_keys || v_more;
            ELSIF p.kind = 'cache' THEN
                EXECUTE format('SELECT array_agg(key) FROM (SELECT key FROM %s ORDER BY random() LIMIT %s) s',
                               isomer._cache_store(p.name), p_sample / 5 + 1) INTO v_more;
                v_keys := v_keys || v_more;
            ELSIF p.kind = 'vector' THEN
                EXECUTE format('SELECT array_agg(key) FROM (SELECT key FROM %s WHERE model = %L ORDER BY random() LIMIT %s) s',
                               isomer._vector_store(p.name), p.model, p_sample / 5 + 1) INTO v_more;
                v_keys := v_keys || v_more;
            END IF;
            v_keys := ARRAY(SELECT DISTINCT k FROM unnest(v_keys) k WHERE k IS NOT NULL);
            v_checks := 0;
            -- Sync projections have no lag; only a concurrent write can make them briefly differ.
            v_min := CASE WHEN p.freshness = 'sync' THEN interval '2 seconds'
                          ELSE greatest(p.lag_budget * 2, interval '2 seconds') END;

            FOR r IN SELECT * FROM isomer._audit_pairs(p, v_keys) LOOP
                -- Work in flight is not drift.
                CONTINUE WHEN EXISTS (SELECT 1 FROM isomer.pending q WHERE q.projection = p.name AND q.key = r.key)
                           OR EXISTS (SELECT 1 FROM isomer.key_log q WHERE q.projection = p.name AND q.key = r.key)
                           OR EXISTS (SELECT 1 FROM isomer.embed_jobs j WHERE j.projection = p.name AND j.key = r.key);
                v_checks := v_checks + 1;
                IF r.expected IS NOT DISTINCT FROM r.actual THEN
                    DELETE FROM isomer.audit_suspects WHERE projection = p.name AND key = r.key;
                    CONTINUE;
                END IF;
                v_hash := md5(coalesce(r.expected::text, '<absent>'));
                SELECT * INTO v_s FROM isomer.audit_suspects WHERE projection = p.name AND key = r.key;
                IF NOT FOUND OR v_s.expected_hash IS DISTINCT FROM v_hash THEN
                    INSERT INTO isomer.audit_suspects (projection, key, expected_hash) VALUES (p.name, r.key, v_hash)
                    ON CONFLICT (projection, key) DO UPDATE SET expected_hash = EXCLUDED.expected_hash, first_seen = clock_timestamp();
                ELSIF clock_timestamp() - v_s.first_seen >= v_min THEN
                    INSERT INTO isomer.drift_samples (projection, key, expected, actual) VALUES (p.name, r.key, r.expected, r.actual);
                    DELETE FROM isomer.audit_suspects WHERE projection = p.name AND key = r.key;
                    UPDATE isomer.audit_stats SET drift = drift + 1 WHERE projection = p.name;
                    PERFORM isomer._log('error', p.name, format(
                        'DRIFT: key %s differs from what the tables produce (pinned as a drift sample); expected %s, found %s',
                        r.key, left(coalesce(r.expected::text, 'nothing'), 200), left(coalesce(r.actual::text, 'nothing'), 200)));
                    v_drift := v_drift + 1;
                END IF;
            END LOOP;

            INSERT INTO isomer.audit_stats AS s (projection, checks, last_run) VALUES (p.name, v_checks, now())
            ON CONFLICT (projection) DO UPDATE SET checks = s.checks + EXCLUDED.checks, last_run = EXCLUDED.last_run;
        EXCEPTION WHEN OTHERS THEN
            PERFORM isomer._log('error', p.name, format('audit failed: %s', SQLERRM));
        END;
    END LOOP;
    RETURN v_drift;
END $$;

-- An analytics projection is checked whole: while its sources are as they were at the last build,
-- the columnar table must hold exactly what the query returns. Returns the rows that differ, or
-- NULL when the sources have moved on since (the next refresh will catch up).
CREATE FUNCTION isomer.analytics_differences(p_projection text)
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
    b isomer.analytics_builds;
    v bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection AND kind = 'analytics';
    SELECT * INTO STRICT b FROM isomer.analytics_builds WHERE projection = p.name;
    IF b.fingerprint IS DISTINCT FROM isomer._sources_fingerprint(p.name) THEN
        RETURN NULL;
    END IF;
    EXECUTE format(
        $q$WITH a AS (SELECT to_jsonb(q) AS r FROM (%1$s) q), b AS (SELECT to_jsonb(t) AS r FROM %2$I.%3$I t)
           SELECT (SELECT count(*) FROM (SELECT r FROM a EXCEPT ALL SELECT r FROM b) x)
                + (SELECT count(*) FROM (SELECT r FROM b EXCEPT ALL SELECT r FROM a) y)$q$,
        p.query, p.target_schema, p.name) INTO v;
    RETURN v;
END $$;

CREATE OR REPLACE FUNCTION isomer.certificate(p_projection text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        isomer.projections;
    v_checks jsonb := '[]';
    v_rules  jsonb;
    v_assume jsonb;
    v_ok     boolean;
    v_detail text;
    v_reason text;
    s        record;
    v_tables regclass[];
    st       record;
    v_n      bigint;
    v_dist   boolean;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;

    IF p.kind = 'event' THEN
        v_rules := jsonb_build_array(
            'capture: each change to a source row is recorded with its old and new images, in the writing transaction, on the node holding the row',
            'delivery: changes recorded on a worker reach the coordinator exactly once (a prepared transaction on the worker, committed after the coordinator records the pull)',
            'derivation: each change is turned into the events whose conditions hold for its old and new rows, and appended to the feed under the feed lock, so ids follow commit order');
        v_assume := jsonb_build_array(
            'the capture trigger is on every placement of the source, ENABLE ALWAYS',
            'changes to one row are on one node, so its events keep their order; events of different rows written on different nodes are ordered as they are pulled',
            format('events are kept for %s', p.history));
        FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
            v_dist := (isomer._distribution(s.source_table::regclass)).method IN ('h', 'n');
            v_ok := CASE WHEN v_dist THEN isomer._triggers_on_all_placements(s.source_table::regclass, p.name)
                         ELSE EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = s.source_table::regclass AND tgname = 'isomer_' || p.name AND tgenabled = 'A') END;
            v_checks := v_checks || jsonb_build_object('check', 'capture on ' || s.source_table, 'passed', v_ok,
                'detail', CASE WHEN v_ok THEN 'present, ENABLE ALWAYS' ELSE 'missing or disabled: run isomer._create_source_triggers(' || quote_literal(p.name) || ')' END);
        END LOOP;
    ELSIF p.kind = 'analytics' THEN
        v_rules := jsonb_build_array(
            'recompute: the query is run in full into a new columnar table, which replaces the old one in one transaction',
            format('when: the sources'' statistics (rows inserted, updated, deleted, and their files) have changed since the last build, checked every 30 seconds, at most once per %s', p.lag_budget));
        v_assume := jsonb_build_array('table statistics count every change (they do for SQL writes, replication and TRUNCATE alike)');
        SELECT * INTO st FROM isomer.analytics_builds WHERE projection = p.name;
        v_checks := v_checks || jsonb_build_object('check', 'last build', 'passed', st.last_error IS NULL AND st.built_at IS NOT NULL,
            'detail', coalesce('failed: ' || st.last_error, format('%s rows, built %s', st.rows, st.built_at)));
        v_n := isomer.analytics_differences(p.name);
        v_checks := v_checks || jsonb_build_object('check', 'equals the query', 'passed', coalesce(v_n = 0, true),
            'detail', CASE WHEN v_n IS NULL THEN 'the sources changed since the last build; checked again after the next'
                           WHEN v_n = 0 THEN 'the columnar table holds exactly what the query returns'
                           ELSE format('%s row(s) differ', v_n) END);
    ELSIF p.strategy = 'incremental' THEN
        SELECT c.rules, c.assumptions, c.checks INTO v_rules, v_assume, v_checks FROM isomer._engine_certificate(p) c;
    ELSIF p.kind IN ('document', 'cache', 'vector') THEN
        v_rules := jsonb_build_array(
            'key-recompute: a write to a source row marks the projection keys it maps to (old and new values of the mapped column)',
            CASE WHEN p.kind = 'vector'
                 THEN 'for each marked key the text is re-derived with the projection''s own query; an embedding is requested only if md5(text) changed, and written only if the job''s hash still matches'
                 ELSE 'for each marked key the value is re-derived with the projection''s own query and written; keys the query no longer produces are deleted' END,
            'so the stored value for a key equals recomputation from the tables at the time it was applied');
        v_assume := jsonb_build_array(
            'every relation the query reads is a mapped source (completeness)',
            'each mapped column holds the projection key for that row, or the table is a lookup table (null mapping: any change re-derives every key)',
            CASE WHEN p.freshness = 'sync' THEN 'sync: the triggers run in the writing transaction, on the shard holding every row the key depends on (co-location), under a per-key lock'
                 ELSE format('bounded: queued keys (including those queued on worker nodes) are applied within the lag budget (%s); read-your-writes via isomer.wait_for', p.lag_budget) END);

        v_tables := ARRAY(SELECT isomer._query_tables(p.query));
        SELECT bool_and(EXISTS (SELECT 1 FROM isomer.projection_sources ps WHERE ps.projection = p.name AND ps.source_table::regclass = t)),
               string_agg(t::text, ', ') FILTER (WHERE NOT EXISTS (SELECT 1 FROM isomer.projection_sources ps WHERE ps.projection = p.name AND ps.source_table::regclass = t))
        INTO v_ok, v_detail FROM unnest(v_tables) t;
        v_checks := v_checks || jsonb_build_object('check', 'completeness', 'passed', coalesce(v_ok, false),
            'detail', CASE WHEN v_ok THEN format('%s relation(s) read, all mapped', cardinality(v_tables)) ELSE 'unmapped: ' || v_detail END);

        FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
            v_dist := (isomer._distribution(s.source_table::regclass)).method = 'h';
            IF v_dist THEN
                v_ok := isomer._triggers_on_all_placements(s.source_table::regclass, p.name);
            ELSE
                v_ok := EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = s.source_table::regclass
                                AND tgname = 'isomer_' || p.name AND tgenabled = 'A');
            END IF;
            v_checks := v_checks || jsonb_build_object('check', 'capture on ' || s.source_table, 'passed', v_ok,
                'detail', CASE WHEN v_ok THEN 'row and truncate triggers present, ENABLE ALWAYS' || CASE WHEN v_dist THEN ', on every shard placement on every node' ELSE '' END
                               ELSE 'missing or disabled: run isomer._create_source_triggers(' || quote_literal(p.name) || ')' END);
        END LOOP;

        IF p.freshness = 'sync' THEN
            v_reason := isomer._check_sources(p.name, 'sync');
            v_checks := v_checks || jsonb_build_object('check', 'sync preconditions', 'passed', v_reason IS NULL,
                'detail', coalesce(v_reason, 'IsomerDB is primary for every source and distributed sources are co-located on the key'));
        END IF;
    ELSE
        SELECT * INTO st FROM pgtrickle.pgt_stream_tables WHERE pgt_schema = p.target_schema AND pgt_name = p.name;
        v_rules := jsonb_build_array(
            format('pg_trickle %s maintenance: %s', coalesce(st.effective_refresh_mode, st.refresh_mode),
                   CASE WHEN coalesce(st.effective_refresh_mode, st.refresh_mode) = 'DIFFERENTIAL'
                        THEN 'changes are captured per source row and the query''s delta is applied (DBSP-style incremental rules)'
                        WHEN coalesce(st.effective_refresh_mode, st.refresh_mode) = 'IMMEDIATE'
                        THEN 'the delta is applied inside the writing transaction'
                        ELSE 'the query is recomputed in full each refresh' END),
            'BM25 index maintained by pg_search on the stream table');
        v_assume := jsonb_build_array('change capture fires for every write to every source, including replicated ones (CDC triggers ENABLE ALWAYS, row mode)');
        SELECT bool_and(t.tgenabled = 'A'), count(*) INTO v_ok, v_n
        FROM pgtrickle.pgt_dependencies d JOIN pg_trigger t ON t.tgrelid = d.source_relid AND t.tgname LIKE 'pg_trickle_cdc_%'
        WHERE d.pgt_id = st.pgt_id;
        v_checks := v_checks || jsonb_build_object('check', 'capture on sources', 'passed', coalesce(v_ok, false),
            'detail', CASE WHEN v_ok THEN 'pg_trickle CDC triggers present and ENABLE ALWAYS' ELSE 'CDC triggers missing or not ENABLE ALWAYS' END);
        v_checks := v_checks || jsonb_build_object('check', 'stream table healthy', 'passed', coalesce(st.status = 'ACTIVE', false),
            'detail', format('status %s', coalesce(st.status, 'missing')));
    END IF;

    SELECT * INTO st FROM isomer.audit_stats WHERE projection = p.name;
    RETURN jsonb_build_object(
        'projection', p.name, 'kind', p.kind, 'freshness', p.freshness, 'strategy', p.strategy,
        'strategy_reason', p.strategy_reason,
        'valid', NOT EXISTS (SELECT 1 FROM jsonb_array_elements(v_checks) c WHERE NOT (c ->> 'passed')::boolean),
        'rules', v_rules, 'assumptions', v_assume, 'checks', v_checks,
        'auditor', jsonb_build_object('checks', coalesce(st.checks, 0), 'drift', coalesce(st.drift, 0), 'since', st.since),
        'issued_at', now());
END $$;

REVOKE EXECUTE ON FUNCTION isomer.create_event_projection(text, regclass, jsonb, text, interval), isomer._event_fold(text, integer),
                           isomer._sources_fingerprint(text), isomer.create_analytics_projection(text, text, interval, text),
                           isomer.refresh_analytics(text, boolean), isomer.refresh_analytics_due(), isomer.analytics_differences(text)
    FROM PUBLIC;
