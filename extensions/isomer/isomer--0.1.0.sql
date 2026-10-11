-- IsomerDB 0.1.0: projections and attach mode (PRD Phase 0).
--
-- A projection is another readable form of table data that IsomerDB keeps up to date:
--   * document: one JSON document per key, readable as a MongoDB collection through the
--     DocumentDB gateway. Maintained by key: a write to any source row re-derives only the
--     documents whose key it touches, from the tables, using the projection's own query.
--   * search:   a flat (non-JSON) query kept as a pg_trickle stream table with a BM25 index.
--
-- Freshness:
--   * sync:    updated inside the writing transaction.
--   * bounded: updated asynchronously within a lag budget (default 2s), drained by pg_cron.
--
-- Attach mode follows an external Postgres through native logical replication. Tables keep
-- their source names; projections over them are always bounded.

\echo Use "CREATE EXTENSION isomer" to load this file. \quit

-- ── Catalog ─────────────────────────────────────────────────────────────────

CREATE TABLE isomer.projections (
    name           text PRIMARY KEY CHECK (name ~ '^[a-z_][a-z0-9_]{0,62}$'),
    kind           text NOT NULL CHECK (kind IN ('document', 'search')),
    query          text NOT NULL,
    key_column     text NOT NULL,
    key_type       text NOT NULL,
    freshness      text NOT NULL CHECK (freshness IN ('sync', 'bounded')),
    lag_budget     interval NOT NULL DEFAULT '2 seconds',
    mongo_database text,      -- document: DocumentDB database holding the collection
    target_schema  text,      -- search: schema of the stream table
    index_columns  text[],    -- search: columns in the BM25 index (key first)
    status         text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'broken')),
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- For document projections: how a row of each source table maps to projection keys.
-- key_column NULL marks a reference table: any change to it re-derives every document.
CREATE TABLE isomer.projection_sources (
    projection   text NOT NULL REFERENCES isomer.projections (name) ON DELETE CASCADE,
    source_table text NOT NULL,    -- schema-qualified, quoted as needed
    key_column   text,
    PRIMARY KEY (projection, source_table)
);

-- Keys waiting to be re-derived for bounded document projections.
CREATE TABLE isomer.pending (
    projection  text NOT NULL REFERENCES isomer.projections (name) ON DELETE CASCADE,
    key         text NOT NULL,
    enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection, key)
);

CREATE TABLE isomer.projection_stats (
    projection        text PRIMARY KEY REFERENCES isomer.projections (name) ON DELETE CASCADE,
    last_applied_at   timestamptz,
    last_batch_size   integer,
    last_lag_ms       double precision,
    max_lag_ms        double precision,
    documents_written bigint NOT NULL DEFAULT 0,
    documents_deleted bigint NOT NULL DEFAULT 0,
    budget_breaches   bigint NOT NULL DEFAULT 0
);

CREATE TABLE isomer.attachments (
    name           text PRIMARY KEY CHECK (name ~ '^[a-z_][a-z0-9_]{0,40}$'),
    conninfo       text NOT NULL,   -- includes credentials: superuser-only (see REVOKE below)
    schemas        text[] NOT NULL DEFAULT '{public}',
    publication    text NOT NULL,
    slot           text NOT NULL,
    subscription   text NOT NULL,
    source_version integer NOT NULL,
    status         text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'resyncing')),
    -- Set when tables were added: the subscription is refreshed on the next health check, once
    -- the new local tables are committed and visible to the refresh's own session.
    needs_refresh  boolean NOT NULL DEFAULT false,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE isomer.attached_tables (
    attachment text NOT NULL REFERENCES isomer.attachments (name) ON DELETE CASCADE,
    table_name text NOT NULL,       -- schema-qualified, quoted as needed
    PRIMARY KEY (attachment, table_name)
);

CREATE TABLE isomer.events (
    id      bigserial PRIMARY KEY,
    at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    level   text NOT NULL CHECK (level IN ('info', 'warning', 'error')),
    subject text NOT NULL,
    message text NOT NULL
);

REVOKE ALL ON ALL TABLES IN SCHEMA isomer FROM PUBLIC;

-- Definitions survive pg_dump; derived state (pending, stats, events) does not.
SELECT pg_catalog.pg_extension_config_dump('isomer.projections', '');
SELECT pg_catalog.pg_extension_config_dump('isomer.projection_sources', '');
SELECT pg_catalog.pg_extension_config_dump('isomer.attachments', '');
SELECT pg_catalog.pg_extension_config_dump('isomer.attached_tables', '');

-- ── Helpers ─────────────────────────────────────────────────────────────────

CREATE FUNCTION isomer._log(p_level text, p_subject text, p_message text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    INSERT INTO isomer.events (level, subject, message) VALUES (p_level, p_subject, p_message);
    IF p_level = 'error' THEN
        RAISE WARNING 'isomer: %: %', p_subject, p_message;
    END IF;
END $$;

-- Columns a query produces, without running it.
CREATE FUNCTION isomer._query_columns(p_query text)
RETURNS TABLE (column_name text, column_type text, type_oid oid)
LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_view text := format('pg_temp.isomer_probe_%s', pg_backend_pid());
BEGIN
    EXECUTE format('CREATE OR REPLACE TEMP VIEW %s AS %s', v_view, p_query);
    RETURN QUERY
        SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), a.atttypid
        FROM pg_attribute a
        WHERE a.attrelid = v_view::regclass AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum;
    EXECUTE format('DROP VIEW %s', v_view);
END $$;

-- Ordinary tables a query reads, found through the dependencies of a probe view.
CREATE FUNCTION isomer._query_tables(p_query text)
RETURNS SETOF regclass LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_view text := format('pg_temp.isomer_probe_%s', pg_backend_pid());
BEGIN
    EXECUTE format('CREATE OR REPLACE TEMP VIEW %s AS %s', v_view, p_query);
    RETURN QUERY
        SELECT DISTINCT d.refobjid::regclass
        FROM pg_rewrite r
        JOIN pg_depend d ON d.classid = 'pg_rewrite'::regclass AND d.objid = r.oid
                        AND d.refclassid = 'pg_class'::regclass
        JOIN pg_class c ON c.oid = d.refobjid
        WHERE r.ev_class = v_view::regclass
          AND d.refobjid <> v_view::regclass
          AND c.relkind IN ('r', 'p', 'm');
    EXECUTE format('DROP VIEW %s', v_view);
END $$;

-- Deparse a view under an empty search path, so every table, function and operator in the
-- result is schema-qualified. Stored projection queries are canonicalised this way because
-- they later run inside triggers and replication workers with a different search path.
CREATE FUNCTION isomer._viewdef(p_view regclass)
RETURNS text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT rtrim(pg_get_viewdef(p_view), ';')
$$;

CREATE FUNCTION isomer._qualified(p_table regclass)
RETURNS text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT p_table::text
$$;

CREATE FUNCTION isomer._is_distributed(p_table regclass)
RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = p_table)
$$;

CREATE FUNCTION isomer._attachment_of(p_table regclass)
RETURNS text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT attachment FROM isomer.attached_tables WHERE table_name::regclass = p_table LIMIT 1
$$;

-- Run a function-call SELECT on a remote database and discard its result (dblink_exec only
-- accepts statements that return no rows).
CREATE FUNCTION isomer._remote_call(p_conninfo text, p_select text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    PERFORM 1 FROM public.dblink(p_conninfo, format('SELECT 1 FROM (%s) s', p_select)) AS t(x int);
END $$;

-- Run a command as its own top-level statement over a loopback connection. Needed for
-- commands Postgres refuses inside functions (e.g. ALTER SUBSCRIPTION ... REFRESH).
CREATE FUNCTION isomer._loopback(p_sql text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    PERFORM public.dblink_exec(
        format('dbname=%s user=%s', quote_literal(current_database()), quote_literal(current_user)),
        p_sql);
END $$;

-- ── Document projections: derive, apply, enqueue ────────────────────────────

-- Re-derive the documents for the given keys and write them to the collection: upsert the
-- documents the query still produces, delete the ones it no longer does.
CREATE FUNCTION isomer._apply(p_projection text, p_keys text[])
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p          isomer.projections;
    v_updates  jsonb;
    v_found    text[];
    v_missing  jsonb;
    v_ok       boolean;
    v_result   text;
BEGIN
    IF p_keys IS NULL OR cardinality(p_keys) = 0 THEN
        RETURN;
    END IF;
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;

    EXECUTE format(
        $q$SELECT coalesce(jsonb_agg(jsonb_build_object(
                     'q', jsonb_build_object('_id', to_jsonb(q.%1$I)),
                     'u', q.doc::jsonb || jsonb_build_object('_id', to_jsonb(q.%1$I)),
                     'upsert', true)), '[]'::jsonb),
                  coalesce(array_agg(q.%1$I::text), '{}')
           FROM (%2$s) q
           WHERE q.%1$I = ANY ($1::%3$s[])$q$,
        p.key_column, p.query, p.key_type)
    INTO v_updates, v_found
    USING p_keys;

    IF jsonb_array_length(v_updates) > 0 THEN
        SELECT r.p_success, r.p_result::text INTO v_ok, v_result
        FROM documentdb_api.update(
            p.mongo_database,
            jsonb_build_object('update', p.name, 'updates', v_updates, 'ordered', false)::text::documentdb_core.bson) r;
        IF NOT v_ok THEN
            RAISE EXCEPTION 'isomer: writing documents for projection % failed: %', p.name, v_result;
        END IF;
    END IF;

    EXECUTE format(
        $q$SELECT jsonb_agg(to_jsonb(k::%s)) FROM unnest($1::text[]) k WHERE k <> ALL ($2)$q$,
        p.key_type)
    INTO v_missing
    USING p_keys, v_found;

    IF v_missing IS NOT NULL THEN
        SELECT r.p_success, r.p_result::text INTO v_ok, v_result
        FROM documentdb_api.delete(
            p.mongo_database,
            jsonb_build_object('delete', p.name, 'deletes', jsonb_build_array(jsonb_build_object(
                'q', jsonb_build_object('_id', jsonb_build_object('$in', v_missing)),
                'limit', 0)))::text::documentdb_core.bson) r;
        IF NOT v_ok THEN
            RAISE EXCEPTION 'isomer: deleting documents for projection % failed: %', p.name, v_result;
        END IF;
    END IF;

    INSERT INTO isomer.projection_stats AS s (projection, documents_written, documents_deleted)
    VALUES (p.name, cardinality(v_found), coalesce(jsonb_array_length(v_missing), 0))
    ON CONFLICT (projection) DO UPDATE
        SET documents_written = s.documents_written + EXCLUDED.documents_written,
            documents_deleted = s.documents_deleted + EXCLUDED.documents_deleted;
END $$;

-- Every key the projection currently produces.
CREATE FUNCTION isomer._all_keys(p_projection text)
RETURNS SETOF text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    RETURN QUERY EXECUTE format('SELECT q.%I::text FROM (%s) q', p.key_column, p.query);
END $$;

-- Re-derive every document, in batches, and remove documents whose key no longer exists.
CREATE FUNCTION isomer._rebuild_all(p_projection text, p_batch integer DEFAULT 500)
RETURNS bigint LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p       isomer.projections;
    v_keys  text[];
    v_total bigint := 0;
    v_ok    boolean;
    v_res   text;
    v_live  jsonb;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    FOR v_keys IN
        SELECT array_agg(k) FROM (
            SELECT k, (row_number() OVER () - 1) / p_batch AS chunk FROM isomer._all_keys(p_projection) k
        ) s GROUP BY chunk
    LOOP
        PERFORM isomer._apply(p_projection, v_keys);
        v_total := v_total + cardinality(v_keys);
    END LOOP;
    -- Documents for keys that disappeared while nothing was tracking them. Keys travel as text,
    -- so convert them back to the key's type: _id 42 and _id "42" are different documents.
    EXECUTE format('SELECT coalesce(jsonb_agg(to_jsonb(q.%I)), ''[]''::jsonb) FROM (%s) q', p.key_column, p.query)
    INTO v_live;
    SELECT r.p_success, r.p_result::text INTO v_ok, v_res
    FROM documentdb_api.delete(
        p.mongo_database,
        jsonb_build_object('delete', p.name, 'deletes', jsonb_build_array(jsonb_build_object(
            'q', jsonb_build_object('_id', jsonb_build_object('$nin', v_live)),
            'limit', 0)))::text::documentdb_core.bson) r;
    IF NOT v_ok THEN
        RAISE EXCEPTION 'isomer: removing stale documents for projection % failed: %', p.name, v_res;
    END IF;
    RETURN v_total;
END $$;

-- Row trigger on every source of a document projection.
--   TG_ARGV[0] projection, TG_ARGV[1] key column in this table ('' = reference table),
--   TG_ARGV[2] freshness.
CREATE FUNCTION isomer._on_source_change()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_projection text := TG_ARGV[0];
    v_column     text := TG_ARGV[1];
    v_freshness  text := TG_ARGV[2];
    v_keys       text[];
BEGIN
    IF v_column = '' THEN
        IF v_freshness = 'sync' THEN
            PERFORM isomer._rebuild_all(v_projection);
        ELSE
            INSERT INTO isomer.pending (projection, key)
            SELECT v_projection, k FROM isomer._all_keys(v_projection) k
            ON CONFLICT DO NOTHING;
        END IF;
        RETURN NULL;
    END IF;

    IF TG_OP = 'TRUNCATE' THEN
        IF v_freshness = 'sync' THEN
            PERFORM isomer._rebuild_all(v_projection);
        ELSE
            -- Re-derive everything on the next drain; _rebuild_all also removes orphans.
            PERFORM isomer._log('info', v_projection, format('%s truncated: full rebuild', TG_TABLE_NAME));
            PERFORM isomer._rebuild_all(v_projection);
        END IF;
        RETURN NULL;
    END IF;

    SELECT array_agg(DISTINCT k) INTO v_keys
    FROM unnest(ARRAY[
        CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) ->> v_column END,
        CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) ->> v_column END]) k
    WHERE k IS NOT NULL;

    IF v_freshness = 'sync' THEN
        PERFORM isomer._apply(v_projection, v_keys);
    ELSE
        INSERT INTO isomer.pending (projection, key)
        SELECT v_projection, k FROM unnest(v_keys) k
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NULL;
END $$;

-- Drain pending keys for bounded document projections. Scheduled every second by pg_cron.
CREATE FUNCTION isomer.drain(p_batch integer DEFAULT 500)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        record;
    v_keys   text[];
    v_oldest timestamptz;
    v_n      integer;
    v_total  integer := 0;
    v_lag_ms double precision;
BEGIN
    FOR p IN SELECT name, lag_budget FROM isomer.projections
             WHERE kind = 'document' AND freshness = 'bounded' AND status = 'active'
    LOOP
        LOOP
            BEGIN
                WITH batch AS (
                    DELETE FROM isomer.pending
                    WHERE (projection, key) IN (
                        SELECT projection, key FROM isomer.pending
                        WHERE projection = p.name
                        ORDER BY enqueued_at LIMIT p_batch
                        FOR UPDATE SKIP LOCKED)
                    RETURNING key, enqueued_at)
                SELECT array_agg(key), min(enqueued_at), count(*) INTO v_keys, v_oldest, v_n FROM batch;

                EXIT WHEN v_n = 0;
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
                v_total := v_total + v_n;
            EXCEPTION WHEN OTHERS THEN
                -- The batch's pending rows are restored by the subtransaction rollback.
                PERFORM isomer._log('error', p.name, format('drain failed: %s', SQLERRM));
                EXIT;
            END;
        END LOOP;
    END LOOP;
    RETURN v_total;
END $$;

-- ── Creating and dropping projections ───────────────────────────────────────

-- Create the derived objects for a projection already in the catalog: the collection and
-- source triggers (document) or the stream table and BM25 index (search). Then build it.
CREATE FUNCTION isomer._install(p_projection text)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p         isomer.projections;
    s         record;
    v_trigger text := format('isomer_%s', p_projection);
    v_target  text;
    v_count   bigint;
    v_attached boolean;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;

    IF p.kind = 'document' THEN
        PERFORM documentdb_api.create_collection(p.mongo_database, p.name);
        FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger, s.source_table);
            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger || '_truncate', s.source_table);
            EXECUTE format(
                'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW '
                'EXECUTE FUNCTION isomer._on_source_change(%L, %L, %L)',
                v_trigger, s.source_table, p.name, coalesce(s.key_column, ''), p.freshness);
            EXECUTE format(
                'CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT '
                'EXECUTE FUNCTION isomer._on_source_change(%L, %L, %L)',
                v_trigger || '_truncate', s.source_table, p.name, coalesce(s.key_column, ''), p.freshness);
            -- Fire for changes applied by logical replication too (attach mode).
            EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %I', s.source_table, v_trigger);
            EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %I', s.source_table, v_trigger || '_truncate');
        END LOOP;
        INSERT INTO isomer.projection_stats (projection) VALUES (p.name) ON CONFLICT DO NOTHING;
        v_count := isomer._rebuild_all(p.name);
        RETURN format('document projection %s: %s documents in collection %s.%s (%s)',
                      p.name, v_count, p.mongo_database, p.name, p.freshness);
    END IF;

    -- search
    v_target := format('%I.%I', p.target_schema, p.name);
    PERFORM pgtrickle.create_stream_table(
        v_target, p.query,
        schedule     => CASE WHEN p.freshness = 'sync' THEN NULL
                             ELSE greatest(1, ceil(extract(epoch FROM p.lag_budget) / 2))::int || 's' END,
        refresh_mode => CASE WHEN p.freshness = 'sync' THEN 'IMMEDIATE' ELSE 'AUTO' END);
    EXECUTE format('CREATE INDEX %I ON %s USING bm25 (%s)',
                   p.name || '_bm25', v_target,
                   (SELECT string_agg(format('%I', c), ', ') FROM unnest(p.index_columns) c));
    EXECUTE format('SELECT count(*) FROM %s', v_target) INTO v_count;
    RETURN format('search projection %s: %s rows in %s (%s)', p.name, v_count, v_target, p.freshness);
END $$;

CREATE FUNCTION isomer.create_projection(
    name          text,
    query         text,
    kind          text,
    key           text,
    freshness     text     DEFAULT 'bounded',
    lag           interval DEFAULT '2 seconds',
    sources       jsonb    DEFAULT NULL,
    database      text     DEFAULT 'isomer',
    index_columns text[]   DEFAULT NULL,
    target_schema text     DEFAULT 'public')
-- No SET search_path here: the query and source names resolve with the caller's search path,
-- then get stored schema-qualified. Probe views are local, so Citus must not propagate them.
RETURNS text LANGUAGE plpgsql SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_probe    text := format('pg_temp.isomer_create_%s', pg_backend_pid());
    v_cols     record;
    v_key_type text;
    v_has_doc  boolean := false;
    v_tables   regclass[];
    v_t        regclass;
    v_src      record;
    v_index    text[];
    v_attached text;
BEGIN
    IF kind NOT IN ('document', 'search') THEN
        RAISE EXCEPTION 'isomer: kind must be ''document'' or ''search'', not %', kind;
    END IF;
    IF freshness NOT IN ('sync', 'bounded') THEN
        RAISE EXCEPTION 'isomer: freshness must be ''sync'' or ''bounded'', not %', freshness;
    END IF;

    EXECUTE format('CREATE OR REPLACE TEMP VIEW %s AS %s', v_probe, query);
    query := isomer._viewdef(v_probe::regclass);
    EXECUTE format('DROP VIEW %s', v_probe);

    -- Shape of the query's output.
    FOR v_cols IN SELECT * FROM isomer._query_columns(query) LOOP
        IF v_cols.column_name = key THEN
            v_key_type := v_cols.column_type;
        END IF;
        IF v_cols.column_name = 'doc' AND v_cols.type_oid IN ('jsonb'::regtype, 'json'::regtype) THEN
            v_has_doc := true;
        END IF;
        IF kind = 'search' AND v_cols.column_name <> key
           AND v_cols.type_oid IN ('text'::regtype, 'varchar'::regtype, 'bpchar'::regtype) THEN
            v_index := v_index || v_cols.column_name;
        END IF;
    END LOOP;
    IF v_key_type IS NULL THEN
        RAISE EXCEPTION 'isomer: the query has no column named % (the projection key)', key;
    END IF;
    IF kind = 'document' AND NOT v_has_doc THEN
        RAISE EXCEPTION 'isomer: a document projection''s query must return a jsonb column named doc'
            USING HINT = 'e.g. SELECT c.id, jsonb_build_object(''name'', c.name, ...) AS doc FROM customers c';
    END IF;

    v_tables := ARRAY(SELECT isomer._query_tables(query));
    IF cardinality(v_tables) = 0 THEN
        RAISE EXCEPTION 'isomer: the query reads no tables';
    END IF;

    FOREACH v_t IN ARRAY v_tables LOOP
        IF isomer._is_distributed(v_t) THEN
            RAISE EXCEPTION 'isomer: % is a Citus distributed table; projections over distributed tables are not supported yet (planned for primary mode, PRD Phase 2)', v_t;
        END IF;
        v_attached := coalesce(v_attached, isomer._attachment_of(v_t));
    END LOOP;

    -- FR-A5: attach mode cannot offer same-transaction freshness, since the writes happen elsewhere.
    IF freshness = 'sync' AND v_attached IS NOT NULL THEN
        RAISE EXCEPTION 'isomer: projection % cannot be sync: it reads tables attached from another database (attachment %)', name, v_attached
            USING HINT = 'Use freshness => ''bounded''. Sync projections need IsomerDB to be the primary for their tables.';
    END IF;

    INSERT INTO isomer.projections (name, kind, query, key_column, key_type, freshness, lag_budget,
                                    mongo_database, target_schema, index_columns)
    VALUES (name, kind, query, key, v_key_type, freshness, lag,
            CASE WHEN kind = 'document' THEN database END,
            CASE WHEN kind = 'search' THEN target_schema END,
            CASE WHEN kind = 'search' THEN key || coalesce(index_columns, v_index) END);

    IF kind = 'document' THEN
        IF sources IS NULL THEN
            RAISE EXCEPTION 'isomer: a document projection needs sources: which column of each table holds the projection key'
                USING HINT = format('sources => ''{%s}''::jsonb, with null for lookup tables',
                    (SELECT string_agg(format('"%s": "<column>"', t), ', ') FROM unnest(v_tables) t));
        END IF;
        FOR v_src IN SELECT k, v FROM jsonb_each_text(sources) AS e(k, v) LOOP
            INSERT INTO isomer.projection_sources (projection, source_table, key_column)
            VALUES (name, isomer._qualified(v_src.k::regclass), v_src.v);
            IF v_src.v IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM pg_attribute WHERE attrelid = v_src.k::regclass AND attname = v_src.v AND NOT attisdropped) THEN
                RAISE EXCEPTION 'isomer: % has no column %', v_src.k, v_src.v;
            END IF;
        END LOOP;
        -- Completeness: every table the query reads must say how its rows map to keys,
        -- otherwise a write to it would leave documents stale without anyone noticing.
        FOREACH v_t IN ARRAY v_tables LOOP
            IF NOT EXISTS (SELECT 1 FROM isomer.projection_sources s
                           WHERE s.projection = name AND s.source_table::regclass = v_t) THEN
                RAISE EXCEPTION 'isomer: the query reads % but sources does not map it', v_t
                    USING HINT = format('Add "%s": "<column holding the key>", or "%s": null if it is a lookup table.', v_t, v_t);
            END IF;
        END LOOP;
    END IF;

    RETURN isomer._install(name);
END $$;

CREATE FUNCTION isomer.drop_projection(name text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
    s record;
BEGIN
    SELECT * INTO p FROM isomer.projections WHERE projections.name = drop_projection.name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', name;
    END IF;
    IF p.kind = 'document' THEN
        FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
            IF to_regclass(s.source_table) IS NOT NULL THEN
                EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', 'isomer_' || p.name, s.source_table);
                EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', 'isomer_' || p.name || '_truncate', s.source_table);
            END IF;
        END LOOP;
        PERFORM documentdb_api.drop_collection(p.mongo_database, p.name);
    ELSE
        PERFORM set_config('isomer.dropping_projection', p.name, true);
        PERFORM pgtrickle.drop_stream_table(format('%I.%I', p.target_schema, p.name), false);
    END IF;
    DELETE FROM isomer.projections WHERE projections.name = p.name;
END $$;

-- FR-S1b: recreate derived objects for every projection in the catalog, e.g. after
-- restoring a pg_dump (definitions are dumped; collections, triggers and stream tables are
-- rebuilt from them).
CREATE FUNCTION isomer.restore_projections()
RETURNS SETOF text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p record;
BEGIN
    FOR p IN SELECT name FROM isomer.projections ORDER BY created_at LOOP
        RETURN NEXT isomer._install(p.name);
    END LOOP;
END $$;

-- The calls that recreate every projection, for moving definitions between clusters.
CREATE FUNCTION isomer.dump_projections()
RETURNS SETOF text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT format('SELECT isomer.create_projection(name => %L, query => %L, kind => %L, key => %L, '
                  'freshness => %L, lag => %L, sources => %L, database => %L, index_columns => %L, target_schema => %L);',
                  p.name, p.query, p.kind, p.key_column, p.freshness, p.lag_budget,
                  (SELECT jsonb_object_agg(s.source_table, s.key_column) FROM isomer.projection_sources s WHERE s.projection = p.name),
                  coalesce(p.mongo_database, 'isomer'), p.index_columns, coalesce(p.target_schema, 'public'))
    FROM isomer.projections p ORDER BY p.created_at
$$;

-- Refuse to drop a table a projection reads: drop the projection first. (Dropping a source
-- first also leaves pg_trickle unable to drop its stream table.)
CREATE FUNCTION isomer._guard_source_drop()
RETURNS event_trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    d record;
    v_projection text;
BEGIN
    IF to_regclass('isomer.projection_sources') IS NULL THEN
        RETURN;  -- DROP EXTENSION isomer
    END IF;
    FOR d IN SELECT * FROM pg_event_trigger_dropped_objects()
             WHERE object_type = 'table' AND original LOOP
        SELECT s.projection INTO v_projection
        FROM isomer.projection_sources s
        WHERE s.source_table IN (d.object_identity, format('%I.%I', d.schema_name, d.object_name))
        LIMIT 1;
        IF v_projection IS NULL THEN
            SELECT p.name INTO v_projection
            FROM isomer.projections p
            WHERE p.kind = 'search'
              AND p.name IS DISTINCT FROM current_setting('isomer.dropping_projection', true)
              AND p.query ~* ('\m' || d.object_name || '\M')
              AND p.name <> d.object_name
            LIMIT 1;
        END IF;
        IF v_projection IS NOT NULL THEN
            RAISE EXCEPTION 'isomer: % is read by projection %', d.object_identity, v_projection
                USING HINT = format('Run SELECT isomer.drop_projection(%L) first.', v_projection);
        END IF;
    END LOOP;
END $$;

CREATE EVENT TRIGGER isomer_guard_source_drop ON sql_drop
    EXECUTE FUNCTION isomer._guard_source_drop();

CREATE VIEW isomer.projection_status AS
SELECT p.name, p.kind, p.freshness, p.lag_budget, p.status,
       (SELECT count(*) FROM isomer.pending q WHERE q.projection = p.name) AS pending_keys,
       (SELECT clock_timestamp() - min(q.enqueued_at) FROM isomer.pending q WHERE q.projection = p.name) AS oldest_pending,
       s.last_applied_at, round(s.last_lag_ms::numeric, 1) AS last_lag_ms,
       round(s.max_lag_ms::numeric, 1) AS max_lag_ms, s.budget_breaches,
       s.documents_written, s.documents_deleted,
       t.staleness AS search_staleness, t.consecutive_errors AS search_errors
FROM isomer.projections p
LEFT JOIN isomer.projection_stats s ON s.projection = p.name
LEFT JOIN pgtrickle.pgt_status() t ON p.kind = 'search' AND t.name = format('%s.%s', p.target_schema, p.name);

-- ── Attach mode ─────────────────────────────────────────────────────────────

-- FR-A8: check every table in the source schemas before publishing anything. A table with
-- no primary key or replica identity is refused because publishing it would make UPDATE and
-- DELETE on that table fail in the customer's own database.
CREATE FUNCTION isomer.preflight(conninfo text, schemas text[] DEFAULT '{public}')
RETURNS TABLE (table_name text, ok boolean, problems text[], warnings text[])
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RETURN QUERY
    SELECT r.table_name,
           cardinality(r.problems) = 0,
           r.problems,
           r.warnings
    FROM public.dblink(conninfo, format($remote$
        SELECT format('%%I.%%I', n.nspname, c.relname),
               array_remove(ARRAY[
                   CASE WHEN c.relkind = 'f' THEN 'foreign table: not replicated by Postgres' END,
                   CASE WHEN c.relkind = 'm' THEN 'materialized view: not replicated by Postgres' END,
                   CASE WHEN c.relpersistence = 'u' THEN 'unlogged table: not replicated by Postgres' END,
                   CASE WHEN c.relkind IN ('r', 'p') AND c.relpersistence <> 'u'
                         AND c.relreplident IN ('d', 'n')
                         AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary)
                        THEN 'no primary key or replica identity: publishing it would make UPDATE and DELETE on this table fail in your database' END,
                   CASE WHEN c.relreplident = 'f' AND EXISTS (
                            SELECT 1 FROM pg_attribute a
                            WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                              AND NOT EXISTS (SELECT 1 FROM pg_opclass o JOIN pg_am m ON m.oid = o.opcmethod
                                              WHERE o.opcintype = a.atttypid AND o.opcdefault AND m.amname IN ('btree', 'hash')))
                        THEN 'REPLICA IDENTITY FULL on a column type with no equality operator' END,
                   CASE WHEN EXISTS (
                            SELECT 1 FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
                            WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                              AND (t.typtype IN ('e', 'd', 'c') OR t.typnamespace NOT IN ('pg_catalog'::regnamespace)))
                        THEN 'uses a custom type (enum, domain or composite): create the type in IsomerDB first' END
               ], NULL),
               array_remove(ARRAY[
                   CASE WHEN EXISTS (SELECT 1 FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
                                     WHERE a.attrelid = c.oid AND a.attnum > 0
                                     AND NOT a.attisdropped AND t.typname IN ('oid', 'lo'))
                        THEN 'has an oid column: large objects it points to are not replicated' END,
                   CASE WHEN EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0
                                     AND NOT a.attisdropped AND a.attgenerated = 's')
                        THEN 'has stored generated columns: they are copied as plain values' END
               ], NULL)
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY (%L::text[])
          AND c.relkind IN ('r', 'p', 'f', 'm')
          AND NOT c.relispartition
        ORDER BY 1
    $remote$, schemas)) AS r(table_name text, problems text[], warnings text[]);
END $$;

-- Column definitions and primary key of a source table, as CREATE TABLE text.
CREATE FUNCTION isomer._remote_table_ddl(p_conninfo text, p_table text)
RETURNS text LANGUAGE sql SET search_path = pg_catalog AS $$
    SELECT r.ddl FROM public.dblink(p_conninfo, format($remote$
        SELECT format('CREATE TABLE IF NOT EXISTS %%I.%%I (%%s%%s)', n.nspname, c.relname,
                      string_agg(format('%%I %%s%%s', a.attname, format_type(a.atttypid, a.atttypmod),
                                        CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END), ', ' ORDER BY a.attnum),
                      coalesce((SELECT format(', PRIMARY KEY (%%s)', string_agg(format('%%I', ka.attname), ', ' ORDER BY k.ord))
                                FROM pg_index i
                                CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                                JOIN pg_attribute ka ON ka.attrelid = i.indrelid AND ka.attnum = k.attnum
                                WHERE i.indrelid = c.oid AND i.indisprimary), ''))
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE c.oid = %L::regclass AND a.attnum > 0 AND NOT a.attisdropped
        GROUP BY c.oid, n.nspname, c.relname
    $remote$, p_table)) AS r(ddl text)
$$;

CREATE FUNCTION isomer.attach(
    name     text,
    conninfo text,
    schemas  text[] DEFAULT '{public}',
    tables   text[] DEFAULT NULL)
RETURNS TABLE (table_name text, status text, detail text)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_version int;
    v_pub     text := 'isomer_' || name;
    v_chosen  text[];
    v_t       text;
    v_schema  text;
    r         record;
BEGIN
    IF EXISTS (SELECT 1 FROM isomer.attachments a WHERE a.name = attach.name) THEN
        RAISE EXCEPTION 'isomer: attachment % already exists', name;
    END IF;

    SELECT v::int INTO v_version FROM public.dblink(conninfo, 'SHOW server_version_num') AS t(v text);

    -- Preflight, then choose: the named tables (each must pass), or every table that passes.
    FOR r IN SELECT * FROM isomer.preflight(conninfo, schemas) LOOP
        IF tables IS NULL OR r.table_name = ANY (tables) THEN
            IF r.ok THEN
                v_chosen := v_chosen || r.table_name;
                table_name := r.table_name; status := 'attached';
                detail := nullif(array_to_string(r.warnings, '; '), '');
            ELSIF tables IS NOT NULL THEN
                RAISE EXCEPTION 'isomer: % cannot be attached: %', r.table_name, array_to_string(r.problems, '; ');
            ELSE
                table_name := r.table_name; status := 'skipped'; detail := array_to_string(r.problems, '; ');
            END IF;
            RETURN NEXT;
        END IF;
    END LOOP;
    IF v_chosen IS NULL THEN
        RAISE EXCEPTION 'isomer: no table in % passed preflight', schemas;
    END IF;

    -- Local tables, under the same names (logical replication matches tables by name).
    FOREACH v_t IN ARRAY v_chosen LOOP
        v_schema := (parse_ident(v_t))[1];
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);
        EXECUTE isomer._remote_table_ddl(conninfo, v_t);
    END LOOP;

    -- FR-A7: an explicit table list, never FOR ALL TABLES (needs superuser on most providers).
    BEGIN
        PERFORM public.dblink_exec(conninfo, format(
            'CREATE PUBLICATION %I FOR TABLE %s WITH (publish_via_partition_root = true)',
            v_pub, array_to_string(v_chosen, ', ')));
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'isomer: could not create the publication on the source: %', SQLERRM
            USING HINT = format('Run this on the source as the tables'' owner, then attach again: CREATE PUBLICATION %I FOR TABLE %s;',
                                v_pub, array_to_string(v_chosen, ', '));
    END;

    -- FR-A9: on PG 17+ sources the slot is a failover slot, so it survives a source failover.
    PERFORM isomer._remote_call(conninfo, format(
        'SELECT pg_create_logical_replication_slot(%L, ''pgoutput'', false, false%s)',
        v_pub, CASE WHEN v_version >= 170000 THEN ', true' ELSE '' END));

    INSERT INTO isomer.attachments (name, conninfo, schemas, publication, slot, subscription, source_version)
    VALUES (name, conninfo, schemas, v_pub, v_pub, v_pub, v_version);
    INSERT INTO isomer.attached_tables (attachment, table_name)
    SELECT name, t FROM unnest(v_chosen) t;

    EXECUTE format(
        'CREATE SUBSCRIPTION %I CONNECTION %L PUBLICATION %I WITH (create_slot = false, slot_name = %L, copy_data = true)',
        v_pub, conninfo, v_pub, v_pub);

    PERFORM isomer._log('info', name, format('attached %s tables from source (server_version_num %s)',
                                             cardinality(v_chosen), v_version));
END $$;

CREATE FUNCTION isomer.detach(name text, drop_tables boolean DEFAULT false)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a isomer.attachments;
    t record;
BEGIN
    SELECT * INTO a FROM isomer.attachments WHERE attachments.name = detach.name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no attachment named %', name;
    END IF;
    EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', a.subscription);
    EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', a.subscription);
    EXECUTE format('DROP SUBSCRIPTION %I', a.subscription);
    BEGIN
        PERFORM isomer._remote_call(a.conninfo, format('SELECT pg_drop_replication_slot(%L)', a.slot));
        PERFORM public.dblink_exec(a.conninfo, format('DROP PUBLICATION IF EXISTS %I', a.publication));
    EXCEPTION WHEN OTHERS THEN
        PERFORM isomer._log('warning', a.name, format('could not clean up on the source: %s (drop slot %s and publication %s there)', SQLERRM, a.slot, a.publication));
    END;
    IF drop_tables THEN
        FOR t IN SELECT table_name FROM isomer.attached_tables WHERE attachment = a.name LOOP
            EXECUTE format('DROP TABLE IF EXISTS %s', t.table_name);
        END LOOP;
    END IF;
    DELETE FROM isomer.attachments WHERE attachments.name = a.name;
END $$;

-- Replication health, read from both sides.
CREATE FUNCTION isomer.attachment_status(name text DEFAULT NULL)
RETURNS TABLE (attachment text, status text, slot_active boolean, wal_status text,
               retained_wal_bytes bigint, safe_wal_bytes bigint, apply_lag interval,
               tables_ready integer, tables_total integer, apply_errors bigint, sync_errors bigint)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a isomer.attachments;
BEGIN
    FOR a IN SELECT * FROM isomer.attachments WHERE attachment_status.name IS NULL OR attachments.name = attachment_status.name LOOP
        attachment := a.name; status := a.status;
        BEGIN
            SELECT r.active, r.wal_status, r.retained, r.safe INTO slot_active, wal_status, retained_wal_bytes, safe_wal_bytes
            FROM public.dblink(a.conninfo, format(
                'SELECT active, wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, safe_wal_size '
                'FROM pg_replication_slots WHERE slot_name = %L', a.slot))
                AS r(active boolean, wal_status text, retained bigint, safe bigint);
        EXCEPTION WHEN OTHERS THEN
            slot_active := NULL; wal_status := 'unreachable: ' || SQLERRM;
        END;
        SELECT now() - st.latest_end_time INTO apply_lag
        FROM pg_stat_subscription st WHERE st.subname = a.subscription AND st.relid IS NULL;
        SELECT count(*) FILTER (WHERE sr.srsubstate = 'r'), count(*) INTO tables_ready, tables_total
        FROM pg_subscription_rel sr JOIN pg_subscription s ON s.oid = sr.srsubid WHERE s.subname = a.subscription;
        SELECT ss.apply_error_count, ss.sync_error_count INTO apply_errors, sync_errors
        FROM pg_stat_subscription_stats ss WHERE ss.subname = a.subscription;
        RETURN NEXT;
    END LOOP;
END $$;

-- FR-A4: additive schema changes on the source are applied here automatically; destructive
-- ones pause the attachment. New tables in attached schemas are preflighted and added.
CREATE FUNCTION isomer._sync_schema(a isomer.attachments)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    t      record;
    c      record;
    r      record;
    v_new  text[];
BEGIN
    FOR t IN SELECT table_name FROM isomer.attached_tables WHERE attachment = a.name LOOP
        FOR c IN
            SELECT rc.attname, rc.coltype, lc.coltype AS local_type
            FROM public.dblink(a.conninfo, format(
                'SELECT attname::text, format_type(atttypid, atttypmod) FROM pg_attribute '
                'WHERE attrelid = %L::regclass AND attnum > 0 AND NOT attisdropped', t.table_name))
                AS rc(attname text, coltype text)
            FULL JOIN (SELECT attname::text, format_type(atttypid, atttypmod) AS coltype FROM pg_attribute
                       WHERE attrelid = t.table_name::regclass AND attnum > 0 AND NOT attisdropped) lc
                USING (attname)
            WHERE rc.coltype IS DISTINCT FROM lc.coltype
        LOOP
            IF c.local_type IS NULL THEN
                EXECUTE format('ALTER TABLE %s ADD COLUMN %I %s', t.table_name, c.attname, c.coltype);
                PERFORM isomer._log('info', a.name, format('added column %s.%s (%s) from source', t.table_name, c.attname, c.coltype));
            ELSE
                EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', a.subscription);
                UPDATE isomer.attachments SET status = 'paused' WHERE name = a.name;
                PERFORM isomer._log('error', a.name, format(
                    'column %s.%s changed on the source (%s -> %s): attachment paused until the change is applied here',
                    t.table_name, coalesce(c.attname, '?'), coalesce(c.local_type, 'none'), coalesce(c.coltype, 'dropped')));
                RETURN;
            END IF;
        END LOOP;
    END LOOP;

    -- New tables in the attached schemas.
    FOR r IN SELECT * FROM isomer.preflight(a.conninfo, a.schemas) p
             WHERE NOT EXISTS (SELECT 1 FROM isomer.attached_tables x WHERE x.attachment = a.name AND x.table_name = p.table_name)
    LOOP
        IF NOT r.ok THEN
            CONTINUE;
        END IF;
        EXECUTE isomer._remote_table_ddl(a.conninfo, r.table_name);
        PERFORM public.dblink_exec(a.conninfo, format('ALTER PUBLICATION %I ADD TABLE %s', a.publication, r.table_name));
        INSERT INTO isomer.attached_tables (attachment, table_name) VALUES (a.name, r.table_name);
        v_new := v_new || r.table_name;
    END LOOP;
    IF v_new IS NOT NULL THEN
        UPDATE isomer.attachments SET needs_refresh = true WHERE name = a.name;
        PERFORM isomer._log('info', a.name, format('added new source tables: %s', array_to_string(v_new, ', ')));
    END IF;
END $$;

-- Health checks for every attachment. Scheduled every 10 seconds by pg_cron.
CREATE FUNCTION isomer.check_attachments(p_max_retained_wal bigint DEFAULT 1024 * 1024 * 1024)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a  isomer.attachments;
    st record;
BEGIN
    FOR a IN SELECT * FROM isomer.attachments WHERE status = 'active' LOOP
        BEGIN
            -- Heartbeat: write a little WAL in the source database so the slot can advance
            -- even when the attached tables are idle and other databases are busy.
            BEGIN
                PERFORM isomer._remote_call(a.conninfo, 'SELECT pg_logical_emit_message(false, ''isomer.heartbeat'', now()::text)');
            EXCEPTION WHEN insufficient_privilege THEN
                NULL;
            END;

            SELECT * INTO st FROM isomer.attachment_status(a.name);
            IF st.wal_status IN ('lost') OR (st.slot_active IS NULL AND st.wal_status IS NULL) THEN
                PERFORM isomer._log('error', a.name, format('replication slot %s is %s on the source: re-snapshotting',
                                                            a.slot, coalesce(st.wal_status, 'missing')));
                PERFORM isomer.resync(a.name);
                CONTINUE;
            END IF;
            IF st.retained_wal_bytes > p_max_retained_wal THEN
                PERFORM isomer._log('warning', a.name, format('slot %s is holding %s of WAL on the source',
                                                              a.slot, pg_size_pretty(st.retained_wal_bytes)));
            END IF;

            IF a.needs_refresh THEN
                PERFORM isomer._loopback(format('ALTER SUBSCRIPTION %I REFRESH PUBLICATION', a.subscription));
                UPDATE isomer.attachments SET needs_refresh = false WHERE name = a.name;
            END IF;
            PERFORM isomer._sync_schema(a);
        EXCEPTION WHEN OTHERS THEN
            PERFORM isomer._log('error', a.name, format('health check failed: %s', SQLERRM));
        END;
    END LOOP;
END $$;

-- Start an attachment again from a fresh snapshot: used when the source dropped or
-- invalidated our slot. Projections over the attached tables are rebuilt afterwards.
CREATE FUNCTION isomer.resync(name text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a isomer.attachments;
    t record;
BEGIN
    SELECT * INTO STRICT a FROM isomer.attachments WHERE attachments.name = resync.name;
    UPDATE isomer.attachments SET status = 'resyncing' WHERE attachments.name = a.name;
    EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', a.subscription);
    EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', a.subscription);
    EXECUTE format('DROP SUBSCRIPTION %I', a.subscription);
    FOR t IN SELECT table_name FROM isomer.attached_tables WHERE attachment = a.name LOOP
        EXECUTE format('TRUNCATE %s', t.table_name);
    END LOOP;
    BEGIN
        PERFORM isomer._remote_call(a.conninfo, format('SELECT pg_drop_replication_slot(%L)', a.slot));
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
    PERFORM isomer._remote_call(a.conninfo, format(
        'SELECT pg_create_logical_replication_slot(%L, ''pgoutput'', false, false%s)',
        a.slot, CASE WHEN a.source_version >= 170000 THEN ', true' ELSE '' END));
    EXECUTE format(
        'CREATE SUBSCRIPTION %I CONNECTION %L PUBLICATION %I WITH (create_slot = false, slot_name = %L, copy_data = true)',
        a.subscription, a.conninfo, a.publication, a.slot);
    UPDATE isomer.attachments SET status = 'active' WHERE attachments.name = a.name;
    PERFORM isomer._log('info', a.name, 'resync started from a fresh snapshot');
END $$;
