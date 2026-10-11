-- IsomerDB 0.3.0: primary mode (PRD Phase 2).
--
--   * Write tokens and read-your-writes: every source change is stamped with a token from the
--     Citus cluster clock (HLC); isomer.wait_for(token) blocks until projections caught up.
--   * Cache projections: precomputed key -> value in a Postgres table.
--   * Projections over Citus distributed tables, with the co-location rules that make sync
--     (same-transaction) maintenance possible.
--   * Cutover from attach mode to primary: sequences, defaults, verification, optional
--     reverse replication for rollback.
--   * The auditor: samples projections, re-derives them from the tables, and counts drift.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.3.0'" to load this file. \quit

-- ── Write tokens (FR-R3/R4/R5) ──────────────────────────────────────────────

-- A token is the Citus cluster clock packed into a bigint: logical milliseconds << 22 | counter.
-- Tokens taken later on this node are larger. (A Mongo Timestamp(t, i) maps to it as
-- t = ms / 1000, i = (ms % 1000) << 22 | counter; see the gateway.)
CREATE FUNCTION isomer.current_token()
RETURNS bigint LANGUAGE sql VOLATILE SET search_path = pg_catalog AS $$
    SELECT (split_part(c, ',', 1)::bigint << 22) | split_part(c, ',', 2)::bigint
    FROM (SELECT trim(both '()' FROM pg_catalog.citus_get_node_clock()::text) AS c) s
$$;

-- The token of this session's most recent change to a projection's source table. Any token
-- taken after a write commits (isomer.current_token()) works too.
CREATE FUNCTION isomer.last_write_token()
RETURNS bigint LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT nullif(current_setting('isomer.last_write_token', true), '')::bigint
$$;

ALTER TABLE isomer.pending ADD COLUMN token bigint NOT NULL DEFAULT 0;
ALTER TABLE isomer.embed_jobs ADD COLUMN token bigint NOT NULL DEFAULT 0;
CREATE INDEX pending_token ON isomer.pending (projection, token);

-- ── Catalog changes ─────────────────────────────────────────────────────────

ALTER TABLE isomer.projections DROP CONSTRAINT projections_kind_check;
ALTER TABLE isomer.projections ADD CONSTRAINT projections_kind_check
    CHECK (kind IN ('document', 'search', 'vector', 'cache'));
ALTER TABLE isomer.attachments DROP CONSTRAINT attachments_status_check;
ALTER TABLE isomer.attachments ADD CONSTRAINT attachments_status_check
    CHECK (status IN ('active', 'paused', 'resyncing', 'cutover'));
ALTER TABLE isomer.attachments ADD COLUMN cutover_at timestamptz;
-- After cutover with rollback: how the source reaches IsomerDB, until its subscription exists.
-- The subscription is created by the next health check, once cutover has committed: creating its
-- replication slot here waits for every open transaction, including the cutover itself.
ALTER TABLE isomer.attachments ADD COLUMN rollback_conninfo text;
ALTER TABLE isomer.attachments ADD COLUMN rollback_status text;

CREATE SCHEMA isomer_cache;
REVOKE ALL ON SCHEMA isomer_cache FROM PUBLIC;

CREATE FUNCTION isomer._cache_store(p_projection text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT format('isomer_cache.%I', p_projection)
$$;

-- Attachments that are cut over no longer constrain their tables.
CREATE OR REPLACE FUNCTION isomer._attachment_of(p_table regclass)
RETURNS text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT t.attachment FROM isomer.attached_tables t JOIN isomer.attachments a ON a.name = t.attachment
    WHERE t.table_name::regclass = p_table AND a.status <> 'cutover' LIMIT 1
$$;

CREATE FUNCTION isomer._distribution(p_table regclass, OUT method "char", OUT column_name text, OUT colocation integer)
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT p.partmethod,
           CASE WHEN p.partmethod = 'h' THEN pg_catalog.column_to_column_name(p.logicalrelid, p.partkey) END,
           p.colocationid
    FROM pg_dist_partition p WHERE p.logicalrelid = p_table
$$;

CREATE FUNCTION isomer._multi_node()
RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT count(DISTINCT (nodename, nodeport)) > 1 FROM pg_dist_node WHERE isactive AND noderole = 'primary'
$$;

-- FR-S2: can this projection be maintained with this freshness over these sources? Returns the
-- reason it can't, or NULL. Sync needs every distributed source co-located on the projection
-- key (so each write's documents live on the writing shard), and IsomerDB to be the primary.
CREATE FUNCTION isomer._check_sources(p_projection text, p_freshness text)
RETURNS text LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    s       record;
    d       record;
    v_coloc integer;
    v_att   text;
BEGIN
    FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p_projection LOOP
        v_att := isomer._attachment_of(s.source_table::regclass);
        IF p_freshness = 'sync' AND v_att IS NOT NULL THEN
            RETURN format('%s is attached from another database (attachment %s); sync needs IsomerDB to be the primary. Cut over first (isomer.cutover).', s.source_table, v_att);
        END IF;
        SELECT * INTO d FROM isomer._distribution(s.source_table::regclass);
        IF NOT FOUND OR d.method IS NULL THEN
            CONTINUE;   -- a local table
        END IF;
        IF isomer._multi_node() THEN
            RETURN format('%s is distributed across several nodes; projections over multi-node clusters are not supported yet (PRD Phase 3)', s.source_table);
        END IF;
        IF p_freshness = 'sync' AND d.method = 'h' THEN
            IF s.key_column IS NULL OR s.key_column <> d.column_name THEN
                RETURN format('%s is distributed by %s, but its rows map to the projection key through %s: a write to it would touch documents on other shards. Distribute it by %s, or use freshness => ''bounded''.',
                              s.source_table, d.column_name, coalesce(s.key_column, '(nothing: lookup table)'), coalesce(s.key_column, 'the key'));
            END IF;
            IF v_coloc IS NOT NULL AND v_coloc <> d.colocation THEN
                RETURN format('%s is not co-located with the other sources; distribute it with colocate_with, or use freshness => ''bounded''.', s.source_table);
            END IF;
            v_coloc := d.colocation;
        END IF;
    END LOOP;
    RETURN NULL;
END $$;

-- ── Applying derived rows: documents and cache entries ──────────────────────

CREATE OR REPLACE FUNCTION isomer._apply(p_projection text, p_keys text[])
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p          isomer.projections;
    v_updates  jsonb;
    v_found    text[];
    v_missing  jsonb;
    v_ok       boolean;
    v_result   text;
    v_written  integer;
    v_deleted  integer;
BEGIN
    IF p_keys IS NULL OR cardinality(p_keys) = 0 THEN
        RETURN;
    END IF;
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;

    IF p.kind = 'cache' THEN
        EXECUTE format(
            $q$WITH src AS (SELECT q.%1$I::text AS key, to_jsonb(q.value) AS value FROM (%2$s) q WHERE q.%1$I = ANY ($1::%3$s[])),
                    up AS (INSERT INTO %4$s AS c (key, value, updated_at) SELECT key, value, clock_timestamp() FROM src
                           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                           WHERE c.value IS DISTINCT FROM EXCLUDED.value RETURNING 1),
                    del AS (DELETE FROM %4$s c WHERE c.key = ANY ($1) AND c.key NOT IN (SELECT key FROM src) RETURNING 1)
               SELECT (SELECT count(*) FROM up)::int, (SELECT count(*) FROM del)::int$q$,
            p.key_column, p.query, p.key_type, isomer._cache_store(p.name))
        INTO v_written, v_deleted USING p_keys;
        UPDATE isomer.projection_stats SET documents_written = documents_written + v_written,
                                           documents_deleted = documents_deleted + v_deleted
        WHERE projection = p.name;
        RETURN;
    END IF;

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

CREATE OR REPLACE FUNCTION isomer._rebuild_all(p_projection text, p_batch integer DEFAULT 500)
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

    IF p.kind = 'cache' THEN
        EXECUTE format('DELETE FROM %s WHERE key NOT IN (SELECT isomer._all_keys(%L))', isomer._cache_store(p.name), p.name);
        RETURN v_total;
    END IF;

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

CREATE OR REPLACE FUNCTION isomer._full_refresh(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p isomer.projections;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    IF p.kind IN ('document', 'cache') THEN
        PERFORM isomer._rebuild_all(p.name);
    ELSIF p.kind = 'vector' THEN
        EXECUTE format('DELETE FROM %s WHERE key NOT IN (SELECT isomer._all_keys(%L))',
                       isomer._vector_store(p.name), p.name);
        DELETE FROM isomer.embed_jobs j WHERE j.projection = p.name
            AND j.key NOT IN (SELECT isomer._all_keys(p.name));
        INSERT INTO isomer.pending (projection, key, token)
        SELECT p.name, k, isomer.current_token() FROM isomer._all_keys(p.name) k ON CONFLICT DO NOTHING;
    END IF;
END $$;

-- Source trigger: stamps each change with a write token.
CREATE OR REPLACE FUNCTION isomer._on_source_change()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_projection text := TG_ARGV[0];
    v_column     text := TG_ARGV[1];
    v_freshness  text := TG_ARGV[2];
    v_keys       text[];
    v_token      bigint := isomer.current_token();
BEGIN
    PERFORM set_config('isomer.last_write_token', v_token::text, false);

    IF TG_OP = 'TRUNCATE' THEN
        PERFORM isomer._log('info', v_projection, format('%s truncated: full refresh', TG_TABLE_NAME));
        PERFORM isomer._full_refresh(v_projection);
        RETURN NULL;
    END IF;

    IF v_column = '' THEN
        IF v_freshness = 'sync' THEN
            PERFORM isomer._rebuild_all(v_projection);
        ELSE
            INSERT INTO isomer.pending (projection, key, token)
            SELECT v_projection, k, v_token FROM isomer._all_keys(v_projection) k
            ON CONFLICT DO NOTHING;
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
        INSERT INTO isomer.pending (projection, key, token)
        SELECT v_projection, k, v_token FROM unnest(v_keys) k
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NULL;
END $$;

-- (Re)create the row and TRUNCATE triggers on every source of a key-maintained projection.
-- Citus must propagate these triggers to every shard (the rows live there), whatever the caller
-- set citus.enable_ddl_propagation to.
CREATE FUNCTION isomer._create_source_triggers(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_unsafe_triggers = on SET citus.enable_ddl_propagation = on AS $$
DECLARE
    p         isomer.projections;
    s         record;
    v_trigger text := format('isomer_%s', p_projection);
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
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
END $$;

CREATE OR REPLACE FUNCTION isomer._install(p_projection text)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off AS $$
DECLARE
    p         isomer.projections;
    v_target  text;
    v_count   bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;

    IF p.kind IN ('document', 'vector', 'cache') THEN
        IF p.kind = 'document' THEN
            PERFORM documentdb_api.create_collection(p.mongo_database, p.name);
        ELSIF p.kind = 'vector' THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %s (key text NOT NULL, model text NOT NULL, embedding public.vector NOT NULL, '
                'source_hash text NOT NULL, embedded_at timestamptz NOT NULL, PRIMARY KEY (key, model))',
                isomer._vector_store(p.name));
            PERFORM isomer._vector_index(p.name, p.model);
            PERFORM isomer._vector_view(p.name);
        ELSE
            EXECUTE format('CREATE TABLE IF NOT EXISTS %s (key text PRIMARY KEY, value jsonb, updated_at timestamptz NOT NULL)',
                           isomer._cache_store(p.name));
            EXECUTE format('DROP VIEW IF EXISTS %I.%I', p.target_schema, p.name);
            EXECUTE format('CREATE VIEW %I.%I AS SELECT key::%s AS %I, value, updated_at FROM %s',
                           p.target_schema, p.name, p.key_type, p.key_column, isomer._cache_store(p.name));
        END IF;
        PERFORM isomer._create_source_triggers(p.name);
        INSERT INTO isomer.projection_stats (projection) VALUES (p.name) ON CONFLICT DO NOTHING;
        IF p.kind IN ('document', 'cache') THEN
            v_count := isomer._rebuild_all(p.name);
            IF p.kind = 'cache' THEN
                RETURN format('cache projection %s: %s entries, read it as %I.%I or isomer.cache_get(%L, key) (%s)',
                              p.name, v_count, p.target_schema, p.name, p.name, p.freshness);
            END IF;
            RETURN format('document projection %s: %s documents in collection %s.%s (%s)',
                          p.name, v_count, p.mongo_database, p.name, p.freshness);
        END IF;
        -- Backfill work carries token 0: read-your-writes waits cover writes, not initial builds.
        INSERT INTO isomer.pending (projection, key, phase, token)
        SELECT p.name, k, 'backfill', 0 FROM isomer._all_keys(p.name) k
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS v_count = ROW_COUNT;
        RETURN format('vector projection %s: %s keys queued for embedding with %s; read it as %I.%I',
                      p.name, v_count, p.model, p.target_schema, p.name);
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

CREATE FUNCTION isomer.cache_get(p_projection text, p_key text)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    v jsonb;
BEGIN
    EXECUTE format('SELECT value FROM %s WHERE key = $1', isomer._cache_store(p_projection)) INTO v USING p_key;
    RETURN v;
END $$;

-- ── create / alter / drop ───────────────────────────────────────────────────

DROP FUNCTION isomer.create_projection(text, text, text, text, text, interval, jsonb, text, text[], text, text);

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
    target_schema text     DEFAULT 'public',
    model         text     DEFAULT NULL)
-- No SET search_path here: the query and source names resolve with the caller's search path,
-- then get stored schema-qualified. Probe views are local, so Citus must not propagate them.
RETURNS text LANGUAGE plpgsql SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_probe     text := format('pg_temp.isomer_create_%s', pg_backend_pid());
    v_cols      record;
    v_key_type  text;
    v_has_doc   boolean := false;
    v_has_input boolean := false;
    v_has_value boolean := false;
    v_tables    regclass[];
    v_t         regclass;
    v_src       record;
    v_index     text[];
    v_reason    text;
BEGIN
    IF kind NOT IN ('document', 'search', 'vector', 'cache') THEN
        RAISE EXCEPTION 'isomer: kind must be ''document'', ''search'', ''vector'' or ''cache'', not %', kind;
    END IF;
    IF freshness NOT IN ('sync', 'bounded') THEN
        RAISE EXCEPTION 'isomer: freshness must be ''sync'' or ''bounded'', not %', freshness;
    END IF;
    IF kind = 'vector' THEN
        IF freshness = 'sync' THEN
            RAISE EXCEPTION 'isomer: vector projection % cannot be sync: an embedding call cannot run inside the writing transaction', name
                USING HINT = 'Use freshness => ''bounded'' with a lag budget, e.g. lag => ''30 seconds''.';
        END IF;
        IF model IS NULL THEN
            RAISE EXCEPTION 'isomer: a vector projection needs model => one of %',
                (SELECT string_agg(m.name, ', ') FROM isomer.embedding_models m);
        END IF;
        PERFORM 1 FROM isomer.embedding_models m WHERE m.name = create_projection.model;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'isomer: no embedding model named %', model
                USING HINT = 'Register one with isomer.add_embedding_model().';
        END IF;
    END IF;

    EXECUTE format('CREATE OR REPLACE TEMP VIEW %s AS %s', v_probe, query);
    query := isomer._viewdef(v_probe::regclass);
    EXECUTE format('DROP VIEW %s', v_probe);

    FOR v_cols IN SELECT * FROM isomer._query_columns(query) LOOP
        IF v_cols.column_name = key THEN
            v_key_type := v_cols.column_type;
        END IF;
        IF v_cols.column_name = 'doc' AND v_cols.type_oid IN ('jsonb'::regtype, 'json'::regtype) THEN
            v_has_doc := true;
        END IF;
        IF v_cols.column_name = 'input' AND v_cols.type_oid IN ('text'::regtype, 'varchar'::regtype, 'bpchar'::regtype) THEN
            v_has_input := true;
        END IF;
        IF v_cols.column_name = 'value' THEN
            v_has_value := true;
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
    IF kind = 'vector' AND NOT v_has_input THEN
        RAISE EXCEPTION 'isomer: a vector projection''s query must return a text column named input (the text to embed)'
            USING HINT = 'e.g. SELECT p.sku, p.title || '' '' || p.description AS input FROM products p';
    END IF;
    IF kind = 'cache' AND NOT v_has_value THEN
        RAISE EXCEPTION 'isomer: a cache projection''s query must return a column named value'
            USING HINT = 'e.g. SELECT c.id, count(o.*) AS value FROM customers c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.id';
    END IF;

    v_tables := ARRAY(SELECT isomer._query_tables(query));
    IF cardinality(v_tables) = 0 THEN
        RAISE EXCEPTION 'isomer: the query reads no tables';
    END IF;
    IF kind = 'search' THEN
        FOREACH v_t IN ARRAY v_tables LOOP
            IF freshness = 'sync' AND isomer._attachment_of(v_t) IS NOT NULL THEN
                RAISE EXCEPTION 'isomer: projection % cannot be sync: % is attached from another database (attachment %)',
                    name, v_t, isomer._attachment_of(v_t)
                    USING HINT = 'Use freshness => ''bounded'', or cut over first (isomer.cutover). Sync projections need IsomerDB to be the primary for their tables.';
            END IF;
            IF (isomer._distribution(v_t)).method IS NOT NULL THEN
                RAISE EXCEPTION 'isomer: search projections over Citus distributed tables are not supported yet (%)', v_t;
            END IF;
        END LOOP;
    END IF;

    INSERT INTO isomer.projections (name, kind, query, key_column, key_type, freshness, lag_budget,
                                    mongo_database, target_schema, index_columns, model)
    VALUES (name, kind, query, key, v_key_type, freshness, lag,
            CASE WHEN kind = 'document' THEN database END,
            CASE WHEN kind IN ('search', 'vector', 'cache') THEN target_schema END,
            CASE WHEN kind = 'search' THEN key || coalesce(index_columns, v_index) END,
            CASE WHEN kind = 'vector' THEN model END);

    IF kind IN ('document', 'vector', 'cache') THEN
        IF sources IS NULL THEN
            RAISE EXCEPTION 'isomer: a % projection needs sources: which column of each table holds the projection key', kind
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
        FOREACH v_t IN ARRAY v_tables LOOP
            IF NOT EXISTS (SELECT 1 FROM isomer.projection_sources s
                           WHERE s.projection = name AND s.source_table::regclass = v_t) THEN
                RAISE EXCEPTION 'isomer: the query reads % but sources does not map it', v_t
                    USING HINT = format('Add "%s": "<column holding the key>", or "%s": null if it is a lookup table.', v_t, v_t);
            END IF;
        END LOOP;
        v_reason := isomer._check_sources(name, freshness);
        IF v_reason IS NOT NULL THEN
            RAISE EXCEPTION 'isomer: projection % cannot be %: %', name, freshness, v_reason;
        END IF;
    END IF;

    RETURN isomer._install(name);
END $$;

-- Change a key-maintained projection's freshness or lag budget (e.g. bounded -> sync after cutover).
CREATE FUNCTION isomer.alter_projection(name text, freshness text DEFAULT NULL, lag interval DEFAULT NULL)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        isomer.projections;
    v_reason text;
BEGIN
    SELECT * INTO p FROM isomer.projections WHERE projections.name = alter_projection.name FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', name;
    END IF;
    IF freshness IS NOT NULL AND freshness <> p.freshness THEN
        IF p.kind NOT IN ('document', 'cache') THEN
            RAISE EXCEPTION 'isomer: only document and cache projections can change freshness (this is a % projection)', p.kind;
        END IF;
        v_reason := isomer._check_sources(p.name, freshness);
        IF v_reason IS NOT NULL THEN
            RAISE EXCEPTION 'isomer: projection % cannot be %: %', p.name, freshness, v_reason;
        END IF;
        UPDATE isomer.projections SET freshness = alter_projection.freshness WHERE projections.name = p.name;
        PERFORM isomer._create_source_triggers(p.name);
        -- Anything still queued is applied now, so the projection is current as it changes mode.
        PERFORM isomer._apply(p.name, ARRAY(SELECT key FROM isomer.pending WHERE projection = p.name));
        DELETE FROM isomer.pending WHERE projection = p.name;
    END IF;
    IF lag IS NOT NULL THEN
        UPDATE isomer.projections SET lag_budget = lag WHERE projections.name = p.name;
    END IF;
    RETURN format('%s: freshness %s, lag budget %s', p.name, coalesce(freshness, p.freshness), coalesce(lag, p.lag_budget));
END $$;

CREATE FUNCTION isomer._drop_source_triggers(p_projection text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_unsafe_triggers = on SET citus.enable_ddl_propagation = on AS $$
DECLARE
    s record;
BEGIN
    FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p_projection LOOP
        IF to_regclass(s.source_table) IS NOT NULL THEN
            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', 'isomer_' || p_projection, s.source_table);
            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', 'isomer_' || p_projection || '_truncate', s.source_table);
        END IF;
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION isomer.drop_projection(name text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off SET citus.enable_unsafe_triggers = on AS $$
DECLARE
    p isomer.projections;
    s record;
BEGIN
    SELECT * INTO p FROM isomer.projections WHERE projections.name = drop_projection.name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', name;
    END IF;
    IF p.kind IN ('document', 'vector', 'cache') THEN
        PERFORM isomer._drop_source_triggers(p.name);
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

-- ── Draining: documents, caches and vectors, carrying tokens ────────────────

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
BEGIN
    FOR p IN SELECT name, kind, lag_budget FROM isomer.projections
             WHERE kind IN ('document', 'vector', 'cache') AND freshness = 'bounded' AND status = 'active'
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

-- ── Read your writes ────────────────────────────────────────────────────────

-- The newest token a projection fully reflects: every change stamped at or before it has been
-- applied. Sync projections are always current.
CREATE FUNCTION isomer.freshness(p_projection text)
RETURNS bigint LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    p     isomer.projections;
    v_min bigint;
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    IF p.freshness = 'sync' THEN
        RETURN isomer.current_token();
    END IF;
    SELECT least((SELECT min(token) FROM isomer.pending WHERE projection = p.name AND token > 0),
                 (SELECT min(token) FROM isomer.embed_jobs WHERE projection = p.name AND token > 0))
    INTO v_min;
    RETURN coalesce(v_min - 1, isomer.current_token());
END $$;

-- Wait until a projection (or every projection) reflects every change stamped at or before the
-- token, or raise a clear "not yet" error (SQLSTATE IS001) after the timeout. Search projections
-- are refreshed on demand instead.
CREATE FUNCTION isomer.wait_for(p_token bigint, p_projection text DEFAULT NULL, p_timeout interval DEFAULT '5 seconds')
RETURNS bigint LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    v_until timestamptz := clock_timestamp() + p_timeout;
    s       record;
BEGIN
    IF p_token IS NULL THEN
        RETURN isomer.current_token();
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'isomer.wait_for needs READ COMMITTED isolation (this transaction is %)',
            current_setting('transaction_isolation');
    END IF;
    -- The caller's own write can't be applied until it commits: waiting for it inside the same
    -- transaction would only time out.
    IF EXISTS (SELECT 1 FROM isomer.pending
               WHERE xmin = pg_current_xact_id_if_assigned()::text::xid
                 AND token > 0 AND token <= p_token AND (p_projection IS NULL OR projection = p_projection)) THEN
        RAISE EXCEPTION 'isomer.wait_for: the write you are waiting for is not committed yet'
            USING HINT = 'Commit the write first, then call isomer.wait_for(isomer.last_write_token()) in a new transaction.';
    END IF;
    FOR s IN SELECT name, target_schema FROM isomer.projections
             WHERE kind = 'search' AND freshness = 'bounded' AND (p_projection IS NULL OR name = p_projection) LOOP
        PERFORM pgtrickle.refresh_stream_table(format('%I.%I', s.target_schema, s.name));
    END LOOP;
    LOOP
        EXIT WHEN NOT EXISTS (SELECT 1 FROM isomer.pending
                              WHERE token > 0 AND token <= p_token AND (p_projection IS NULL OR projection = p_projection))
              AND NOT EXISTS (SELECT 1 FROM isomer.embed_jobs
                              WHERE token > 0 AND token <= p_token AND (p_projection IS NULL OR projection = p_projection));
        IF clock_timestamp() > v_until THEN
            RAISE EXCEPTION 'isomer: % has not caught up to token % yet (not yet)', coalesce(p_projection, 'a projection'), p_token
                USING ERRCODE = 'IS001',
                      HINT = 'Retry the read, or raise the timeout. isomer.projection_status shows the backlog.';
        END IF;
        PERFORM pg_sleep(0.005);
    END LOOP;
    RETURN p_token;
END $$;

-- For the MongoDB gateway: which projection, if any, a collection is.
CREATE FUNCTION isomer.projection_for_collection(p_database text, p_collection text)
RETURNS text LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT name FROM isomer.projections WHERE kind = 'document' AND mongo_database = p_database AND name = p_collection
$$;

-- A token as the MongoDB cluster Timestamp(t, i) a driver session can be advanced to
-- (session.advanceOperationTime), so its causally consistent reads wait for that write.
CREATE FUNCTION isomer.token_to_mongo_timestamp(p_token bigint, OUT t bigint, OUT i bigint)
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $$
    SELECT (p_token >> 22) / 1000, (((p_token >> 22) % 1000) << 22) | (p_token & 4194303)
$$;

-- The gateway runs reads as the MongoDB user's own role: let any role wait on projections and
-- look them up, without reading IsomerDB's tables directly.
ALTER FUNCTION isomer.wait_for(bigint, text, interval) SECURITY DEFINER;
ALTER FUNCTION isomer.freshness(text) SECURITY DEFINER;
-- Source triggers run as whichever role writes to the table: run them as the extension's owner
-- so ordinary application roles can write without access to IsomerDB's internal tables.
ALTER FUNCTION isomer._on_source_change() SECURITY DEFINER;
ALTER FUNCTION isomer.embed(text, text, interval) SECURITY DEFINER;
ALTER FUNCTION isomer.embed_for(text, text) SECURITY DEFINER;
ALTER FUNCTION isomer.cache_get(text, text) SECURITY DEFINER;
GRANT USAGE ON SCHEMA isomer TO PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA isomer FROM PUBLIC;
GRANT EXECUTE ON FUNCTION isomer.wait_for(bigint, text, interval), isomer.freshness(text),
                          isomer.projection_for_collection(text, text), isomer.current_token(),
                          isomer.last_write_token(), isomer.token_to_mongo_timestamp(bigint),
                          isomer.cache_get(text, text), isomer.embed(text, text, interval),
                          isomer.embed_for(text, text)
    TO PUBLIC;

-- ── Sharding (FR-P2/P3) ─────────────────────────────────────────────────────

-- Distribute tables by an entity key, co-located, so joins and sync projections on that key
-- stay on one shard.
CREATE FUNCTION isomer.distribute(tables regclass[], by text, shard_count integer DEFAULT NULL)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_unsafe_triggers = on AS $$
DECLARE
    v_t     regclass;
    v_first regclass;
BEGIN
    FOREACH v_t IN ARRAY tables LOOP
        IF isomer._attachment_of(v_t) IS NOT NULL THEN
            RAISE EXCEPTION 'isomer: % is attached from another database; cut over before distributing it', v_t;
        END IF;
        IF v_first IS NULL THEN
            PERFORM pg_catalog.create_distributed_table(v_t::text, by, shard_count => coalesce(shard_count, current_setting('citus.shard_count')::int));
            v_first := v_t;
        ELSE
            PERFORM pg_catalog.create_distributed_table(v_t::text, by, colocate_with => v_first::text);
        END IF;
    END LOOP;
    RETURN format('distributed %s by %s, co-located with %s', array_to_string(tables, ', '), by, v_first);
END $$;

-- Copy small lookup tables to every shard, so joins with them are always local.
CREATE FUNCTION isomer.make_reference(tables regclass[])
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_unsafe_triggers = on AS $$
DECLARE
    v_t regclass;
BEGIN
    FOREACH v_t IN ARRAY tables LOOP
        PERFORM pg_catalog.create_reference_table(v_t::text);
    END LOOP;
    RETURN format('reference tables: %s', array_to_string(tables, ', '));
END $$;

-- ── Cutover: attach -> primary (FR-A10, FR-P1) ──────────────────────────────

-- Run a command on the source even while cutover has made it read-only for new sessions.
CREATE FUNCTION isomer._remote_exec_rw(p_conninfo text, p_sql text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    -- New sessions start read-only, so open an explicit read-write transaction.
    PERFORM public.dblink_exec(p_conninfo, 'BEGIN READ WRITE; ' || p_sql || '; COMMIT');
END $$;

-- Make IsomerDB the primary for an attachment's tables:
--   1. stop writes on the source (new sessions read-only) and wait for open write transactions;
--   2. wait until everything the source committed has been applied here, and check the
--      subscription saw no conflicts;
--   3. copy sequence positions, column defaults and identity columns from the source;
--   4. stop replication (subscription, slot, publication);
--   5. optionally replicate IsomerDB's writes back to the source, so the old database stays a
--      current fallback (rollback_conninfo: how the source reaches IsomerDB).
-- Point the application's writes at IsomerDB after this returns.
CREATE FUNCTION isomer.cutover(name text, rollback_conninfo text DEFAULT NULL, timeout interval DEFAULT '60 seconds')
RETURNS TABLE (step text, detail text)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a        isomer.attachments;
    v_db     text;
    v_lsn    pg_lsn;
    v_until  timestamptz := clock_timestamp() + timeout;
    v_writes integer;
    v_confl  bigint;
    v_n      integer := 0;
    r        record;
    v_pub    text;
BEGIN
    SELECT * INTO a FROM isomer.attachments WHERE attachments.name = cutover.name FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no attachment named %', name;
    END IF;
    IF a.status <> 'active' THEN
        RAISE EXCEPTION 'isomer: attachment % is %, not active', a.name, a.status;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_subscription_rel sr JOIN pg_subscription s ON s.oid = sr.srsubid
               WHERE s.subname = a.subscription AND sr.srsubstate <> 'r') THEN
        RAISE EXCEPTION 'isomer: attachment % is still copying tables; cut over once isomer.attachment_status shows every table ready', a.name;
    END IF;

    -- 1. Freeze the source.
    SELECT d INTO v_db FROM public.dblink(a.conninfo, 'SELECT current_database()') AS t(d text);
    PERFORM public.dblink_exec(a.conninfo, format('ALTER DATABASE %I SET default_transaction_read_only = on', v_db));
    LOOP
        SELECT n INTO v_writes FROM public.dblink(a.conninfo,
            'SELECT count(*)::int FROM pg_stat_activity WHERE datname = current_database() '
            'AND backend_xid IS NOT NULL AND pid <> pg_backend_pid() AND backend_type = ''client backend''')
            AS t(n int);
        EXIT WHEN v_writes = 0;
        IF clock_timestamp() > v_until THEN
            PERFORM isomer._remote_exec_rw(a.conninfo, format('ALTER DATABASE %I RESET default_transaction_read_only', v_db));
            RAISE EXCEPTION 'isomer: % write transaction(s) still open on the source after %; the source was unfrozen, nothing changed', v_writes, timeout
                USING HINT = 'Stop the application''s writes to the source, then run cutover again.';
        END IF;
        PERFORM pg_sleep(0.2);
    END LOOP;
    step := 'freeze source'; detail := format('database %s is read-only for new sessions; no open write transactions', v_db); RETURN NEXT;

    -- 2. Catch up and verify.
    SELECT l INTO v_lsn FROM public.dblink(a.conninfo, 'SELECT pg_current_wal_lsn()') AS t(l pg_lsn);
    PERFORM isomer._remote_call(a.conninfo, 'SELECT pg_logical_emit_message(false, ''isomer.cutover'', now()::text)');
    LOOP
        EXIT WHEN (SELECT latest_end_lsn FROM pg_stat_subscription WHERE subname = a.subscription AND relid IS NULL) >= v_lsn;
        IF clock_timestamp() > v_until THEN
            PERFORM isomer._remote_exec_rw(a.conninfo, format('ALTER DATABASE %I RESET default_transaction_read_only', v_db));
            RAISE EXCEPTION 'isomer: replication did not reach the source position % within %; the source was unfrozen, nothing changed', v_lsn, timeout;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    SELECT confl_insert_exists + confl_update_origin_differs + confl_update_exists + confl_update_missing
           + confl_delete_origin_differs + confl_delete_missing + confl_multiple_unique_conflicts
    INTO v_confl FROM pg_stat_subscription_stats WHERE subname = a.subscription;
    IF coalesce(v_confl, 0) > 0 THEN
        PERFORM isomer._remote_exec_rw(a.conninfo, format('ALTER DATABASE %I RESET default_transaction_read_only', v_db));
        RAISE EXCEPTION 'isomer: the subscription recorded % conflict(s); the copy may differ from the source. The source was unfrozen, nothing changed', v_confl
            USING HINT = 'Inspect pg_stat_subscription_stats and the server log, resync (isomer.resync), then cut over again.';
    END IF;
    step := 'catch up'; detail := format('applied everything up to %s; 0 conflicts', v_lsn); RETURN NEXT;

    -- 3a. Sequences, at the source's positions.
    FOR r IN SELECT * FROM public.dblink(a.conninfo, format(
            'SELECT format(''%%I.%%I'', schemaname, sequencename), data_type::text, start_value, increment_by, '
            'min_value, max_value, cycle, last_value FROM pg_sequences WHERE schemaname = ANY (%L::text[]) '
            -- Identity columns own their sequence; it is recreated with the column below.
            'AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = ''pg_class''::regclass '
            'AND d.objid = format(''%%I.%%I'', schemaname, sequencename)::regclass AND d.deptype = ''i'')', a.schemas))
            AS t(seq text, data_type text, start_value bigint, increment_by bigint, min_value bigint, max_value bigint,
                 cycle boolean, last_value bigint)
    LOOP
        EXECUTE format('CREATE SEQUENCE IF NOT EXISTS %s AS %s INCREMENT %s MINVALUE %s MAXVALUE %s START %s %s',
                       r.seq, r.data_type, r.increment_by, r.min_value, r.max_value, r.start_value,
                       CASE WHEN r.cycle THEN 'CYCLE' ELSE 'NO CYCLE' END);
        IF r.last_value IS NULL THEN
            PERFORM setval(r.seq::regclass, r.start_value, false);
        ELSE
            PERFORM setval(r.seq::regclass, r.last_value, true);
        END IF;
        v_n := v_n + 1;
    END LOOP;
    step := 'sequences'; detail := format('%s sequence(s) set to the source''s positions', v_n); RETURN NEXT;

    -- 3b. Column defaults and identity columns of the attached tables.
    v_n := 0;
    FOR r IN SELECT * FROM public.dblink(a.conninfo, format(
            'SELECT format(''%%I.%%I'', n.nspname, c.relname), n.nspname, a.attname::text, a.attidentity::text, '
            'pg_get_expr(d.adbin, d.adrelid), pg_get_serial_sequence(format(''%%I.%%I'', n.nspname, c.relname), a.attname), '
            '(SELECT s.last_value FROM pg_sequences s WHERE format(''%%I.%%I'', s.schemaname, s.sequencename) = pg_get_serial_sequence(format(''%%I.%%I'', n.nspname, c.relname), a.attname)) '
            'FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace '
            'LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum '
            'WHERE format(''%%I.%%I'', n.nspname, c.relname) = ANY (%L::text[]) AND a.attnum > 0 AND NOT a.attisdropped '
            'AND (d.adbin IS NOT NULL OR a.attidentity <> '''')',
            (SELECT array_agg(table_name) FROM isomer.attached_tables WHERE attachment = a.name)))
            AS t(tbl text, nsp text, col text, identity text, default_expr text, serial_seq text, seq_last bigint)
    LOOP
        IF r.identity IN ('a', 'd') THEN
            EXECUTE format('ALTER TABLE %s ALTER COLUMN %I ADD GENERATED %s AS IDENTITY', r.tbl, r.col,
                           CASE WHEN r.identity = 'a' THEN 'ALWAYS' ELSE 'BY DEFAULT' END);
            IF r.seq_last IS NOT NULL THEN
                PERFORM setval(pg_get_serial_sequence(r.tbl, r.col)::regclass, r.seq_last, true);
            END IF;
        ELSIF r.default_expr IS NOT NULL THEN
            -- Defaults print relative to the source's search path; resolve them in the table's schema.
            PERFORM set_config('search_path', format('%I, public, pg_catalog', r.nsp), true);
            EXECUTE format('ALTER TABLE %s ALTER COLUMN %I SET DEFAULT %s', r.tbl, r.col, r.default_expr);
            PERFORM set_config('search_path', 'pg_catalog', true);
        END IF;
        v_n := v_n + 1;
    END LOOP;
    step := 'defaults'; detail := format('%s column default(s) and identity column(s) copied', v_n); RETURN NEXT;

    -- 4. Stop replication.
    EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', a.subscription);
    EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', a.subscription);
    EXECUTE format('DROP SUBSCRIPTION %I', a.subscription);
    -- The source's slot and publication: a failure here must not undo a finished cutover.
    BEGIN
        PERFORM isomer._remote_call(a.conninfo, format('SELECT pg_drop_replication_slot(%L)', a.slot));
        PERFORM isomer._remote_exec_rw(a.conninfo, format('DROP PUBLICATION IF EXISTS %I', a.publication));
        step := 'stop replication'; detail := 'subscription, slot and publication removed';
    EXCEPTION WHEN OTHERS THEN
        step := 'stop replication';
        detail := format('subscription removed; could not clean up the source (%s): drop slot %s and publication %s there', SQLERRM, a.slot, a.publication);
        PERFORM isomer._log('warning', a.name, detail);
    END;
    RETURN NEXT;

    -- 5. Optional: replicate back to the source, which stays a current fallback. The publication
    --    is created now; the source's subscription by the next health check (see the column comment).
    IF rollback_conninfo IS NOT NULL THEN
        v_pub := 'isomer_rollback_' || a.name;
        EXECUTE format('CREATE PUBLICATION %I FOR TABLE %s', v_pub,
                       (SELECT string_agg(table_name, ', ') FROM isomer.attached_tables WHERE attachment = a.name));
        -- The source's apply worker must be able to write, so lift the freeze; the application
        -- must now write only to IsomerDB.
        PERFORM isomer._remote_exec_rw(a.conninfo, format('ALTER DATABASE %I RESET default_transaction_read_only', v_db));
        UPDATE isomer.attachments SET rollback_conninfo = cutover.rollback_conninfo, rollback_status = 'pending'
        WHERE attachments.name = a.name;
        step := 'rollback replication';
        detail := format('publication %s created; the source subscribes to it within 10 seconds (isomer.attachment_status shows it)', v_pub);
        RETURN NEXT;
    END IF;

    UPDATE isomer.attachments SET status = 'cutover', cutover_at = now() WHERE attachments.name = a.name;
    PERFORM isomer._log('info', a.name, 'cut over: IsomerDB is now the primary for the attached tables');
    step := 'done'; detail := 'IsomerDB is the primary. Point the application''s writes here; projections can now be sync (isomer.alter_projection).';
    RETURN NEXT;
END $$;

-- Health checks also finish rollback replication for cut-over attachments.
CREATE OR REPLACE FUNCTION isomer.check_attachments(p_max_retained_wal bigint DEFAULT 1024 * 1024 * 1024)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a  isomer.attachments;
    st record;
BEGIN
    FOR a IN SELECT * FROM isomer.attachments WHERE status = 'cutover' AND rollback_status = 'pending' LOOP
        BEGIN
            PERFORM public.dblink_exec(a.conninfo, format(
                'CREATE SUBSCRIPTION %I CONNECTION %L PUBLICATION %I WITH (copy_data = false)',
                'isomer_rollback_' || a.name, a.rollback_conninfo, 'isomer_rollback_' || a.name));
            UPDATE isomer.attachments SET rollback_status = 'active' WHERE name = a.name;
            PERFORM isomer._log('info', a.name, 'rollback replication active: the source subscribes to IsomerDB''s writes');
        EXCEPTION WHEN OTHERS THEN
            UPDATE isomer.attachments SET rollback_status = 'failed' WHERE name = a.name;
            PERFORM isomer._log('error', a.name, format(
                'could not subscribe the source for rollback (%s). Run on the source: CREATE SUBSCRIPTION %I CONNECTION %L PUBLICATION %I WITH (copy_data = false);',
                SQLERRM, 'isomer_rollback_' || a.name, a.rollback_conninfo, 'isomer_rollback_' || a.name));
        END;
    END LOOP;

    FOR a IN SELECT * FROM isomer.attachments WHERE status = 'active' LOOP
        BEGIN
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
            IF coalesce(st.apply_errors, 0) > a.last_apply_errors THEN
                PERFORM isomer._log('warning', a.name, format(
                    'the apply worker hit %s error(s) since the last check (a source schema change not yet applied here causes this until the next check applies it)',
                    st.apply_errors - a.last_apply_errors));
                UPDATE isomer.attachments SET last_apply_errors = st.apply_errors WHERE name = a.name;
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

-- ── The auditor (FR-K1/K2/K3) ───────────────────────────────────────────────

CREATE TABLE isomer.audit_stats (
    projection text PRIMARY KEY REFERENCES isomer.projections (name) ON DELETE CASCADE,
    checks     bigint NOT NULL DEFAULT 0,
    drift      bigint NOT NULL DEFAULT 0,
    last_run   timestamptz,
    since      timestamptz NOT NULL DEFAULT now()
);

-- A mismatch is only drift if it persists: lag makes a bounded projection briefly differ from
-- its tables. A suspect is confirmed when the same expected value still mismatches on a later
-- run, at least twice the lag budget later (and at least 2 seconds, for sync projections).
CREATE TABLE isomer.audit_suspects (
    projection    text NOT NULL,
    key           text NOT NULL,
    expected_hash text,
    first_seen    timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection, key)
);

-- Confirmed drift, with the inputs pinned for reproduction.
CREATE TABLE isomer.drift_samples (
    id          bigserial PRIMARY KEY,
    projection  text NOT NULL,
    key         text NOT NULL,
    expected    jsonb,
    actual      jsonb,
    detected_at timestamptz NOT NULL DEFAULT now()
);

REVOKE ALL ON ALL TABLES IN SCHEMA isomer FROM PUBLIC;

-- MongoDB extended JSON (canonical) -> plain JSON values, so stored documents compare with
-- what the projection query produces.
CREATE FUNCTION isomer._ejson_relax(j jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog AS $$
DECLARE
    k text;
BEGIN
    CASE jsonb_typeof(j)
    WHEN 'object' THEN
        IF (SELECT count(*) FROM jsonb_object_keys(j)) = 1 THEN
            k := (SELECT jsonb_object_keys(j) LIMIT 1);
            IF k IN ('$numberInt', '$numberLong', '$numberDouble', '$numberDecimal') THEN
                RETURN to_jsonb((j ->> k)::numeric);
            ELSIF k = '$date' THEN
                RETURN j -> k;
            END IF;
        END IF;
        RETURN coalesce((SELECT jsonb_object_agg(e.key, isomer._ejson_relax(e.value)) FROM jsonb_each(j) e), '{}'::jsonb);
    WHEN 'array' THEN
        RETURN coalesce((SELECT jsonb_agg(isomer._ejson_relax(e) ORDER BY o) FROM jsonb_array_elements(j) WITH ORDINALITY AS t(e, o)), '[]'::jsonb);
    ELSE
        RETURN j;
    END CASE;
END $$;

-- Numbers as stored: documents carry numbers as BSON doubles or integers, so compare both sides
-- at double precision (0.1 stored as a double prints back as 0.10000000000000000555).
CREATE FUNCTION isomer._num_canon(j jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog AS $$
BEGIN
    CASE jsonb_typeof(j)
    WHEN 'number' THEN
        RETURN to_jsonb((j::text)::float8);
    WHEN 'object' THEN
        RETURN coalesce((SELECT jsonb_object_agg(e.key, isomer._num_canon(e.value)) FROM jsonb_each(j) e), '{}'::jsonb);
    WHEN 'array' THEN
        RETURN coalesce((SELECT jsonb_agg(isomer._num_canon(e) ORDER BY o) FROM jsonb_array_elements(j) WITH ORDINALITY AS t(e, o)), '[]'::jsonb);
    ELSE
        RETURN j;
    END CASE;
END $$;

-- The table that stores a DocumentDB collection's documents. Read directly by the auditor:
-- documentdb_api.collection() only works at the top level of a query.
CREATE FUNCTION isomer._collection_table(p_database text, p_collection text)
RETURNS text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT format('documentdb_data.documents_%s', collection_id)
    FROM documentdb_api_catalog.collections WHERE database_name = p_database AND collection_name = p_collection
$$;

-- What the projection should hold for these keys, and what it holds. NULL means absent.
CREATE FUNCTION isomer._audit_pairs(p isomer.projections, p_keys text[])
RETURNS TABLE (key text, expected jsonb, actual jsonb)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF p.kind = 'document' THEN
        RETURN QUERY EXECUTE format(
            $q$SELECT k, isomer._num_canon(e.doc), isomer._num_canon(a.doc)
               FROM unnest($1::text[]) k
               LEFT JOIN (SELECT q.%1$I::text AS key, q.doc::jsonb || jsonb_build_object('_id', to_jsonb(q.%1$I)) AS doc
                          FROM (%2$s) q WHERE q.%1$I = ANY ($1::%3$s[])) e ON e.key = k
               LEFT JOIN (SELECT isomer._ejson_relax(documentdb_core.bson_to_json_string(c.document)::text::jsonb) AS doc
                          FROM %4$s c) a ON a.doc ->> '_id' = k$q$,
            p.key_column, p.query, p.key_type, isomer._collection_table(p.mongo_database, p.name))
        USING p_keys;
    ELSIF p.kind = 'cache' THEN
        RETURN QUERY EXECUTE format(
            $q$SELECT k, e.value, a.value
               FROM unnest($1::text[]) k
               LEFT JOIN (SELECT q.%1$I::text AS key, to_jsonb(q.value) AS value FROM (%2$s) q WHERE q.%1$I = ANY ($1::%3$s[])) e ON e.key = k
               LEFT JOIN %4$s a ON a.key = k$q$,
            p.key_column, p.query, p.key_type, isomer._cache_store(p.name))
        USING p_keys;
    ELSIF p.kind = 'vector' THEN
        -- The embedding itself can't be recomputed cheaply; the contract is that it was made from
        -- the current text, which its stored hash proves.
        RETURN QUERY EXECUTE format(
            $q$SELECT k, to_jsonb(e.hash), to_jsonb(a.source_hash)
               FROM unnest($1::text[]) k
               LEFT JOIN (SELECT q.%1$I::text AS key, md5(q.input::text) AS hash FROM (%2$s) q
                          WHERE q.%1$I = ANY ($1::%3$s[]) AND q.input IS NOT NULL) e ON e.key = k
               LEFT JOIN %4$s a ON a.key = k AND a.model = %5$L$q$,
            p.key_column, p.query, p.key_type, isomer._vector_store(p.name), p.model)
        USING p_keys;
    ELSE
        RETURN QUERY EXECUTE format(
            $q$SELECT k, e.row, a.row
               FROM unnest($1::text[]) k
               LEFT JOIN (SELECT q.%1$I::text AS key, to_jsonb(q) AS row FROM (%2$s) q WHERE q.%1$I = ANY ($1::%3$s[])) e ON e.key = k
               LEFT JOIN (SELECT t.%1$I::text AS key, to_jsonb(t) - '__pgt_row_id' AS row FROM %4$I.%5$I t) a ON a.key = k$q$,
            p.key_column, p.query, p.key_type, p.target_schema, p.name)
        USING p_keys;
    END IF;
END $$;

-- Sample keys from both sides (what the tables produce, and what the projection holds, so
-- orphans are found too), compare, and count confirmed drift. Scheduled every 30 seconds.
CREATE FUNCTION isomer.audit(p_sample integer DEFAULT 50)
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
    FOR p IN SELECT * FROM isomer.projections WHERE status = 'active' LOOP
        BEGIN
            -- Keys from the tables...
            EXECUTE format('SELECT array_agg(k) FROM (SELECT q.%I::text AS k FROM (%s) q ORDER BY random() LIMIT %s) s',
                           p.key_column, p.query, p_sample) INTO v_keys;
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

-- Repair drifted keys by re-deriving them (after a drift sample has been looked at).
CREATE FUNCTION isomer.repair(p_projection text, p_keys text[] DEFAULT NULL)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_keys text[] := coalesce(p_keys, ARRAY(SELECT DISTINCT key FROM isomer.drift_samples WHERE projection = p_projection));
BEGIN
    PERFORM isomer._apply(p_projection, v_keys);
    RETURN cardinality(v_keys);
END $$;

-- The public drift counter: "drift 0 rows · 4.1M checks · 30 days".
CREATE VIEW isomer.drift_report AS
SELECT p.name AS projection, p.kind, p.freshness, coalesce(a.drift, 0) AS drift, coalesce(a.checks, 0) AS checks,
       a.since, a.last_run,
       format('drift %s rows · %s checks · since %s', coalesce(a.drift, 0), coalesce(a.checks, 0),
              to_char(coalesce(a.since, p.created_at), 'YYYY-MM-DD HH24:MI')) AS summary
FROM isomer.projections p LEFT JOIN isomer.audit_stats a ON a.projection = p.name;

INSERT INTO isomer.audit_stats (projection) SELECT name FROM isomer.projections ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION isomer.dump_projections()
RETURNS SETOF text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT format('SELECT isomer.create_projection(name => %L, query => %L, kind => %L, key => %L, '
                  'freshness => %L, lag => %L, sources => %L, database => %L, index_columns => %L, target_schema => %L, model => %L);',
                  p.name, p.query, p.kind, p.key_column, p.freshness, p.lag_budget,
                  (SELECT jsonb_object_agg(s.source_table, s.key_column) FROM isomer.projection_sources s WHERE s.projection = p.name),
                  coalesce(p.mongo_database, 'isomer'), p.index_columns, coalesce(p.target_schema, 'public'), p.model)
    FROM isomer.projections p ORDER BY p.created_at
$$;

-- Alerts gain confirmed drift.
CREATE OR REPLACE VIEW isomer.alerts AS
SELECT 'lag_budget' AS alert, name AS subject,
       format('pending for %s (budget %s)', oldest_pending, lag_budget) AS detail
FROM isomer.projection_status
WHERE kind IN ('document', 'vector', 'cache') AND oldest_pending > lag_budget
UNION ALL
SELECT 'embed_lag', name, format('oldest embedding job waiting %s (budget %s)', oldest_embed_job, lag_budget)
FROM isomer.projection_status WHERE kind = 'vector' AND status = 'active' AND oldest_embed_job > lag_budget
UNION ALL
SELECT 'search_stale', name, format('stale for %s (budget %s)', search_staleness, lag_budget)
FROM isomer.projection_status WHERE kind = 'search' AND search_staleness > lag_budget * 3
UNION ALL
SELECT 'dead_letters', name, format('%s embeddings failed permanently', embed_dead_letters)
FROM isomer.projection_status WHERE embed_dead_letters > 0
UNION ALL
SELECT 'budget_paused', name, format('monthly cap $%s reached', monthly_cap_usd)
FROM isomer.projection_status WHERE status = 'paused_budget'
UNION ALL
SELECT 'drift', projection, summary FROM isomer.drift_report WHERE drift > 0
UNION ALL
SELECT 'attachment', attachment, format('%s: slot wal_status %s', status, wal_status)
FROM isomer.attachment_status()
WHERE status NOT IN ('active', 'cutover') OR (status = 'active' AND wal_status IS DISTINCT FROM 'reserved' AND wal_status IS DISTINCT FROM 'extended')
UNION ALL
SELECT level, subject, message FROM isomer.events
WHERE level IN ('error', 'warning') AND at > now() - interval '15 minutes';

-- The auditor runs every 30 seconds.
SELECT cron.schedule('isomer-audit', '30 seconds', 'SELECT isomer.audit()');
