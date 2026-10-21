-- IsomerDB 0.25.0: projections of queries that read the clock are refused.
--
--   * A projection holds what its query returned when its rows were last written. A query whose
--     result changes with no table changing can't be kept that way, by any strategy: one that
--     reads the clock (now(), current_date, age(t)) or calls a volatile function (random(),
--     clock_timestamp()). A condition on now() was folded once, when the row was written, and
--     never again; and the auditor, running the query later, saw drift on every pass and repaired
--     it forever. create_projection and create_analytics_projection now refuse such a query,
--     naming the functions. Event projections are unchanged: their rules run once per change,
--     so now() there is the time of the event. Projections created before keep running.
--   * alter_projection(strategy => ...) on an analytics projection builds it again with that
--     strategy ('rebuild', 'incremental' or 'auto'), as create_analytics_projection does, in one
--     transaction. Its state still stays on the coordinator.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.25.0'" to load this file. \quit

-- The functions in a query whose result changes with no table changing (see isomer_engine.c).
CREATE FUNCTION isomer._engine_clock(query text)
RETURNS text[] LANGUAGE C STRICT AS 'isomer_router', 'isomer_engine_clock';

CREATE FUNCTION isomer._refuse_clock(p_name text, p_query text)
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_fns text[] := isomer._engine_clock(p_query);
BEGIN
    IF cardinality(v_fns) > 0 THEN
        RAISE EXCEPTION 'isomer: projection % reads %, whose result changes with no table changing, so it can''t be kept up to date',
            p_name, (SELECT string_agg(f || '()', ', ') FROM unnest(v_fns) f)
            USING HINT = 'Leave the time out of the projection and apply it when reading (WHERE created_at > now() - interval ''1 day''), or store the time in a column when the row is written.';
    END IF;
END $$;

-- create_projection (see 0.21.0), refusing a query that reads the clock.
CREATE OR REPLACE FUNCTION isomer.create_projection(
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
    model         text     DEFAULT NULL,
    strategy      text     DEFAULT 'auto',
    state         text     DEFAULT 'coordinator')
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
    v_strategy  text := 'recompute';
    v_why       text;
    v_shape     jsonb;
    v_result    text;
    v_diff      bigint;
BEGIN
    IF kind NOT IN ('document', 'search', 'vector', 'cache') THEN
        RAISE EXCEPTION 'isomer: kind must be ''document'', ''search'', ''vector'' or ''cache'', not %', kind;
    END IF;
    IF freshness NOT IN ('sync', 'bounded') THEN
        RAISE EXCEPTION 'isomer: freshness must be ''sync'' or ''bounded'', not %', freshness;
    END IF;
    IF strategy NOT IN ('auto', 'recompute', 'incremental') THEN
        RAISE EXCEPTION 'isomer: strategy must be ''auto'', ''recompute'' or ''incremental'', not %', strategy;
    END IF;
    IF state NOT IN ('coordinator', 'distributed') THEN
        RAISE EXCEPTION 'isomer: state must be ''coordinator'' or ''distributed'', not %', state;
    END IF;
    -- A distributed state is created, built and read again in this transaction: Citus must use
    -- one connection per node from the first statement on.
    IF state = 'distributed' THEN
        PERFORM set_config('citus.multi_shard_modify_mode', 'sequential', true);
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
    PERFORM isomer._refuse_clock(name, query);
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

    -- How it is maintained (FR-S7).
    IF kind IN ('document', 'cache') THEN
        IF create_projection.strategy = 'incremental' THEN
            IF freshness = 'sync' THEN
                v_reason := isomer._sync_incremental_blocker(v_tables);
                IF v_reason IS NOT NULL THEN
                    RAISE EXCEPTION 'isomer: projection % cannot be sync and incremental: %', name, v_reason;
                END IF;
            END IF;
            v_shape := isomer._engine_shape(query, key, kind);
            IF NOT (v_shape ->> 'ok')::boolean THEN
                RAISE EXCEPTION 'isomer: projection % cannot be maintained incrementally: %', name, v_shape ->> 'reason'
                    USING HINT = 'Use strategy => ''recompute'' with sources.';
            END IF;
            v_strategy := 'incremental';
            v_why := 'chosen by create_projection(strategy => ''incremental'')';
        ELSIF create_projection.strategy = 'auto' THEN
            SELECT c.strategy, c.reason INTO v_strategy, v_why
            FROM isomer.choose_strategy(query, key, kind, freshness, sources) c;
        ELSE
            v_why := 'chosen by create_projection(strategy => ''recompute'')';
        END IF;
    END IF;

    IF state = 'distributed' THEN
        IF v_strategy <> 'incremental' THEN
            RAISE EXCEPTION 'isomer: projection % is maintained by key, which keeps no state to distribute (%)', name, v_why
                USING HINT = 'Pass strategy => ''incremental''.';
        END IF;
        IF freshness = 'sync' THEN
            RAISE EXCEPTION 'isomer: projection % is sync: it folds in the writing transaction, on its tables'' node', name;
        END IF;
        v_reason := isomer._distribute_blocker(isomer._engine_shape(query, key, kind));
        IF v_reason IS NOT NULL THEN
            RAISE EXCEPTION 'isomer: the state of projection % can''t be distributed: %', name, v_reason;
        END IF;
        v_reason := NULL;
    END IF;
    INSERT INTO isomer.projections (name, kind, query, key_column, key_type, freshness, lag_budget,
                                    mongo_database, target_schema, index_columns, model, strategy, strategy_reason, state_placement)
    VALUES (name, kind, query, key, v_key_type, freshness, lag,
            CASE WHEN kind = 'document' THEN database END,
            CASE WHEN kind IN ('search', 'vector', 'cache') THEN target_schema END,
            CASE WHEN kind = 'search' THEN key || coalesce(index_columns, v_index) END,
            CASE WHEN kind = 'vector' THEN model END,
            v_strategy, v_why, state);

    IF kind IN ('document', 'vector', 'cache') THEN
        IF v_strategy = 'incremental' THEN
            -- Every table read is a source; mappings, if given, are kept for reference only.
            INSERT INTO isomer.projection_sources (projection, source_table, key_column)
            SELECT name, isomer._qualified(t), sources ->> isomer._qualified(t) FROM unnest(v_tables) t;
        ELSE
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
        END IF;
        v_reason := CASE WHEN v_strategy = 'incremental' AND freshness = 'sync'
                         THEN isomer._sync_incremental_blocker(v_tables)
                         ELSE isomer._check_sources(name, freshness) END;
        IF v_reason IS NOT NULL THEN
            RAISE EXCEPTION 'isomer: projection % cannot be %: %', name, freshness, v_reason;
        END IF;
    END IF;

    v_result := isomer._install(name);
    IF v_strategy = 'incremental' THEN
        -- The derivation from state must give what the query gives from the tables.
        v_diff := isomer._engine_differences(name);
        IF v_diff > 0 THEN
            RAISE EXCEPTION 'isomer: projection %: maintained incrementally, % row(s) differ from the query', name, v_diff
                USING HINT = 'Please report this. Create it with strategy => ''recompute'' meanwhile.';
        END IF;
        v_result := v_result || ', maintained incrementally' || CASE WHEN state = 'distributed' THEN ', its state distributed across the cluster' ELSE '' END;
    END IF;
    RETURN v_result;
END $$;

-- create_analytics_projection (see 0.23.0), refusing a query that reads the clock.
CREATE OR REPLACE FUNCTION isomer.create_analytics_projection(
    name          text,
    query         text,
    refresh       interval DEFAULT '5 minutes',
    target_schema text DEFAULT 'public',
    strategy      text DEFAULT 'auto')
-- The query resolves with the caller's search path, then is stored schema-qualified.
RETURNS text LANGUAGE plpgsql SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_probe    text := format('pg_temp.isomer_create_%s', pg_backend_pid());
    v_first    text;
    v_result   text;
    v_keys     text[];
    v_reason   text;
    v_shape    jsonb;
    v_strategy text := 'recompute';
    v_why      text;
    v_cols     jsonb;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_am WHERE amname = 'columnar') THEN
        RAISE EXCEPTION 'isomer: analytics projections need columnar storage'
            USING HINT = 'CREATE EXTENSION citus_columnar; (new clusters have it).';
    END IF;
    IF strategy NOT IN ('auto', 'rebuild', 'incremental') THEN
        RAISE EXCEPTION 'isomer: strategy must be ''auto'', ''rebuild'' or ''incremental'', not %', strategy;
    END IF;
    EXECUTE format('CREATE OR REPLACE TEMP VIEW %s AS %s', v_probe, query);
    query := isomer._viewdef(v_probe::regclass);
    PERFORM isomer._refuse_clock(name, query);
    EXECUTE format('DROP VIEW %s', v_probe);
    SELECT column_name INTO v_first FROM isomer._query_columns(query) LIMIT 1;

    SELECT k.keys, k.reason INTO v_keys, v_reason FROM isomer._analytics_keys(query) k;
    IF v_reason IS NULL THEN
        v_shape := isomer._engine_shape(query, coalesce(v_keys[1], v_first), 'analytics');
        IF NOT (v_shape ->> 'ok')::boolean THEN
            v_reason := v_shape ->> 'reason';
        END IF;
    END IF;
    IF create_analytics_projection.strategy = 'incremental' AND v_reason IS NOT NULL THEN
        RAISE EXCEPTION 'isomer: analytics projection % cannot be maintained incrementally: %', name, v_reason
            USING HINT = 'Use strategy => ''rebuild''.';
    END IF;
    IF create_analytics_projection.strategy = 'incremental' THEN
        v_strategy := 'incremental';
        v_why := 'chosen by create_analytics_projection(strategy => ''incremental'')';
    ELSIF create_analytics_projection.strategy = 'auto' AND v_reason IS NULL THEN
        IF EXISTS (SELECT 1 FROM isomer._engine_shape_aggs(v_shape) g WHERE g ->> 'cls' = 'keep') THEN
            v_why := format('rebuilt as a columnar table when its sources change, at most every %s: maintained incrementally it would keep a copy of every row its %s read',
                            refresh, (SELECT string_agg(DISTINCT g ->> 'fn', ', ') FROM isomer._engine_shape_aggs(v_shape) g WHERE g ->> 'cls' = 'keep'));
        ELSE
            v_strategy := 'incremental';
            v_why := format('each change is folded into partial aggregates and only the rows it reaches are rewritten, merged into a new columnar table at most every %s', refresh);
        END IF;
    ELSE
        v_why := format('rebuilt as a columnar table when its sources change, at most every %s', refresh)
              || CASE WHEN create_analytics_projection.strategy = 'auto' THEN format(' (incremental maintenance does not apply: %s)', v_reason) ELSE '' END;
    END IF;

    INSERT INTO isomer.projections (name, kind, query, key_column, key_type, freshness, lag_budget, target_schema, strategy, strategy_reason, definition)
    VALUES (name, 'analytics', query, coalesce(v_keys[1], v_first), 'text', 'bounded',
            CASE WHEN v_strategy = 'incremental' THEN interval '2 seconds' ELSE refresh END, target_schema, v_strategy, v_why,
            jsonb_build_object('refresh', refresh, 'strategy', create_analytics_projection.strategy));
    INSERT INTO isomer.projection_sources (projection, source_table, key_column)
    SELECT name, isomer._qualified(t), NULL FROM isomer._query_tables(query) t;
    INSERT INTO isomer.projection_stats (projection) VALUES (name) ON CONFLICT DO NOTHING;
    INSERT INTO isomer.analytics_builds (projection, key_columns, refresh)
    VALUES (name, CASE WHEN v_strategy = 'incremental' THEN v_keys END, CASE WHEN v_strategy = 'incremental' THEN refresh END);

    IF v_strategy = 'incremental' THEN
        PERFORM isomer._engine_compile(name);
        SELECT jsonb_agg(column_name ORDER BY n) INTO v_cols FROM isomer._query_columns(query) WITH ORDINALITY q(column_name, column_type, type_oid, n);
        UPDATE isomer.engine_plans e
        SET plan = e.plan || jsonb_build_object(
            'key_columns', to_jsonb(v_keys),
            'key_types', (SELECT coalesce(jsonb_agg(c.column_type ORDER BY k.n), '[]') FROM unnest(v_keys) WITH ORDINALITY k(name, n)
                          JOIN isomer._query_columns(query) c ON c.column_name = k.name),
            'key_expr', format('jsonb_build_array(%s)::text', (SELECT coalesce(string_agg(format('q.%I', k), ', '), '') FROM unnest(v_keys) k)),
            'columns', v_cols)
        WHERE e.projection = name;
        EXECUTE format('CREATE TABLE %s AS SELECT NULL::text AS __k, false AS __deleted, q.* FROM (%s) q WITH NO DATA',
                       isomer._analytics_delta(name), query);
        EXECUTE format('ALTER TABLE %1$s ALTER COLUMN __k SET NOT NULL, ALTER COLUMN __deleted SET NOT NULL, ADD PRIMARY KEY (__k)',
                       isomer._analytics_delta(name));
        -- The capture triggers lock every shard against writes until this transaction commits,
        -- so the state, the base table and the deltas that follow meet without a gap.
        PERFORM isomer._create_source_triggers(name);
        v_result := isomer._analytics_rebuild(name, true);
    ELSE
        v_result := isomer.refresh_analytics(name, force => true);
    END IF;
    RETURN format('analytics projection %s: %s; read it as %I.%I', name, v_result, target_schema, name);
END $$;

-- alter_projection (see 0.24.0): a new strategy for an analytics projection builds it again.
CREATE OR REPLACE FUNCTION isomer.alter_projection(name text, freshness text DEFAULT NULL, lag interval DEFAULT NULL,
                                        strategy text DEFAULT NULL, sources jsonb DEFAULT NULL, state text DEFAULT NULL)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p        isomer.projections;
    v_reason text;
    v_shape  jsonb;
    v_src    record;
BEGIN
    -- NO KEY UPDATE: the auditor's foreign-key checks (KEY SHARE on this row) must not wait on us,
    -- or, holding a lock on a source table we need for the triggers, deadlock with us.
    SELECT * INTO p FROM isomer.projections WHERE projections.name = alter_projection.name FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', name;
    END IF;
    -- An analytics projection is a columnar table, not documents: its state stays on the
    -- coordinator, and a new strategy builds it again as create_analytics_projection would, in
    -- this transaction (readers see the old table until it commits).
    IF p.kind = 'analytics' THEN
        IF state IS NOT NULL AND state <> p.state_placement THEN
            RAISE EXCEPTION 'isomer: % is an analytics projection, and its state stays on the coordinator', p.name;
        END IF;
        IF strategy IS NOT NULL THEN
            IF strategy NOT IN ('auto', 'rebuild', 'recompute', 'incremental') THEN
                RAISE EXCEPTION 'isomer: strategy must be ''auto'', ''rebuild'' or ''incremental'', not %', strategy;
            END IF;
            PERFORM isomer.drop_projection(p.name);
            RETURN isomer.create_analytics_projection(p.name, p.query, coalesce((p.definition ->> 'refresh')::interval, interval '5 minutes'),
                                                      p.target_schema, CASE strategy WHEN 'recompute' THEN 'rebuild' ELSE strategy END);
        END IF;
    END IF;
    -- A distributed state is dropped, created or built and read again in this transaction:
    -- Citus must use one connection per node from the first statement on.
    IF p.state_placement = 'distributed' OR alter_projection.state = 'distributed' THEN
        PERFORM set_config('citus.multi_shard_modify_mode', 'sequential', true);
    END IF;
    IF strategy IS NOT NULL AND strategy <> p.strategy THEN
        IF strategy NOT IN ('recompute', 'incremental') THEN
            RAISE EXCEPTION 'isomer: strategy must be ''recompute'' or ''incremental'', not %', strategy;
        END IF;
        IF strategy = 'incremental' THEN
            IF coalesce(freshness, p.freshness) = 'sync' THEN
                v_reason := isomer._sync_incremental_blocker(ARRAY(SELECT ps.source_table::regclass FROM isomer.projection_sources ps WHERE ps.projection = p.name));
                IF v_reason IS NOT NULL THEN
                    RAISE EXCEPTION 'isomer: projection % is sync and can''t be incremental: %', p.name, v_reason;
                END IF;
            END IF;
            v_shape := isomer._engine_shape(p.query, p.key_column, p.kind);
            IF NOT (v_shape ->> 'ok')::boolean THEN
                RAISE EXCEPTION 'isomer: projection % cannot be maintained incrementally: %', p.name, v_shape ->> 'reason';
            END IF;
        ELSE
            IF sources IS NOT NULL THEN
                FOR v_src IN SELECT k, v FROM jsonb_each_text(sources) AS e(k, v) LOOP
                    IF v_src.v IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM pg_attribute WHERE attrelid = v_src.k::regclass AND attname = v_src.v AND NOT attisdropped) THEN
                        RAISE EXCEPTION 'isomer: % has no column %', v_src.k, v_src.v;
                    END IF;
                    UPDATE isomer.projection_sources ps SET key_column = v_src.v
                    WHERE ps.projection = p.name AND ps.source_table::regclass = v_src.k::regclass;
                END LOOP;
            END IF;
            -- A sharded table mapped to nothing would re-derive every key from inside every shard's
            -- trigger on every write.
            SELECT string_agg(ps.source_table, ', ') INTO v_reason FROM isomer.projection_sources ps
            WHERE ps.projection = p.name AND ps.key_column IS NULL
              AND (isomer._distribution(ps.source_table::regclass)).method = 'h';
            IF v_reason IS NOT NULL THEN
                RAISE EXCEPTION 'isomer: % cannot be re-derived by key: % % not mapped to the key', p.name, v_reason,
                    CASE WHEN v_reason LIKE '%,%' THEN 'are' ELSE 'is' END
                    USING HINT = 'Pass sources => ''{"table": "column holding the key"}''.';
            END IF;
            v_reason := NULL;
        END IF;
        PERFORM isomer._drop_source_triggers(p.name);
        IF p.strategy = 'incremental' THEN
            PERFORM isomer._engine_drop_state(p.name);
        END IF;
        UPDATE isomer.projections
        SET strategy = alter_projection.strategy, derive_query = NULL,
            strategy_reason = format('set by alter_projection(strategy => %L)', alter_projection.strategy)
        WHERE projections.name = p.name;
        DELETE FROM isomer.deltas WHERE projection = p.name;
        IF strategy = 'incremental' THEN
            PERFORM isomer._engine_compile(p.name);
            PERFORM isomer._create_source_triggers(p.name);
            PERFORM isomer._engine_build(p.name);
        ELSE
            PERFORM isomer._create_source_triggers(p.name);
        END IF;
        PERFORM isomer._rebuild_all(p.name);
        DELETE FROM isomer.pending WHERE projection = p.name;
        SELECT * INTO p FROM isomer.projections WHERE projections.name = alter_projection.name;
    END IF;
    IF freshness IS NOT NULL AND freshness <> p.freshness THEN
        IF p.kind NOT IN ('document', 'cache') THEN
            RAISE EXCEPTION 'isomer: only document and cache projections can change freshness (this is a % projection)', p.kind;
        END IF;
        v_reason := CASE WHEN freshness = 'sync' AND p.strategy = 'incremental'
                         THEN isomer._sync_incremental_blocker(ARRAY(SELECT ps.source_table::regclass FROM isomer.projection_sources ps WHERE ps.projection = p.name))
                         ELSE isomer._check_sources(p.name, freshness) END;
        IF v_reason IS NOT NULL THEN
            RAISE EXCEPTION 'isomer: projection % cannot be %: %', p.name, freshness, v_reason;
        END IF;
        UPDATE isomer.projections SET freshness = alter_projection.freshness WHERE projections.name = p.name;
        PERFORM isomer._create_source_triggers(p.name);
        -- Anything still queued is applied now, so the projection is current as it changes mode.
        IF p.strategy = 'incremental' THEN
            PERFORM isomer._sync_fold(p.name);
        END IF;
        PERFORM isomer._apply(p.name, ARRAY(SELECT key FROM isomer.pending WHERE projection = p.name));
        DELETE FROM isomer.pending WHERE projection = p.name;
    END IF;
    IF state IS NOT NULL AND state <> p.state_placement THEN
        IF state NOT IN ('coordinator', 'distributed') THEN
            RAISE EXCEPTION 'isomer: state must be ''coordinator'' or ''distributed'', not %', state;
        END IF;
        IF p.strategy <> 'incremental' THEN
            RAISE EXCEPTION 'isomer: projection % is maintained by key, which keeps no state to distribute', p.name
                USING HINT = 'Switch it to strategy => ''incremental'' first.';
        END IF;
        IF state = 'distributed' THEN
            IF p.freshness = 'sync' THEN
                RAISE EXCEPTION 'isomer: projection % is sync: it folds in the writing transaction, on its tables'' node', p.name;
            END IF;
            v_reason := isomer._distribute_blocker(isomer._engine_shape(p.query, p.key_column, p.kind));
            IF v_reason IS NOT NULL THEN
                RAISE EXCEPTION 'isomer: the state of projection % can''t be distributed: %', p.name, v_reason;
            END IF;
        END IF;
        -- Built again where it now lives. Dropping the capture triggers locks every shard against
        -- writes until commit, so the deltas still queued (here and on the workers) are all in the
        -- tables the new state is built from.
        PERFORM isomer._drop_source_triggers(p.name);
        PERFORM isomer._engine_drop_state(p.name);
        UPDATE isomer.projections SET state_placement = alter_projection.state WHERE projections.name = p.name;
        DELETE FROM isomer.deltas WHERE projection = p.name;
        IF isomer._multi_node() THEN
            PERFORM run_command_on_workers(format('DELETE FROM isomer.node_deltas WHERE projection = %L', p.name));
        END IF;
        PERFORM isomer._engine_compile(p.name);
        PERFORM isomer._create_source_triggers(p.name);
        PERFORM isomer._engine_build(p.name);
        PERFORM isomer._rebuild_all(p.name);
        DELETE FROM isomer.pending WHERE projection = p.name;
    END IF;
    IF lag IS NOT NULL THEN
        UPDATE isomer.projections SET lag_budget = lag WHERE projections.name = p.name;
    END IF;
    SELECT * INTO p FROM isomer.projections WHERE projections.name = alter_projection.name;
    RETURN format('%s: freshness %s, lag budget %s, strategy %s%s', p.name, p.freshness, p.lag_budget, p.strategy,
                  CASE WHEN p.strategy = 'incremental' THEN ', state on the ' || CASE p.state_placement WHEN 'distributed' THEN 'cluster' ELSE 'coordinator' END ELSE '' END);
END $$;
