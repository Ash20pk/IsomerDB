-- IsomerDB 0.5.0: native DDL for projections (PRD FR-S1a, §15.2).
--
--   CREATE MATERIALIZED VIEW shop.customer_profile USING isomer_bounded AS
--     SELECT c.id, jsonb_build_object(...) AS doc FROM shop.customers c ...;
--   COMMENT ON MATERIALIZED VIEW shop.customer_profile IS 'isomer {"lag": "2 seconds"}';  -- optional
--   CREATE UNIQUE INDEX ON shop.customer_profile (id);      -- registers the projection
--
-- The access method sets the freshness (isomer_sync / isomer_bounded); the unique index sets the
-- key; the view's schema is the MongoDB database; the comment can set lag, database and source
-- mappings. Mappings not given are inferred from the view's join equalities; a table that can't be
-- connected to the key is treated as a lookup table (any change re-derives every document), which
-- is always correct and only slower. Everything here is ordinary DDL that pg_dump reproduces:
-- restoring re-registers the projection. DROP MATERIALIZED VIEW drops it. The view itself keeps no
-- rows: the readable form is the MongoDB collection.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.5.0'" to load this file. \quit

-- Both store nothing themselves (heap, emptied after registration); they mark the freshness.
CREATE ACCESS METHOD isomer_sync TYPE TABLE HANDLER heap_tableam_handler;
CREATE ACCESS METHOD isomer_bounded TYPE TABLE HANDLER heap_tableam_handler;

-- The materialized view that declared a projection, if it was declared natively.
ALTER TABLE isomer.projections ADD COLUMN native_view text;

-- Options from the view's comment: 'isomer {"lag": "...", "database": "...", "sources": {...}}'.
CREATE FUNCTION isomer._native_options(p_view regclass)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    v_comment text := obj_description(p_view, 'pg_class');
BEGIN
    IF v_comment IS NULL OR v_comment !~* '^\s*isomer\s*\{' THEN
        RETURN '{}';
    END IF;
    RETURN regexp_replace(v_comment, '^\s*isomer\s*', '', 'i')::jsonb;
EXCEPTION WHEN invalid_text_representation THEN
    RAISE EXCEPTION 'isomer: the comment on % is not valid: expected isomer {...json...}', p_view;
END $$;

-- Infer which column of each table holds the key, from a deparsed query, conservatively:
--   * the key's origin is "alias.key" in the top-level select list, or an unqualified key when the
--     top level reads exactly one table (the deparser drops the alias there);
--   * the key is propagated through alias.a = alias.b equalities;
--   * tables that can't be connected map to null (lookup tables: always correct, only slower).
-- If the key's origin is ambiguous it returns NULL, and the caller asks for explicit mappings: a
-- wrong mapping would silently send writes to the wrong documents.
CREATE FUNCTION isomer._infer_sources(p_query text, p_key text)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    q         text := regexp_replace(p_query, '\s+', ' ', 'g');
    v_top     text;
    v_prev    text;
    v_alias   jsonb := '{}';
    v_top_alias jsonb := '{}';
    v_map     jsonb := '{}';
    m         text[];
    v_changed boolean := true;
    v_out     jsonb := '{}';
    a         text;
    v_from    text := '(?:from|join) ([a-z_][\w]*\.[a-z_][\w]*)(?: (?:as )?(?!on\y|where\y|join\y|left\y|right\y|inner\y|full\y|cross\y|group\y|order\y|limit\y)([a-z_]\w*))?';
BEGIN
    FOR m IN SELECT regexp_matches(q, v_from, 'gi') LOOP
        v_alias := v_alias || jsonb_build_object(coalesce(m[2], m[1]), m[1]);
    END LOOP;

    -- The top level: strip parenthesised groups (subqueries, function calls) until none remain.
    v_top := q;
    LOOP
        v_prev := v_top;
        v_top := regexp_replace(v_top, '\([^()]*\)', '', 'g');
        EXIT WHEN v_top = v_prev;
    END LOOP;
    FOR m IN SELECT regexp_matches(v_top, v_from, 'gi') LOOP
        v_top_alias := v_top_alias || jsonb_build_object(coalesce(m[2], m[1]), m[1]);
    END LOOP;

    m := regexp_match(v_top, format('^ ?select (?:distinct )?(?:.*, )?([a-z_]\w*)\.([a-z_]\w*)(?: as %s)?(?:,| from )', p_key), 'i');
    IF m IS NOT NULL AND (m[2] = p_key OR v_top ~* format('%s\.%s as %s(?:,| from )', m[1], m[2], p_key)) THEN
        v_map := jsonb_build_object(m[1], m[2]);
    ELSIF v_top ~* format('^ ?select (?:distinct )?(?:.*, )?%s(?:,| from )', p_key)
          AND (SELECT count(*) FROM jsonb_object_keys(v_top_alias)) = 1 THEN
        v_map := jsonb_build_object((SELECT k FROM jsonb_object_keys(v_top_alias) k), p_key);
    ELSE
        RETURN NULL;
    END IF;

    WHILE v_changed LOOP
        v_changed := false;
        FOR m IN SELECT regexp_matches(q, '([a-z_]\w*)\.([a-z_]\w*) = ([a-z_]\w*)\.([a-z_]\w*)', 'gi') LOOP
            IF v_map ->> m[1] = m[2] AND NOT v_map ? m[3] THEN
                v_map := v_map || jsonb_build_object(m[3], m[4]); v_changed := true;
            ELSIF v_map ->> m[3] = m[4] AND NOT v_map ? m[1] THEN
                v_map := v_map || jsonb_build_object(m[1], m[2]); v_changed := true;
            END IF;
        END LOOP;
    END LOOP;
    FOR a IN SELECT jsonb_object_keys(v_alias) LOOP
        v_out := v_out || jsonb_build_object(v_alias ->> a, v_map -> a);   -- null when unconnected
    END LOOP;
    RETURN v_out;
END $$;

-- Register (or re-register, after a restore) the projection a native matview declares.
CREATE FUNCTION isomer._native_register(p_view regclass, p_key text)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_ddl_propagation = off AS $$
DECLARE
    v_name      text;
    v_schema    text;
    v_am        text;
    v_query     text;
    v_opts      jsonb := isomer._native_options(p_view);
    v_sources   jsonb;
    v_inferred  jsonb;
    v_existing  isomer.projections;
    v_res       text;
    v_lookups   text;
BEGIN
    SELECT c.relname, n.nspname, a.amname INTO v_name, v_schema, v_am
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_am a ON a.oid = c.relam
    WHERE c.oid = p_view;
    IF coalesce(v_opts ->> 'kind', 'document') <> 'document' THEN
        RAISE EXCEPTION 'isomer: native syntax declares document projections; use isomer.create_projection() for % projections', v_opts ->> 'kind';
    END IF;
    v_query := isomer._viewdef(p_view);

    SELECT * INTO v_existing FROM isomer.projections WHERE name = v_name;
    IF FOUND THEN
        IF v_existing.query = v_query AND v_existing.key_column = p_key THEN
            v_res := isomer._install(v_name);       -- a restore: the catalog came back with the dump
            EXECUTE format('REFRESH MATERIALIZED VIEW %s WITH NO DATA', p_view);
            RETURN v_res;
        END IF;
        RAISE EXCEPTION 'isomer: a projection named % already exists with a different definition', v_name;
    END IF;

    v_inferred := isomer._infer_sources(v_query, p_key);
    IF v_inferred IS NULL THEN
        RAISE EXCEPTION 'isomer: could not tell which table the key % of % comes from', p_key, p_view
            USING HINT = 'Select it as alias.column, or give the mappings: COMMENT ON MATERIALIZED VIEW ... IS ''isomer {"sources": {"schema.table": "column"}}''';
    END IF;
    v_sources := v_inferred || coalesce(v_opts -> 'sources', '{}');
    SELECT string_agg(k, ', ') INTO v_lookups FROM jsonb_each(v_sources) e(k, v) WHERE jsonb_typeof(v) = 'null';

    v_res := isomer.create_projection(
        name      => v_name,
        query     => v_query,
        kind      => 'document',
        key       => p_key,
        freshness => CASE v_am WHEN 'isomer_sync' THEN 'sync' ELSE 'bounded' END,
        lag       => coalesce((v_opts ->> 'lag')::interval, '2 seconds'),
        sources   => v_sources,
        database  => coalesce(v_opts ->> 'database', v_schema));
    UPDATE isomer.projections SET native_view = format('%I.%I', v_schema, v_name) WHERE name = v_name;
    EXECUTE format('REFRESH MATERIALIZED VIEW %s WITH NO DATA', p_view);
    IF v_lookups IS NOT NULL THEN
        RAISE NOTICE 'isomer: % treated as lookup table(s): any change to them re-derives every document. To map them, COMMENT ON MATERIALIZED VIEW % IS ''isomer {"sources": {"<table>": "<column holding the key>"}}''', v_lookups, p_view;
    END IF;
    RETURN v_res;
END $$;

CREATE FUNCTION isomer._native_ddl()
RETURNS event_trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    r       record;
    v_view  regclass;
    v_key   text;
    v_am    text;
    v_opts  jsonb;
    v_name  text;
BEGIN
    IF to_regclass('isomer.projections') IS NULL THEN
        RETURN;
    END IF;
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF r.command_tag = 'CREATE INDEX' THEN
            SELECT i.indrelid::regclass, a.amname,
                   CASE WHEN i.indnatts = 1 AND i.indisunique THEN (SELECT attname FROM pg_attribute WHERE attrelid = i.indrelid AND attnum = i.indkey[0]) END
            INTO v_view, v_am, v_key
            FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid JOIN pg_am a ON a.oid = c.relam
            WHERE i.indexrelid = r.objid AND c.relkind = 'm';
            IF v_am IN ('isomer_sync', 'isomer_bounded') THEN
                IF v_key IS NULL THEN
                    RAISE EXCEPTION 'isomer: declare the key of % with a unique index on one column', v_view;
                END IF;
                RAISE NOTICE '%', isomer._native_register(v_view, v_key);
            END IF;
        ELSIF r.command_tag IN ('COMMENT', 'ALTER MATERIALIZED VIEW') AND r.object_type = 'materialized view' THEN
            v_view := r.objid::regclass;
            SELECT c.relname, a.amname INTO v_name, v_am FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.oid = v_view;
            CONTINUE WHEN v_am NOT IN ('isomer_sync', 'isomer_bounded')
                       OR NOT EXISTS (SELECT 1 FROM isomer.projections WHERE name = v_name);
            v_opts := isomer._native_options(v_view);
            -- ALTER ... SET ACCESS METHOD switches freshness; a comment can change the lag budget.
            PERFORM isomer.alter_projection(v_name,
                freshness => CASE v_am WHEN 'isomer_sync' THEN 'sync' ELSE 'bounded' END,
                lag       => (v_opts ->> 'lag')::interval);
            IF r.command_tag = 'ALTER MATERIALIZED VIEW' THEN
                EXECUTE format('REFRESH MATERIALIZED VIEW %s WITH NO DATA', v_view);
            END IF;
        END IF;
    END LOOP;
END $$;

CREATE EVENT TRIGGER isomer_native_ddl ON ddl_command_end
    WHEN TAG IN ('CREATE INDEX', 'COMMENT', 'ALTER MATERIALIZED VIEW')
    EXECUTE FUNCTION isomer._native_ddl();

-- DROP MATERIALIZED VIEW of a native projection drops the projection.
CREATE FUNCTION isomer._native_drop()
RETURNS event_trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    d record;
BEGIN
    IF to_regclass('isomer.projections') IS NULL THEN
        RETURN;
    END IF;
    FOR d IN SELECT * FROM pg_event_trigger_dropped_objects() WHERE object_type = 'materialized view' AND original LOOP
        IF EXISTS (SELECT 1 FROM isomer.projections WHERE native_view = format('%I.%I', d.schema_name, d.object_name)) THEN
            PERFORM isomer.drop_projection(d.object_name);
        END IF;
    END LOOP;
END $$;

CREATE EVENT TRIGGER isomer_native_drop ON sql_drop
    WHEN TAG IN ('DROP MATERIALIZED VIEW', 'DROP SCHEMA')
    EXECUTE FUNCTION isomer._native_drop();

REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA isomer FROM PUBLIC;
GRANT EXECUTE ON FUNCTION isomer.wait_for(bigint, text, interval), isomer.freshness(text),
                          isomer.projection_for_collection(text, text), isomer.current_token(),
                          isomer.last_write_token(), isomer.token_to_mongo_timestamp(bigint),
                          isomer.cache_get(text, text), isomer.embed(text, text, interval),
                          isomer.embed_for(text, text)
    TO PUBLIC;
