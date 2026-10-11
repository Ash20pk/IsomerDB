-- IsomerDB 0.9.0: read-your-writes from plain SQL (PRD FR-R4).
--
--   * SET isomer.read_after = <token> makes every later read of a projection in the session (or
--     transaction, with SET LOCAL) wait until the projection reflects that token. The wait is done
--     by isomer_router's executor hook, which then reads with a fresh snapshot.
--   * isomer.get() and isomer.cache_get() honour it too: they are now VOLATILE, so the read after
--     the wait sees what was waited for.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.9.0'" to load this file. \quit

-- Every relation a projection is read through, for isomer_router to match against a query's
-- relations. Called by the router as the bootstrap superuser; not granted to applications.
CREATE FUNCTION isomer._projection_relations()
RETURNS TABLE (relid oid, projection text, mongo_database text, collection text)
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT r.relid, p.name, p.mongo_database, CASE WHEN p.kind = 'document' THEN p.name END
    FROM isomer.projections p
    CROSS JOIN LATERAL (VALUES
        (CASE WHEN p.target_schema IS NOT NULL THEN to_regclass(format('%I.%I', p.target_schema, p.name)) END),
        (to_regclass(p.native_view)),
        (CASE p.kind
             WHEN 'document' THEN to_regclass(isomer._collection_table(p.mongo_database, p.name))
             WHEN 'vector'   THEN to_regclass(isomer._vector_store(p.name))
             WHEN 'cache'    THEN to_regclass(isomer._cache_store(p.name))
         END)) AS r (relid)
    WHERE r.relid IS NOT NULL
    UNION
    -- Document projections are also read with documentdb_api.collection(db, name), which has no
    -- relation: the router matches the call's arguments instead.
    SELECT NULL, p.name, p.mongo_database, p.name FROM isomer.projections p WHERE p.kind = 'document'
$$;

-- Wait for isomer.read_after on one projection, if it is set. For functions that read
-- projections themselves; plain queries are handled by isomer_router.
CREATE FUNCTION isomer._honour_read_after(p_projection text)
RETURNS void LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    v_token text := current_setting('isomer.read_after', true);
BEGIN
    IF v_token IS NULL OR v_token = '' THEN
        RETURN;
    END IF;
    PERFORM isomer.wait_for(v_token::bigint, p_projection,
                            coalesce(nullif(current_setting('isomer.read_after_timeout', true), ''), '5s')::interval);
END $$;

CREATE OR REPLACE FUNCTION isomer.cache_get(p_projection text, p_key text)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    v jsonb;
BEGIN
    PERFORM isomer._honour_read_after(p_projection);
    EXECUTE format('SELECT value FROM %s WHERE key = $1', isomer._cache_store(p_projection)) INTO v USING p_key;
    RETURN v;
END $$;

CREATE OR REPLACE FUNCTION isomer.get(p_projection text, p_key text)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    p    isomer.projections;
    v_id jsonb;
    v    jsonb;
BEGIN
    SELECT * INTO p FROM isomer.projections WHERE name = p_projection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no projection named %', p_projection USING ERRCODE = 'IS004';
    END IF;
    IF p.kind = 'cache' THEN
        RETURN isomer.cache_get(p.name, p_key);
    ELSIF p.kind <> 'document' THEN
        RAISE EXCEPTION 'isomer: % is a % projection; look-up by key is for document and cache projections', p.name, p.kind
            USING ERRCODE = 'IS005';
    END IF;
    PERFORM isomer._honour_read_after(p.name);
    EXECUTE format('SELECT to_jsonb($1::%s)', p.key_type) INTO v_id USING p_key;
    EXECUTE format(
        $q$SELECT isomer._ejson_relax(documentdb_core.bson_to_json_string(c.document)::text::jsonb)
           FROM %s c
           WHERE c.shard_key_value = %s
             AND c.object_id OPERATOR(documentdb_core.=) jsonb_build_object('', $1)::text::documentdb_core.bson$q$,
        isomer._collection_table(p.mongo_database, p.name),
        (SELECT collection_id FROM documentdb_api_catalog.collections
         WHERE database_name = p.mongo_database AND collection_name = p.name))
    INTO v USING v_id;
    RETURN v;
END $$;

REVOKE EXECUTE ON FUNCTION isomer._projection_relations(), isomer._honour_read_after(text) FROM PUBLIC;
