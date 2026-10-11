-- IsomerDB 0.7.0: query routing (PRD FR-M4).
--
-- isomer.route_proposal() turns a proposal into a sync cache projection holding, per key, the rows
-- the original keyed read returns, plus a replacement query that reads them back with the original
-- column names and types. It enables the route only when the replacement returns the same rows as
-- the original on sample keys and is measurably faster. The isomer_router planner hook (a separate
-- C library, preloaded after Citus) then plans the replacement in place of the original statement,
-- matched by query id: applications change nothing.
--
-- Only sync projections are routed, so a routed read sees exactly what the tables hold.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.7.0'" to load this file. \quit

CREATE SCHEMA isomer_routes;
REVOKE ALL ON SCHEMA isomer_routes FROM PUBLIC;

CREATE TABLE isomer.routes (
    queryid          bigint PRIMARY KEY,
    proposal         bigint REFERENCES isomer.proposals (id) ON DELETE SET NULL,
    projection       text NOT NULL REFERENCES isomer.projections (name) ON DELETE CASCADE,
    source_query     text NOT NULL,
    replacement_sql  text NOT NULL,
    param_types      regtype[] NOT NULL,
    measured_speedup double precision,
    verified_keys    integer NOT NULL,
    enabled          boolean NOT NULL DEFAULT true,
    created_at       timestamptz NOT NULL DEFAULT now()
);
SELECT pg_catalog.pg_extension_config_dump('isomer.routes', '');
REVOKE ALL ON isomer.routes FROM PUBLIC;

-- The enabled routes, for the router: SECURITY DEFINER so it can load them as any role. (The
-- router checks the caller's rights on the original query's tables before routing.)
CREATE FUNCTION isomer._routes(OUT queryid bigint, OUT replacement_sql text, OUT param_types oid[], OUT measured_speedup double precision)
RETURNS SETOF record LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT queryid, replacement_sql, param_types::oid[], measured_speedup FROM isomer.routes WHERE enabled
$$;
GRANT EXECUTE ON FUNCTION isomer._routes() TO PUBLIC;

-- What this backend's router has done (the isomer_router library must be preloaded).
CREATE FUNCTION isomer.router_stats(OUT routed bigint, OUT shadow_matches bigint, OUT routes_loaded bigint)
LANGUAGE C STRICT AS 'isomer_router', 'isomer_router_stats';

-- Run a parameterised query with $1 bound to a literal, returning its rows as one sorted jsonb array
-- (a multiset, so row order doesn't matter).
CREATE FUNCTION isomer._rows_for(p_query text, p_param_type regtype, p_key text)
RETURNS jsonb LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v jsonb;
BEGIN
    EXECUTE format('SELECT coalesce(jsonb_agg(r ORDER BY r::text), ''[]''::jsonb) FROM (SELECT to_jsonb(t) AS r FROM (%s) t) s',
                   replace(p_query, '$1', format('%L::%s', p_key, p_param_type)))
    INTO v;
    RETURN v;
END $$;

-- Average milliseconds to run a parameterised query for the given keys.
CREATE FUNCTION isomer._time_ms(p_query text, p_param_type regtype, p_keys text[], p_rounds integer DEFAULT 3)
RETURNS double precision LANGUAGE plpgsql SET search_path = pg_catalog SET isomer.routing = off AS $$
DECLARE
    v_start timestamptz;
    v_n     integer := 0;
    k       text;
    i       integer;
BEGIN
    v_start := clock_timestamp();
    FOR i IN 1..p_rounds LOOP
        FOREACH k IN ARRAY p_keys LOOP
            EXECUTE format('SELECT count(*) FROM (%s) t', replace(p_query, '$1', format('%L::%s', k, p_param_type)));
            v_n := v_n + 1;
        END LOOP;
    END LOOP;
    RETURN extract(epoch FROM clock_timestamp() - v_start) * 1000 / greatest(v_n, 1);
END $$;

CREATE FUNCTION isomer.route_proposal(p_id bigint, sample integer DEFAULT 20)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog SET isomer.routing = off AS $$
DECLARE
    pr          isomer.proposals;
    q           text;
    v_ptype     regtype;
    v_probe     text := format('pg_temp.isomer_route_probe_%s', pg_backend_pid());
    v_cols      record;
    v_defs      text;
    v_select    text;
    v_names     text[] := '{}';
    v_name      text;
    v_res       text;
    v_repl      text;
    v_keys      text[];
    k           text;
    v_t_orig    double precision;
    v_t_repl    double precision;
    v_speedup   double precision;
    v_min       double precision := coalesce(nullif(current_setting('isomer.routing_min_speedup', true), '')::float8, 1.2);
BEGIN
    SELECT * INTO STRICT pr FROM isomer.proposals WHERE id = p_id;
    q := regexp_replace(pr.source_query, '^\s*prepare\s+\w+\s*(\([^)]*\))?\s+as\s+', '', 'i');
    IF cardinality(pr.problems) > 0 THEN
        RAISE EXCEPTION 'isomer: proposal % has problems (%), so it cannot be routed', p_id, array_to_string(pr.problems, '; ');
    END IF;
    IF q ~* '\sorder\s+by\s' THEN
        RAISE EXCEPTION 'isomer: routing queries with ORDER BY is not supported yet';
    END IF;
    IF EXISTS (SELECT 1 FROM jsonb_object_keys(pr.draft -> 'sources') t JOIN pg_class c ON c.oid = t::regclass WHERE c.relrowsecurity) THEN
        RAISE EXCEPTION 'isomer: the query reads a table with row-level security; routing would bypass it, so it is not routed';
    END IF;

    -- The key parameter's type, as the original statement's planner sees it.
    EXECUTE format('PREPARE isomer_route_type_probe AS %s', q);
    SELECT parameter_types[1] INTO v_ptype FROM pg_prepared_statements WHERE name = 'isomer_route_type_probe';
    DEALLOCATE isomer_route_type_probe;

    -- The original's output columns, names and types.
    PERFORM set_config('citus.enable_ddl_propagation', 'off', true);
    PERFORM set_config('search_path', 'public, pg_catalog', true);
    EXECUTE format('CREATE OR REPLACE TEMP VIEW %s AS %s', v_probe, replace(q, '$1', format('NULL::%s', v_ptype)));
    PERFORM set_config('search_path', 'pg_catalog', true);
    FOR v_cols IN SELECT * FROM isomer._query_columns(format('SELECT * FROM %s', v_probe)) LOOP
        IF v_cols.column_name = ANY (v_names) THEN
            EXECUTE format('DROP VIEW %s', v_probe);
            RAISE EXCEPTION 'isomer: the query returns column % twice; give the columns distinct names to route it', v_cols.column_name;
        END IF;
        v_names := v_names || v_cols.column_name;
        v_defs := concat_ws(', ', v_defs, format('%I %s', v_cols.column_name, v_cols.column_type));
        v_select := concat_ws(', ', v_select, format('x.%I', v_cols.column_name));
    END LOOP;
    EXECUTE format('DROP VIEW %s', v_probe);
    PERFORM set_config('citus.enable_ddl_propagation', 'on', true);

    -- A sync cache projection holding, per key, the rows the original returns.
    v_name := 'route_' || p_id;
    v_res := isomer.create_projection(
        name => v_name, kind => 'cache', key => 'key', freshness => 'sync', target_schema => 'isomer_routes',
        query => format('SELECT d.key, d.doc -> ''rows'' AS value FROM (%s) d', pr.draft ->> 'query'),
        sources => pr.draft -> 'sources');

    v_repl := format('SELECT %s FROM %s c CROSS JOIN LATERAL jsonb_to_recordset(c.value) AS x(%s) WHERE c.key = ($1)::text',
                     v_select, isomer._cache_store(v_name), v_defs);

    -- Same rows as the original, on keys that exist and a few that don't.
    v_keys := ARRAY(SELECT ak FROM isomer._all_keys(v_name) ak ORDER BY random() LIMIT sample)
              || ARRAY['-1', '0', '999999999'];
    FOREACH k IN ARRAY v_keys LOOP
        IF isomer._rows_for(q, v_ptype, k) IS DISTINCT FROM isomer._rows_for(v_repl, v_ptype, k) THEN
            PERFORM isomer.drop_projection(v_name);
            RAISE EXCEPTION 'isomer: the routed query returns different rows for key %; not routing', k;
        END IF;
    END LOOP;

    v_t_orig := isomer._time_ms(q, v_ptype, v_keys);
    v_t_repl := isomer._time_ms(v_repl, v_ptype, v_keys);
    v_speedup := v_t_orig / greatest(v_t_repl, 0.0001);

    INSERT INTO isomer.routes (queryid, proposal, projection, source_query, replacement_sql, param_types,
                               measured_speedup, verified_keys, enabled)
    VALUES (pr.queryid, p_id, v_name, q, v_repl, ARRAY[v_ptype], v_speedup, cardinality(v_keys), v_speedup >= v_min)
    ON CONFLICT (queryid) DO UPDATE
        SET projection = EXCLUDED.projection, replacement_sql = EXCLUDED.replacement_sql,
            param_types = EXCLUDED.param_types, measured_speedup = EXCLUDED.measured_speedup,
            verified_keys = EXCLUDED.verified_keys, enabled = EXCLUDED.enabled, created_at = now();
    UPDATE isomer.proposals SET status = 'accepted', projection = v_name, decided_at = now() WHERE id = p_id;

    RETURN format('route for query %s: %s (verified on %s keys; %s ms -> %s ms, %sx)%s',
                  pr.queryid, CASE WHEN v_speedup >= v_min THEN 'enabled' ELSE 'kept disabled' END,
                  cardinality(v_keys), round(v_t_orig::numeric, 2), round(v_t_repl::numeric, 3), round(v_speedup::numeric, 1),
                  CASE WHEN v_speedup < v_min THEN format(' — below isomer.routing_min_speedup (%s)', v_min) ELSE '' END);
END $$;

-- Read your writes across nodes, closing a commit-visibility gap. The drain writes a document on
-- whichever node holds the collection's shard and deletes the queued key on the coordinator in one
-- distributed transaction. Citus commits the coordinator's side first and then sends COMMIT
-- PREPARED to the others, so for a moment the queue is empty while the document update is not yet
-- visible. After the queues are clear, wait for every two-phase commit in flight at that moment
-- to finish.
CREATE FUNCTION isomer._prepared_gids()
RETURNS text[] LANGUAGE sql SET search_path = pg_catalog AS $$
    SELECT array_agg(g) FROM (
        SELECT gid AS g FROM pg_prepared_xacts
        UNION ALL
        SELECT unnest(string_to_array(result, ','))
        FROM run_command_on_workers('SELECT coalesce(string_agg(gid, '',''), '''') FROM pg_prepared_xacts')
        WHERE success AND result <> '') s
$$;

CREATE OR REPLACE FUNCTION isomer.wait_for(p_token bigint, p_projection text DEFAULT NULL, p_timeout interval DEFAULT '5 seconds')
RETURNS bigint LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    v_until   timestamptz := clock_timestamp() + p_timeout;
    v_multi   boolean := EXISTS (SELECT 1 FROM isomer._workers());
    v_barrier timestamptz;
    v_gids    text[];
    s         record;
BEGIN
    IF p_token IS NULL AND NOT v_multi THEN
        RETURN isomer.current_token();
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'isomer.wait_for needs READ COMMITTED isolation (this transaction is %)',
            current_setting('transaction_isolation');
    END IF;
    IF EXISTS (SELECT 1 FROM isomer.pending
               WHERE xmin = pg_current_xact_id_if_assigned()::text::xid
                 AND token > 0 AND (p_projection IS NULL OR projection = p_projection)) THEN
        RAISE EXCEPTION 'isomer.wait_for: the write you are waiting for is not committed yet'
            USING HINT = 'Commit the write first, then call isomer.wait_for(isomer.last_write_token()) in a new transaction.';
    END IF;
    FOR s IN SELECT name, target_schema FROM isomer.projections
             WHERE kind = 'search' AND freshness = 'bounded' AND (p_projection IS NULL OR name = p_projection) LOOP
        PERFORM pgtrickle.refresh_stream_table(format('%I.%I', s.target_schema, s.name));
    END LOOP;
    IF v_multi THEN
        v_barrier := isomer._barrier();
    END IF;
    LOOP
        IF v_multi THEN
            EXIT WHEN NOT isomer._queued_before(v_barrier, p_projection);
        ELSE
            EXIT WHEN NOT EXISTS (SELECT 1 FROM isomer.pending
                                  WHERE token > 0 AND token <= p_token AND (p_projection IS NULL OR projection = p_projection))
                  AND NOT EXISTS (SELECT 1 FROM isomer.embed_jobs
                                  WHERE token > 0 AND token <= p_token AND (p_projection IS NULL OR projection = p_projection));
        END IF;
        IF clock_timestamp() > v_until THEN
            RAISE EXCEPTION 'isomer: % has not caught up to token % yet (not yet)', coalesce(p_projection, 'a projection'), coalesce(p_token::text, 'now')
                USING ERRCODE = 'IS001',
                      HINT = 'Retry the read, or raise the timeout. isomer.projection_status shows the backlog.';
        END IF;
        PERFORM pg_sleep(0.02);
    END LOOP;
    IF v_multi THEN
        v_gids := isomer._prepared_gids();
        WHILE v_gids IS NOT NULL AND v_gids && isomer._prepared_gids() LOOP
            IF clock_timestamp() > v_until THEN
                RAISE EXCEPTION 'isomer: % has not caught up yet: a distributed commit is still in flight (not yet)', coalesce(p_projection, 'a projection')
                    USING ERRCODE = 'IS001';
            END IF;
            PERFORM pg_sleep(0.01);
        END LOOP;
    END IF;
    RETURN coalesce(p_token, isomer.current_token());
END $$;

CREATE FUNCTION isomer.set_route(p_queryid bigint, p_enabled boolean)
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    UPDATE isomer.routes SET enabled = p_enabled WHERE queryid = p_queryid
$$;

REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA isomer FROM PUBLIC;
GRANT EXECUTE ON FUNCTION isomer.wait_for(bigint, text, interval), isomer.freshness(text),
                          isomer.projection_for_collection(text, text), isomer.current_token(),
                          isomer.last_write_token(), isomer.token_to_mongo_timestamp(bigint),
                          isomer.cache_get(text, text), isomer.embed(text, text, interval),
                          isomer.embed_for(text, text), isomer._routes(), isomer.router_stats()
    TO PUBLIC;
