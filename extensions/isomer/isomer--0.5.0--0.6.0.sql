-- IsomerDB 0.6.0: multi-node clusters (PRD FR-P5, FR-P6 for key-maintained projections).
--
-- Citus runs IsomerDB's capture triggers on every shard, on whichever node holds it. A worker has
-- no projection catalog, so its triggers queue affected keys in isomer.node_queue; the coordinator
-- pulls every worker's queue into isomer.pending each second (isomer.pull_node_queues) and drains
-- as before. The pull commits its local copy before deleting the worker's rows, so a crash can
-- duplicate work (harmless: re-deriving a key is idempotent) but never lose it.
--
-- Bounded projections work across nodes. Sync projections need every document a write touches to
-- live in the writing transaction's node; across nodes that is not supported yet and is refused.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.6.0'" to load this file. \quit

-- Keys queued on a worker node (no catalog there, so no foreign keys). On the coordinator it
-- stays empty. key '*' means "every key" (a change to a lookup table, or a TRUNCATE).
CREATE TABLE isomer.node_queue (
    projection  text NOT NULL,
    key         text NOT NULL,
    token       bigint NOT NULL,
    enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection, key)
);
REVOKE ALL ON isomer.node_queue FROM PUBLIC;

CREATE OR REPLACE FUNCTION isomer._on_source_change()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    v_projection text := TG_ARGV[0];
    v_column     text := TG_ARGV[1];
    v_freshness  text := TG_ARGV[2];
    v_keys       text[];
    v_token      bigint := isomer.current_token();
BEGIN
    PERFORM set_config('isomer.last_write_token', v_token::text, false);

    -- On a worker node (no catalog): queue the keys for the coordinator to pull.
    IF NOT EXISTS (SELECT 1 FROM isomer.projections WHERE name = v_projection) THEN
        IF TG_OP = 'TRUNCATE' OR v_column = '' THEN
            v_keys := ARRAY['*'];
        ELSE
            SELECT array_agg(DISTINCT k) INTO v_keys
            FROM unnest(ARRAY[
                CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) ->> v_column END,
                CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) ->> v_column END]) k
            WHERE k IS NOT NULL;
        END IF;
        INSERT INTO isomer.node_queue AS q (projection, key, token)
        SELECT v_projection, k, v_token FROM unnest(v_keys) k
        ON CONFLICT (projection, key) DO UPDATE SET (token, enqueued_at) = (least(q.token, EXCLUDED.token), q.enqueued_at);
        RETURN NULL;
    END IF;

    IF TG_OP = 'TRUNCATE' THEN
        PERFORM isomer._log('info', v_projection, format('%s truncated: full refresh', TG_TABLE_NAME));
        PERFORM isomer._full_refresh(v_projection);
        RETURN NULL;
    END IF;

    IF v_column = '' THEN
        IF v_freshness = 'sync' THEN
            PERFORM pg_advisory_xact_lock(hashtext('isomer'), hashtext(v_projection));
            PERFORM isomer._rebuild_all(v_projection);
        ELSE
            INSERT INTO isomer.pending AS q (projection, key, token)
            SELECT v_projection, k, v_token FROM isomer._all_keys(v_projection) k
            ON CONFLICT (projection, key) DO UPDATE
                SET token = CASE WHEN q.token = 0 THEN EXCLUDED.token ELSE least(q.token, EXCLUDED.token) END;
        END IF;
        RETURN NULL;
    END IF;

    SELECT array_agg(DISTINCT k ORDER BY k) INTO v_keys
    FROM unnest(ARRAY[
        CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) ->> v_column END,
        CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) ->> v_column END]) k
    WHERE k IS NOT NULL;

    IF v_freshness = 'sync' THEN
        PERFORM pg_advisory_xact_lock(hashtext(v_projection), hashtext(k)) FROM unnest(v_keys) k ORDER BY k;
        PERFORM isomer._apply(v_projection, v_keys);
    ELSE
        INSERT INTO isomer.pending AS q (projection, key, token)
        SELECT v_projection, k, v_token FROM unnest(v_keys) k
        ON CONFLICT (projection, key) DO UPDATE
            SET token = CASE WHEN q.token = 0 THEN EXCLUDED.token ELSE least(q.token, EXCLUDED.token) END;
    END IF;
    RETURN NULL;
END $$;

-- The worker nodes, with how to reach them (credentials come from the server's .pgpass or
-- pg_dist_authinfo, as for Citus itself).
CREATE FUNCTION isomer._workers()
RETURNS TABLE (nodeid integer, conninfo text)
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT n.nodeid, format('host=%s port=%s dbname=%s user=postgres', n.nodename, n.nodeport, current_database())
    FROM pg_dist_node n
    WHERE n.isactive AND n.noderole = 'primary' AND n.groupid <> 0
$$;

-- Move every worker's queued keys into the coordinator's queue. A procedure, so it can commit the
-- local copy before deleting the worker's rows: a failure in between duplicates keys, never loses
-- them. Scheduled every second.
-- No SET clause: a procedure with one may not commit. Every name below is schema-qualified.
-- The INSERT is kept free of subqueries: Citus refuses an INSERT whose SELECT calls into queries
-- over distributed tables ("subqueries are not supported within INSERT queries"), which expanding
-- a '*' key does. So: take the worker's rows in one statement, expand, then insert from arrays.
CREATE PROCEDURE isomer.pull_node_queues()
LANGUAGE plpgsql AS $$
DECLARE
    w      record;
    r      record;
    v_conn text;
    v_ok   boolean;
    v_rows jsonb;
    v_keys text[];
BEGIN
    -- Only the coordinator has the catalog; a worker running this would discard what it took.
    IF NOT pg_catalog.citus_is_coordinator() THEN
        RETURN;
    END IF;
    FOR w IN SELECT * FROM isomer._workers() LOOP
        v_conn := pg_catalog.concat('isomer_pull_', w.nodeid);
        v_ok := true;
        BEGIN
            IF NOT (v_conn = ANY (coalesce(public.dblink_get_connections(), '{}'::text[]))) THEN
                PERFORM public.dblink_connect(v_conn, w.conninfo);
            END IF;
            PERFORM public.dblink_exec(v_conn, 'BEGIN');
            -- Lock and take everything queued now; writes arriving meanwhile wait, then queue anew.
            SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('projection', d.projection, 'key', d.key,
                                                                      'token', d.token, 'enqueued_at', d.enqueued_at))
            INTO v_rows
            FROM public.dblink(v_conn, 'DELETE FROM isomer.node_queue RETURNING projection, key, token, enqueued_at')
                 AS d(projection text, key text, token bigint, enqueued_at timestamptz);
            FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(coalesce(v_rows, '[]'::jsonb))
                     AS x(projection text, key text, token bigint, enqueued_at timestamptz) LOOP
                CONTINUE WHEN NOT EXISTS (SELECT 1 FROM isomer.projections p WHERE p.name = r.projection);
                IF r.key = '*' THEN
                    v_keys := ARRAY(SELECT isomer._all_keys(r.projection));
                ELSE
                    v_keys := ARRAY[r.key];
                END IF;
                INSERT INTO isomer.pending AS q (projection, key, token, enqueued_at)
                SELECT r.projection, k, r.token, r.enqueued_at FROM pg_catalog.unnest(v_keys) k
                ON CONFLICT (projection, key) DO UPDATE
                    SET (token, enqueued_at) = (CASE WHEN q.token = 0 THEN EXCLUDED.token ELSE least(q.token, EXCLUDED.token) END,
                                                least(q.enqueued_at, EXCLUDED.enqueued_at));
            END LOOP;
        EXCEPTION WHEN OTHERS THEN
            v_ok := false;
            PERFORM isomer._log('error', pg_catalog.concat('node ', w.nodeid), pg_catalog.format('pulling queued keys failed: %s', SQLERRM));
            BEGIN
                PERFORM public.dblink_disconnect(v_conn);   -- also rolls back the worker's transaction
            EXCEPTION WHEN OTHERS THEN
                NULL;
            END;
        END;
        IF v_ok THEN
            COMMIT;                                        -- the keys are safe here first...
            PERFORM public.dblink_exec(v_conn, 'COMMIT');  -- ...then gone from the worker
        END IF;
    END LOOP;
END $$;

-- Read your writes across nodes. A write routed to a worker's shard runs IsomerDB's trigger in the
-- worker's session, so the client's session never sees that write's token, and tokens from
-- different nodes' clocks can't be compared safely anyway. So on a multi-node cluster wait_for is a
-- barrier: it reads every node's clock once, then waits until no queue on any node holds a change
-- enqueued before that point. The client's write committed before the call, so it is covered.
CREATE FUNCTION isomer._barrier()
RETURNS timestamptz LANGUAGE sql SET search_path = pg_catalog AS $$
    SELECT greatest(clock_timestamp(), (SELECT max(result::timestamptz) FROM run_command_on_workers('SELECT clock_timestamp()') WHERE success))
$$;

CREATE FUNCTION isomer._queued_before(p_barrier timestamptz, p_projection text)
RETURNS boolean LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RETURN EXISTS (SELECT 1 FROM isomer.pending
                   WHERE token > 0 AND enqueued_at <= p_barrier AND (p_projection IS NULL OR projection = p_projection))
        OR EXISTS (SELECT 1 FROM isomer.embed_jobs
                   WHERE token > 0 AND enqueued_at <= p_barrier AND (p_projection IS NULL OR projection = p_projection))
        -- Fail closed: a worker that doesn't answer counts as still having queued work.
        OR coalesce((SELECT bool_or(NOT success OR result = 't') FROM run_command_on_workers(format(
               'SELECT EXISTS (SELECT 1 FROM isomer.node_queue WHERE enqueued_at <= %L %s)',
               p_barrier, CASE WHEN p_projection IS NULL THEN '' ELSE format('AND projection = %L', p_projection) END))), false);
END $$;

CREATE OR REPLACE FUNCTION isomer.wait_for(p_token bigint, p_projection text DEFAULT NULL, p_timeout interval DEFAULT '5 seconds')
RETURNS bigint LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    v_until   timestamptz := clock_timestamp() + p_timeout;
    v_multi   boolean := EXISTS (SELECT 1 FROM isomer._workers());
    v_barrier timestamptz;
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
    RETURN coalesce(p_token, isomer.current_token());
END $$;

-- Sync needs every document a write touches on the writing node; across nodes it is refused.
CREATE OR REPLACE FUNCTION isomer._check_sources(p_projection text, p_freshness text)
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
            CONTINUE;
        END IF;
        IF p_freshness = 'sync' AND isomer._multi_node() THEN
            RETURN format('%s is distributed across several nodes; sync projections are single-node for now. Use freshness => ''bounded'' (maintained on every node, read-your-writes via isomer.wait_for).', s.source_table);
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

-- Are a projection's capture triggers on every placement of a distributed source, on every node,
-- and ENABLE ALWAYS? (Citus names shard triggers with the shard id appended.)
CREATE FUNCTION isomer._triggers_on_all_placements(p_table regclass, p_projection text)
RETURNS boolean LANGUAGE sql SET search_path = pg_catalog AS $$
    SELECT coalesce(bool_and(success AND result::int >= 1), false)
    FROM run_command_on_placements(p_table::text, format(
        $q$SELECT count(*) FROM pg_trigger WHERE tgrelid = '%%s'::regclass AND tgname LIKE %L AND tgenabled = 'A'$q$,
        -- run_command_on_placements formats the command again (%s = shard name), so double the %.
        replace('isomer\_' || replace(p_projection, '_', '\_') || '%', '%', '%%')))
$$;

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

    IF p.kind IN ('document', 'cache', 'vector') THEN
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
        'projection', p.name, 'kind', p.kind, 'freshness', p.freshness,
        'valid', NOT EXISTS (SELECT 1 FROM jsonb_array_elements(v_checks) c WHERE NOT (c ->> 'passed')::boolean),
        'rules', v_rules, 'assumptions', v_assume, 'checks', v_checks,
        'auditor', jsonb_build_object('checks', coalesce(st.checks, 0), 'drift', coalesce(st.drift, 0), 'since', st.since),
        'issued_at', now());
END $$;

-- Add a worker node to the cluster and (by default) start moving shards onto it online. Shards
-- carry their capture triggers with them; isomer.rebalance_finished() re-checks them.
CREATE FUNCTION isomer.add_node(host text, port integer DEFAULT 5432, rebalance boolean DEFAULT true)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_id  integer;
    v_job bigint;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_dist_node WHERE groupid = 0 AND nodename IN ('localhost', '127.0.0.1')) THEN
        RAISE EXCEPTION 'isomer: the coordinator is registered as localhost, which worker nodes cannot reach'
            USING HINT = 'SELECT citus_set_coordinator_host(''<hostname workers can reach>'', 5432); (the image sets it from ISOMER_COORDINATOR_HOST)';
    END IF;
    v_id := citus_add_node(host, port);
    IF rebalance THEN
        v_job := citus_rebalance_start();
        RETURN format('node %s:%s added (id %s); rebalancing online (job %s, see citus_rebalance_status())', host, port, v_id, v_job);
    END IF;
    RETURN format('node %s:%s added (id %s)', host, port, v_id);
END $$;

-- After a rebalance: make sure every distributed source still has its capture triggers on every
-- placement (recreating them where missing). Returns the projections it repaired.
CREATE FUNCTION isomer.rebalance_finished()
RETURNS SETOF text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT DISTINCT s.projection, s.source_table FROM isomer.projection_sources s
             WHERE (isomer._distribution(s.source_table::regclass)).method = 'h' LOOP
        IF NOT isomer._triggers_on_all_placements(r.source_table::regclass, r.projection) THEN
            PERFORM isomer._create_source_triggers(r.projection);
            RETURN NEXT r.projection;
        END IF;
    END LOOP;
END $$;

-- Creating and dropping a projection's collection creates and drops a Citus distributed table,
-- so Citus must see that DDL: with propagation off, a dropped collection left its entry in
-- pg_dist_partition and adding the next node failed ("cache lookup failed for relation").
-- create_projection turns propagation off for its probe views; these functions turn it back on.
ALTER FUNCTION isomer._install(text) SET citus.enable_ddl_propagation = on;
ALTER FUNCTION isomer.drop_projection(text) SET citus.enable_ddl_propagation = on;

-- Pull worker queues every second, on the coordinator only (worker nodes unschedule IsomerDB's jobs).
SELECT cron.schedule('isomer-pull', '1 seconds', 'CALL isomer.pull_node_queues()')
    WHERE pg_catalog.citus_is_coordinator();  -- Citus propagates extension DDL to workers; jobs run on the coordinator only

REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA isomer FROM PUBLIC;
GRANT EXECUTE ON FUNCTION isomer.wait_for(bigint, text, interval), isomer.freshness(text),
                          isomer.projection_for_collection(text, text), isomer.current_token(),
                          isomer.last_write_token(), isomer.token_to_mongo_timestamp(bigint),
                          isomer.cache_get(text, text), isomer.embed(text, text, interval),
                          isomer.embed_for(text, text)
    TO PUBLIC;
