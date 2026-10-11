-- IsomerDB 0.4.0: advisors and proof (PRD Phase 3, single-node parts).
--
--   * The proposer reads pg_stat_statements for repeated join-and-lookup reads and drafts a
--     projection for each, with estimated read benefit and write cost (FR-M1/M2/M3/M5).
--   * The shard advisor reads Citus statistics and the projection catalog and recommends
--     shard keys, reference tables and co-location (FR-M6/M7).
--   * Certificates state, per projection, the maintenance rule, its assumptions, and live checks
--     that the assumptions hold (FR-S5).
--   * isomer.verify() compares a whole projection with recomputation (the auditor samples).

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.4.0'" to load this file. \quit

CREATE TABLE isomer.settings (
    key   text PRIMARY KEY,
    value text
);
SELECT pg_catalog.pg_extension_config_dump('isomer.settings', '');
-- Auto-accept is off unless a cost threshold is set (FR-M3).
INSERT INTO isomer.settings VALUES ('proposer.auto_accept_max_write_ms_per_s', NULL),
                                   ('proposer.min_calls', '50'),
                                   ('proposer.projection_read_ms', '0.3'),
                                   ('unused.quiet_period', '7 days');

CREATE FUNCTION isomer._setting(p_key text)
RETURNS text LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT value FROM isomer.settings WHERE key = p_key
$$;

CREATE FUNCTION isomer.set_setting(p_key text, p_value text)
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    INSERT INTO isomer.settings VALUES (p_key, p_value) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
$$;

-- ── Concurrency-safe capture ────────────────────────────────────────────────

-- Source trigger, with two races closed (both found by tests/phase3.sh's randomized workers):
--   * sync: two transactions touching the same key could each derive the document from a
--     snapshot taken before the other committed, and the later writer would overwrite it with a
--     stale result. A per-key transaction lock, taken in key order before deriving, means each
--     derivation sees every earlier committed change to that key.
--   * bounded: a write that found its key already queued did nothing (ON CONFLICT DO NOTHING),
--     so a drain that had already read the queue could miss it. The conflict now locks the
--     queued row: the drain skips locked keys until the write commits. The queued row keeps the
--     oldest real token, so read-your-writes waits for every write merged into it.
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

-- ── Verification ────────────────────────────────────────────────────────────

-- Compare every key of a projection with what the tables produce now: keys from both sides,
-- nothing sampled. Returns the mismatches (empty = the projection equals recomputation).
-- Bounded projections should be waited for first (isomer.wait_for).
CREATE FUNCTION isomer.verify(p_projection text)
RETURNS TABLE (key text, expected jsonb, actual jsonb)
LANGUAGE plpgsql SET search_path = pg_catalog SET paradedb.planner_warnings = off AS $$
DECLARE
    p      isomer.projections;
    v_keys text[];
    v_more text[];
BEGIN
    SELECT * INTO STRICT p FROM isomer.projections WHERE name = p_projection;
    v_keys := ARRAY(SELECT isomer._all_keys(p.name));
    IF p.kind = 'document' THEN
        EXECUTE format('SELECT array_agg(isomer._ejson_relax(documentdb_core.bson_to_json_string(c.document)::text::jsonb) ->> ''_id'') FROM %s c',
                       isomer._collection_table(p.mongo_database, p.name)) INTO v_more;
    ELSIF p.kind = 'cache' THEN
        EXECUTE format('SELECT array_agg(key) FROM %s', isomer._cache_store(p.name)) INTO v_more;
    ELSIF p.kind = 'vector' THEN
        EXECUTE format('SELECT array_agg(key) FROM %s WHERE model = %L', isomer._vector_store(p.name), p.model) INTO v_more;
    ELSE
        EXECUTE format('SELECT array_agg(t.%I::text) FROM %I.%I t', p.key_column, p.target_schema, p.name) INTO v_more;
    END IF;
    v_keys := ARRAY(SELECT DISTINCT k FROM unnest(v_keys || coalesce(v_more, '{}')) k WHERE k IS NOT NULL);
    RETURN QUERY SELECT a.key, a.expected, a.actual FROM isomer._audit_pairs(p, v_keys) a
                 WHERE a.expected IS DISTINCT FROM a.actual;
END $$;

-- ── Certificates (FR-S5) ────────────────────────────────────────────────────

-- What makes a projection correct, and whether that holds right now. A projection whose checks
-- fail is not trustworthy until fixed; create_projection already refuses the definitions that
-- can't be certified (unmapped tables, sync without co-location).
CREATE FUNCTION isomer.certificate(p_projection text)
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
            CASE WHEN p.freshness = 'sync' THEN 'sync: the triggers run in the writing transaction, on the shard holding every row the key depends on (co-location)'
                 ELSE format('bounded: queued keys are applied within the lag budget (%s); read-your-writes via isomer.wait_for', p.lag_budget) END);

        -- Completeness: the relations the stored query reads now vs the mapped sources.
        v_tables := ARRAY(SELECT isomer._query_tables(p.query));
        SELECT bool_and(EXISTS (SELECT 1 FROM isomer.projection_sources ps WHERE ps.projection = p.name AND ps.source_table::regclass = t)),
               string_agg(t::text, ', ') FILTER (WHERE NOT EXISTS (SELECT 1 FROM isomer.projection_sources ps WHERE ps.projection = p.name AND ps.source_table::regclass = t))
        INTO v_ok, v_detail FROM unnest(v_tables) t;
        v_checks := v_checks || jsonb_build_object('check', 'completeness', 'passed', coalesce(v_ok, false),
            'detail', CASE WHEN v_ok THEN format('%s relation(s) read, all mapped', cardinality(v_tables)) ELSE 'unmapped: ' || v_detail END);

        -- Triggers present and firing for replicated changes, on every source (and every shard).
        FOR s IN SELECT * FROM isomer.projection_sources WHERE projection = p.name LOOP
            v_ok := EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = s.source_table::regclass
                            AND tgname = 'isomer_' || p.name AND tgenabled = 'A');
            IF v_ok AND (isomer._distribution(s.source_table::regclass)).method = 'h' THEN
                SELECT count(*) = count(t.tgname) INTO v_ok
                FROM pg_dist_shard sh
                LEFT JOIN pg_trigger t ON t.tgrelid = to_regclass(shard_name(sh.logicalrelid, sh.shardid)) AND t.tgname = 'isomer_' || p.name
                WHERE sh.logicalrelid = s.source_table::regclass;
            END IF;
            v_checks := v_checks || jsonb_build_object('check', 'capture on ' || s.source_table, 'passed', v_ok,
                'detail', CASE WHEN v_ok THEN 'row and truncate triggers present, ENABLE ALWAYS' || CASE WHEN (isomer._distribution(s.source_table::regclass)).method = 'h' THEN ', on every shard' ELSE '' END
                               ELSE 'missing or disabled: run isomer.restore_projections()' END);
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

-- ── The proposer (FR-M1/M2/M3) ──────────────────────────────────────────────

CREATE TABLE isomer.proposals (
    id                bigserial PRIMARY KEY,
    queryid           bigint UNIQUE,
    source_query      text NOT NULL,
    calls             bigint,
    mean_ms           double precision,
    est_speedup       double precision,
    benefit_ms_per_s  double precision,   -- read time saved per second
    cost_ms_per_s     double precision,   -- maintenance time added per second
    draft             jsonb,              -- isomer.create_projection arguments
    problems          text[] NOT NULL DEFAULT '{}',
    status            text NOT NULL DEFAULT 'proposed'
                      CHECK (status IN ('proposed', 'needs_review', 'accepted', 'auto_accepted', 'rejected')),
    projection        text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    decided_at        timestamptz
);

-- Draft a key-maintained document projection from a parameterised read such as
--   SELECT ... FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.id = $1
-- The key is the column compared with $1; each joined table maps to the key through the join
-- equalities. The draft groups the query's rows per key into one document.
CREATE FUNCTION isomer._draft_projection(p_query text, OUT draft jsonb, OUT problems text[])
LANGUAGE plpgsql SET search_path = pg_catalog SET citus.enable_ddl_propagation = off AS $$
DECLARE
    q         text := regexp_replace(p_query, '\s+', ' ', 'g');
    v_alias   jsonb := '{}';    -- alias -> table
    v_map     jsonb := '{}';    -- alias -> column holding the key
    m         text[];
    v_ka      text;
    v_kc      text;
    v_changed boolean := true;
    v_body    text;
    v_sources jsonb := '{}';
    a         text;
BEGIN
    problems := '{}';
    IF q !~* '^\s*select\s' THEN
        problems := problems || 'not a SELECT'; RETURN;
    END IF;
    IF q ~* '\$[2-9]' THEN
        problems := problems || 'has parameters other than the key ($1)';
    END IF;
    IF q ~* '\slimit\s|\soffset\s' THEN
        problems := problems || 'uses LIMIT/OFFSET, which a per-key document cannot reproduce';
    END IF;

    -- Tables and aliases from FROM and JOIN clauses.
    FOR m IN SELECT regexp_matches(q, '(?:from|join)\s+([a-z_][\w.]*)(?:\s+(?:as\s+)?(?!on\y|where\y|join\y|left\y|right\y|inner\y|full\y|cross\y|group\y|order\y|limit\y)([a-z_]\w*))?', 'gi') LOOP
        v_alias := v_alias || jsonb_build_object(coalesce(m[2], m[1]), m[1]);
    END LOOP;

    m := regexp_match(q, 'where\s+(?:.*\sand\s+)?([a-z_]\w*)\.([a-z_]\w*)\s*=\s*\$1', 'i');
    IF m IS NULL THEN
        problems := problems || 'no "alias.column = $1" lookup to key the projection on'; RETURN;
    END IF;
    v_ka := m[1]; v_kc := m[2];
    v_map := jsonb_build_object(v_ka, v_kc);

    -- Propagate the key through join equalities a.x = b.y until nothing changes.
    WHILE v_changed LOOP
        v_changed := false;
        FOR m IN SELECT regexp_matches(q, '([a-z_]\w*)\.([a-z_]\w*)\s*=\s*([a-z_]\w*)\.([a-z_]\w*)', 'gi') LOOP
            IF v_map ->> m[1] = m[2] AND NOT v_map ? m[3] THEN
                v_map := v_map || jsonb_build_object(m[3], m[4]); v_changed := true;
            ELSIF v_map ->> m[3] = m[4] AND NOT v_map ? m[1] THEN
                v_map := v_map || jsonb_build_object(m[1], m[2]); v_changed := true;
            END IF;
        END LOOP;
    END LOOP;

    -- The document query: drop the key predicate, select the key, group rows per key.
    v_body := regexp_replace(q, format('\s(and)\s+%s\.%s\s*=\s*\$1', v_ka, v_kc), ' ', 'i');
    v_body := regexp_replace(v_body, format('where\s+%s\.%s\s*=\s*\$1\s+and\s', v_ka, v_kc), 'WHERE ', 'i');
    v_body := regexp_replace(v_body, format('where\s+%s\.%s\s*=\s*\$1', v_ka, v_kc), '', 'i');
    v_body := regexp_replace(v_body, '^\s*select\s', format('SELECT %s.%s AS __isomer_key, ', v_ka, v_kc), 'i');
    v_body := format('SELECT r.__isomer_key AS key, jsonb_build_object(''rows'', jsonb_agg(to_jsonb(r) - ''__isomer_key'')) AS doc FROM (%s) r GROUP BY r.__isomer_key', v_body);

    FOR a IN SELECT jsonb_object_keys(v_alias) LOOP
        IF v_map ? a THEN
            v_sources := v_sources || jsonb_build_object(v_alias ->> a, v_map ->> a);
        ELSE
            problems := problems || format('cannot map %s (%s) to the key: no join equality connects it', v_alias ->> a, a);
        END IF;
    END LOOP;

    -- Resolve names as an application on the default search path would, then store them
    -- schema-qualified so the draft means the same thing wherever it is accepted.
    PERFORM set_config('search_path', 'public, pg_catalog', true);
    BEGIN
        EXECUTE format('CREATE OR REPLACE TEMP VIEW isomer_draft_%s AS %s', pg_backend_pid(), v_body);
        v_body := isomer._viewdef(format('pg_temp.isomer_draft_%s', pg_backend_pid())::regclass);
        EXECUTE format('DROP VIEW pg_temp.isomer_draft_%s', pg_backend_pid());
        v_sources := coalesce((SELECT jsonb_object_agg(isomer._qualified(e.key::regclass), e.value) FROM jsonb_each(v_sources) e), '{}');
    EXCEPTION WHEN OTHERS THEN
        problems := problems || format('draft does not run: %s', SQLERRM);
    END;
    PERFORM set_config('search_path', 'pg_catalog', true);

    draft := jsonb_build_object('kind', 'document', 'key', 'key', 'query', v_body, 'sources', v_sources);
END $$;

-- Write counters per table at the last snapshot, so the proposer measures recent write rates
-- (lifetime averages are dominated by bulk loads). Snapshotted with track_reads, every 5 minutes.
CREATE TABLE isomer.table_activity (
    relid  oid PRIMARY KEY,
    writes bigint NOT NULL,
    at     timestamptz NOT NULL
);

CREATE FUNCTION isomer.snapshot_activity()
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    INSERT INTO isomer.table_activity (relid, writes, at)
    SELECT relid, n_tup_ins + n_tup_upd + n_tup_del, clock_timestamp() FROM pg_stat_user_tables
    ON CONFLICT (relid) DO UPDATE SET writes = EXCLUDED.writes, at = EXCLUDED.at
$$;

-- Writes per second to a table since the last snapshot (or over its lifetime, without one).
CREATE FUNCTION isomer._write_rate(p_table regclass)
RETURNS double precision LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT CASE WHEN a.relid IS NOT NULL
                THEN greatest(t.n_tup_ins + t.n_tup_upd + t.n_tup_del - a.writes, 0)
                     / greatest(extract(epoch FROM clock_timestamp() - a.at), 1)
                ELSE (t.n_tup_ins + t.n_tup_upd + t.n_tup_del)
                     / greatest(extract(epoch FROM now() - pg_postmaster_start_time()), 1) END
    FROM pg_stat_user_tables t LEFT JOIN isomer.table_activity a ON a.relid = t.relid
    WHERE t.relid = p_table
$$;

-- Mine pg_stat_statements for frequent keyed reads and record proposals. Returns how many new
-- proposals were made. Runs hourly; also callable by hand.
CREATE FUNCTION isomer.propose(p_min_calls bigint DEFAULT NULL)
RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    r          record;
    d          record;
    v_age_s    double precision;
    v_writes   double precision;
    v_read_ms  double precision := coalesce(isomer._setting('proposer.projection_read_ms')::float8, 0.3);
    v_benefit  double precision;
    v_cost     double precision;
    v_new      integer := 0;
    v_auto     double precision := isomer._setting('proposer.auto_accept_max_write_ms_per_s')::float8;
    v_id       bigint;
BEGIN
    IF to_regclass('public.pg_stat_statements') IS NULL THEN
        RAISE EXCEPTION 'isomer.propose needs the pg_stat_statements extension (CREATE EXTENSION pg_stat_statements)';
    END IF;
    SELECT extract(epoch FROM now() - coalesce(stats_reset, pg_postmaster_start_time())) INTO v_age_s
    FROM public.pg_stat_statements_info;
    v_age_s := greatest(v_age_s, 1);

    FOR r IN
        SELECT s.queryid, s.query, s.calls, s.mean_exec_time
        FROM (SELECT ps.queryid, ps.calls, ps.mean_exec_time,
                     -- Prepared statements (what drivers send) are recorded with their PREPARE wrapper.
                     regexp_replace(ps.query, '^\s*prepare\s+\w+\s*(\([^)]*\))?\s+as\s+', '', 'i') AS query
              FROM public.pg_stat_statements ps) s
        WHERE s.calls >= coalesce(p_min_calls, isomer._setting('proposer.min_calls')::bigint, 50)
          AND s.query ~* '^\s*select\s' AND s.query ~* '\sjoin\s' AND s.query ~ '\$1'
          AND s.query !~* '(isomer|documentdb|pg_catalog|cron|citus|pgtrickle|information_schema)\.'
          AND NOT EXISTS (SELECT 1 FROM isomer.proposals p WHERE p.queryid = s.queryid)
        ORDER BY s.calls * s.mean_exec_time DESC
        LIMIT 20
    LOOP
        SELECT * INTO d FROM isomer._draft_projection(r.query);
        -- Re-deriving one key costs about one run of the read, once per write to a source row.
        SELECT coalesce(sum(isomer._write_rate(k::regclass)), 0) INTO v_writes
        FROM jsonb_object_keys(coalesce(d.draft -> 'sources', '{}')) k;
        v_benefit := r.calls / v_age_s * greatest(r.mean_exec_time - v_read_ms, 0);
        v_cost := v_writes * r.mean_exec_time;

        INSERT INTO isomer.proposals (queryid, source_query, calls, mean_ms, est_speedup, benefit_ms_per_s, cost_ms_per_s,
                                      draft, problems, status)
        VALUES (r.queryid, r.query, r.calls, r.mean_exec_time, r.mean_exec_time / v_read_ms, v_benefit, v_cost,
                d.draft, d.problems, CASE WHEN cardinality(d.problems) = 0 THEN 'proposed' ELSE 'needs_review' END)
        RETURNING id INTO v_id;
        v_new := v_new + 1;

        IF v_auto IS NOT NULL AND cardinality(d.problems) = 0 AND v_benefit > v_cost AND v_cost <= v_auto THEN
            PERFORM isomer.accept_proposal(v_id, auto => true);
        END IF;
    END LOOP;
    RETURN v_new;
END $$;

CREATE FUNCTION isomer.accept_proposal(p_id bigint, name text DEFAULT NULL, freshness text DEFAULT 'bounded',
                                       database text DEFAULT 'isomer', auto boolean DEFAULT false)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    pr     isomer.proposals;
    v_name text;
    v_res  text;
BEGIN
    SELECT * INTO STRICT pr FROM isomer.proposals WHERE id = p_id FOR UPDATE;
    IF pr.status NOT IN ('proposed', 'needs_review') THEN
        RAISE EXCEPTION 'isomer: proposal % is already %', p_id, pr.status;
    END IF;
    v_name := coalesce(accept_proposal.name, 'proposed_' || p_id);
    v_res := isomer.create_projection(name => v_name, kind => 'document', key => pr.draft ->> 'key',
                                      query => pr.draft ->> 'query', sources => pr.draft -> 'sources',
                                      freshness => accept_proposal.freshness, database => accept_proposal.database);
    UPDATE isomer.proposals SET status = CASE WHEN auto THEN 'auto_accepted' ELSE 'accepted' END,
                                projection = v_name, decided_at = now()
    WHERE id = p_id;
    PERFORM isomer._log('info', v_name, format('proposal %s %s: %s', p_id, CASE WHEN auto THEN 'auto-accepted' ELSE 'accepted' END, v_res));
    RETURN v_res;
END $$;

CREATE FUNCTION isomer.reject_proposal(p_id bigint)
RETURNS void LANGUAGE sql SET search_path = pg_catalog AS $$
    UPDATE isomer.proposals SET status = 'rejected', decided_at = now() WHERE id = p_id
$$;

-- ── Unused projections (FR-M5) ──────────────────────────────────────────────

CREATE TABLE isomer.read_counters (
    projection  text PRIMARY KEY REFERENCES isomer.projections (name) ON DELETE CASCADE,
    reads       bigint NOT NULL,
    audit_runs  bigint NOT NULL DEFAULT 0,
    changed_at  timestamptz NOT NULL DEFAULT now()
);

-- Count audit runs per projection, so the auditor's own scans aren't mistaken for reads.
ALTER TABLE isomer.audit_stats ADD COLUMN runs bigint NOT NULL DEFAULT 0;
CREATE FUNCTION isomer._count_audit_run()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NEW.last_run IS DISTINCT FROM OLD.last_run THEN
        NEW.runs := OLD.runs + 1;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER count_runs BEFORE UPDATE ON isomer.audit_stats FOR EACH ROW EXECUTE FUNCTION isomer._count_audit_run();

-- Scans of a projection's storage table.
CREATE FUNCTION isomer._storage_reads(p isomer.projections)
RETURNS bigint LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    v_rel regclass;
BEGIN
    v_rel := CASE p.kind
        WHEN 'document' THEN to_regclass(isomer._collection_table(p.mongo_database, p.name))
        WHEN 'cache'    THEN to_regclass(isomer._cache_store(p.name))
        WHEN 'vector'   THEN to_regclass(isomer._vector_store(p.name))
        ELSE to_regclass(format('%I.%I', p.target_schema, p.name)) END;
    RETURN (SELECT coalesce(seq_scan, 0) + coalesce(idx_scan, 0) FROM pg_stat_all_tables WHERE relid = v_rel);
END $$;

-- Snapshot read counters. A projection counts as read when its storage was scanned more than the
-- auditor alone accounts for (each audit run scans it at most 3 times).
CREATE FUNCTION isomer.track_reads()
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    p      isomer.projections;
    v      bigint;
    v_runs bigint;
BEGIN
    PERFORM isomer.snapshot_activity();
    FOR p IN SELECT * FROM isomer.projections LOOP
        v := coalesce(isomer._storage_reads(p), 0);
        SELECT coalesce(runs, 0) INTO v_runs FROM isomer.audit_stats WHERE projection = p.name;
        INSERT INTO isomer.read_counters AS c (projection, reads, audit_runs) VALUES (p.name, v, coalesce(v_runs, 0))
        ON CONFLICT (projection) DO UPDATE
            SET reads = EXCLUDED.reads, audit_runs = EXCLUDED.audit_runs,
                changed_at = CASE WHEN (EXCLUDED.reads - c.reads) > 3 * (EXCLUDED.audit_runs - c.audit_runs)
                                  THEN now() ELSE c.changed_at END;
    END LOOP;
END $$;

-- Projections nobody has read for the quiet period: candidates for removal (their write cost
-- buys nothing).
CREATE VIEW isomer.unused_projections AS
SELECT p.name, p.kind, c.changed_at AS last_read_seen, now() - c.changed_at AS quiet_for
FROM isomer.projections p JOIN isomer.read_counters c ON c.projection = p.name
WHERE now() - c.changed_at > coalesce(isomer._setting('unused.quiet_period')::interval, interval '7 days');

-- ── The shard advisor (FR-M6/M7) ────────────────────────────────────────────

-- Recommendations from how the cluster is actually used: queries that fan out to every shard and
-- the key that would keep them on one, tables small enough to copy to every shard, and bounded
-- projections that co-location would let become sync.
-- (public is on the path because Citus's citus_stat_statements calls pg_stat_statements()
-- unqualified, and that extension lives in public.)
CREATE FUNCTION isomer.shard_advice(p_reference_max_bytes bigint DEFAULT 64 * 1024 * 1024)
RETURNS TABLE (advice text, subject text, recommendation text, detail text)
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE
    r      record;
    m      text[];
    v_col  text;
BEGIN
    -- 1. Multi-shard queries, hottest first, with the join column that would keep them local.
    IF to_regclass('citus_stat_statements') IS NOT NULL THEN
        FOR r IN SELECT s.query, s.calls, s.executor FROM citus_stat_statements s
                 WHERE s.partition_key IS NULL AND s.executor::text NOT IN ('router', 'local')
                   AND s.query ~* '\sjoin\s' AND s.query !~* '(isomer|documentdb|pg_catalog|citus|cron|pgtrickle)\.'
                 ORDER BY s.calls DESC LIMIT 10 LOOP
            v_col := NULL;
            FOR m IN SELECT regexp_matches(r.query, '([a-z_]\w*)\.([a-z_]\w*)\s*=\s*([a-z_]\w*)\.([a-z_]\w*)', 'gi') LOOP
                v_col := coalesce(v_col, CASE WHEN m[2] = m[4] THEN m[2] ELSE m[4] END);
            END LOOP;
            advice := 'cross_shard_query'; subject := left(regexp_replace(r.query, '\s+', ' ', 'g'), 160);
            recommendation := CASE WHEN v_col IS NOT NULL
                                   THEN format('co-locate the joined tables on %s (isomer.reshard_plan)', v_col)
                                   ELSE 'add an equality join on the distribution column' END;
            detail := format('%s calls, %s executor, no single shard key', r.calls, r.executor);
            RETURN NEXT;
        END LOOP;
    END IF;

    -- 2. Small hash-distributed tables that take part in fan-out queries: copy them to every
    --    shard instead, so those joins stay local.
    FOR r IN SELECT p.logicalrelid::regclass AS t, citus_table_size(p.logicalrelid) AS bytes
             FROM pg_dist_partition p WHERE p.partmethod = 'h'
               AND to_regclass('citus_stat_statements') IS NOT NULL
               AND EXISTS (SELECT 1 FROM citus_stat_statements s
                           WHERE s.partition_key IS NULL AND s.executor::text NOT IN ('router', 'local')
                             AND s.query ~* ('[\s.]' || (SELECT relname FROM pg_class WHERE oid = p.logicalrelid) || '\y'))
    LOOP
        IF r.bytes <= p_reference_max_bytes THEN
            advice := 'reference_candidate'; subject := r.t::text;
            recommendation := format('SELECT isomer.make_reference(ARRAY[%L]::regclass[]) — after undistributing it (SELECT undistribute_table(%L))', r.t, r.t);
            detail := format('%s: small enough to copy to every shard, so joins with it never leave a shard', pg_size_pretty(r.bytes));
            RETURN NEXT;
        END IF;
    END LOOP;

    -- 3. Bounded projections that co-location would make sync-eligible.
    FOR r IN SELECT p.name, isomer._check_sources(p.name, 'sync') AS reason FROM isomer.projections p
             WHERE p.kind IN ('document', 'cache') AND p.freshness = 'bounded' LOOP
        IF r.reason IS NULL THEN
            advice := 'sync_ready'; subject := r.name;
            recommendation := format('SELECT isomer.alter_projection(%L, freshness => ''sync'')', r.name);
            detail := 'every precondition for same-transaction maintenance holds';
            RETURN NEXT;
        ELSIF r.reason ~ 'distributed by|co-located' THEN
            advice := 'sync_blocked_by_colocation'; subject := r.name;
            recommendation := 'co-locate its sources on the projection key (isomer.reshard_plan)';
            detail := r.reason;
            RETURN NEXT;
        END IF;
    END LOOP;
END $$;

-- The steps to change a table's shard key, co-located with another table if given. Changing the
-- distribution column rewrites the table and blocks writes to it while it copies; projections on
-- it get their shard triggers recreated afterwards.
CREATE FUNCTION isomer.reshard_plan(p_table regclass, p_column text, p_colocate_with regclass DEFAULT NULL)
RETURNS SETOF text LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    v_bytes bigint := citus_table_size(p_table);
BEGIN
    RETURN NEXT format('-- %s (%s): writes to it are blocked while it is rewritten', p_table, pg_size_pretty(v_bytes));
    RETURN NEXT format('SELECT alter_distributed_table(%L, distribution_column := %L%s);', p_table, p_column,
                       CASE WHEN p_colocate_with IS NOT NULL THEN format(', colocate_with := %L', p_colocate_with) ELSE '' END);
    RETURN NEXT format('SELECT isomer._create_source_triggers(projection) FROM isomer.projection_sources WHERE source_table::regclass = %L::regclass;', p_table);
END $$;

CREATE FUNCTION isomer.reshard(p_table regclass, p_column text, p_colocate_with regclass DEFAULT NULL)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog
    SET citus.enable_unsafe_triggers = on AS $$
DECLARE
    v_proj text;
BEGIN
    PERFORM alter_distributed_table(p_table::text, distribution_column := p_column,
                                    colocate_with := coalesce(p_colocate_with::text, 'default'));
    FOR v_proj IN SELECT DISTINCT projection FROM isomer.projection_sources WHERE source_table::regclass = p_table LOOP
        PERFORM isomer._create_source_triggers(v_proj);
    END LOOP;
    RETURN format('%s is now distributed by %s', p_table, p_column);
END $$;

-- ── Schedules ───────────────────────────────────────────────────────────────

SELECT cron.schedule('isomer-propose', '17 * * * *', 'SELECT isomer.propose()');
SELECT cron.schedule('isomer-track-reads', '*/5 * * * *', 'SELECT isomer.track_reads()');

-- Alerts gain unused projections.
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
SELECT 'unused', name, format('no reads for %s: its write cost buys nothing (isomer.drop_projection)', quiet_for)
FROM isomer.unused_projections
UNION ALL
SELECT 'attachment', attachment, format('%s: slot wal_status %s', status, wal_status)
FROM isomer.attachment_status()
WHERE status NOT IN ('active', 'cutover') OR (status = 'active' AND wal_status IS DISTINCT FROM 'reserved' AND wal_status IS DISTINCT FROM 'extended')
UNION ALL
SELECT level, subject, message FROM isomer.events
WHERE level IN ('error', 'warning') AND at > now() - interval '15 minutes';

REVOKE ALL ON ALL TABLES IN SCHEMA isomer FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA isomer FROM PUBLIC;
GRANT EXECUTE ON FUNCTION isomer.wait_for(bigint, text, interval), isomer.freshness(text),
                          isomer.projection_for_collection(text, text), isomer.current_token(),
                          isomer.last_write_token(), isomer.token_to_mongo_timestamp(bigint),
                          isomer.cache_get(text, text), isomer.embed(text, text, interval),
                          isomer.embed_for(text, text)
    TO PUBLIC;
