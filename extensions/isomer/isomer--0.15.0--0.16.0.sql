-- IsomerDB 0.16.0: a cutover plan generated for each attached database (PRD FR-A6).
--
--   * isomer.cutover_plan(attachment) reads the team's database and IsomerDB's copy of it and
--     returns, in order, what has to be true before cutover, what the copy does not carry yet
--     (attach copies columns and primary keys; indexes, constraints, triggers, functions, views,
--     extensions and grants are listed here with the SQL that recreates each), the cutover itself
--     (which applications to repoint, the quietest hour seen, the command) and what cutover
--     unlocks afterwards (same-transaction projections, sharding, retiring the source).
--   * isomer.cutover_plan_markdown(attachment) renders the same plan as a document to share.
--   * isomer.sample_attachments() records writes to the attached tables per hour, every minute,
--     so the plan can name the quietest hour to cut over in.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.16.0'" to load this file. \quit

CREATE TABLE isomer.attachment_activity (
    attachment text NOT NULL REFERENCES isomer.attachments (name) ON DELETE CASCADE,
    hour       timestamptz NOT NULL,
    writes     bigint NOT NULL DEFAULT 0,             -- rows inserted, updated or deleted on the source
    seconds    double precision NOT NULL DEFAULT 0,   -- how much of the hour was sampled
    PRIMARY KEY (attachment, hour)
);

CREATE TABLE isomer.attachment_activity_cursor (
    attachment text PRIMARY KEY REFERENCES isomer.attachments (name) ON DELETE CASCADE,
    writes     bigint NOT NULL,
    at         timestamptz NOT NULL
);

-- Writes to the attached tables since the last sample, from the source's statistics. A gap of
-- more than five minutes, or a statistics reset, starts again from a baseline instead of guessing.
CREATE FUNCTION isomer.sample_attachments()
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a       isomer.attachments;
    c       isomer.attachment_activity_cursor;
    v_total bigint;
    v_now   timestamptz := clock_timestamp();
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('isomer'), hashtext('attachment_activity'));
    FOR a IN SELECT * FROM isomer.attachments WHERE status <> 'cutover' LOOP
        BEGIN
            SELECT t.n INTO v_total FROM public.dblink(a.conninfo, format(
                'SELECT coalesce(sum(n_tup_ins + n_tup_upd + n_tup_del), 0)::bigint FROM pg_stat_user_tables '
                'WHERE format(''%%I.%%I'', schemaname, relname) = ANY (%L::text[])',
                (SELECT array_agg(x.table_name) FROM isomer.attached_tables x WHERE x.attachment = a.name)))
                AS t(n bigint);
        EXCEPTION WHEN OTHERS THEN
            CONTINUE;   -- unreachable: the health check reports it
        END;
        SELECT * INTO c FROM isomer.attachment_activity_cursor WHERE attachment = a.name;
        IF FOUND AND v_total >= c.writes AND v_now - c.at <= interval '5 minutes' THEN
            INSERT INTO isomer.attachment_activity AS x (attachment, hour, writes, seconds)
            VALUES (a.name, date_trunc('hour', v_now), v_total - c.writes, extract(epoch FROM v_now - c.at))
            ON CONFLICT (attachment, hour) DO UPDATE
                SET writes = x.writes + EXCLUDED.writes, seconds = x.seconds + EXCLUDED.seconds;
        END IF;
        INSERT INTO isomer.attachment_activity_cursor (attachment, writes, at) VALUES (a.name, v_total, v_now)
        ON CONFLICT (attachment) DO UPDATE SET writes = EXCLUDED.writes, at = EXCLUDED.at;
    END LOOP;
    DELETE FROM isomer.attachment_activity WHERE hour < now() - interval '35 days';
END $$;

CREATE FUNCTION isomer._rate(p_writes numeric, p_seconds double precision)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN p_seconds <= 0 THEN 'unknown'
                WHEN p_writes / p_seconds::numeric >= 10 THEN round(p_writes / p_seconds::numeric) || ' writes/s'
                ELSE round(p_writes / p_seconds::numeric, 2) || ' writes/s' END
$$;

-- Whether a grant on the source is missing here: the role does not exist, or lacks a privilege.
CREATE FUNCTION isomer._grant_missing(p_role text, p_table text, p_privs text[])
RETURNS boolean LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = p_role) THEN
        RETURN true;
    END IF;
    RETURN EXISTS (SELECT 1 FROM unnest(p_privs) p WHERE NOT has_table_privilege(p_role, p_table, p));
END $$;

-- The plan. Status is one of: ok (nothing to do), blocked (must be fixed before cutover),
-- todo (a step to take, with its SQL when there is one) and info.
CREATE FUNCTION isomer.cutover_plan(name text, shard_min_bytes bigint DEFAULT 1024 * 1024 * 1024)
RETURNS TABLE (ord integer, phase text, item text, status text, detail text, sql text)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a          isomer.attachments;
    st         record;
    r          record;
    v_conn     text := 'isomer_cutover_plan';
    v_tables   text[];
    v_problems text[];
    v_lines    text[];
    v_sql      text[];
    v_n        bigint;
    v_m        bigint;
    v_blocked  integer := 0;
    v_cut      boolean;
BEGIN
    SELECT * INTO a FROM isomer.attachments WHERE attachments.name = cutover_plan.name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no attachment named %', name;
    END IF;
    v_cut := a.status = 'cutover';
    SELECT array_agg(x.table_name ORDER BY x.table_name) INTO v_tables FROM isomer.attached_tables x WHERE x.attachment = a.name;
    ord := 0;

    IF v_cut THEN
        ord := ord + 1; phase := 'cutover'; item := 'Cut over'; status := 'ok'; sql := NULL;
        detail := format('IsomerDB has been the primary for %s table(s) since %s', cardinality(v_tables),
                         to_char(a.cutover_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI "UTC"'));
        RETURN NEXT;
    ELSE
        IF v_conn = ANY (coalesce(public.dblink_get_connections(), '{}')) THEN
            PERFORM public.dblink_disconnect(v_conn);
        END IF;
        PERFORM public.dblink_connect(v_conn, a.conninfo);
        -- Every name the source prints comes back schema-qualified.
        PERFORM public.dblink_exec(v_conn, 'SET search_path = pg_catalog');
      BEGIN
        ------------------------------------------------------------------ before: readiness
        SELECT * INTO st FROM isomer.attachment_status(a.name);
        ord := ord + 1; phase := 'before'; item := 'Initial copy'; sql := NULL;
        IF st.tables_ready = st.tables_total THEN
            status := 'ok'; detail := format('all %s table(s) copied and following the source', st.tables_total);
        ELSE
            status := 'blocked'; v_blocked := v_blocked + 1;
            detail := format('%s of %s table(s) copied; cutover waits for the rest (isomer.attachment_status)', st.tables_ready, st.tables_total);
        END IF;
        RETURN NEXT;

        SELECT coalesce(confl_insert_exists + confl_update_origin_differs + confl_update_exists + confl_update_missing
                        + confl_delete_origin_differs + confl_delete_missing + confl_multiple_unique_conflicts, 0)
        INTO v_n FROM pg_stat_subscription_stats WHERE subname = a.subscription;
        v_problems := array_remove(ARRAY[
            CASE WHEN a.status <> 'active' THEN format('the attachment is %s', a.status) END,
            CASE WHEN st.slot_active IS NOT TRUE THEN 'the replication slot is not active' END,
            CASE WHEN st.wal_status IS NOT NULL AND st.wal_status NOT IN ('reserved', 'extended') THEN format('slot WAL is %s', st.wal_status) END,
            CASE WHEN coalesce(st.apply_errors, 0) + coalesce(st.sync_errors, 0) > 0
                 THEN format('%s apply and %s copy error(s)', coalesce(st.apply_errors, 0), coalesce(st.sync_errors, 0)) END,
            CASE WHEN coalesce(v_n, 0) > 0 THEN format('%s replication conflict(s): the copy may differ from the source', v_n) END
        ], NULL);
        ord := ord + 1; item := 'Replication'; sql := NULL;
        IF cardinality(v_problems) = 0 THEN
            status := 'ok';
            detail := format('slot active, WAL %s, %s behind the source, no errors or conflicts',
                             coalesce(st.wal_status, 'reserved'),
                             coalesce(round(extract(epoch FROM st.apply_lag)::numeric, 1) || 's', 'not measured yet'));
        ELSE
            status := 'blocked'; v_blocked := v_blocked + 1;
            detail := array_to_string(v_problems, '; ');
            sql := format('SELECT * FROM isomer.attachment_status(%L);', a.name);
        END IF;
        RETURN NEXT;

        -- The auditor's evidence that projections over these tables are right.
        SELECT count(*) AS n, count(*) FILTER (WHERE coalesce(s.checks, 0) = 0) AS unaudited, coalesce(sum(s.checks), 0) AS checks,
               string_agg(p.name, ', ' ORDER BY p.name) FILTER (WHERE s.drift > 0) AS drifted
        INTO r FROM isomer.projections p LEFT JOIN isomer.audit_stats s ON s.projection = p.name
        WHERE EXISTS (SELECT 1 FROM isomer.projection_sources ps WHERE ps.projection = p.name AND ps.source_table = ANY (v_tables));
        ord := ord + 1; item := 'Projections audited'; sql := NULL;
        IF r.n = 0 THEN
            status := 'info'; detail := 'no projections over these tables yet';
        ELSIF r.drifted IS NOT NULL THEN
            status := 'blocked'; v_blocked := v_blocked + 1;
            detail := format('the auditor found drift in %s: repair, then audit again until it is clean', r.drifted);
            sql := 'SELECT isomer.repair(projection) FROM isomer.audit_stats WHERE drift > 0;' || E'\n' || 'SELECT isomer.audit();';
        ELSIF r.unaudited > 0 THEN
            status := 'todo'; detail := format('%s of %s projection(s) have not been audited yet', r.unaudited, r.n);
            sql := 'SELECT isomer.audit();';
        ELSE
            status := 'ok'; detail := format('%s projection(s), %s rows checked against the tables, no drift', r.n, r.checks);
        END IF;
        RETURN NEXT;

        -- Tables cutover would leave behind: the source becomes read-only, and they are not copied.
        SELECT array_agg(format('%s (%s)', p.table_name, coalesce(nullif(array_to_string(p.problems, '; '), ''), 'not chosen when attaching'))
                         ORDER BY p.table_name)
        INTO v_lines FROM isomer.preflight(a.conninfo, a.schemas) p
        WHERE NOT (p.table_name = ANY (v_tables)) AND NOT EXISTS (SELECT 1 FROM unnest(p.problems) x WHERE x LIKE 'materialized view%');
        ord := ord + 1; item := 'Tables not copied'; sql := NULL;
        IF v_lines IS NULL THEN
            status := 'ok'; detail := format('every table in %s is copied', array_to_string(a.schemas, ', '));
        ELSE
            status := 'blocked'; v_blocked := v_blocked + 1;
            detail := format('cutover makes the source read-only and these are not copied: %s. Fix each on the source '
                             '(a primary key, usually); IsomerDB copies it within 10 seconds', array_to_string(v_lines, ', '));
        END IF;
        RETURN NEXT;

        SELECT array_agg(t.tbl ORDER BY t.tbl) INTO v_lines FROM public.dblink(v_conn, format(
            'SELECT format(''%%I.%%I'', schemaname, relname) FROM pg_stat_user_tables '
            'WHERE NOT (schemaname = ANY (%L::text[])) AND n_tup_ins + n_tup_upd + n_tup_del > 0', a.schemas)) AS t(tbl text);
        ord := ord + 1; item := 'Writes outside the attached schemas'; sql := NULL;
        IF v_lines IS NULL THEN
            status := 'ok'; detail := format('nothing outside %s has been written to', array_to_string(a.schemas, ', '));
        ELSE
            status := 'todo';
            detail := format('cutover makes the whole source database read-only, and these tables outside %s are written to: %s. '
                             'Attach their schema too, or move what writes to them first', array_to_string(a.schemas, ', '),
                             array_to_string(v_lines, ', '));
        END IF;
        RETURN NEXT;

        ------------------------------------------------------------------ schema the copy does not carry
        phase := 'schema';
        SELECT array_agg(format('CREATE EXTENSION IF NOT EXISTS %I;', t.extname) ORDER BY t.extname)
                   FILTER (WHERE av.name IS NOT NULL),
               array_agg(t.extname ORDER BY t.extname) FILTER (WHERE av.name IS NULL)
        INTO v_sql, v_lines
        FROM public.dblink(v_conn, 'SELECT extname::text FROM pg_extension WHERE extname <> ''plpgsql''') AS t(extname text)
        LEFT JOIN pg_available_extensions av ON av.name = t.extname
        WHERE NOT EXISTS (SELECT 1 FROM pg_extension e WHERE e.extname = t.extname);
        ord := ord + 1; item := 'Extensions'; sql := array_to_string(v_sql, E'\n');
        IF v_sql IS NULL AND v_lines IS NULL THEN
            status := 'ok'; detail := 'every extension the source has is installed here';
        ELSE
            status := 'todo';
            detail := concat_ws('. ',
                CASE WHEN v_sql IS NOT NULL THEN format('%s to install', cardinality(v_sql)) END,
                CASE WHEN v_lines IS NOT NULL THEN format('not available in IsomerDB: %s; check the application does not depend on it',
                                                         array_to_string(v_lines, ', ')) END);
        END IF;
        RETURN NEXT;

        SELECT array_agg(t.def || ';' ORDER BY t.oid), array_agg(t.sig ORDER BY t.oid)
        INTO v_sql, v_lines
        FROM public.dblink(v_conn, format(
            'SELECT p.oid::bigint, p.oid::regprocedure::text, pg_get_functiondef(p.oid) FROM pg_proc p '
            'JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = ANY (%L::text[]) AND p.prokind IN (''f'', ''p'') '
            'AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = ''pg_proc''::regclass AND d.objid = p.oid AND d.deptype = ''e'')',
            a.schemas)) AS t(oid bigint, sig text, def text)
        WHERE to_regprocedure(t.sig) IS NULL;
        ord := ord + 1; item := 'Functions and procedures'; sql := array_to_string(v_sql, E'\n');
        IF v_sql IS NULL THEN
            status := 'ok'; detail := 'none missing';
        ELSE
            status := 'todo'; detail := format('%s missing: %s', cardinality(v_sql), array_to_string(v_lines, ', '));
        END IF;
        RETURN NEXT;

        -- Indexes not backing a constraint (constraints come next, with their own index).
        SELECT array_agg(regexp_replace(t.def, '^CREATE (UNIQUE )?INDEX ', 'CREATE \1INDEX IF NOT EXISTS ') || ';' ORDER BY t.idx),
               array_agg(t.idx ORDER BY t.idx)
        INTO v_sql, v_lines
        FROM public.dblink(v_conn, format(
            'SELECT format(''%%I.%%I'', n.nspname, ic.relname), pg_get_indexdef(i.indexrelid) FROM pg_index i '
            'JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace '
            'WHERE format(''%%I.%%I'', n.nspname, c.relname) = ANY (%L::text[]) AND NOT i.indisprimary '
            'AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid)', v_tables)) AS t(idx text, def text)
        WHERE to_regclass(t.idx) IS NULL;
        ord := ord + 1; item := 'Indexes'; sql := array_to_string(v_sql, E'\n');
        IF v_sql IS NULL THEN
            status := 'ok'; detail := 'every index on the source exists here';
        ELSE
            status := 'todo';
            detail := format('%s missing: %s. Build them before cutover, so the application''s queries are as fast here on day one',
                             cardinality(v_sql), array_to_string(v_lines, ', '));
        END IF;
        RETURN NEXT;

        CREATE TEMP TABLE IF NOT EXISTS isomer_plan_constraints (tbl text, conname text, contype text, def text, ref text) ON COMMIT DROP;
        TRUNCATE isomer_plan_constraints;
        INSERT INTO isomer_plan_constraints
        SELECT t.* FROM public.dblink(v_conn, format(
            'SELECT format(''%%I.%%I'', n.nspname, c.relname), k.conname::text, k.contype::text, pg_get_constraintdef(k.oid), '
            'CASE WHEN k.confrelid <> 0 THEN k.confrelid::regclass::text END '
            'FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace '
            'WHERE format(''%%I.%%I'', n.nspname, c.relname) = ANY (%L::text[]) AND k.contype IN (''u'', ''c'', ''x'', ''f'') AND k.conislocal',
            v_tables)) AS t(tbl text, conname text, contype text, def text, ref text)
        WHERE NOT EXISTS (SELECT 1 FROM pg_constraint lk WHERE lk.conrelid = to_regclass(t.tbl) AND lk.conname = t.conname);

        SELECT array_agg(format('ALTER TABLE %s ADD CONSTRAINT %I %s;', tbl, conname, def) ORDER BY tbl, conname),
               array_agg(format('%s on %s', conname, tbl) ORDER BY tbl, conname)
        INTO v_sql, v_lines FROM isomer_plan_constraints WHERE contype <> 'f';
        ord := ord + 1; item := 'Unique, check and exclusion constraints'; sql := array_to_string(v_sql, E'\n');
        IF v_sql IS NULL THEN
            status := 'ok'; detail := 'none missing';
        ELSE
            status := 'todo'; detail := format('%s missing: %s', cardinality(v_sql), array_to_string(v_lines, ', '));
        END IF;
        RETURN NEXT;

        -- Foreign keys: added NOT VALID, then validated, so adding one never blocks writes for a scan.
        SELECT array_agg(format('ALTER TABLE %s ADD CONSTRAINT %I %s NOT VALID;', tbl, conname, def) || E'\n'
                         || format('ALTER TABLE %s VALIDATE CONSTRAINT %I;', tbl, conname) ORDER BY tbl, conname),
               array_agg(format('%s on %s', conname, tbl) ORDER BY tbl, conname),
               array_agg(DISTINCT ref) FILTER (WHERE NOT (ref = ANY (v_tables)))
        INTO v_sql, v_lines, v_problems FROM isomer_plan_constraints WHERE contype = 'f';
        ord := ord + 1; item := 'Foreign keys'; sql := array_to_string(v_sql, E'\n');
        IF v_sql IS NULL THEN
            status := 'ok'; detail := 'none missing';
        ELSE
            status := CASE WHEN v_problems IS NULL THEN 'todo' ELSE 'blocked' END;
            v_blocked := v_blocked + (v_problems IS NOT NULL)::int;
            detail := format('%s missing: %s', cardinality(v_sql), array_to_string(v_lines, ', '))
                      || CASE WHEN v_problems IS NOT NULL
                              THEN format('. They reference tables that are not copied: %s', array_to_string(v_problems, ', ')) ELSE '' END;
        END IF;
        RETURN NEXT;

        SELECT array_agg(regexp_replace(t.def, '^CREATE (CONSTRAINT )?TRIGGER ', 'CREATE OR REPLACE \1TRIGGER ') || ';' ORDER BY t.tbl, t.tgname),
               array_agg(format('%s on %s (calls %s)', t.tgname, t.tbl, t.fn) ORDER BY t.tbl, t.tgname)
        INTO v_sql, v_lines
        FROM public.dblink(v_conn, format(
            'SELECT format(''%%I.%%I'', n.nspname, c.relname), t.tgname::text, pg_get_triggerdef(t.oid), t.tgfoid::regprocedure::text '
            'FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace '
            'WHERE NOT t.tgisinternal AND format(''%%I.%%I'', n.nspname, c.relname) = ANY (%L::text[])', v_tables))
            AS t(tbl text, tgname text, def text, fn text)
        WHERE NOT EXISTS (SELECT 1 FROM pg_trigger lt WHERE lt.tgrelid = to_regclass(t.tbl) AND lt.tgname = t.tgname);
        ord := ord + 1; item := 'Triggers'; sql := array_to_string(v_sql, E'\n');
        IF v_sql IS NULL THEN
            status := 'ok'; detail := 'none missing';
        ELSE
            status := 'todo';
            detail := format('%s missing: %s. Create their functions first. Replicated rows do not fire them, so they '
                             'start firing for the application''s writes once it writes here', cardinality(v_sql), array_to_string(v_lines, ', '));
        END IF;
        RETURN NEXT;

        -- Views in creation order, which is an order their dependencies allow.
        SELECT array_agg(CASE WHEN t.kind = 'v' THEN format('CREATE OR REPLACE VIEW %s AS%s', t.view, t.def)
                              ELSE format('CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS%s', t.view, rtrim(t.def, ';')) || ';' END
                         ORDER BY t.oid),
               array_agg(t.view || CASE WHEN t.kind = 'm' THEN ' (materialized)' ELSE '' END ORDER BY t.oid)
        INTO v_sql, v_lines
        FROM public.dblink(v_conn, format(
            'SELECT c.oid::bigint, format(''%%I.%%I'', n.nspname, c.relname), c.relkind::text, pg_get_viewdef(c.oid) '
            'FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ANY (%L::text[]) AND c.relkind IN (''v'', ''m'') '
            'AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = ''pg_class''::regclass AND d.objid = c.oid AND d.deptype = ''e'')',
            a.schemas)) AS t(oid bigint, view text, kind text, def text)
        WHERE to_regclass(t.view) IS NULL;
        ord := ord + 1; item := 'Views'; sql := array_to_string(v_sql, E'\n');
        IF v_sql IS NULL THEN
            status := 'ok'; detail := 'none missing';
        ELSE
            status := 'todo';
            detail := format('%s missing: %s', cardinality(v_sql), array_to_string(v_lines, ', '))
                      || CASE WHEN array_to_string(v_lines, ',') LIKE '%(materialized)%'
                              THEN '. A materialized view that is refreshed on a schedule can become a projection instead, kept current by IsomerDB'
                              ELSE '' END;
        END IF;
        RETURN NEXT;

        -- Privileges on the attached tables, and the roles that hold them or connect.
        CREATE TEMP TABLE IF NOT EXISTS isomer_plan_grants (tbl text, grantee text, privs text[]) ON COMMIT DROP;
        TRUNCATE isomer_plan_grants;
        INSERT INTO isomer_plan_grants
        SELECT t.* FROM public.dblink(v_conn, format(
            'SELECT format(''%%I.%%I'', n.nspname, c.relname), r.rolname::text, array_agg(DISTINCT x.privilege_type::text) '
            'FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace CROSS JOIN LATERAL aclexplode(c.relacl) x '
            'JOIN pg_roles r ON r.oid = x.grantee WHERE format(''%%I.%%I'', n.nspname, c.relname) = ANY (%L::text[]) '
            'AND x.grantee <> c.relowner GROUP BY 1, 2', v_tables)) AS t(tbl text, grantee text, privs text[]);
        SELECT array_agg(DISTINCT x.rolname) INTO v_lines FROM (
            SELECT grantee AS rolname FROM isomer_plan_grants
            UNION
            SELECT t.u FROM public.dblink(v_conn,
                'SELECT DISTINCT usename::text FROM pg_stat_activity WHERE datname = current_database() '
                'AND backend_type = ''client backend'' AND pid <> pg_backend_pid() AND usename IS NOT NULL') AS t(u text)
        ) x WHERE NOT EXISTS (SELECT 1 FROM pg_roles lr WHERE lr.rolname = x.rolname);
        SELECT coalesce(array_agg(format('CREATE ROLE %I LOGIN;  -- then: ALTER ROLE %I PASSWORD ''...''', u, u) ORDER BY u), '{}')
               || coalesce((SELECT array_agg(format('GRANT %s ON %s TO %I;', array_to_string(g.privs, ', '), g.tbl, g.grantee)
                                             ORDER BY g.grantee, g.tbl)
                            FROM isomer_plan_grants g
                            WHERE isomer._grant_missing(g.grantee, g.tbl, g.privs)), '{}')
        INTO v_sql FROM unnest(coalesce(v_lines, '{}')) u;
        ord := ord + 1; item := 'Roles and privileges'; sql := nullif(array_to_string(v_sql, E'\n'), '');
        IF sql IS NULL THEN
            status := 'ok'; detail := 'every role the application uses exists here with the same privileges';
        ELSE
            status := 'todo';
            sql := sql || E'\n' || (SELECT string_agg(DISTINCT format('GRANT USAGE ON SCHEMA %I TO %I;', split_part(g.tbl, '.', 1), g.grantee), E'\n')
                                    FROM isomer_plan_grants g);
            detail := concat_ws('. ',
                CASE WHEN v_lines IS NOT NULL THEN format('roles to create: %s', array_to_string(v_lines, ', ')) END,
                format('%s grant(s) to copy', (SELECT count(*) FROM isomer_plan_grants g
                                                WHERE isomer._grant_missing(g.grantee, g.tbl, g.privs))));
        END IF;
        RETURN NEXT;

        SELECT array_agg(format('%s.%s = %s', t.tbl, t.col, t.expr) ORDER BY t.tbl, t.col) INTO v_lines
        FROM public.dblink(v_conn, format(
            'SELECT format(''%%I.%%I'', n.nspname, c.relname), a.attname::text, pg_get_expr(d.adbin, d.adrelid) '
            'FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace '
            'JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum '
            'WHERE a.attgenerated = ''s'' AND format(''%%I.%%I'', n.nspname, c.relname) = ANY (%L::text[])', v_tables))
            AS t(tbl text, col text, expr text);
        IF v_lines IS NOT NULL THEN
            ord := ord + 1; item := 'Generated columns'; status := 'todo'; sql := NULL;
            detail := format('copied as plain values, so IsomerDB will not compute them after cutover: %s. '
                             'Have the application write them, or recreate the column as GENERATED ALWAYS AS (...) STORED',
                             array_to_string(v_lines, '; '));
            RETURN NEXT;
        END IF;

        ------------------------------------------------------------------ cutover
        phase := 'cutover';
        SELECT array_agg(format('%s connection(s) as %s from %s%s', t.n, t.u, coalesce(t.addr, 'a local socket'),
                                CASE WHEN t.app <> '' THEN format(' (%s)', t.app) ELSE '' END) ORDER BY t.n DESC)
        INTO v_lines FROM public.dblink(v_conn,
            'SELECT usename::text, application_name, host(client_addr), count(*) FROM pg_stat_activity '
            'WHERE datname = current_database() AND backend_type = ''client backend'' AND pid <> pg_backend_pid() '
            'AND application_name NOT LIKE ''isomer%'' AND application_name NOT IN (''dblink'', ''pg_cron'') GROUP BY 1, 2, 3')
            AS t(u text, app text, addr text, n bigint);
        ord := ord + 1; item := 'Applications to repoint'; sql := NULL;
        IF v_lines IS NULL THEN
            status := 'info'; detail := 'nothing else is connected to the source right now';
        ELSE
            status := 'todo';
            detail := format('connected to the source now: %s. Each must point at IsomerDB once the source is frozen',
                             array_to_string(v_lines, '; '));
        END IF;
        RETURN NEXT;

        ord := ord + 1; item := 'When to cut over'; status := 'info'; sql := NULL;
        SELECT coalesce(sum(x.writes), 0), coalesce(sum(x.seconds), 0) INTO v_n, v_m
        FROM isomer.attachment_activity x WHERE x.attachment = a.name;
        IF v_m < 60 THEN
            detail := 'no traffic measured yet: IsomerDB samples writes to the attached tables every minute, and names the quietest hour once it has seen a few';
        ELSE
            SELECT x.h, isomer._rate(sum(x.writes), sum(x.seconds)) AS rate, sum(x.writes) / sum(x.seconds) AS per_s
            INTO r FROM (SELECT extract(hour FROM y.hour AT TIME ZONE 'UTC')::int AS h, y.writes, y.seconds
                         FROM isomer.attachment_activity y WHERE y.attachment = a.name) x
            GROUP BY x.h ORDER BY sum(x.writes) / sum(x.seconds), x.h LIMIT 1;
            detail := format('%s on average over %s hour(s) of traffic seen; quietest hour so far %s:00-%s:00 UTC at %s',
                             isomer._rate(v_n, v_m), round((v_m / 3600)::numeric, 1), lpad(r.h::text, 2, '0'),
                             lpad(((r.h + 1) % 24)::text, 2, '0'), r.rate);
            SELECT x.h, isomer._rate(sum(x.writes), sum(x.seconds)) AS rate
            INTO r FROM (SELECT extract(hour FROM y.hour AT TIME ZONE 'UTC')::int AS h, y.writes, y.seconds
                         FROM isomer.attachment_activity y WHERE y.attachment = a.name) x
            GROUP BY x.h ORDER BY sum(x.writes) / sum(x.seconds) DESC, x.h LIMIT 1;
            detail := detail || format(', busiest %s:00 UTC at %s', lpad(r.h::text, 2, '0'), r.rate);
        END IF;
        detail := detail || format('. Writes stop from the freeze until the application points here; catching up takes about %s',
                                   coalesce(round(extract(epoch FROM st.apply_lag)::numeric, 1) || 's', 'as long as the replication lag'));
        RETURN NEXT;

        ord := ord + 1; item := 'Cut over';
        IF v_blocked > 0 THEN
            status := 'blocked'; detail := format('%s item(s) above must be fixed first', v_blocked);
        ELSE
            status := 'todo';
            detail := 'freezes the source, waits for the last writes, copies sequences and defaults, stops replication. '
                      'With rollback_conninfo the source keeps receiving IsomerDB''s writes, so it stays a current fallback';
        END IF;
        sql := format('SELECT * FROM isomer.cutover(%L, rollback_conninfo => %L);', a.name,
                      format('host=<IsomerDB, as the source reaches it> port=5432 dbname=%s user=<a role allowed to replicate> password=<...>',
                             current_database()));
        RETURN NEXT;

        PERFORM public.dblink_disconnect(v_conn);
      EXCEPTION WHEN OTHERS THEN
        IF v_conn = ANY (coalesce(public.dblink_get_connections(), '{}')) THEN
            PERFORM public.dblink_disconnect(v_conn);
        END IF;
        RAISE;
      END;
    END IF;

    ------------------------------------------------------------------ after cutover
    phase := 'after';
    SELECT array_agg(format('SELECT isomer.alter_projection(%L, freshness => ''sync'');', p.name) ORDER BY p.name),
           array_agg(p.name ORDER BY p.name)
    INTO v_sql, v_lines FROM isomer.projections p
    WHERE p.kind IN ('document', 'cache') AND p.freshness = 'bounded'
      AND EXISTS (SELECT 1 FROM isomer.projection_sources ps WHERE ps.projection = p.name AND ps.source_table = ANY (v_tables))
      AND (NOT v_cut OR isomer._check_sources(p.name, 'sync') IS NULL);
    ord := ord + 1; item := 'Same-transaction projections'; sql := array_to_string(v_sql, E'\n');
    IF v_sql IS NULL THEN
        status := 'info'; detail := 'no bounded projections over these tables to move to sync';
    ELSE
        status := 'todo';
        detail := format('attach mode keeps projections bounded; %s once IsomerDB is the primary, so a write and the documents it changes '
                         'commit together: %s', CASE WHEN v_cut THEN 'these can be sync now' ELSE 'these can be sync' END,
                         array_to_string(v_lines, ', '));
    END IF;
    RETURN NEXT;

    -- Tables big enough to shard, by their first primary-key column; small tables they reference
    -- are copied to every node instead, so joins with them stay on one node.
    SELECT array_agg(format('%s (%s)', t.tbl, pg_size_pretty(t.bytes)) ORDER BY t.bytes DESC),
           array_agg(format('SELECT isomer.distribute(ARRAY[%L]::regclass[], %L);', t.tbl, t.pk) ORDER BY t.bytes DESC)
    INTO v_lines, v_sql
    FROM (SELECT x.table_name AS tbl, pg_total_relation_size(to_regclass(x.table_name)) AS bytes,
                 (SELECT ka.attname FROM pg_index i JOIN pg_attribute ka ON ka.attrelid = i.indrelid AND ka.attnum = i.indkey[0]
                  WHERE i.indrelid = to_regclass(x.table_name) AND i.indisprimary) AS pk
          FROM unnest(v_tables) x(table_name)
          WHERE to_regclass(x.table_name) IS NOT NULL AND NOT isomer._is_distributed(to_regclass(x.table_name))) t
    WHERE t.bytes >= shard_min_bytes AND t.pk IS NOT NULL;
    ord := ord + 1; item := 'Sharding'; sql := array_to_string(v_sql, E'\n');
    IF v_sql IS NULL THEN
        status := 'info';
        detail := format('no table is over %s, so one node is enough for now. After cutover, isomer.shard_advice() '
                         'suggests distribution keys from the queries the application actually runs', pg_size_pretty(shard_min_bytes));
    ELSE
        status := 'todo';
        detail := format('over %s: %s. Sharded by primary key here; tables that are joined should share a key '
                         '(isomer.shard_advice and isomer.reshard_plan, after a day of traffic)', pg_size_pretty(shard_min_bytes),
                         array_to_string(v_lines, ', '));
    END IF;
    RETURN NEXT;

    ord := ord + 1; item := 'Retire the source';
    IF v_cut AND a.rollback_status IS NOT NULL THEN
        status := 'todo';
        detail := format('the source is a current fallback (rollback replication %s). Once you no longer need to go back, stop it', a.rollback_status);
        sql := format('-- on the source:%sDROP SUBSCRIPTION isomer_rollback_%s;%s-- here:%sDROP PUBLICATION isomer_rollback_%s;',
                      E'\n', a.name, E'\n', E'\n', a.name);
    ELSIF v_cut THEN
        status := 'info'; detail := 'cut over without rollback replication: the source holds the data as of cutover'; sql := NULL;
    ELSE
        status := 'info'; sql := NULL;
        detail := 'cutting over with rollback_conninfo keeps the source current, so going back is a repoint; drop that replication once you are sure';
    END IF;
    RETURN NEXT;
END $$;

-- The plan as a document: the readiness checks, the gaps with their SQL, the cutover, what follows.
CREATE FUNCTION isomer.cutover_plan_markdown(name text, shard_min_bytes bigint DEFAULT 1024 * 1024 * 1024)
RETURNS text LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    a       isomer.attachments;
    r       record;
    v_out   text;
    v_phase text := '';
    v_block integer;
    v_todo  integer;
BEGIN
    SELECT * INTO a FROM isomer.attachments WHERE attachments.name = cutover_plan_markdown.name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'isomer: no attachment named %', name;
    END IF;
    CREATE TEMP TABLE IF NOT EXISTS isomer_plan_rows (ord integer, phase text, item text, status text, detail text, sql text) ON COMMIT DROP;
    TRUNCATE isomer_plan_rows;
    INSERT INTO isomer_plan_rows SELECT * FROM isomer.cutover_plan(name, shard_min_bytes);
    SELECT count(*) FILTER (WHERE status = 'blocked' AND item <> 'Cut over'), count(*) FILTER (WHERE status = 'todo')
    INTO v_block, v_todo FROM isomer_plan_rows;

    v_out := format(E'# Cutover plan: %s\n\nGenerated %s for the database attached as `%s` (PostgreSQL %s.%s, %s table(s) in %s).\n\n',
                    a.name, to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI "UTC"'), a.name,
                    a.source_version / 10000, a.source_version % 10000,
                    (SELECT count(*) FROM isomer.attached_tables x WHERE x.attachment = a.name), array_to_string(a.schemas, ', '))
          || CASE WHEN a.status = 'cutover' THEN E'IsomerDB is the primary. What remains is below.\n'
                  WHEN v_block > 0 THEN format(E'**%s item(s) block cutover**, and %s step(s) are to do.\n', v_block, v_todo)
                  ELSE format(E'Nothing blocks cutover. %s step(s) are to do; run them in order.\n', v_todo) END;
    FOR r IN SELECT * FROM isomer_plan_rows ORDER BY ord LOOP
        IF r.phase <> v_phase THEN
            v_phase := r.phase;
            v_out := rtrim(v_out, E'\n') || E'\n\n## ' || CASE r.phase WHEN 'before' THEN 'Before cutover'
                                                       WHEN 'schema' THEN 'What the copy does not carry yet'
                                                       WHEN 'cutover' THEN 'Cutover'
                                                       ELSE 'After cutover' END || E'\n\n';
        END IF;
        v_out := v_out || CASE r.status WHEN 'ok' THEN '- [x] ' WHEN 'info' THEN '- ' ELSE '- [ ] ' END
                       || '**' || r.item || '.** '
                       || CASE WHEN r.status <> 'blocked' THEN '' WHEN r.item = 'Cut over' THEN '*Blocked.* ' ELSE '*Blocks cutover.* ' END
                       || upper(left(r.detail, 1)) || substr(r.detail, 2) || E'.\n';
        IF r.sql IS NOT NULL AND r.status <> 'ok' THEN
            v_out := v_out || E'\n  ```sql\n  ' || replace(r.sql, E'\n', E'\n  ') || E'\n  ```\n\n';
        END IF;
    END LOOP;
    RETURN v_out;
END $$;

-- Cutover, unchanged except that it no longer copies a stored generated column's expression as
-- a column default (which Postgres refuses: a default cannot refer to other columns). The column
-- keeps the values replicated so far; the plan's "Generated columns" step says what to do.
CREATE OR REPLACE FUNCTION isomer.cutover(name text, rollback_conninfo text DEFAULT NULL, timeout interval DEFAULT '60 seconds')
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
            'AND (d.adbin IS NOT NULL OR a.attidentity <> '''') AND a.attgenerated = ''''',
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

REVOKE EXECUTE ON FUNCTION isomer.sample_attachments(), isomer.cutover_plan(text, bigint),
                           isomer.cutover_plan_markdown(text, bigint) FROM PUBLIC;

SELECT cron.schedule('isomer-sample-attachments', '* * * * *', 'SELECT isomer.sample_attachments()')
WHERE pg_catalog.citus_is_coordinator();
