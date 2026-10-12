-- IsomerDB 0.19.0: managed Postgres providers (PRD FR-A11).
--
--   * isomer.provider_check(conninfo) recognises the provider an attach is pointed at (Amazon RDS
--     and Aurora, Cloud SQL, Azure Flexible Server, Supabase, Neon, AlloyDB, Crunchy Bridge, and
--     the unsupported Heroku and Tiger Cloud) from the roles and settings each one creates, and
--     checks what attach needs there: logical decoding on, a role that may replicate, a free
--     replication slot and WAL sender, the right to create the publication and, for cutover, to
--     freeze the database. A failed check says what to change in that provider's own terms.
--   * isomer.attach runs it first and refuses with those instructions, before creating anything.
--   * Attach and cutover work as an ordinary role with only what managed providers grant (owner
--     of the database and tables, REPLICATION): tests/managed_source.sh runs them that way.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.19.0'" to load this file. \quit

CREATE FUNCTION isomer.provider_check(conninfo text)
RETURNS TABLE (provider text, item text, ok boolean, blocking boolean, detail text, hint text)
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    s           jsonb;
    v_provider  text;
    v_user      text;
    v_wal       text;
BEGIN
    -- One round trip: what the provider leaves behind, and what this role may do.
    SELECT r.facts INTO s FROM public.dblink(conninfo, $remote$
        SELECT jsonb_build_object(
            'user', current_user,
            'version', current_setting('server_version_num')::int,
            'roles', (SELECT coalesce(jsonb_agg(rolname), '[]') FROM pg_roles WHERE rolname IN (
                         'rds_superuser', 'rds_replication', 'cloudsqlsuperuser', 'azure_pg_admin', 'azuresu',
                         'supabase_admin', 'neon_superuser', 'alloydbsuperuser', 'crunchy_superuser',
                         'tsdbadmin', 'timescaledb_role')),
            'super', (SELECT rolsuper FROM pg_roles WHERE rolname = current_user),
            'replication', (SELECT rolreplication FROM pg_roles WHERE rolname = current_user),
            'in_rds_replication', EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication')
                                  AND pg_has_role(current_user, 'rds_replication', 'member'),
            'wal_level', current_setting('wal_level'),
            'rds_logical', current_setting('rds.logical_replication', true),
            'cloudsql_logical', current_setting('cloudsql.logical_decoding', true),
            'alloydb_logical', current_setting('alloydb.logical_decoding', true),
            'max_slots', current_setting('max_replication_slots')::int,
            'used_slots', (SELECT count(*) FROM pg_replication_slots),
            'max_senders', current_setting('max_wal_senders')::int,
            'used_senders', (SELECT count(*) FROM pg_stat_replication),
            'db_owner', (SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()),
            'can_create', has_database_privilege(current_database(), 'CREATE'),
            'host', inet_server_addr()::text)
    $remote$) AS r(facts jsonb);

    v_user := s ->> 'user';
    v_wal := s ->> 'wal_level';
    v_provider := CASE
        WHEN s -> 'roles' ? 'tsdbadmin' OR s -> 'roles' ? 'timescaledb_role' THEN 'tiger-cloud'
        WHEN s -> 'roles' ? 'rds_superuser' THEN 'rds'
        WHEN s -> 'roles' ? 'cloudsqlsuperuser' THEN 'cloud-sql'
        WHEN s -> 'roles' ? 'alloydbsuperuser' THEN 'alloydb'
        WHEN s -> 'roles' ? 'azure_pg_admin' OR s -> 'roles' ? 'azuresu' THEN 'azure'
        WHEN s -> 'roles' ? 'supabase_admin' THEN 'supabase'
        WHEN s -> 'roles' ? 'neon_superuser' THEN 'neon'
        WHEN s -> 'roles' ? 'crunchy_superuser' THEN 'crunchy-bridge'
        ELSE 'postgres' END;

    provider := v_provider;

    IF v_provider = 'tiger-cloud' THEN
        item := 'provider'; ok := false; blocking := true;
        detail := 'Tiger Cloud (Timescale) does not allow logical replication out of the service';
        hint := 'Attach is not available here; dump and restore into IsomerDB instead.';
        RETURN NEXT;
        RETURN;
    END IF;

    item := 'provider'; ok := true; blocking := false;
    detail := CASE v_provider
        WHEN 'rds' THEN 'Amazon RDS or Aurora (Tier 1)'
        WHEN 'cloud-sql' THEN 'Google Cloud SQL (Tier 1)'
        WHEN 'azure' THEN 'Azure Database for PostgreSQL, Flexible Server (Tier 1)'
        WHEN 'supabase' THEN 'Supabase (Tier 1)'
        WHEN 'neon' THEN 'Neon (Tier 1)'
        WHEN 'alloydb' THEN 'Google AlloyDB (Tier 2)'
        WHEN 'crunchy-bridge' THEN 'Crunchy Bridge (Tier 2)'
        ELSE 'self-managed Postgres, or a provider not recognised' END;
    hint := NULL;
    RETURN NEXT;

    item := 'server version'; ok := (s ->> 'version')::int >= 140000; blocking := NOT ok;
    detail := format('server_version_num %s', s ->> 'version');
    hint := CASE WHEN NOT ok THEN 'Postgres 14 or later is needed.'
                 WHEN (s ->> 'version')::int < 170000 THEN 'Before Postgres 17 the slot is not a failover slot: a source failover drops it and attach re-snapshots.' END;
    RETURN NEXT;

    item := 'logical decoding'; ok := v_wal = 'logical'; blocking := NOT ok;
    detail := format('wal_level = %s', v_wal);
    hint := CASE WHEN ok THEN NULL ELSE CASE v_provider
        WHEN 'rds' THEN 'Set rds.logical_replication = 1 in the DB parameter group (the cluster parameter group for Aurora), then reboot the instance.'
        WHEN 'cloud-sql' THEN 'Set the database flag cloudsql.logical_decoding = on (the instance restarts).'
        WHEN 'alloydb' THEN 'Set the database flag alloydb.logical_decoding = on.'
        WHEN 'azure' THEN 'Set the server parameter wal_level = logical, then restart the server.'
        WHEN 'neon' THEN 'Turn on logical replication in the project''s settings (Settings, Logical Replication).'
        WHEN 'supabase' THEN 'Logical replication is on by default on Supabase; contact support if wal_level is not logical.'
        ELSE 'Set wal_level = logical in postgresql.conf and restart the server.' END END;
    RETURN NEXT;

    item := 'replication role';
    ok := (s ->> 'replication')::boolean OR (s ->> 'super')::boolean OR (s ->> 'in_rds_replication')::boolean;
    blocking := NOT ok;
    detail := format('%s %s', v_user, CASE WHEN ok THEN 'may create replication slots and stream changes' ELSE 'may not replicate' END);
    hint := CASE WHEN ok THEN NULL ELSE CASE v_provider
        WHEN 'rds' THEN format('As the master user: GRANT rds_replication TO %I;', v_user)
        WHEN 'cloud-sql' THEN format('As a cloudsqlsuperuser: ALTER USER %I WITH REPLICATION;', v_user)
        WHEN 'alloydb' THEN format('As an alloydbsuperuser: ALTER USER %I WITH REPLICATION;', v_user)
        WHEN 'azure' THEN format('As the admin user (azure_pg_admin): ALTER ROLE %I WITH REPLICATION;', v_user)
        WHEN 'neon' THEN 'Use a role created in the Neon console or API (neon_superuser members can replicate once logical replication is on).'
        WHEN 'supabase' THEN 'Attach as the postgres role, which may replicate.'
        ELSE format('As a superuser: ALTER ROLE %I WITH REPLICATION;', v_user) END END;
    RETURN NEXT;

    item := 'replication slot'; ok := (s ->> 'used_slots')::int < (s ->> 'max_slots')::int; blocking := NOT ok;
    detail := format('%s of %s slots in use', s ->> 'used_slots', s ->> 'max_slots');
    hint := CASE WHEN ok THEN NULL ELSE 'Drop an unused slot (SELECT * FROM pg_replication_slots), or raise max_replication_slots.' END;
    RETURN NEXT;

    item := 'WAL sender'; ok := (s ->> 'used_senders')::int < (s ->> 'max_senders')::int; blocking := NOT ok;
    detail := format('%s of %s WAL senders in use', s ->> 'used_senders', s ->> 'max_senders');
    hint := CASE WHEN ok THEN NULL ELSE 'Raise max_wal_senders.' END;
    RETURN NEXT;

    item := 'publication'; ok := (s ->> 'can_create')::boolean OR (s ->> 'super')::boolean; blocking := NOT ok;
    detail := CASE WHEN ok THEN 'may create the publication (it must also own the tables published)'
                   ELSE format('%s may not create objects in this database', v_user) END;
    hint := CASE WHEN ok THEN NULL ELSE format('GRANT CREATE ON DATABASE %s TO %I;', '<database>', v_user) END;
    RETURN NEXT;

    -- Cutover freezes the database (ALTER DATABASE ... SET default_transaction_read_only), which
    -- needs its owner. Attach works without it.
    item := 'cutover'; ok := s ->> 'db_owner' = v_user OR (s ->> 'super')::boolean; blocking := false;
    detail := format('the database belongs to %s', s ->> 'db_owner');
    hint := CASE WHEN ok THEN NULL
                 ELSE format('Cutover needs the database''s owner: attach as %s, or ALTER DATABASE ... OWNER TO %I first.', s ->> 'db_owner', v_user) END;
    RETURN NEXT;
END $$;

-- Attach checks the provider first, and refuses with what to change there.
CREATE OR REPLACE FUNCTION isomer.attach(
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
    v_blocked text;
BEGIN
    IF EXISTS (SELECT 1 FROM isomer.attachments a WHERE a.name = attach.name) THEN
        RAISE EXCEPTION 'isomer: attachment % already exists', name;
    END IF;

    SELECT string_agg(format('%s: %s. %s', c.item, c.detail, c.hint), E'\n') INTO v_blocked
    FROM isomer.provider_check(conninfo) c WHERE c.blocking;
    IF v_blocked IS NOT NULL THEN
        RAISE EXCEPTION 'isomer: % cannot be attached yet', name
            USING DETAIL = v_blocked, HINT = 'isomer.provider_check(conninfo) lists every check.';
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

REVOKE EXECUTE ON FUNCTION isomer.provider_check(text) FROM PUBLIC;
