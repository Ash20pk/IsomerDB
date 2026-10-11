-- IsomerDB 0.13.0: cluster-consistent restore points (PRD FR-CL1, backups).
--
--   * isomer.create_restore_point(label) creates a named restore point on the coordinator and
--     every worker at the same moment (citus_create_restore_point blocks distributed commits for
--     that instant), so restoring every node to it gives a consistent cluster. The name carries
--     its UTC time, which a restore uses to pick each node's base backup.
--   * isomer.restore_points records them. It is ordinary table data, so a restored cluster knows
--     the points that existed when it was saved.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.13.0'" to load this file. \quit

CREATE TABLE isomer.restore_points (
    name       text PRIMARY KEY,
    lsn        pg_lsn NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
SELECT pg_catalog.pg_extension_config_dump('isomer.restore_points', '');

CREATE FUNCTION isomer.create_restore_point(label text DEFAULT NULL)
RETURNS text LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    v_name text := 'isomer-' || to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYYMMDD"T"HH24MISS"Z"')
                   || coalesce('-' || nullif(regexp_replace(label, '[^a-zA-Z0-9_.-]', '_', 'g'), ''), '');
    v_lsn  pg_lsn;
BEGIN
    IF length(v_name) > 63 THEN
        RAISE EXCEPTION 'isomer: restore point name % is longer than 63 characters; use a shorter label', v_name;
    END IF;
    -- Recorded first, in its own committed row, so the point itself is in every restore to it.
    PERFORM public.dblink_exec(format('dbname=%s user=postgres', quote_literal(current_database())),
        format('INSERT INTO isomer.restore_points (name, lsn) VALUES (%L, pg_current_wal_lsn())', v_name));
    SELECT pg_catalog.citus_create_restore_point(v_name) INTO v_lsn;
    PERFORM public.dblink_exec(format('dbname=%s user=postgres', quote_literal(current_database())),
        format('UPDATE isomer.restore_points SET lsn = %L WHERE name = %L', v_lsn, v_name));
    RETURN v_name;
END $$;

REVOKE EXECUTE ON FUNCTION isomer.create_restore_point(text) FROM PUBLIC;
