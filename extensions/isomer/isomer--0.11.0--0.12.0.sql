-- IsomerDB 0.12.0: IsomerDB's own connections to workers authenticate like Citus's.
--
--   * isomer._workers() adds citus.node_conninfo to each worker's connection string, so when the
--     cluster uses TLS and certificate authentication between nodes (as the Kubernetes operator
--     sets up), the coordinator's dblink connections that drain workers' queues use them too.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.12.0'" to load this file. \quit

CREATE OR REPLACE FUNCTION isomer._workers()
RETURNS TABLE (nodeid integer, conninfo text)
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT n.nodeid,
           concat_ws(' ', format('host=%s port=%s dbname=%s user=postgres', n.nodename, n.nodeport, current_database()),
                     nullif(current_setting('citus.node_conninfo', true), ''))
    FROM pg_dist_node n
    WHERE n.isactive AND n.noderole = 'primary' AND n.groupid <> 0
$$;
