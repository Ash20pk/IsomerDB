#!/bin/bash
# First-boot setup. ISOMER_ROLE=coordinator (default) sets up the coordinator, which also holds
# shards; ISOMER_ROLE=worker sets up a Citus worker node (extensions only; the coordinator adds it).
set -euo pipefail

# Nodes connect to each other with the cluster password (dev setup; use certificates and
# pg_dist_authinfo in production). Citus moves a shard as its table's owner, and DocumentDB's
# collection tables belong to documentdb_admin_role, so that role needs the password too (set
# below, on the coordinator; Citus propagates it to the workers).
printf '%s\n' "*:*:*:postgres:${POSTGRES_PASSWORD}" "*:*:*:documentdb_admin_role:${POSTGRES_PASSWORD}" > "$HOME/.pgpass"
chmod 600 "$HOME/.pgpass"

if [ "${ISOMER_ROLE:-coordinator}" = "worker" ]; then
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<'SQL'
CREATE EXTENSION IF NOT EXISTS citus;
CREATE EXTENSION IF NOT EXISTS documentdb_distributed CASCADE;
CREATE EXTENSION IF NOT EXISTS documentdb_extended_rum;
CREATE EXTENSION IF NOT EXISTS pg_search;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE;
GRANT USAGE ON SCHEMA documentdb_core, documentdb_api_catalog, documentdb_api, documentdb_api_internal TO PUBLIC;
CREATE EXTENSION IF NOT EXISTS dblink;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
-- Columnar storage, for analytics projections.
CREATE EXTENSION IF NOT EXISTS citus_columnar;
-- Workers run IsomerDB's capture triggers on their shards and queue keys locally; the
-- coordinator pulls the queues and does all the scheduled work.
CREATE EXTENSION IF NOT EXISTS isomer;
SELECT cron.unschedule(jobname) FROM cron.job WHERE jobname LIKE 'isomer-%';
SQL
  exit 0
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<'SQL'
CREATE EXTENSION IF NOT EXISTS citus;
CREATE EXTENSION IF NOT EXISTS documentdb_distributed CASCADE;
CREATE EXTENSION IF NOT EXISTS documentdb_extended_rum;
CREATE EXTENSION IF NOT EXISTS pg_search;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE;

SELECT citus_set_coordinator_host('localhost', 5432);
SELECT citus_set_node_property('localhost', 5432, 'shouldhaveshards', true);
SELECT documentdb_api_distributed.initialize_cluster();

-- DocumentDB's planner hooks resolve its own schemas for every query (sharded tables, and any
-- set-returning function in FROM, even generate_series), so every role needs USAGE on them.
-- Objects inside keep their own privileges: DocumentDB has no SECURITY DEFINER functions, so this
-- gives no access to its data.
GRANT USAGE ON SCHEMA documentdb_core, documentdb_api_catalog, documentdb_api, documentdb_api_internal TO PUBLIC;

CREATE EXTENSION IF NOT EXISTS dblink;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
-- Columnar storage, for analytics projections.
CREATE EXTENSION IF NOT EXISTS citus_columnar;
CREATE EXTENSION IF NOT EXISTS isomer;
-- Bounded projections are drained every second; attachments are health-checked every 10s.
SELECT cron.schedule('isomer-drain', '1 seconds', 'SELECT isomer.drain()') WHERE citus_is_coordinator();
SELECT cron.schedule('isomer-check-attachments', '10 seconds', 'SELECT isomer.check_attachments()') WHERE citus_is_coordinator();
SQL

# Shard moves connect as the table's owner (see .pgpass above).
psql -v ON_ERROR_STOP=1 -v pw="${POSTGRES_PASSWORD}" --username "$POSTGRES_USER" --dbname postgres <<'SQL'
ALTER ROLE documentdb_admin_role PASSWORD :'pw';
SQL

# The name worker nodes use to reach this coordinator (compose sets it; single-node can stay local).
if [ -n "${ISOMER_COORDINATOR_HOST:-}" ]; then
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -c "SELECT citus_set_coordinator_host('${ISOMER_COORDINATOR_HOST}', 5432)"
fi

if [ -n "${ISOMER_MONGO_USER:-}" ] && [ -n "${ISOMER_MONGO_PASSWORD:-}" ]; then
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -c \
    "SELECT documentdb_api.create_user('{\"createUser\":\"${ISOMER_MONGO_USER}\", \"pwd\":\"${ISOMER_MONGO_PASSWORD}\", \"roles\":[{\"role\":\"readWriteAnyDatabase\",\"db\":\"admin\"}, {\"role\":\"clusterAdmin\",\"db\":\"admin\"}]}');"
fi
