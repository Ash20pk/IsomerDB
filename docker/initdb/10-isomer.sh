#!/bin/bash
# First-boot setup of a single-node IsomerDB cluster: the coordinator also holds shards.
set -euo pipefail

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
SQL

if [ -n "${ISOMER_MONGO_USER:-}" ] && [ -n "${ISOMER_MONGO_PASSWORD:-}" ]; then
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -c \
    "SELECT documentdb_api.create_user('{\"createUser\":\"${ISOMER_MONGO_USER}\", \"pwd\":\"${ISOMER_MONGO_PASSWORD}\", \"roles\":[{\"role\":\"readWriteAnyDatabase\",\"db\":\"admin\"}, {\"role\":\"clusterAdmin\",\"db\":\"admin\"}]}');"
fi
