#!/bin/bash
# Smoke-test a running local stack (docker compose up): every base extension is installed,
# Citus distributes a table, pg_search and pgvector answer queries, pg_trickle maintains a
# stream table, and the MongoDB gateway accepts a write and a read.
set -euo pipefail

PG="docker compose exec -T db psql -U postgres -d postgres -v ON_ERROR_STOP=1 -X -q -t -A"

echo "== extensions"
$PG -c "SELECT string_agg(extname || ' ' || extversion, ', ' ORDER BY extname) FROM pg_extension
        WHERE extname IN ('citus','documentdb','documentdb_core','documentdb_distributed','pg_search','vector','pg_trickle');"

echo "== citus: distributed table"
$PG <<'SQL'
DROP TABLE IF EXISTS smoke_orders CASCADE;
CREATE TABLE smoke_orders (id bigint, customer_id bigint, status text, note text, embedding vector(3));
SELECT create_distributed_table('smoke_orders', 'customer_id');
INSERT INTO smoke_orders VALUES
  (1, 42, 'shipped', 'quiet mechanical keyboard', '[1,0,0]'),
  (2, 42, 'pending', 'loud gaming mouse',          '[0,1,0]'),
  (3, 7,  'shipped', 'mechanical pencil set',      '[0.9,0.1,0]');
SELECT 'shards=' || count(*) FROM citus_shards WHERE table_name = 'smoke_orders'::regclass;
SQL

echo "== pgvector: nearest neighbour"
$PG -c "SELECT id FROM smoke_orders ORDER BY embedding <=> '[1,0,0]' LIMIT 1;"

echo "== pg_search: BM25 on a distributed table"
$PG <<'SQL'
CREATE INDEX smoke_orders_bm25 ON smoke_orders USING bm25 (id, note) WITH (key_field = 'id');
SELECT string_agg(id::text, ',' ORDER BY id) FROM smoke_orders WHERE note @@@ 'mechanical';
SQL

echo "== pg_trickle: stream table"
$PG <<'SQL'
DROP TABLE IF EXISTS smoke_local CASCADE;
CREATE TABLE smoke_local (id bigint PRIMARY KEY, status text);
INSERT INTO smoke_local VALUES (1,'shipped'), (2,'pending'), (3,'shipped');
SELECT pgtrickle.create_stream_table('smoke_status_counts',
  'SELECT status, count(*) AS n FROM smoke_local GROUP BY status', refresh_mode => 'IMMEDIATE');
INSERT INTO smoke_local VALUES (4,'shipped');
SELECT string_agg(status || '=' || n, ',' ORDER BY status) FROM smoke_status_counts;
SQL

echo "== gateway: MongoDB wire"
docker run --rm --network "$(basename "$PWD" | tr '[:upper:]' '[:lower:]')_default" mongo:8 \
  mongosh "mongodb://isomer:isomer@gateway:10260/?tls=true&tlsAllowInvalidCertificates=true&authMechanism=SCRAM-SHA-256" \
  --quiet --eval 'db = db.getSiblingDB("smoke"); db.items.drop(); db.items.insertOne({_id: 1, name: "Asha", tier: "gold"}); printjson(db.items.findOne({_id: 1}))'

echo "== all checks passed"
