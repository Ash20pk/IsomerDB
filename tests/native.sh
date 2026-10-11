#!/bin/bash
# Native DDL for projections (FR-S1a): CREATE MATERIALIZED VIEW ... USING isomer_sync|isomer_bounded,
# a unique index for the key, options in a COMMENT, and the lifecycle around them.
#
#   docker compose up -d && ./tests/native.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
NET="$(basename "$PWD" | tr '[:upper:]' '[:lower:]')_default"
MONGO=(docker run --rm --network "$NET" mongo:8 mongosh
       "mongodb://isomer:isomer@gateway:10260/?tls=true&tlsAllowInvalidCertificates=true" --quiet)

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }
mongo_eval() { "${MONGO[@]}" --eval "$1" 2>&1 | tail -1; }

echo "== reset"
$ISO <<'SQL' >/dev/null
DROP SCHEMA IF EXISTS shopn CASCADE;
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name = 'customer_card';
CREATE SCHEMA shopn;
CREATE TABLE shopn.plans (tier text PRIMARY KEY, discount numeric);
CREATE TABLE shopn.customers (id bigint PRIMARY KEY, name text, tier text);
CREATE TABLE shopn.orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL, status text);
CREATE TABLE shopn.order_items (order_id bigint, customer_id bigint NOT NULL, sku text, qty int, PRIMARY KEY (order_id, sku));
INSERT INTO shopn.plans VALUES ('gold', 0.1), ('silver', 0.05);
INSERT INTO shopn.customers VALUES (1, 'Asha', 'gold'), (2, 'Ben', 'silver');
INSERT INTO shopn.orders VALUES (10, 1, 'shipped'), (11, 2, 'open');
INSERT INTO shopn.order_items VALUES (10, 1, 'KB-01', 1), (11, 2, 'MS-02', 2);
SQL

echo "== declare a projection with ordinary DDL"
out=$($ISO 2>&1 <<'SQL'
SET client_min_messages = notice;
CREATE MATERIALIZED VIEW shopn.customer_card USING isomer_bounded AS
  SELECT c.id, jsonb_build_object(
           'name', c.name,
           'discount', (SELECT p.discount FROM shopn.plans p WHERE p.tier = c.tier),
           'orders', coalesce((SELECT jsonb_agg(jsonb_build_object('id', o.id, 'status', o.status,
               'items', (SELECT jsonb_agg(jsonb_build_object('sku', i.sku, 'qty', i.qty)) FROM shopn.order_items i WHERE i.order_id = o.id))
             ORDER BY o.id) FROM shopn.orders o WHERE o.customer_id = c.id), '[]'::jsonb)) AS doc
  FROM shopn.customers c;
COMMENT ON MATERIALIZED VIEW shopn.customer_card IS 'isomer {"lag": "3 seconds", "sources": {"shopn.order_items": "customer_id"}}';
CREATE UNIQUE INDEX ON shopn.customer_card (id);
SQL
)
contains "the unique index registers the projection" "document projection customer_card: 2 documents" "$out"
contains "an unconnected table is treated as a lookup, with a notice" "shopn.plans treated as lookup" "$out"
expect "freshness comes from the access method, lag from the comment" "bounded|00:00:03" \
  "$($ISO -c "SELECT freshness || '|' || lag_budget FROM isomer.projections WHERE name = 'customer_card'")"
expect "mappings: inferred through joins, overridden by the comment, lookup for the rest" \
  'shopn.customers=id shopn.order_items=customer_id shopn.orders=customer_id shopn.plans=' \
  "$($ISO -c "SELECT string_agg(source_table || '=' || coalesce(key_column, ''), ' ' ORDER BY source_table) FROM isomer.projection_sources WHERE projection = 'customer_card'")"
expect "the view keeps no rows itself" "f" "$($ISO -c "SELECT relispopulated FROM pg_class WHERE oid = 'shopn.customer_card'::regclass")"
expect "the view's schema is the MongoDB database" "KB-01" \
  "$(mongo_eval 'print(db.getSiblingDB("shopn").customer_card.findOne({_id: 1}).orders[0].items[0].sku)')"
expect "the certificate is valid" "true" "$($ISO -c "SELECT isomer.certificate('customer_card') ->> 'valid'")"

echo "== writes, and changing freshness and lag with DDL"
$ISO -c "INSERT INTO shopn.order_items VALUES (10, 1, 'CB-07', 3)" -c "SELECT isomer.wait_for(isomer.last_write_token(), 'customer_card') IS NOT NULL" >/dev/null
expect "a write to a comment-mapped table reaches the document" "2" \
  "$(mongo_eval 'print(db.getSiblingDB("shopn").customer_card.findOne({_id: 1}).orders[0].items.length)')"
$ISO -c "UPDATE shopn.plans SET discount = 0.2 WHERE tier = 'gold'" -c "SELECT isomer.wait_for(isomer.last_write_token(), 'customer_card') IS NOT NULL" >/dev/null
expect "a write to a lookup table re-derives the documents" "0.2" \
  "$(mongo_eval 'print(db.getSiblingDB("shopn").customer_card.findOne({_id: 1}).discount)')"
$ISO -c "ALTER MATERIALIZED VIEW shopn.customer_card SET ACCESS METHOD isomer_sync" >/dev/null
expect "ALTER ... SET ACCESS METHOD switches to sync" "sync" "$($ISO -c "SELECT freshness FROM isomer.projections WHERE name = 'customer_card'")"
$ISO -c "UPDATE shopn.customers SET name = 'Asha Rao' WHERE id = 1" >/dev/null
expect "and the next write is applied in its transaction" "Asha Rao" "$(mongo_eval 'print(db.getSiblingDB("shopn").customer_card.findOne({_id: 1}).name)')"
$ISO -c "COMMENT ON MATERIALIZED VIEW shopn.customer_card IS 'isomer {\"lag\": \"5 seconds\", \"sources\": {\"shopn.order_items\": \"customer_id\"}}'" >/dev/null
expect "a new comment changes the lag budget" "00:00:05" "$($ISO -c "SELECT lag_budget FROM isomer.projections WHERE name = 'customer_card'")"

echo "== pg_dump keeps the declaration; re-creating it re-registers"
dump=$(docker compose exec -T db pg_dump -U postgres -s -n shopn postgres)
contains "the dump sets the access method" "default_table_access_method = isomer_sync" "$dump"
contains "the dump keeps the comment" "isomer {" "$dump"
contains "the dump keeps the key index" "CREATE UNIQUE INDEX customer_card_id_idx" "$dump"
# Simulate a restore: lose the derived objects, then replay the index from the dump.
$ISO <<'SQL' >/dev/null
SELECT isomer._drop_source_triggers('customer_card');
SELECT documentdb_api.drop_collection('shopn', 'customer_card');
DROP INDEX shopn.customer_card_id_idx;
SQL
$ISO -c "CREATE UNIQUE INDEX customer_card_id_idx ON shopn.customer_card USING btree (id)" >/dev/null
expect "replaying the index rebuilds the collection" "Asha Rao" "$(mongo_eval 'print(db.getSiblingDB("shopn").customer_card.findOne({_id: 1}).name)')"
expect "and the projection equals recomputation" "0" "$($ISO -c "SELECT count(*) FROM isomer.verify('customer_card')")"

echo "== DROP MATERIALIZED VIEW drops the projection"
$ISO -c "DROP MATERIALIZED VIEW shopn.customer_card" >/dev/null
expect "the projection is gone" "0" "$($ISO -c "SELECT count(*) FROM isomer.projections WHERE name = 'customer_card'")"
expect "and so is its collection" "0" "$(mongo_eval 'print(db.getSiblingDB("shopn").getCollectionNames().filter(n => n === "customer_card").length)')"

echo "== native syntax passed"
