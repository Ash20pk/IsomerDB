#!/bin/bash
# Phase 0 acceptance test: attach to an existing Postgres, keep document and search projections
# fresh from it, and read them through SQL and the MongoDB wire protocol.
#
#   docker compose --profile attach up -d && ./tests/phase0.sh
#
# Each check maps to a PRD requirement (FR-xx). The script exits non-zero on the first failure.
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
SRC="docker compose exec -T source psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
NET="$(basename "$PWD" | tr '[:upper:]' '[:lower:]')_default"
MONGO=(docker run --rm --network "$NET" mongo:8 mongosh
       "mongodb://isomer:isomer@gateway:10260/?tls=true&tlsAllowInvalidCertificates=true" --quiet)
CONN="host=source port=5432 dbname=postgres user=postgres password=source"

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
# Poll until a command prints the expected value, for up to $4 seconds.
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=${4:-10} got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 0.5
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}
mongo_eval() { "${MONGO[@]}" --eval "$1" 2>/dev/null | tail -1; }

echo "== reset"
$ISO -c "SELECT isomer.drop_projection(name) FROM isomer.projections;" >/dev/null
$ISO -c "SELECT isomer.detach(name, drop_tables => true) FROM isomer.attachments;" >/dev/null
$ISO -c "DELETE FROM isomer.events;" >/dev/null
$SRC <<'SQL'
DROP PUBLICATION IF EXISTS isomer_shop;
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'isomer_shop';
DROP TABLE IF EXISTS customers, orders, order_items, audit_log, scratch, coupons CASCADE;
CREATE TABLE customers   (id bigint PRIMARY KEY, name text NOT NULL, tier text);
CREATE TABLE orders      (id bigint PRIMARY KEY, customer_id bigint NOT NULL, status text, note text);
CREATE TABLE order_items (order_id bigint, customer_id bigint NOT NULL, sku text, qty int, price numeric,
                          PRIMARY KEY (order_id, sku));
CREATE TABLE audit_log   (at timestamptz, message text);            -- no primary key
CREATE UNLOGGED TABLE scratch (id int PRIMARY KEY);                 -- not replicable
INSERT INTO customers VALUES (42, 'Asha Rao', 'gold'), (7, 'Ben Ross', 'silver');
INSERT INTO orders VALUES (9001, 42, 'shipped', 'quiet mechanical keyboard'),
                          (9002, 7, 'pending', 'wireless gaming mouse');
INSERT INTO order_items VALUES (9001, 42, 'KB-01', 1, 89.00), (9002, 7, 'MS-02', 2, 25.50);
SQL

echo "== FR-A8 preflight"
expect "table without a primary key is refused" "f" \
  "$($ISO -c "SELECT ok FROM isomer.preflight('$CONN') WHERE table_name = 'public.audit_log'")"
expect "unlogged table is refused" "f" \
  "$($ISO -c "SELECT ok FROM isomer.preflight('$CONN') WHERE table_name = 'public.scratch'")"
expect "keyed tables pass" "3" \
  "$($ISO -c "SELECT count(*) FROM isomer.preflight('$CONN') WHERE ok")"

echo "== FR-A1/A2/A7 attach"
expect "attach reports 3 attached, 2 skipped" "attached=3 skipped=2" \
  "$($ISO -c "SELECT string_agg(status || '=' || n, ' ' ORDER BY status) FROM (SELECT status, count(*) n FROM isomer.attach('shop', '$CONN') GROUP BY status) s")"
expect "publication lists tables explicitly (not FOR ALL TABLES)" "f" \
  "$($SRC -c "SELECT puballtables FROM pg_publication WHERE pubname = 'isomer_shop'")"
eventually "initial copy finished" "3/3" \
  "$ISO -c \"SELECT tables_ready || '/' || tables_total FROM isomer.attachment_status('shop')\"" 30
expect "rows copied" "2" "$($ISO -c "SELECT count(*) FROM public.orders")"

echo "== FR-A5 sync projections are refused in attach mode"
out=$($ISO -c "SELECT isomer.create_projection(name => 'x', kind => 'document', key => 'id', freshness => 'sync',
  query => 'SELECT id, jsonb_build_object(''n'', name) AS doc FROM customers', sources => '{\"customers\": \"id\"}')" 2>&1 || true)
case "$out" in *"cannot be sync"*) pass "sync refused with a reason";; *) fail "sync refused" "cannot be sync" "$out";; esac

echo "== FR-S1/D1 document projection over attached tables"
$ISO <<'SQL'
SELECT isomer.create_projection(
  name => 'customer_profile', kind => 'document', key => 'id', lag => '2 seconds', database => 'shop',
  query => $q$
    SELECT c.id, jsonb_build_object('name', c.name, 'tier', c.tier,
      'orders', coalesce((SELECT jsonb_agg(jsonb_build_object('id', o.id, 'status', o.status,
          'items', coalesce((SELECT jsonb_agg(jsonb_build_object('sku', i.sku, 'qty', i.qty) ORDER BY i.sku)
                             FROM order_items i WHERE i.order_id = o.id), '[]'::jsonb)) ORDER BY o.id)
        FROM orders o WHERE o.customer_id = c.id), '[]'::jsonb)) AS doc
    FROM customers c $q$,
  sources => '{"customers": "id", "orders": "customer_id", "order_items": "customer_id"}');
SQL
expect "documents built on create" "2" "$(mongo_eval 'db.getSiblingDB("shop").customer_profile.countDocuments()')"
out=$($ISO -c "SELECT isomer.create_projection(name => 'y', kind => 'document', key => 'id',
  query => 'SELECT c.id, jsonb_build_object(''n'', (SELECT count(*) FROM orders o WHERE o.customer_id = c.id)) AS doc FROM customers c',
  sources => '{\"customers\": \"id\"}')" 2>&1 || true)
case "$out" in *"does not map"*) pass "unmapped source table refused (completeness check)";; *) fail "completeness check" "does not map" "$out";; esac

echo "== FR-Q1 search projection over attached tables"
$ISO <<'SQL'
SELECT isomer.create_projection(
  name => 'order_search', kind => 'search', key => 'id', lag => '2 seconds',
  query => $q$ SELECT o.id, c.name, o.status, o.note FROM orders o JOIN customers c ON c.id = o.customer_id $q$);
SQL
expect "search finds by note" "9001" "$($ISO -c "SELECT id FROM public.order_search WHERE note @@@ 'keyboard'")"

echo "== writes on the source reach both projections within budget"
$SRC <<'SQL'
INSERT INTO orders VALUES (9003, 42, 'pending', 'ergonomic split keyboard');
INSERT INTO order_items VALUES (9003, 42, 'KB-09', 1, 149.00);
UPDATE orders SET status = 'delivered' WHERE id = 9001;
DELETE FROM order_items WHERE order_id = 9002; DELETE FROM orders WHERE id = 9002; DELETE FROM customers WHERE id = 7;
SQL
eventually "document updated (insert + update)" "9001:delivered,9003:pending" \
  "mongo_eval 'print(db.getSiblingDB(\"shop\").customer_profile.findOne({_id: 42}).orders.map(o => o.id + \":\" + o.status).join(\",\"))'" 10
eventually "document deleted" "0" \
  "mongo_eval 'db.getSiblingDB(\"shop\").customer_profile.countDocuments({_id: 7})'" 10
eventually "search sees new and updated rows" "9001,9003" \
  "$ISO -c \"SELECT string_agg(id::text, ',' ORDER BY id) FROM public.order_search WHERE note @@@ 'keyboard'\"" 10
eventually "search drops deleted rows" "0" \
  "$ISO -c \"SELECT count(*) FROM public.order_search WHERE id = 9002\"" 10
expect "no lag-budget breaches" "0" \
  "$($ISO -c "SELECT coalesce(sum(budget_breaches), 0) FROM isomer.projection_status")"

echo "== FR-A4 schema changes on the source"
$SRC -c "ALTER TABLE customers ADD COLUMN email text; INSERT INTO customers VALUES (8, 'Cleo Diaz', 'gold', 'cleo@example.com');"
eventually "added column applied here and row replicated" "cleo@example.com" \
  "$ISO -c \"SELECT email FROM public.customers WHERE id = 8\"" 25
eventually "new row reaches the document projection" "1" \
  "mongo_eval 'db.getSiblingDB(\"shop\").customer_profile.countDocuments({_id: 8})'" 10

echo "== FR-A7 new tables on the source are added"
$SRC -c "CREATE TABLE coupons (code text PRIMARY KEY, pct int); INSERT INTO coupons VALUES ('WELCOME', 10);"
eventually "new table attached and copied" "10" \
  "$ISO -c \"SELECT pct FROM public.coupons WHERE code = 'WELCOME'\"" 30

echo "== FR-A9 slot health is visible"
expect "slot active, wal ok" "true reserved" \
  "$($ISO -c "SELECT slot_active || ' ' || wal_status FROM isomer.attachment_status('shop')")"

echo "== guards and dump"
out=$($ISO -c "DROP TABLE public.orders" 2>&1 || true)
case "$out" in *"read by projection"*) pass "dropping a projection's source is refused";; *) fail "drop guard" "read by projection" "$out";; esac
expect "dump_projections returns both definitions" "2" "$($ISO -c "SELECT count(*) FROM isomer.dump_projections()")"
expect "no errors logged" "0" "$($ISO -c "SELECT count(*) FROM isomer.events WHERE level = 'error'")"

echo "== phase 0 passed"
