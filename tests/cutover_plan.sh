#!/bin/bash
# FR-A6 acceptance test: the generated cutover plan. A source with what attach does not copy
# (indexes, constraints, foreign keys, a trigger and its function, views, an extension, a role
# and its grants, a generated column, a table with no primary key), an application connected to
# it, and a projection over it. The plan must find each gap; applying the plan's own SQL must close
# them; after cutover the copy must behave like the source did.
#
#   docker compose --profile attach up -d && ./tests/cutover_plan.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
SRC="docker compose exec -T source psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
CONN="host=source port=5432 dbname=postgres user=postgres password=source"
BACK="host=db port=5432 dbname=postgres user=postgres password=isomer"

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=${4:-15} got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 0.5
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}
plan() { $ISO -c "SELECT $1 FROM isomer.cutover_plan('shop') WHERE item = '$2'"; }

echo "== reset"
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name = 'shop_customer';
SELECT isomer.detach(name, drop_tables => true) FROM isomer.attachments WHERE name = 'shop' AND status <> 'cutover';
DELETE FROM isomer.attachments WHERE name = 'shop';
DROP PUBLICATION IF EXISTS isomer_rollback_shop;
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'isomer_rollback_shop' AND NOT active;
DROP SCHEMA IF EXISTS shop CASCADE;
DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'shop_app') THEN DROP OWNED BY shop_app; DROP ROLE shop_app; END IF; END $$;
DROP EXTENSION IF EXISTS citext;
SQL
$SRC <<'SQL' >/dev/null
SET default_transaction_read_only = off;
ALTER DATABASE postgres RESET default_transaction_read_only;
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'shop-api';
DROP SUBSCRIPTION IF EXISTS isomer_rollback_shop;
DROP PUBLICATION IF EXISTS isomer_shop;
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'isomer_shop';
DROP SCHEMA IF EXISTS shop CASCADE;
DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'shop_app') THEN DROP OWNED BY shop_app; DROP ROLE shop_app; END IF; END $$;
SQL
$SRC <<'SQL' >/dev/null
CREATE EXTENSION IF NOT EXISTS citext;
CREATE SCHEMA shop;
CREATE TABLE shop.customers (id bigserial PRIMARY KEY, email text NOT NULL CONSTRAINT customers_email_key UNIQUE, name text);
CREATE TABLE shop.orders (
  id          bigserial PRIMARY KEY,
  customer_id bigint NOT NULL REFERENCES shop.customers (id),
  total       numeric NOT NULL CONSTRAINT orders_total_check CHECK (total >= 0),
  total_cents bigint GENERATED ALWAYS AS ((total * 100)::bigint) STORED,
  status      text NOT NULL DEFAULT 'open',
  updated_at  timestamptz);
CREATE INDEX orders_customer_idx ON shop.orders (customer_id);
CREATE INDEX customers_lower_name_idx ON shop.customers (lower(name));
CREATE TABLE shop.audit_log (at timestamptz DEFAULT now(), message text);
CREATE FUNCTION shop.touch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at := clock_timestamp(); RETURN NEW; END $$;
CREATE TRIGGER orders_touch BEFORE UPDATE ON shop.orders FOR EACH ROW EXECUTE FUNCTION shop.touch();
CREATE VIEW shop.big_orders AS SELECT o.id, c.email, o.total FROM shop.orders o JOIN shop.customers c ON c.id = o.customer_id WHERE o.total > 100;
CREATE MATERIALIZED VIEW shop.totals AS SELECT customer_id, sum(total) AS total FROM shop.orders GROUP BY customer_id;
CREATE ROLE shop_app LOGIN PASSWORD 'shop';
GRANT USAGE ON SCHEMA shop TO shop_app;
GRANT SELECT, INSERT, UPDATE ON shop.customers, shop.orders TO shop_app;
INSERT INTO shop.customers (email, name) SELECT 'c' || g || '@example.com', 'Customer ' || g FROM generate_series(1, 30) g;
INSERT INTO shop.orders (customer_id, total) SELECT 1 + g % 30, g * 7 FROM generate_series(1, 90) g;
SQL
# The application, connected to the source.
docker compose exec -d -e PGAPPNAME=shop-api -e PGPASSWORD=shop source \
  psql -h 127.0.0.1 -U shop_app -d postgres -c "SELECT pg_sleep(600)"

echo "== attach, and the plan finds every gap"
$ISO -c "SELECT count(*) FROM isomer.attach('shop', '$CONN', schemas => '{shop}')" >/dev/null
eventually "attached and copied" "2/2" "$ISO -c \"SELECT tables_ready || '/' || tables_total FROM isomer.attachment_status('shop')\"" 30
$ISO <<'SQL' >/dev/null
SELECT isomer.create_projection(
  name => 'shop_customer', kind => 'document', key => 'id', database => 'shop', lag => '2 seconds',
  query => $q$ SELECT c.id, jsonb_build_object('email', c.email, 'orders', (SELECT count(*) FROM shop.orders o WHERE o.customer_id = c.id)) AS doc
              FROM shop.customers c $q$,
  sources => '{"shop.customers": "id", "shop.orders": "customer_id"}');
SQL
expect "the copy is ready" "ok" "$(plan status 'Initial copy')"
expect "replication is healthy" "ok" "$(plan status 'Replication')"
expect "an unaudited projection is a step" "todo" "$(plan status 'Projections audited')"
expect "a table with no primary key blocks cutover" "blocked" "$(plan status 'Tables not copied')"
contains "  and is named, with why" "shop.audit_log (no primary key" "$(plan detail 'Tables not copied')"
expect "a missing extension" "CREATE EXTENSION IF NOT EXISTS citext;" "$(plan sql 'Extensions')"
contains "the trigger's function" "shop.touch()" "$(plan detail 'Functions and procedures')"
contains "indexes" "orders_customer_idx" "$(plan detail 'Indexes')"
contains "  including expression indexes" "CREATE INDEX IF NOT EXISTS customers_lower_name_idx ON shop.customers USING btree (lower(name));" "$(plan sql 'Indexes')"
contains "unique and check constraints" "customers_email_key on shop.customers, orders_total_check on shop.orders" "$(plan detail 'Unique, check and exclusion constraints')"
contains "foreign keys, added NOT VALID then validated" "REFERENCES shop.customers(id) NOT VALID;" "$(plan sql 'Foreign keys')"
contains "triggers" "orders_touch on shop.orders (calls shop.touch())" "$(plan detail 'Triggers')"
contains "views, and materialized views" "shop.big_orders, shop.totals (materialized)" "$(plan detail 'Views')"
contains "roles and grants" "roles to create: shop_app" "$(plan detail 'Roles and privileges')"
contains "  with the source's privileges" "GRANT INSERT, SELECT, UPDATE ON shop.orders TO shop_app;" "$(plan sql 'Roles and privileges')"
contains "generated columns" "shop.orders.total_cents = ((total * (100)::numeric))::bigint" "$(plan detail 'Generated columns')"
contains "the application to repoint" "1 connection(s) as shop_app from" "$(plan detail 'Applications to repoint')"
expect "cutover is blocked" "blocked" "$(plan status 'Cut over')"
contains "the projection can be sync after cutover" "SELECT isomer.alter_projection('shop_customer', freshness => 'sync');" "$(plan sql 'Same-transaction projections')"
contains "large tables get a shard key" "SELECT isomer.distribute(ARRAY['shop.orders']::regclass[], 'id');" \
  "$($ISO -c "SELECT sql FROM isomer.cutover_plan('shop', shard_min_bytes => 1) WHERE item = 'Sharding'")"

echo "== traffic: the quietest hour"
$ISO -c "SELECT isomer.sample_attachments()" >/dev/null
$SRC -c "INSERT INTO shop.orders (customer_id, total) SELECT 1 + g % 30, g FROM generate_series(1, 120) g" >/dev/null
# Pretend the last sample was 60 seconds ago, so the 120 writes are spread over a minute.
$ISO -c "UPDATE isomer.attachment_activity_cursor SET at = clock_timestamp() - interval '60 seconds' WHERE attachment = 'shop'"
$ISO -c "SELECT isomer.sample_attachments()" >/dev/null
contains "the write rate is measured" "2.00 writes/s on average" "$(plan detail 'When to cut over')"
contains "  and the quietest hour named" "quietest hour so far $(date -u +%H):00" "$(plan detail 'When to cut over')"

echo "== apply the plan"
$SRC -c "ALTER TABLE shop.audit_log ADD COLUMN id bigserial PRIMARY KEY" >/dev/null
eventually "the fixed table is copied" "ok" "$ISO -c \"SELECT status FROM isomer.cutover_plan('shop') WHERE item = 'Tables not copied'\"" 30
$ISO -c "SELECT sql FROM isomer.cutover_plan('shop') WHERE phase = 'schema' AND status <> 'ok' AND sql IS NOT NULL ORDER BY ord" | $ISO >/dev/null
$ISO -c "SELECT isomer.audit()" >/dev/null
expect "nothing is missing any more" "Extensions|ok Foreign keys|ok Functions and procedures|ok Generated columns|todo Indexes|ok Roles and privileges|ok Triggers|ok Unique, check and exclusion constraints|ok Views|ok" \
  "$($ISO -c "SELECT string_agg(item || '|' || status, ' ' ORDER BY item) FROM isomer.cutover_plan('shop') WHERE phase = 'schema'")"
expect "the projection was audited" "ok" "$(plan status 'Projections audited')"
expect "nothing blocks cutover" "todo" "$(plan status 'Cut over')"
md=$($ISO -c "SELECT isomer.cutover_plan_markdown('shop')")
contains "the plan as a document" "# Cutover plan: shop" "$md"
contains "  says it is ready" "Nothing blocks cutover." "$md"
contains "  with the command" "SELECT * FROM isomer.cutover('shop', rollback_conninfo =>" "$md"

echo "== cut over, and the copy behaves like the source"
$ISO -c "SELECT count(*) FROM isomer.cutover('shop', rollback_conninfo => '$BACK')" >/dev/null
expect "cut over" "cutover" "$($ISO -c "SELECT status FROM isomer.attachments WHERE name = 'shop'")"
out=$($ISO -c "INSERT INTO shop.orders (customer_id, total) VALUES (999, 1)" 2>&1 || true)
contains "the foreign key holds" "violates foreign key constraint \"orders_customer_id_fkey\"" "$out"
out=$($ISO -c "INSERT INTO shop.customers (email) VALUES ('c1@example.com')" 2>&1 || true)
contains "the unique constraint holds" "customers_email_key" "$out"
out=$($ISO -c "INSERT INTO shop.orders (customer_id, total) VALUES (1, -5)" 2>&1 || true)
contains "the check constraint holds" "orders_total_check" "$out"
expect "the trigger fires for writes made here" "t" \
  "$($ISO -c "UPDATE shop.orders SET status = 'shipped' WHERE id = 1 RETURNING updated_at IS NOT NULL")"
expect "the application's role has its privileges" "30" "$($ISO -c "SET ROLE shop_app; SELECT count(*) FROM shop.customers")"
out=$($ISO -c "SET ROLE shop_app; DELETE FROM shop.orders" 2>&1 || true)
contains "  and no more" "permission denied for table orders" "$out"
expect "the view works" "t" "$($ISO -c "SELECT count(*) > 0 FROM shop.big_orders")"
expect "the plan now looks ahead" "Cut over|ok Retire the source|todo Same-transaction projections|todo Sharding|info" \
  "$($ISO -c "SELECT string_agg(item || '|' || status, ' ' ORDER BY item) FROM isomer.cutover_plan('shop')")"
$ISO -c "SELECT sql FROM isomer.cutover_plan('shop') WHERE item = 'Same-transaction projections'" | $ISO >/dev/null
expect "  and its projection step works" "sync" "$($ISO -c "SELECT freshness FROM isomer.projections WHERE name = 'shop_customer'")"

$SRC -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'shop-api'" >/dev/null
echo "all cutover plan tests passed"
