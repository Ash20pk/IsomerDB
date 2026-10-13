#!/bin/bash
# Incremental maintenance (FR-P6, FR-S5) and the strategy cost model (FR-S7): projections that
# aggregate a sharded table through joins, kept from row deltas folded into their own state.
# Runs on a single node and on the cluster; with worker nodes it also checks that deltas reach
# the coordinator exactly once, including after a pull is interrupted.
#
#   docker compose --profile attach up -d && ./tests/incremental.sh
#   docker compose --profile cluster up -d && ./tests/incremental.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
ISO_NOFAIL="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A"
NET="$(basename "$PWD" | tr '[:upper:]' '[:lower:]')_default"
MONGO=(docker run --rm --network "$NET" mongo:8 mongosh
       "mongodb://isomer:isomer@gateway:10260/?tls=true&tlsAllowInvalidCertificates=true" --quiet)

pass() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"
  echo "-- state at failure --"
  $ISO_NOFAIL -c "SELECT 'deltas', projection, count(*) FROM isomer.deltas GROUP BY projection" \
              -c "SELECT 'pending', projection, count(*) FROM isomer.pending GROUP BY projection" \
              -c "SELECT level, subject, left(message, 200) FROM isomer.events ORDER BY id DESC LIMIT 5" 2>&1 | sed 's/^/       /'
  exit 1
}
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=${4:-30} got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 1
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}
drift() { $ISO -c "SELECT count(*) FROM isomer.verify('$1')"; }
MULTI=$($ISO -c "SELECT count(*) > 0 FROM pg_dist_node WHERE groupid <> 0 AND isactive")

echo "== reset"
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection(name) FROM isomer.projections;
DELETE FROM isomer.events; DELETE FROM isomer.drift_samples; DELETE FROM isomer.audit_suspects;
DROP SCHEMA IF EXISTS ic CASCADE;
DROP PROCEDURE IF EXISTS public.ic_writer(int, int);
CREATE SCHEMA ic;
CREATE TABLE ic.categories (id int PRIMARY KEY, name text NOT NULL);
CREATE TABLE ic.products (id int PRIMARY KEY, category_id int, title text);
CREATE TABLE ic.lines (order_id bigint NOT NULL, line int NOT NULL, product_id int, qty int, price numeric, status text,
                       PRIMARY KEY (order_id, line));
CREATE TABLE ic.customers (id bigint PRIMARY KEY, name text, tier text);
CREATE TABLE ic.orders (id bigint NOT NULL, customer_id bigint NOT NULL, total numeric, paid boolean, PRIMARY KEY (customer_id, id));
SELECT create_reference_table('ic.categories');
SELECT create_distributed_table('ic.products', 'id');
SELECT create_distributed_table('ic.lines', 'order_id', colocate_with => 'none');
SELECT create_distributed_table('ic.customers', 'id', colocate_with => 'none');
-- Orders are sharded by customer but deliberately not co-located with customers.
SELECT create_distributed_table('ic.orders', 'customer_id', colocate_with => 'ic.lines');
INSERT INTO ic.categories SELECT g, 'category ' || g FROM generate_series(1, 6) g;
INSERT INTO ic.products SELECT g, 1 + g % 6, 'product ' || g FROM generate_series(1, 120) g;
INSERT INTO ic.lines SELECT 1 + g / 4, g % 4, 1 + g % 120, 1 + g % 5, (g % 19) + 0.25, CASE WHEN g % 9 = 0 THEN 'void' ELSE 'paid' END
FROM generate_series(1, 20000) g;
INSERT INTO ic.customers SELECT g, 'customer ' || g, CASE WHEN g % 3 = 0 THEN 'gold' ELSE 'std' END FROM generate_series(1, 400) g;
INSERT INTO ic.orders SELECT g, 1 + g % 350, g % 41, g % 4 <> 0 FROM generate_series(1, 4000) g;
CREATE TABLE ic.notes (customer_id bigint, body text);   -- no primary key
-- Plain tables, not sharded: what a sync incremental projection reads.
CREATE TABLE ic.shelf (id int PRIMARY KEY, aisle int NOT NULL);
CREATE TABLE ic.stock (shelf_id int NOT NULL, sku int NOT NULL, qty int NOT NULL, PRIMARY KEY (shelf_id, sku));
INSERT INTO ic.shelf SELECT g, 1 + g % 8 FROM generate_series(1, 200) g;
INSERT INTO ic.stock SELECT 1 + g % 200, g, 1 + g % 7 FROM generate_series(1, 5000) g;
ANALYZE ic.categories, ic.products, ic.lines, ic.customers, ic.orders;
SQL

CAT_SALES='SELECT c.id, jsonb_build_object(
    '"'"'name'"'"', c.name, '"'"'units'"'"', sum(l.qty), '"'"'revenue'"'"', sum(l.qty * l.price), '"'"'lines'"'"', count(*),
    '"'"'top_price'"'"', max(l.price), '"'"'low_price'"'"', min(l.price), '"'"'avg_price'"'"', avg(l.price),
    '"'"'big'"'"', count(*) FILTER (WHERE l.qty >= 4)) AS doc
  FROM ic.lines l JOIN ic.products p ON p.id = l.product_id JOIN ic.categories c ON c.id = p.category_id
  WHERE l.status = '"'"'paid'"'"' GROUP BY c.id'

echo "== FR-S7 the cost model picks a strategy, and says why"
contains "lines reach a category only through products: incremental" "incremental|ic.lines reach the key only through a join" \
  "$($ISO -c "SELECT strategy || '|' || reason FROM isomer.choose_strategy(\$q\$$CAT_SALES\$q\$, 'id', 'document', 'bounded', '{\"ic.categories\": \"id\", \"ic.products\": \"category_id\", \"ic.lines\": null}')")"
contains "grouping a sharded table by another column: incremental (every shard, every write)" "it asks every shard" \
  "$($ISO -c "SELECT strategy || '|' || reason FROM isomer.choose_strategy('SELECT l.product_id, sum(l.qty) AS value FROM ic.lines l GROUP BY l.product_id', 'product_id', 'cache', 'bounded', '{\"ic.lines\": \"product_id\"}')")"
contains "no mappings needed when incremental" "incremental|no key mappings were given" \
  "$($ISO -c "SELECT strategy || '|' || reason FROM isomer.choose_strategy(\$q\$$CAT_SALES\$q\$, 'id', 'document')")"
contains "lines joined to products by product: incremental (a repartition per key)" "incremental|re-deriving one key reshuffles rows" \
  "$($ISO -c "SELECT strategy || '|' || reason FROM isomer.choose_strategy('SELECT p.id, sum(l.qty) AS value FROM ic.lines l JOIN ic.products p ON p.id = l.product_id GROUP BY p.id', 'id', 'cache', 'bounded', '{\"ic.lines\": \"product_id\", \"ic.products\": \"id\"}')")"
contains "per-order totals on the order's own shard: key recompute (cheap)" "recompute|" \
  "$($ISO -c "SELECT strategy || '|' FROM isomer.choose_strategy('SELECT l.order_id, sum(l.qty) AS value FROM ic.lines l GROUP BY l.order_id', 'order_id', 'cache', 'bounded', '{\"ic.lines\": \"order_id\"}')")"
contains "a nested document (jsonb_agg) fits, kept as written" "incremental|no key mappings were given" \
  "$($ISO -c "SELECT strategy || '|' || reason FROM isomer.choose_strategy('SELECT o.customer_id, jsonb_build_object(''ids'', jsonb_agg(o.id)) AS doc FROM ic.orders o GROUP BY o.customer_id', 'customer_id', 'document')")"
contains "but not over a table with no primary key to tell its rows apart" "ic.notes has no primary key" \
  "$($ISO -c "SELECT reason FROM isomer.choose_strategy('SELECT n.customer_id, jsonb_build_object(''notes'', jsonb_agg(n.body)) AS doc FROM ic.notes n GROUP BY n.customer_id', 'customer_id', 'document')")"
contains "a volatile grouping expression is refused" "groups by a volatile expression" \
  "$($ISO -c "SELECT reason FROM isomer.choose_strategy('SELECT (random() * 3)::int AS b, count(*) AS value FROM ic.orders o GROUP BY 1', 'b', 'cache')")"
contains "a join landing on a non-unique column is refused, with the reason" "not a unique key there" \
  "$($ISO_NOFAIL -c "SELECT isomer.create_projection('ic_bad', 'SELECT p.category_id, sum(p.id) AS value FROM ic.products p JOIN ic.lines l ON l.product_id = p.id GROUP BY p.category_id', 'cache', 'category_id', strategy => 'incremental')" 2>&1)"
contains "sync and incremental: refused over sharded tables, with the reason" "a statement across its shards runs their triggers over parallel connections" \
  "$($ISO_NOFAIL -c "SELECT isomer.create_projection('ic_bad', \$q\$$CAT_SALES\$q\$, 'document', 'id', freshness => 'sync', strategy => 'incremental')" 2>&1)"

echo "== FR-P6 a projection aggregating a sharded table through two joins"
contains "created without key mappings, maintained incrementally" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('ic_cat_sales', \$q\$$CAT_SALES\$q\$, 'document', 'id')")"
expect "it equals the query" "0" "$(drift ic_cat_sales)"
expect "the state is per product, not per line" "120" "$($ISO -c "SELECT count(*) FROM isomer_state.$($ISO -c "SELECT isomer._engine_prefix('ic_cat_sales')")_p")"

$ISO <<'SQL' >/dev/null
INSERT INTO ic.lines SELECT 6000 + g / 3, g % 3, 1 + (g * 7) % 120, 1 + g % 6, (g % 23) + 0.5, 'paid' FROM generate_series(1, 900) g;
UPDATE ic.lines SET qty = qty + 2 WHERE order_id % 17 = 0;
UPDATE ic.lines SET status = 'void' WHERE order_id % 23 = 0;
UPDATE ic.lines SET status = 'paid' WHERE order_id % 29 = 0;
DELETE FROM ic.lines WHERE price = (SELECT max(price) FROM ic.lines);    -- the current maximum leaves
UPDATE ic.products SET category_id = 1 + (category_id + 1) % 6 WHERE id % 5 = 0;   -- products move
UPDATE ic.categories SET name = name || ' (renamed)' WHERE id = 3;        -- reference table: one change, not one per node
DELETE FROM ic.products WHERE id = 7;
INSERT INTO ic.categories VALUES (7, 'category 7');
UPDATE ic.products SET category_id = 7 WHERE id IN (11, 13);
SQL
eventually "after inserts, updates, deletes and moves, it equals the query" "0" "drift ic_cat_sales"
expect "the renamed category shows" "category 3 (renamed)" "$($ISO -c "SELECT isomer.get('ic_cat_sales', '3') ->> 'name'")"
expect "the new category has the moved products' sales" \
  "$($ISO -c "SELECT sum(l.qty) FROM ic.lines l JOIN ic.products p ON p.id = l.product_id WHERE p.category_id = 7 AND l.status = 'paid'")" \
  "$($ISO -c "SELECT isomer.get('ic_cat_sales', '7') ->> 'units'")"
expect "MongoDB clients read the same document" \
  "$($ISO -c "SELECT (isomer.get('ic_cat_sales', '2') ->> 'revenue')::numeric::float8")" \
  "$("${MONGO[@]}" --eval 'print(db.getSiblingDB("isomer").ic_cat_sales.findOne({_id: 2}).revenue)' 2>&1 | tail -1)"

echo "== state distributed across the cluster"
if [ "$MULTI" = "t" ]; then
  contains "sales per category, its state on every node" "its state distributed across the cluster" \
    "$($ISO -c "SELECT isomer.create_projection('ic_cat_dist', \$q\$$CAT_SALES\$q\$, 'document', 'id', state => 'distributed')")"
  expect "it equals the query" "0" "$(drift ic_cat_dist)"
  DPFX=$($ISO -c "SELECT isomer._engine_prefix('ic_cat_dist')")
  expect "its partial aggregates are sharded across the nodes" "t" \
    "$($ISO -c "SELECT count(DISTINCT pl.groupid) >= 2 FROM pg_dist_shard sh JOIN pg_dist_placement pl USING (shardid) WHERE sh.logicalrelid = 'isomer_state.${DPFX}_p'::regclass")"
  expect "the joined tables' columns are on every node" "2" \
    "$($ISO -c "SELECT count(*) FROM pg_dist_partition WHERE partmethod = 'n' AND logicalrelid::text LIKE 'isomer_state.${DPFX}_s%'")"
  contains "the certificate says how it is kept" "distributed state" "$($ISO -c "SELECT isomer.certificate('ic_cat_dist') -> 'rules'")"
  expect "and checks it" "true" "$($ISO -c "SELECT isomer.certificate('ic_cat_dist') ->> 'valid'")"
  $ISO -c "UPDATE ic.lines SET qty = qty + 1 WHERE order_id % 13 = 0" -c "UPDATE ic.products SET category_id = 3 WHERE id IN (11, 12)" \
       -c "UPDATE ic.categories SET name = 'renamed again' WHERE id = 4" -c "DELETE FROM ic.lines WHERE order_id BETWEEN 500 AND 520" >/dev/null
  eventually "writes to every table reach it" "0" "drift ic_cat_dist"
  contains "moved to the coordinator" "state on the coordinator" "$($ISO -c "SELECT isomer.alter_projection('ic_cat_dist', state => 'coordinator')")"
  expect "where it still equals the query" "0" "$(drift ic_cat_dist)"
  contains "and back to the cluster" "state on the cluster" "$($ISO -c "SELECT isomer.alter_projection('ic_cat_dist', state => 'distributed')")"
  $ISO -c "UPDATE ic.lines SET qty = qty + 1 WHERE order_id % 19 = 0" >/dev/null
  eventually "where it still follows writes" "0" "drift ic_cat_dist"
  $ISO -c "UPDATE isomer_state.$($ISO -c "SELECT isomer._engine_prefix('ic_cat_dist')")_p SET __n = __n + 7 WHERE product_id = 2" >/dev/null
  $ISO -c "SELECT isomer.repair('ic_cat_dist', ARRAY(SELECT id::text FROM ic.categories))" >/dev/null
  expect "repair builds its state again from the tables" "0" "$(drift ic_cat_dist)"
else
  contains "on one node there is nothing to distribute it across" "no worker nodes" \
    "$($ISO_NOFAIL -c "SELECT isomer.create_projection('ic_bad', \$q\$$CAT_SALES\$q\$, 'document', 'id', state => 'distributed')" 2>&1)"
fi

echo "== nested documents, DISTINCT, grouping expressions"
# Each customer with their orders, newest first: jsonb_agg kept as written over a copy of orders.
CUST_DOCS='SELECT c.id, jsonb_build_object('"'"'name'"'"', c.name, '"'"'orders'"'"',
    coalesce(jsonb_agg(jsonb_build_object('"'"'id'"'"', o.id, '"'"'total'"'"', o.total) ORDER BY o.id DESC) FILTER (WHERE o.id IS NOT NULL), '"'"'[]'"'"'),
    '"'"'paid_states'"'"', count(DISTINCT o.paid)) AS doc
  FROM ic.customers c LEFT JOIN ic.orders o ON o.customer_id = c.id GROUP BY c.id'
contains "customers with their orders nested" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('ic_cust_docs', \$q\$$CUST_DOCS\$q\$, 'document', 'id')")"
expect "it equals the query" "0" "$(drift ic_cust_docs)"
# Each order's lines with their product and category (read from the joined tables), DISTINCT over
# the lines' own column and over a joined one.
ORDER_DOCS='SELECT l.order_id, jsonb_build_object(
    '"'"'lines'"'"', jsonb_agg(jsonb_build_object('"'"'line'"'"', l.line, '"'"'product'"'"', p.title, '"'"'category'"'"', c.name, '"'"'qty'"'"', l.qty) ORDER BY l.line),
    '"'"'products'"'"', count(DISTINCT l.product_id), '"'"'categories'"'"', count(DISTINCT p.category_id), '"'"'units'"'"', sum(l.qty)) AS doc
  FROM ic.lines l JOIN ic.products p ON p.id = l.product_id JOIN ic.categories c ON c.id = p.category_id GROUP BY l.order_id'
contains "orders with their lines nested" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('ic_order_docs', \$q\$$ORDER_DOCS\$q\$, 'document', 'order_id', strategy => 'incremental')")"
expect "it equals the query" "0" "$(drift ic_order_docs)"
expect "each kind of aggregate is kept as it needs" "keep,distinct,keep,sum" \
  "$($ISO -c "SELECT string_agg(g ->> 'cls', ',' ORDER BY (g ->> 'i')::int) FROM isomer.engine_plans e, jsonb_array_elements(e.plan -> 'aggs') g WHERE e.projection = 'ic_order_docs'")"
contains "grouped by an expression, with a median" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('ic_status', 'SELECT upper(l.status) AS status, jsonb_build_object(''lines'', count(*), ''median_price'', percentile_disc(0.5) WITHIN GROUP (ORDER BY l.price), ''products'', count(DISTINCT l.product_id)) AS value FROM ic.lines l GROUP BY upper(l.status)', 'cache', 'status', strategy => 'incremental')")"
expect "it equals the query" "0" "$(drift ic_status)"
$ISO -c "UPDATE ic.orders SET total = total + 1 WHERE customer_id = 7" -c "DELETE FROM ic.orders WHERE customer_id = 8 AND id % 2 = 0" \
     -c "UPDATE ic.products SET title = 'retitled' WHERE id = 3" -c "UPDATE ic.lines SET status = 'held' WHERE order_id = 10" >/dev/null
eventually "nested orders follow writes" "0" "drift ic_cust_docs"
eventually "nested lines follow a joined table's change" "0" "drift ic_order_docs"
eventually "a new group appears under its expression" "1" "$ISO -c \"SELECT count(*) FROM public.ic_status WHERE status = 'HELD'\""
contains "the certificate names the rule for aggregates kept as written" "kept as written" "$($ISO -c "SELECT isomer.certificate('ic_order_docs') -> 'rules'")"
echo "== sync incremental"
STOCK='SELECT s.aisle, jsonb_build_object('"'"'units'"'"', sum(k.qty), '"'"'skus'"'"', count(DISTINCT k.sku), '"'"'fullest'"'"', max(k.qty)) AS doc
  FROM ic.stock k JOIN ic.shelf s ON s.id = k.shelf_id GROUP BY s.aisle'
contains "stock per aisle, sync and incremental" "(sync), maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('ic_sync', \$q\$$STOCK\$q\$, 'document', 'aisle', freshness => 'sync', strategy => 'incremental')")"
expect "a write is in the document before the transaction ends" "t" \
  "$($ISO -c "BEGIN" -c "INSERT INTO ic.stock VALUES (1, 900001, 50)" -c "SELECT (isomer.get('ic_sync', '2') ->> 'units')::int = (SELECT sum(k.qty) FROM ic.stock k JOIN ic.shelf s ON s.id = k.shelf_id WHERE s.aisle = 2)" -c "ROLLBACK" | tail -1)"
$ISO -c "UPDATE ic.shelf SET aisle = 3 WHERE id BETWEEN 1 AND 20" >/dev/null
expect "a joined table's change, the moment it commits" "0" "$(drift ic_sync)"
$ISO -c "UPDATE ic.stock SET qty = qty + 1 WHERE sku % 3 = 0" -c "DELETE FROM ic.stock WHERE sku % 11 = 0" >/dev/null
expect "statements touching every aisle, the moment they commit" "0" "$(drift ic_sync)"

echo "== read your writes"
CAT=$($ISO -c "SELECT category_id FROM ic.products WHERE id = 5")
got=$($ISO <<SQL
INSERT INTO ic.lines VALUES (99999, 1, 5, 40, 2, 'paid');
SELECT isomer.wait_for(isomer.last_write_token(), 'ic_cat_sales', '10 seconds') IS NOT NULL;
SELECT isomer.get('ic_cat_sales', '$CAT') ->> 'units';
SQL
)
expect "a write is visible after wait_for" \
  "t $($ISO -c "SELECT sum(l.qty) FROM ic.lines l JOIN ic.products p ON p.id = l.product_id WHERE p.category_id = $CAT AND l.status = 'paid'")" \
  "$(echo $got)"

echo "== shapes the cluster can't run as written, global aggregates, HAVING"
contains "a LEFT JOIN between tables that aren't co-located is still maintained" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('ic_cust_orders', 'SELECT c.id, count(o.*) AS value FROM ic.customers c LEFT JOIN ic.orders o ON o.customer_id = c.id AND o.paid GROUP BY c.id', 'cache', 'id')")"
contains "and why" "cannot run this query as written" "$($ISO -c "SELECT strategy_reason FROM isomer.projections WHERE name = 'ic_cust_orders'")"
expect "it is checked against the tables through a fresh derivation" "t" "$($ISO -c "SELECT truth_query IS NOT NULL FROM isomer.projections WHERE name = 'ic_cust_orders'")"
expect "customers with no orders count 0" "0" "$($ISO -c "SELECT value FROM public.ic_cust_orders WHERE id = 399")"
contains "a global aggregate under a constant key" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('ic_totals', 'SELECT ''all''::text AS id, jsonb_build_object(''orders'', count(*), ''revenue'', sum(o.total), ''paid'', bool_and(o.paid), ''any_paid'', bool_or(o.paid), ''largest'', max(o.total)) AS doc FROM ic.orders o', 'document', 'id')")"
contains "HAVING keeps only busy tiers" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('ic_tiers', 'SELECT c.tier, jsonb_build_object(''orders'', count(*)) AS doc FROM ic.orders o JOIN ic.customers c ON c.id = o.customer_id GROUP BY c.tier HAVING count(*) > 1000', 'document', 'tier')")"
$ISO <<'SQL' >/dev/null
INSERT INTO ic.orders SELECT g, 1 + g % 400, g % 53, g % 3 = 0 FROM generate_series(4001, 5500) g;
UPDATE ic.orders SET paid = NOT paid WHERE id % 7 = 0;
DELETE FROM ic.orders WHERE customer_id = 12;
UPDATE ic.customers SET tier = 'gold' WHERE id % 10 = 1;
DELETE FROM ic.customers WHERE id = 5;
INSERT INTO ic.customers VALUES (401, 'newcomer', 'bronze');
SQL
eventually "the LEFT JOIN cache equals the tables" "0" "drift ic_cust_orders"
eventually "the global aggregate equals the query" "0" "drift ic_totals"
eventually "the HAVING projection equals the query" "0" "drift ic_tiers"
expect "a new customer appears with 0" "0" "$($ISO -c "SELECT value FROM public.ic_cust_orders WHERE id = 401")"

echo "== concurrent writers"
$ISO <<'SQL' >/dev/null
CREATE PROCEDURE public.ic_writer(seed int, n int) LANGUAGE plpgsql AS $$
DECLARE
    o bigint;
    v int;
BEGIN
    PERFORM setseed(seed / 100.0);
    FOR i IN 1 .. n LOOP
        -- Citus refuses volatile functions inside writes to distributed tables: pick values first.
        o := 1 + (random() * 5000)::int;
        v := 1 + (random() * 119)::int;
        CASE (random() * 5)::int
            WHEN 0 THEN INSERT INTO ic.lines VALUES (200000 + seed * 10000 + i, 1, v, 1 + i % 4, (i % 20) + 0.75, 'paid');
            WHEN 1 THEN UPDATE ic.lines SET qty = qty + 1 WHERE order_id = o;
            WHEN 2 THEN DELETE FROM ic.lines WHERE order_id = o AND line = 0;
            WHEN 3 THEN UPDATE ic.products SET category_id = 1 + i % 7 WHERE id = v;
            ELSE UPDATE ic.lines SET status = CASE WHEN status = 'paid' THEN 'void' ELSE 'paid' END WHERE order_id = o;
        END CASE;
        COMMIT;
    END LOOP;
END $$;
SQL
pids=()
for w in 1 2 3 4; do $ISO -c "CALL public.ic_writer($w, 150)" >/dev/null & pids+=($!); done
for pid in "${pids[@]}"; do wait "$pid" || fail "a writer failed" "exit 0" "exit $?"; done
eventually "after 600 concurrent transactions, every key equals the query" "0" "drift ic_cat_sales" 60
eventually "and every nested order" "0" "drift ic_order_docs" 60
eventually "and every expression group" "0" "drift ic_status" 60
if [ "$MULTI" = "t" ]; then
  eventually "and the distributed state" "0" "drift ic_cat_dist" 60
fi


echo "== TRUNCATE rebuilds the state"
$ISO -c "TRUNCATE ic.orders" -c "INSERT INTO ic.orders VALUES (1, 1, 10, true), (2, 2, 20, false)" >/dev/null
eventually "the LEFT JOIN cache after a truncate" "0" "drift ic_cust_orders"
eventually "the global aggregate after a truncate" "0" "drift ic_totals"
eventually "the nested orders after a truncate" "0" "drift ic_cust_docs"
expect "two orders remain" "2" "$($ISO -c "SELECT isomer.get('ic_totals', 'all') ->> 'orders'")"

# Checked above; the auditor below samples every projection, so they go first.
$ISO -c "SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name IN ('ic_cust_docs', 'ic_order_docs', 'ic_status', 'ic_sync', 'ic_cat_dist')" >/dev/null

echo "== FR-S5 the certificate names the rules"
expect "valid" "true" "$($ISO -c "SELECT isomer.certificate('ic_cat_sales') ->> 'valid'")"
contains "states the aggregation rule and its condition" "ic.lines is pre-aggregated by (product_id)" "$($ISO -c "SELECT isomer.certificate('ic_cat_sales') -> 'rules'")"
contains "states exactly-once delivery" "exactly once" "$($ISO -c "SELECT isomer.certificate('ic_cat_sales') -> 'rules'")"
$ISO -c "SET citus.enable_unsafe_triggers = on" -c "ALTER TABLE ic.products DISABLE TRIGGER isomer_ic_cat_sales" >/dev/null
expect "a disabled capture trigger invalidates it" "false" "$($ISO -c "SELECT isomer.certificate('ic_cat_sales') ->> 'valid'")"
$ISO -c "SELECT isomer._create_source_triggers('ic_cat_sales')" >/dev/null
expect "recreating the triggers restores it" "true" "$($ISO -c "SELECT isomer.certificate('ic_cat_sales') ->> 'valid'")"

echo "== the auditor and repair"
PFX=$($ISO -c "SELECT isomer._engine_prefix('ic_cat_sales')")
$ISO -c "UPDATE isomer_state.${PFX}_p SET __n = __n + 1000 WHERE product_id = 1" \
     -c "UPDATE ic.lines SET qty = qty WHERE order_id = (SELECT min(order_id) FROM ic.lines WHERE product_id = 1)" \
     -c "INSERT INTO ic.lines VALUES (99998, 1, 1, 1, 1, 'paid')" >/dev/null
eventually "corrupted state reaches the documents and the auditor pins it" "t" \
  "$ISO -c \"SELECT isomer.audit(500) >= 0 AND EXISTS (SELECT 1 FROM isomer.drift_samples WHERE projection = 'ic_cat_sales')\"" 90
$ISO -c "SELECT isomer.repair('ic_cat_sales')" >/dev/null
expect "repair rebuilds the state from the tables" "0" "$(drift ic_cat_sales)"

echo "== switching strategy"
contains "unmapped sharded sources can't go back to key recompute" "ic.lines are not mapped to the key" \
  "$($ISO_NOFAIL -c "SELECT isomer.alter_projection('ic_cat_sales', strategy => 'recompute')" 2>&1)"
contains "a per-product total, forced incremental" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('ic_units', 'SELECT l.product_id, sum(l.qty) AS value FROM ic.lines l GROUP BY l.product_id', 'cache', 'product_id', strategy => 'incremental')")"
UPFX=$($ISO -c "SELECT isomer._engine_prefix('ic_units')")
contains "to key recompute, with the mapping" "strategy recompute" \
  "$($ISO -c "SELECT isomer.alter_projection('ic_units', strategy => 'recompute', sources => '{\"ic.lines\": \"product_id\"}')")"
expect "its state is gone" "0" "$($ISO -c "SELECT count(*) FROM pg_class WHERE relnamespace = 'isomer_state'::regnamespace AND relname LIKE '${UPFX}%'")"
# One statement across every shard, touching the same keys from several shard connections of one
# distributed transaction: queued keys are only appended, so they never wait on each other.
$ISO -c "UPDATE ic.lines SET qty = qty + 1 WHERE order_id % 31 = 0" >/dev/null
eventually "maintained by key it still equals the query, after a statement across every shard" "0" "drift ic_units"
contains "and back" "strategy incremental" "$($ISO -c "SELECT isomer.alter_projection('ic_units', strategy => 'incremental')")"
$ISO -c "UPDATE ic.lines SET qty = qty + 1 WHERE order_id % 37 = 0" >/dev/null
eventually "equal again" "0" "drift ic_units"
contains "strategy_advice explains each projection" "ic_cat_sales|incremental|incremental" "$($ISO -c "SELECT string_agg(projection || '|' || current_strategy || '|' || advised_strategy, ',') FROM isomer.strategy_advice()")"
contains "dump_projections keeps the strategy" "strategy => 'incremental'" "$($ISO -c "SELECT string_agg(d, ' ') FROM isomer.dump_projections() d")"

if [ "$MULTI" = "t" ]; then
  echo "== cluster: deltas reach the coordinator exactly once"
  eventually "worker queues drain" "0" "$ISO -c \"SELECT coalesce(sum(result::int), 0) FROM run_command_on_workers('SELECT count(*) FROM isomer.node_deltas')\""
  # An interrupted pull, left prepared on a worker and never recorded: rolled back, then pulled again.
  W=$($ISO -c "SELECT nodename FROM pg_dist_node WHERE groupid <> 0 AND isactive ORDER BY nodeid LIMIT 1")
  $ISO -c "SELECT cron.unschedule('isomer-pull')" >/dev/null
  $ISO -c "UPDATE ic.lines SET qty = qty + 3 WHERE order_id BETWEEN 1 AND 400" >/dev/null
  $ISO <<SQL >/dev/null
SELECT dblink_connect('ic_w', format('host=%s port=5432 dbname=postgres user=postgres', '$W'));
SELECT dblink_exec('ic_w', 'BEGIN');
SELECT dblink_exec('ic_w', 'DELETE FROM isomer.node_deltas');
SELECT dblink_exec('ic_w', \$\$PREPARE TRANSACTION 'isomer_pull_0_1'\$\$);
SELECT dblink_disconnect('ic_w');
SQL
  expect "the interrupted pull holds the deltas" "1" "$($ISO -c "SELECT count(*) FROM dblink(format('host=%s port=5432 dbname=postgres user=postgres', '$W'), 'SELECT gid FROM pg_prepared_xacts') AS t(gid text) WHERE gid = 'isomer_pull_0_1'")"
  $ISO -c "SELECT cron.schedule('isomer-pull', '1 seconds', 'CALL isomer.pull_node_queues()')" >/dev/null
  eventually "after it is rolled back and pulled again, every key equals the query" "0" "drift ic_cat_sales" 60
  expect "nothing is left prepared" "0" "$($ISO -c "SELECT count(*) FROM dblink(format('host=%s port=5432 dbname=postgres user=postgres', '$W'), 'SELECT gid FROM pg_prepared_xacts WHERE gid LIKE ''isomer_pull_%''') AS t(gid text)")"
fi

echo "== dropping removes the state"
$ISO -c "SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'ic\_%'" >/dev/null
expect "no state left" "0" "$($ISO -c "SELECT count(*) FROM pg_class WHERE relnamespace = 'isomer_state'::regnamespace")"
expect "no deltas left" "0" "$($ISO -c "SELECT count(*) FROM isomer.deltas")"
echo "incremental: all checks passed"
