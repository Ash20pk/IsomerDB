#!/bin/bash
# Correlated subqueries maintained incrementally (FR-P6): the query and each subquery in it are
# blocks with their own state, a key is derived by the query as written over that state, and the
# keys a change reaches are found through each subquery on the way up. Runs on a single node and
# on the cluster.
#
#   docker compose --profile attach up -d && ./tests/subqueries.sh
#   docker compose --profile cluster up -d && ./tests/subqueries.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
ISO_NOFAIL="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A"

pass() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"
  echo "-- state at failure --"
  $ISO_NOFAIL -c "SELECT 'deltas', projection, count(*) FROM isomer.deltas GROUP BY projection" \
              -c "SELECT 'pending', projection, count(*) FROM isomer.pending GROUP BY projection" \
              -c "SELECT level, subject, left(message, 300) FROM isomer.events ORDER BY id DESC LIMIT 5" 2>&1 | sed 's/^/       /'
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
# Documents that differ from the query, once nothing is queued for the projection.
drift() {
  $ISO -c "SELECT CASE WHEN EXISTS (SELECT 1 FROM isomer.deltas WHERE projection = '$1')
                         OR EXISTS (SELECT 1 FROM isomer.pending WHERE projection = '$1') THEN -1
                       ELSE (SELECT count(*) FROM isomer.verify('$1')) END"
}
MULTI=$($ISO -c "SELECT count(*) > 0 FROM pg_dist_node WHERE groupid <> 0 AND isactive")

echo "== reset"
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection(name) FROM isomer.projections;
DELETE FROM isomer.events; DELETE FROM isomer.drift_samples; DELETE FROM isomer.audit_suspects;
DROP SCHEMA IF EXISTS sq CASCADE;
CREATE SCHEMA sq;
CREATE TABLE sq.customers (id bigint PRIMARY KEY, name text, tier text);
CREATE TABLE sq.orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL, total numeric, status text);
CREATE TABLE sq.products (id int PRIMARY KEY, title text);
CREATE TABLE sq.lines (order_id bigint NOT NULL, line int NOT NULL, product_id int, qty int, PRIMARY KEY (order_id, line));
CREATE TABLE sq.tags (customer_id bigint NOT NULL, tag text NOT NULL, PRIMARY KEY (customer_id, tag));
CREATE TABLE sq.blocked (customer_id bigint PRIMARY KEY);
CREATE TABLE sq.tiers (name text PRIMARY KEY, discount int);
CREATE TABLE sq.notes (customer_id bigint, body text);   -- no primary key
INSERT INTO sq.customers SELECT g, 'customer ' || g, CASE WHEN g % 3 = 0 THEN 'gold' ELSE 'basic' END FROM generate_series(1, 50) g;
INSERT INTO sq.products SELECT g, 'product ' || g FROM generate_series(1, 20) g;
INSERT INTO sq.orders SELECT g, 1 + g % 40, (g * 37) % 250, CASE WHEN g % 4 = 0 THEN 'open' ELSE 'paid' END FROM generate_series(1, 300) g;
INSERT INTO sq.lines SELECT o, l, 1 + (o * l) % 20, 1 + (o + l) % 5 FROM generate_series(1, 300) o, generate_series(1, 3) l;
INSERT INTO sq.tags SELECT g, t FROM generate_series(1, 50) g, unnest(ARRAY['a', 'b', 'c']) t WHERE (g + length(t)) % 2 = 0;
INSERT INTO sq.blocked VALUES (7), (8);
INSERT INTO sq.tiers VALUES ('gold', 10), ('basic', 0);
SQL

# A customer's document: counts, sums and nested arrays from correlated subqueries, two levels deep.
DOC=$(cat <<'Q'
SELECT c.id, jsonb_build_object('name', c.name,
  'n_orders', (SELECT count(*) FROM sq.orders o WHERE o.customer_id = c.id),
  'paid', (SELECT sum(o.total) FROM sq.orders o WHERE o.customer_id = c.id AND o.status = 'paid'),
  'biggest', (SELECT max(o.total) FROM sq.orders o WHERE o.customer_id = c.id),
  'orders', coalesce((SELECT jsonb_agg(jsonb_build_object('id', o.id, 'total', o.total,
        'lines', (SELECT jsonb_agg(jsonb_build_object('line', l.line, 'title', p.title, 'qty', l.qty) ORDER BY l.line)
                  FROM sq.lines l JOIN sq.products p ON p.id = l.product_id WHERE l.order_id = o.id)) ORDER BY o.id)
     FROM sq.orders o WHERE o.customer_id = c.id), '[]'),
  'latest', (SELECT o.id FROM sq.orders o WHERE o.customer_id = c.id ORDER BY o.id DESC LIMIT 1),
  'has_big', EXISTS (SELECT 1 FROM sq.orders o WHERE o.customer_id = c.id AND o.total > 100),
  'tags', ARRAY(SELECT t.tag FROM sq.tags t WHERE t.customer_id = c.id ORDER BY t.tag),
  'discount', (SELECT t.discount FROM sq.tiers t WHERE t.name = c.tier),
  'big_spender', c.id IN (SELECT o.customer_id FROM sq.orders o WHERE o.total > 240)) AS doc
FROM sq.customers c WHERE NOT EXISTS (SELECT 1 FROM sq.blocked b WHERE b.customer_id = c.id)
Q
)

echo "== FR-P6 a document built from correlated subqueries"
contains "the cost model picks incremental maintenance, without key mappings" "incremental|no key mappings were given" \
  "$($ISO -c "SELECT strategy || '|' || reason FROM isomer.choose_strategy(\$q\$$DOC\$q\$, 'id', 'document')")"
contains "created, maintained incrementally" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('sq_doc', \$q\$$DOC\$q\$, 'document', 'id')")"
expect "it equals the query" "0" "$(drift sq_doc)"
PFX=$($ISO -c "SELECT isomer._engine_prefix('sq_doc')")
expect "a count of a customer's orders is one partial row per customer, not one per order" "40" \
  "$($ISO -c "SELECT count(*) FROM isomer_state.${PFX}_b1p")"
contains "the certificate names the subquery rules" "keys through subqueries" \
  "$($ISO -c "SELECT isomer.certificate('sq_doc') -> 'rules'")"

$ISO <<'SQL' >/dev/null
INSERT INTO sq.orders VALUES (1001, 3, 500, 'paid'), (1002, 41, 20, 'open');       -- customer 41 had no orders
INSERT INTO sq.lines VALUES (1001, 1, 5, 2), (1001, 2, 6, 1);
UPDATE sq.orders SET status = 'paid' WHERE id % 8 = 0;                              -- in and out of a subquery's filter
UPDATE sq.orders SET customer_id = 10 WHERE id IN (5, 45);                          -- orders move between customers
DELETE FROM sq.orders WHERE total = (SELECT max(total) FROM sq.orders WHERE customer_id = 12) AND customer_id = 12;  -- a maximum leaves
DELETE FROM sq.orders WHERE id = 299;
UPDATE sq.lines SET qty = qty + 10 WHERE order_id = 12;                             -- a nested line changes
UPDATE sq.lines SET order_id = 13, line = 9 WHERE order_id = 14 AND line = 3;       -- a line moves to another order
UPDATE sq.products SET title = 'renamed' WHERE id = 4;                              -- a joined table, two levels down
INSERT INTO sq.tags VALUES (2, 'z');
DELETE FROM sq.tags WHERE customer_id = 3;
INSERT INTO sq.blocked VALUES (9);                                                  -- WHERE NOT EXISTS: a customer leaves
DELETE FROM sq.blocked WHERE customer_id = 7;                                       -- and one comes back
UPDATE sq.tiers SET discount = 15 WHERE name = 'gold';                              -- a lookup every gold customer reads
UPDATE sq.customers SET tier = 'gold' WHERE id = 1;
UPDATE sq.orders SET total = 245 WHERE id = 20;                                     -- IN (...): customer 21 becomes a big spender
INSERT INTO sq.customers VALUES (51, 'new', 'basic');
SQL
eventually "after writes to every table, at every level, it equals the query" "0" "drift sq_doc"
expect "a customer's first order shows" "1" "$($ISO -c "SELECT isomer.get('sq_doc', '41') ->> 'n_orders'")"
expect "a customer no longer blocked is back" "t" "$($ISO -c "SELECT isomer.get('sq_doc', '7') IS NOT NULL")"
expect "a blocked customer is gone" "t" "$($ISO -c "SELECT isomer.get('sq_doc', '9') IS NULL")"
expect "a renamed product shows two levels down" "t" \
  "$($ISO -c "SELECT isomer.get('sq_doc', '4') @? '\$.orders[*].lines[*] ? (@.title == \"renamed\")'")"
expect "the new gold discount reaches every gold customer" "15" "$($ISO -c "SELECT isomer.get('sq_doc', '1') ->> 'discount'")"
expect "IN (...) follows a changed order" "true" "$($ISO -c "SELECT isomer.get('sq_doc', '21') ->> 'big_spender'")"

# The keys one write reaches: written and folded in one transaction, then rolled back.
reached() {
  $ISO -c "BEGIN" -c "$2" -c "SELECT isomer._engine_fold('$1')" \
       -c "SELECT coalesce(string_agg(key, ',' ORDER BY key::bigint), '') FROM isomer.pending WHERE projection = '$1'" -c "ROLLBACK" | tail -1
}
echo "== a change reaches only the keys it is correlated with"
eventually "nothing queued" "0" "$ISO -c \"SELECT count(*) FROM isomer.pending WHERE projection = 'sq_doc'\""
expect "a line of order 12 reaches its customer only (13), through the order" "13" \
  "$(reached sq_doc "UPDATE sq.lines SET qty = qty + 1 WHERE order_id = 12 AND line = 1")"
expect "an order moved from customer 31 to 32 reaches both" "31,32" \
  "$(reached sq_doc "UPDATE sq.orders SET customer_id = 32 WHERE id = 30")"
expect "IN (...) keeps its comparison: a big order reaches its own customer" "16" \
  "$(reached sq_doc "UPDATE sq.orders SET total = 249 WHERE id = 15")"
expect "a product reaches the customers whose orders' lines name it" \
  "$($ISO -c "SELECT string_agg(DISTINCT o.customer_id::text, ',' ORDER BY o.customer_id::text) FROM (SELECT DISTINCT o.customer_id FROM sq.orders o JOIN sq.lines l ON l.order_id = o.id WHERE l.product_id = 17 AND o.customer_id NOT IN (SELECT customer_id FROM sq.blocked) ORDER BY 1) o" | tr ',' '\n' | sort -n | paste -sd, -)" \
  "$(reached sq_doc "UPDATE sq.products SET title = 'x' WHERE id = 17")"
expect "a tag reaches its customer" "5" "$(reached sq_doc "INSERT INTO sq.tags VALUES (5, 'q')")"

echo "== what is refused, and why"
contains "a subquery in a JOIN's ON" "it has a subquery in a JOIN's ON" \
  "$($ISO -c "SELECT reason FROM isomer.choose_strategy('SELECT c.id, count(*) AS value FROM sq.customers c JOIN sq.tags t ON t.customer_id = c.id AND EXISTS (SELECT 1 FROM sq.orders o WHERE o.customer_id = c.id) GROUP BY c.id', 'id', 'cache')")"
contains "a refusal inside a subquery names its tables" "a subquery (sq.orders): it uses window functions" \
  "$($ISO -c "SELECT reason FROM isomer.choose_strategy('SELECT c.id, (SELECT o.id FROM sq.orders o WHERE o.customer_id = c.id ORDER BY rank() OVER (ORDER BY o.total) LIMIT 1) AS value FROM sq.customers c', 'id', 'cache')")"
contains "a subquery in the WHERE of a query that aggregates" "has a subquery outside its aggregates" \
  "$($ISO -c "SELECT reason FROM isomer.choose_strategy('SELECT o.customer_id, count(*) AS value FROM sq.orders o WHERE EXISTS (SELECT 1 FROM sq.lines l WHERE l.order_id = o.id) GROUP BY o.customer_id', 'customer_id', 'cache')")"
contains "a subquery inside a sum: the sum is kept as written, over a copy of the orders" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('sq_line_count', 'SELECT o.customer_id, sum((SELECT count(*) FROM sq.lines l WHERE l.order_id = o.id)) AS value FROM sq.orders o GROUP BY o.customer_id', 'cache', 'customer_id')")"
expect "it equals the query" "0" "$(drift sq_line_count)"
contains "a subquery reading a table without a primary key, kept as written" "sq.notes has no primary key" \
  "$($ISO -c "SELECT reason FROM isomer.choose_strategy('SELECT c.id, (SELECT jsonb_agg(n.body) FROM sq.notes n WHERE n.customer_id = c.id) AS value FROM sq.customers c', 'id', 'cache')")"
contains "subqueries in an analytics projection" "maintained incrementally in document and cache projections" \
  "$($ISO -c "SELECT isomer._engine_shape('SELECT c.tier, count(*) AS n, (SELECT count(*) FROM sq.orders) AS all_orders FROM sq.customers c GROUP BY c.tier', 'tier', 'analytics') ->> 'reason'")"
contains "still refused without any subquery or aggregate" "it has no aggregate" \
  "$($ISO -c "SELECT reason FROM isomer.choose_strategy('SELECT c.id, c.name AS value FROM sq.customers c', 'id', 'cache')")"

echo "== a cache of scalar subqueries, sync"
SPEND='SELECT c.id, (SELECT coalesce(sum(o.total), 0) FROM sq.orders o WHERE o.customer_id = c.id AND o.status = '"'"'paid'"'"')
              + (SELECT count(*) FROM sq.tags t WHERE t.customer_id = c.id) AS value FROM sq.customers c'
contains "created sync and incremental" "(sync), maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('sq_spend', \$q\$$SPEND\$q\$, 'cache', 'id', freshness => 'sync', strategy => 'incremental')")"
expect "it equals the query" "0" "$(drift sq_spend)"
expect "a write is in the cache before the transaction ends" "t" \
  "$($ISO -c "BEGIN" -c "INSERT INTO sq.orders VALUES (2001, 4, 1000, 'paid')" \
          -c "SELECT (SELECT value::text::numeric FROM public.sq_spend WHERE id = 4) = (SELECT coalesce(sum(total), 0) FROM sq.orders WHERE customer_id = 4 AND status = 'paid') + (SELECT count(*) FROM sq.tags WHERE customer_id = 4)" -c "ROLLBACK" | tail -1)"
$ISO -c "UPDATE sq.orders SET status = 'open' WHERE customer_id = 6" -c "DELETE FROM sq.tags WHERE customer_id = 6" >/dev/null
expect "statements on both subqueries' tables, the moment they commit" "0" "$(drift sq_spend)"

echo "== a subquery not correlated at all"
SHARE='SELECT c.id, round((SELECT coalesce(sum(o.total), 0) FROM sq.orders o WHERE o.customer_id = c.id)
                         / nullif((SELECT sum(o.total) FROM sq.orders o), 0), 6) AS value FROM sq.customers c'
contains "each customer's share of all orders" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('sq_share', \$q\$$SHARE\$q\$, 'cache', 'id')")"
expect "it equals the query" "0" "$(drift sq_share)"
expect "an order reaches every customer, through the total" "$($ISO -c "SELECT count(*) FROM sq.customers")" \
  "$(reached sq_share "UPDATE sq.orders SET total = total + 1 WHERE id = 100" | tr ',' '\n' | grep -c .)"

echo "== a grouped query with a subquery inside its aggregate"
# Tags per tier, each customer with the count of their orders, from a subquery inside jsonb_agg.
TIERS='SELECT c.tier, jsonb_build_object('"'"'customers'"'"', jsonb_agg(jsonb_build_object('"'"'id'"'"', c.id,
          '"'"'orders'"'"', (SELECT count(*) FROM sq.orders o WHERE o.customer_id = c.id)) ORDER BY c.id), '"'"'n'"'"', count(*)) AS doc
        FROM sq.customers c GROUP BY c.tier'
contains "created" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('sq_tiers', \$q\$$TIERS\$q\$, 'document', 'tier')")"
expect "it equals the query" "0" "$(drift sq_tiers)"
$ISO -c "INSERT INTO sq.orders VALUES (2002, 2, 5, 'open')" -c "UPDATE sq.customers SET tier = 'gold' WHERE id = 2" >/dev/null
eventually "an order and a customer changing tier" "0" "drift sq_tiers"

echo "== switching strategy"
contains "to key recompute, with mappings" "strategy recompute" \
  "$($ISO -c "SELECT isomer.alter_projection('sq_tiers', strategy => 'recompute', sources => '{\"sq.customers\": \"tier\", \"sq.orders\": null}')")"
$ISO -c "UPDATE sq.orders SET customer_id = 3 WHERE id = 2002" >/dev/null
eventually "by key it still equals the query" "0" "drift sq_tiers"
contains "and back" "strategy incremental" "$($ISO -c "SELECT isomer.alter_projection('sq_tiers', strategy => 'incremental')")"
$ISO -c "DELETE FROM sq.orders WHERE id = 2002" >/dev/null
eventually "incrementally again" "0" "drift sq_tiers"
contains "dump_projections writes it back" "sq_tiers" "$($ISO -c "SELECT string_agg(d, ' ') FROM isomer.dump_projections() d")"

echo "== concurrent writers"
$ISO <<'SQL' >/dev/null
CREATE OR REPLACE PROCEDURE public.sq_writer(seed int, n int) LANGUAGE plpgsql AS $$
DECLARE
    o bigint;
    c bigint;
BEGIN
    PERFORM setseed(seed / 100.0);
    FOR i IN 1 .. n LOOP
        o := 1 + (random() * 299)::int;
        c := 1 + (random() * 49)::int;
        CASE (random() * 6)::int
            WHEN 0 THEN INSERT INTO sq.orders VALUES (100000 + seed * 10000 + i, c, i % 250, 'paid');
            WHEN 1 THEN UPDATE sq.orders SET customer_id = c WHERE id = o;
            WHEN 2 THEN UPDATE sq.lines SET qty = qty + 1 WHERE order_id = o;
            WHEN 3 THEN DELETE FROM sq.lines WHERE order_id = o AND line = 1;
            WHEN 4 THEN INSERT INTO sq.tags VALUES (c, 't' || i) ON CONFLICT DO NOTHING;
            WHEN 5 THEN UPDATE sq.products SET title = 'p' || i WHERE id = 1 + o % 20;
            ELSE UPDATE sq.orders SET status = CASE WHEN status = 'paid' THEN 'open' ELSE 'paid' END WHERE id = o;
        END CASE;
        COMMIT;
    END LOOP;
END $$;
SQL
pids=()
for w in 1 2 3 4; do $ISO -c "CALL public.sq_writer($w, 150)" >/dev/null & pids+=($!); done
for pid in "${pids[@]}"; do wait "$pid" || fail "a writer failed" "exit 0" "exit $?"; done
eventually "after 600 concurrent transactions, every document equals the query" "0" "drift sq_doc" 60
eventually "and the sync cache" "0" "drift sq_spend" 60
eventually "and the grouped query" "0" "drift sq_tiers" 60
eventually "and the sum over a subquery" "0" "drift sq_line_count" 60
eventually "and the uncorrelated total" "0" "drift sq_share" 60

echo "== TRUNCATE, the auditor and repair"
$ISO -c "TRUNCATE sq.lines" -c "INSERT INTO sq.lines VALUES (1, 1, 1, 1), (2, 1, 2, 2)" >/dev/null
eventually "a truncated subquery table rebuilds the state" "0" "drift sq_doc"
$ISO -c "INSERT INTO sq.orders VALUES (3000, 2, 1, 'paid')" >/dev/null
eventually "customer 2 has an order" "0" "drift sq_doc"
$ISO -c "UPDATE isomer_state.${PFX}_b1p SET __n = __n + 1000 WHERE customer_id = 2" \
     -c "INSERT INTO sq.orders VALUES (3001, 2, 1, 'paid')" >/dev/null
eventually "corrupted state reaches the document" "t" "$ISO -c \"SELECT (isomer.get('sq_doc', '2') ->> 'n_orders')::int > 1000\""
expect "the certificate's state check passes (counts stay positive)" "true" "$($ISO -c "SELECT isomer.certificate('sq_doc') ->> 'valid'")"
$ISO -c "SELECT isomer.repair('sq_doc', ARRAY['2'])" >/dev/null
expect "repair rebuilds it from the tables" "0" "$(drift sq_doc)"

echo "== over sharded tables"
$ISO <<'SQL' >/dev/null
CREATE TABLE sq.d_customers (id bigint PRIMARY KEY, name text, tier_id int);
CREATE TABLE sq.d_orders (id bigint NOT NULL, customer_id bigint NOT NULL, total numeric, PRIMARY KEY (customer_id, id));
CREATE TABLE sq.d_lines (order_id bigint NOT NULL, line int NOT NULL, qty int, PRIMARY KEY (order_id, line));
CREATE TABLE sq.d_tiers (id int PRIMARY KEY, name text);
SELECT create_distributed_table('sq.d_customers', 'id');
SELECT create_distributed_table('sq.d_orders', 'customer_id', colocate_with => 'sq.d_customers');
-- Lines are sharded by order, not co-located with orders.
SELECT create_distributed_table('sq.d_lines', 'order_id', colocate_with => 'none');
SELECT create_reference_table('sq.d_tiers');
INSERT INTO sq.d_tiers VALUES (1, 'basic'), (2, 'gold');
INSERT INTO sq.d_customers SELECT g, 'customer ' || g, 1 + g % 2 FROM generate_series(1, 60) g;
INSERT INTO sq.d_orders SELECT g, 1 + g % 50, g % 90 FROM generate_series(1, 600) g;
INSERT INTO sq.d_lines SELECT o, l, 1 + (o + l) % 4 FROM generate_series(1, 600) o, generate_series(1, 2) l;
SQL
DDOC='SELECT c.id, jsonb_build_object('"'"'tier'"'"', (SELECT t.name FROM sq.d_tiers t WHERE t.id = c.tier_id),
    '"'"'spent'"'"', (SELECT sum(o.total) FROM sq.d_orders o WHERE o.customer_id = c.id),
    '"'"'orders'"'"', (SELECT jsonb_agg(jsonb_build_object('"'"'id'"'"', o.id,
        '"'"'units'"'"', (SELECT sum(l.qty) FROM sq.d_lines l WHERE l.order_id = o.id)) ORDER BY o.id)
      FROM sq.d_orders o WHERE o.customer_id = c.id)) AS doc
  FROM sq.d_customers c'
contains "a document over three sharded tables and a reference table" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('sq_sharded', \$q\$$DDOC\$q\$, 'document', 'id')")"
expect "it equals the query" "0" "$(drift sq_sharded)"
$ISO -c "INSERT INTO sq.d_orders VALUES (5001, 55, 40)" -c "INSERT INTO sq.d_lines VALUES (5001, 1, 3)" \
     -c "UPDATE sq.d_lines SET qty = qty + 5 WHERE order_id % 7 = 0" -c "DELETE FROM sq.d_orders WHERE id % 11 = 0" \
     -c "UPDATE sq.d_tiers SET name = 'platinum' WHERE id = 2" -c "UPDATE sq.d_customers SET tier_id = 2 WHERE id = 3" >/dev/null
eventually "writes on every shard, and to the reference table, reach it" "0" "drift sq_sharded" 60
expect "a line on another shard than its order reaches the order's customer" \
  "$($ISO -c "SELECT sum(l.qty) FROM sq.d_lines l WHERE l.order_id = 5001")" \
  "$($ISO -c "SELECT o ->> 'units' FROM jsonb_array_elements(isomer.get('sq_sharded', '55') -> 'orders') o WHERE o ->> 'id' = '5001'")"
if [ "$MULTI" = "t" ]; then
  contains "its state can't be distributed: it is kept on the coordinator" "kept on the coordinator" \
    "$($ISO_NOFAIL -c "SELECT isomer.alter_projection('sq_sharded', state => 'distributed')" 2>&1)"
fi

echo "== dropping removes the state"
$ISO -c "SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'sq\_%'" -c "DROP PROCEDURE public.sq_writer(int, int)" >/dev/null
expect "no state left" "0" "$($ISO -c "SELECT count(*) FROM pg_class WHERE relnamespace = 'isomer_state'::regnamespace")"
echo "subqueries: all checks passed"
