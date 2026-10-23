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
  local name=$1 want=$2 cmd=$3 timeout=$(( ${4:-30} * ${ISOMER_TIMEOUT_SCALE:-1} )) got="" start=$SECONDS
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

# Analytics rows that differ from the query, once nothing is queued for the projection.
adrift() {
  $ISO -c "SELECT coalesce(isomer.analytics_differences('$1'), -1)"
}
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
contains "a subquery in the WHERE of a query that aggregates" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('sq_with_lines', 'SELECT o.customer_id, count(*) AS value FROM sq.orders o WHERE EXISTS (SELECT 1 FROM sq.lines l WHERE l.order_id = o.id) GROUP BY o.customer_id', 'cache', 'customer_id')")"
expect "it equals the query" "0" "$(drift sq_with_lines)"
PFXW=$($ISO -c "SELECT isomer._engine_prefix('sq_with_lines')")
expect "its partial counts are kept by order, the column the subquery reads" "$($ISO -c "SELECT count(*) FROM sq.orders")" \
  "$($ISO -c "SELECT count(*) FROM isomer_state.${PFXW}_b0p")"
contains "a subquery inside a sum: the sum is kept as written, over a copy of the orders" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_projection('sq_line_count', 'SELECT o.customer_id, sum((SELECT count(*) FROM sq.lines l WHERE l.order_id = o.id)) AS value FROM sq.orders o GROUP BY o.customer_id', 'cache', 'customer_id')")"
expect "it equals the query" "0" "$(drift sq_line_count)"
contains "a subquery reading a table without a primary key, kept as written" "sq.notes has no primary key" \
  "$($ISO -c "SELECT reason FROM isomer.choose_strategy('SELECT c.id, (SELECT jsonb_agg(n.body) FROM sq.notes n WHERE n.customer_id = c.id) AS value FROM sq.customers c', 'id', 'cache')")"
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

echo "== FR-P6 an analytics projection with subqueries"
# Customers per tier who have a big order, not blocked, with their paid total.
STATS='SELECT c.tier, count(*) AS customers, max(c.id) AS last_id,
          sum((SELECT count(*) FROM sq.tags t WHERE t.customer_id = c.id)) AS tags
        FROM sq.customers c
        WHERE EXISTS (SELECT 1 FROM sq.orders o WHERE o.customer_id = c.id AND o.total > 100)
          AND NOT EXISTS (SELECT 1 FROM sq.blocked b WHERE b.customer_id = c.id)
        GROUP BY c.tier'
$ISO -c "SELECT isomer.create_analytics_projection('sq_tier_rebuilt', \$q\$$STATS\$q\$)" >/dev/null
expect "auto rebuilds it: the sum over a subquery keeps a copy of every customer" "recompute" \
  "$($ISO -c "SELECT strategy FROM isomer.projections WHERE name = 'sq_tier_rebuilt'")"
contains "and says why" "keep a copy of every row its sum read" \
  "$($ISO -c "SELECT strategy_reason FROM isomer.projections WHERE name = 'sq_tier_rebuilt'")"
contains "asked for, it is maintained incrementally" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_analytics_projection('sq_tier_stats', \$q\$$STATS\$q\$, strategy => 'incremental')")"
expect "it equals the query" "0" "$(adrift sq_tier_stats)"
WHERE_ONLY='SELECT c.tier, count(*) AS customers FROM sq.customers c
             WHERE EXISTS (SELECT 1 FROM sq.orders o WHERE o.customer_id = c.id AND o.status = '"'"'open'"'"') GROUP BY c.tier'
contains "without the sum, auto picks incremental" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_analytics_projection('sq_tier_open', \$q\$$WHERE_ONLY\$q\$)")"
$ISO <<'SQL' >/dev/null
INSERT INTO sq.orders VALUES (2101, 41, 200, 'open');                               -- a customer gets a big order
UPDATE sq.orders SET total = 0, status = 'paid' WHERE customer_id = 30;            -- and one loses them all
UPDATE sq.customers SET tier = 'gold' WHERE id = 41;                                -- a customer changes tier
INSERT INTO sq.blocked VALUES (12);
INSERT INTO sq.tags VALUES (41, 'big');
SQL
eventually "writes to the customers and both subqueries' tables reach the rows" "0" "adrift sq_tier_stats"
eventually "and the open orders" "0" "adrift sq_tier_open"
expect "each change reaches only its customer's tier" "[\"gold\"]" \
  "$($ISO -c "BEGIN" -c "UPDATE sq.orders SET status = 'paid' WHERE id = 2101" -c "SELECT isomer._engine_fold('sq_tier_open')" \
          -c "SELECT string_agg(key, ',') FROM isomer.pending WHERE projection = 'sq_tier_open'" -c "ROLLBACK" | tail -1)"
contains "its strategy changes: built again as a columnar table rebuilt when sources change" "columnar; read it as public.sq_tier_open" \
  "$($ISO -c "SELECT isomer.alter_projection('sq_tier_open', strategy => 'rebuild')")"
expect "rebuilt, from the same definition" "recompute|0" \
  "$($ISO -c "SELECT strategy || '|' || isomer.analytics_differences('sq_tier_open') FROM isomer.projections WHERE name = 'sq_tier_open'")"
contains "and back to incremental" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.alter_projection('sq_tier_open', strategy => 'incremental')")"
$ISO -c "INSERT INTO sq.orders VALUES (2102, 42, 10, 'open')" >/dev/null
eventually "maintained incrementally again" "0" "adrift sq_tier_open"
$ISO -c "SELECT isomer.drop_projection('sq_tier_rebuilt')" >/dev/null

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
eventually "and the count behind a WHERE subquery" "0" "drift sq_with_lines" 60
eventually "and the analytics projection" "0" "adrift sq_tier_stats" 60
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
DSTATS='SELECT t.name AS tier, count(*) AS customers, max(c.id) AS last_id
  FROM sq.d_customers c JOIN sq.d_tiers t ON t.id = c.tier_id
  WHERE EXISTS (SELECT 1 FROM sq.d_orders o WHERE o.customer_id = c.id
                AND EXISTS (SELECT 1 FROM sq.d_lines l WHERE l.order_id = o.id AND l.qty > 3))
  GROUP BY t.name'
contains "an analytics projection over the sharded tables, two subqueries deep" "maintained incrementally" \
  "$($ISO -c "SELECT isomer.create_analytics_projection('sq_sharded_stats', \$q\$$DSTATS\$q\$)")"
expect "it equals the query" "0" "$(adrift sq_sharded_stats)"
$ISO -c "INSERT INTO sq.d_lines VALUES (7001, 1, 9)" -c "INSERT INTO sq.d_orders VALUES (7001, 61, 5)" \
     -c "INSERT INTO sq.d_customers VALUES (61, 'new', 2)" -c "UPDATE sq.d_lines SET qty = 0 WHERE order_id % 5 = 0" \
     -c "UPDATE sq.d_tiers SET name = 'silver' WHERE id = 1" >/dev/null
eventually "a line, its order and its customer, written on different shards, reach it" "0" "adrift sq_sharded_stats" 60
if [ "$MULTI" = "t" ]; then
  echo "== FR-P6 the state of a query with subqueries, distributed across the cluster"
  contains "an analytics projection keeps its state on the coordinator" "its state stays on the coordinator" \
    "$($ISO_NOFAIL -c "SELECT isomer.alter_projection('sq_sharded_stats', state => 'distributed')" 2>&1)"
  echo "== FR-P6 anchored: an order's lines inside a customer's document, distributed"
  contains "lines are correlated with an order, the orders sharded by customer: anchored" "state on the cluster" \
    "$($ISO -c "SELECT isomer.alter_projection('sq_sharded', state => 'distributed')")"
  expect "it equals the query" "0" "$(drift sq_sharded)"
  APFX=$($ISO -c "SELECT isomer._engine_prefix('sq_sharded')")
  expect "the lines' partial sums sit with their orders and customers" "1" \
    "$($ISO -c "SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'isomer_state.${APFX}%' AND partmethod = 'h'")"
  contains "the certificate names the anchored table" "sq.d_lines by the shard value of the row it is correlated with (anchored)" \
    "$($ISO -c "SELECT r FROM jsonb_array_elements_text(isomer.certificate('sq_sharded') -> 'rules') r WHERE r LIKE 'distributed state:%'")"
  $ISO -c "INSERT INTO sq.d_lines VALUES (7777, 1, 4)" >/dev/null
  eventually "a line whose order doesn't exist yet waits on the coordinator" "1" \
    "$ISO -c \"SELECT count(*) FROM isomer_state.${APFX}_b4po WHERE order_id = 7777\""
  $ISO -c "INSERT INTO sq.d_orders VALUES (7777, 8, 3)" >/dev/null
  eventually "and joins its customer when the order arrives" "4" \
    "$ISO -c \"SELECT o ->> 'units' FROM jsonb_array_elements(isomer.get('sq_sharded', '8') -> 'orders') o WHERE o ->> 'id' = '7777'\""
  $ISO -c "BEGIN" -c "DELETE FROM sq.d_orders WHERE id = 7777" -c "INSERT INTO sq.d_orders VALUES (7777, 9, 3)" -c "COMMIT" >/dev/null
  eventually "an order moving to another customer takes its lines with it" "4" \
    "$ISO -c \"SELECT o ->> 'units' FROM jsonb_array_elements(isomer.get('sq_sharded', '9') -> 'orders') o WHERE o ->> 'id' = '7777'\""
  # The delete and the insert are captured on different workers, and reach the coordinator apart.
  eventually "and leaves the first customer" "0" \
    "$ISO -c \"SELECT count(*) FROM jsonb_array_elements(coalesce(isomer.get('sq_sharded', '8') -> 'orders', '[]')) o WHERE o ->> 'id' = '7777'\""
  $ISO -c "INSERT INTO sq.d_orders VALUES (7777, 10, 1)" -c "UPDATE sq.d_lines SET qty = 6 WHERE order_id = 7777" >/dev/null
  eventually "two orders with one id, under two customers, each have the line" "6|6" \
    "$ISO -c \"SELECT (SELECT o ->> 'units' FROM jsonb_array_elements(isomer.get('sq_sharded', '9') -> 'orders') o WHERE o ->> 'id' = '7777') || '|' || (SELECT o ->> 'units' FROM jsonb_array_elements(isomer.get('sq_sharded', '10') -> 'orders') o WHERE o ->> 'id' = '7777')\""
  $ISO -c "DELETE FROM sq.d_orders WHERE id = 7777" -c "UPDATE sq.d_lines SET qty = 1 WHERE order_id = 7777" >/dev/null
  eventually "orders gone, the line waits again, still changing" "0" "drift sq_sharded" 60
  expect "on the coordinator" "1" "$($ISO -c "SELECT count(*) FROM isomer_state.${APFX}_b4po WHERE order_id = 7777")"
  echo "== FR-P6 anchored min and max, and an anchored subquery inside another"
  $ISO <<'SQL' >/dev/null
CREATE TABLE sq.d_marks (order_id bigint NOT NULL, line int NOT NULL, n int NOT NULL, score int, PRIMARY KEY (order_id, line, n));
SELECT create_distributed_table('sq.d_marks', 'order_id', colocate_with => 'sq.d_lines');
INSERT INTO sq.d_marks SELECT o, l, n, (o * 7 + l * 3 + n) % 50 FROM generate_series(1, 600) o, generate_series(1, 2) l, generate_series(1, 2) n
WHERE (o + l) % 3 <> 0;
SQL
  DNEST='SELECT c.id, jsonb_build_object('"'"'orders'"'"', (SELECT jsonb_agg(jsonb_build_object('"'"'id'"'"', o.id,
      '"'"'most'"'"', (SELECT max(l.qty) FROM sq.d_lines l WHERE l.order_id = o.id),
      '"'"'least'"'"', (SELECT min(l.qty) FROM sq.d_lines l WHERE l.order_id = o.id),
      '"'"'lines'"'"', (SELECT jsonb_agg(jsonb_build_object('"'"'line'"'"', l.line,
          '"'"'marks'"'"', (SELECT count(*) FROM sq.d_marks k WHERE k.order_id = l.order_id AND k.line = l.line),
          '"'"'top'"'"', (SELECT max(k.score) FROM sq.d_marks k WHERE k.order_id = l.order_id AND k.line = l.line)) ORDER BY l.line)
        FROM sq.d_lines l WHERE l.order_id = o.id)) ORDER BY o.id)
    FROM sq.d_orders o WHERE o.customer_id = c.id)) AS doc
  FROM sq.d_customers c'
  contains "min and max over anchored lines, and each line's marks anchored below them" "its state distributed across the cluster" \
    "$($ISO -c "SELECT isomer.create_projection('sq_nest', \$q\$$DNEST\$q\$, 'document', 'id', state => 'distributed')")"
  expect "it equals the query" "0" "$(drift sq_nest)"
  NPFX=$($ISO -c "SELECT isomer._engine_prefix('sq_nest')")
  expect "the marks and the lines' multisets sit with their orders and customers" "1" \
    "$($ISO -c "SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'isomer_state.${NPFX}%' AND partmethod = 'h'")"
  expect "the marks are anchored through their line's order" "order_id" \
    "$($ISO -c "SELECT string_agg(DISTINCT b -> 'anchor' ->> 'col', ',') FROM isomer.engine_plans e, jsonb_array_elements(e.plan -> 'placement' -> 'blocks') b WHERE e.projection = 'sq_nest' AND (b -> 'anchor' ->> 'nested')::boolean")"
  nested() { $ISO -c "SELECT (SELECT o -> '$3' FROM jsonb_array_elements(isomer.get('sq_nest', '$1') -> 'orders') o WHERE o ->> 'id' = '$2')::text"; }
  marks() { $ISO -c "SELECT (SELECT l ->> 'marks' || '/' || coalesce(l ->> 'top', '-') FROM jsonb_array_elements(isomer.get('sq_nest', '$1') -> 'orders') o, jsonb_array_elements(o -> 'lines') l WHERE o ->> 'id' = '$2' AND l ->> 'line' = '$3')"; }
  $ISO -c "INSERT INTO sq.d_orders VALUES (8888, 20, 1)" -c "INSERT INTO sq.d_lines VALUES (8888, 1, 5), (8888, 2, 2)" \
       -c "INSERT INTO sq.d_marks VALUES (8888, 1, 1, 30), (8888, 1, 2, 45), (8888, 2, 1, 7)" >/dev/null
  eventually "a new order's largest and smallest line" "5|2" "echo \$(nested 20 8888 most)'|'\$(nested 20 8888 least)"
  eventually "and its lines' marks, two levels below the order" "2/45" "marks 20 8888 1"
  $ISO -c "UPDATE sq.d_lines SET qty = 1 WHERE order_id = 8888 AND line = 1" -c "DELETE FROM sq.d_marks WHERE order_id = 8888 AND line = 1 AND n = 2" >/dev/null
  eventually "a maximum leaving, at each level" "2|1|1/30" "echo \$(nested 20 8888 most)'|'\$(nested 20 8888 least)'|'\$(marks 20 8888 1)"
  # The coordinator tables holding the marks blocks' orphans.
  MORPH=$($ISO -c "SELECT string_agg(f -> 'anchor' ->> 'orphans', ' ' ORDER BY (f ->> 'block')::int) FROM isomer.engine_plans e, jsonb_array_elements(e.plan -> 'facts') f WHERE e.projection = 'sq_nest' AND (f -> 'anchor' ->> 'nested')::boolean")
  orphans() { local t n=0; for t in $MORPH; do n=$(( n + $($ISO -c "SELECT count(*) FROM $t WHERE order_id = $1") )); done; echo $n; }
  $ISO -c "INSERT INTO sq.d_marks VALUES (8888, 3, 1, 9)" >/dev/null
  $ISO -c "INSERT INTO sq.d_lines VALUES (8888, 3, 4)" >/dev/null
  eventually "a mark written before its line, then the line" "1/9" "marks 20 8888 3"
  $ISO -c "INSERT INTO sq.d_marks VALUES (8889, 1, 1, 12)" -c "INSERT INTO sq.d_lines VALUES (8889, 1, 4)" >/dev/null
  eventually "a mark and a line whose order doesn't exist yet wait on the coordinator, in both marks blocks" "2" "orphans 8889"
  $ISO -c "INSERT INTO sq.d_orders VALUES (8889, 20, 1)" >/dev/null
  eventually "the order arrives: its line joins the customer, and the line's mark follows, a level down" "1/12" "marks 20 8889 1"
  expect "leaving the coordinator" "0" "$(orphans 8889)"
  $ISO -c "BEGIN" -c "DELETE FROM sq.d_orders WHERE id = 8888" -c "INSERT INTO sq.d_orders VALUES (8888, 21, 1)" -c "COMMIT" >/dev/null
  eventually "an order moving to another customer takes its lines' marks with it" "1/30|1/9|4" \
    "echo \$(marks 21 8888 1)'|'\$(marks 21 8888 3)'|'\$(nested 21 8888 most)"
  eventually "and leaves the first customer" "0" \
    "$ISO -c \"SELECT count(*) FROM jsonb_array_elements(coalesce(isomer.get('sq_nest', '20') -> 'orders', '[]')) o WHERE o ->> 'id' = '8888'\""
  $ISO -c "UPDATE sq.d_lines SET qty = 9 WHERE order_id = 8888 AND line = 2" -c "INSERT INTO sq.d_marks VALUES (8888, 3, 2, 49)" >/dev/null
  eventually "the multisets moved with them: writes after the move land under the new customer" "9|2/49" \
    "echo \$(nested 21 8888 most)'|'\$(marks 21 8888 3)"
  $ISO -c "DELETE FROM sq.d_orders WHERE id = 8888" -c "UPDATE sq.d_marks SET score = 3 WHERE order_id = 8888" >/dev/null
  eventually "the order gone, its lines and marks wait again, still changing" "0" "drift sq_nest" 60
  expect "the marks on the coordinator" "6" "$(orphans 8888)"
  $ISO -c "INSERT INTO sq.d_orders VALUES (8888, 22, 1)" >/dev/null
  eventually "and come back with the order" "9|2/3" "echo \$(nested 22 8888 most)'|'\$(marks 22 8888 3)"
  expect "it equals the query" "0" "$(drift sq_nest)"
  contains "a subquery in the WHERE of an anchored subquery is refused, with the reason" "in the WHERE of a subquery whose state is anchored" \
    "$($ISO_NOFAIL -c "SELECT isomer.create_projection('sq_bad', 'SELECT c.id, (SELECT jsonb_agg((SELECT sum(l.qty) FROM sq.d_lines l WHERE l.order_id = o.id AND l.qty > (SELECT min(k.score) FROM sq.d_marks k WHERE k.order_id = l.order_id AND k.line = l.line))) FROM sq.d_orders o WHERE o.customer_id = c.id) AS value FROM sq.d_customers c', 'cache', 'id', state => 'distributed')" 2>&1)"
  DIST='SELECT c.id, jsonb_build_object('"'"'tier'"'"', (SELECT t.name FROM sq.d_tiers t WHERE t.id = c.tier_id),
      '"'"'spent'"'"', (SELECT sum(o.total) FROM sq.d_orders o WHERE o.customer_id = c.id),
      '"'"'biggest'"'"', (SELECT max(o.total) FROM sq.d_orders o WHERE o.customer_id = c.id),
      '"'"'orders'"'"', (SELECT jsonb_agg(o.id ORDER BY o.id) FROM sq.d_orders o WHERE o.customer_id = c.id),
      '"'"'latest'"'"', (SELECT o.id FROM sq.d_orders o WHERE o.customer_id = c.id ORDER BY o.id DESC LIMIT 1)) AS doc
    FROM sq.d_customers c WHERE EXISTS (SELECT 1 FROM sq.d_orders o WHERE o.customer_id = c.id AND o.total > 10)'
  contains "a document of correlated subqueries, its state distributed" "its state distributed across the cluster" \
    "$($ISO -c "SELECT isomer.create_projection('sq_dist', \$q\$$DIST\$q\$, 'document', 'id', state => 'distributed')")"
  expect "it equals the query" "0" "$(drift sq_dist)"
  expect "the certificate holds, every state table distributed" "true|true" \
    "$($ISO -c "SELECT (isomer.certificate('sq_dist') ->> 'valid') || '|' || (c ->> 'passed') FROM jsonb_array_elements(isomer.certificate('sq_dist') -> 'checks') c WHERE c ->> 'check' = 'state distributed'")"
  contains "customers sharded by id, the orders' state by customer_id, the tiers on every node" \
    "sq.d_customers by id, sq.d_orders by customer_id), so a correlated subquery runs on the shard holding its outer row; kept on every node: sq.d_tiers" \
    "$($ISO -c "SELECT r FROM jsonb_array_elements_text(isomer.certificate('sq_dist') -> 'rules') r WHERE r LIKE 'distributed state:%'")"
  DPFX=$($ISO -c "SELECT isomer._engine_prefix('sq_dist')")
  expect "the sharded parts are co-located" "1" \
    "$($ISO -c "SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'isomer_state.${DPFX}%' AND partmethod = 'h'")"
  written() { $ISO -c "SELECT documents_written FROM isomer.projection_stats WHERE projection = '$1'"; }
  before=$(written sq_dist)
  $ISO -c "UPDATE sq.d_orders SET total = total + 1 WHERE id = (SELECT min(id) FROM sq.d_orders WHERE customer_id = 3)" >/dev/null
  eventually "a changed order is folded on its shard" "0" "drift sq_dist" 60
  expect "and re-derives its customer's document only" "1" "$(( $(written sq_dist) - before ))"
  $ISO -c "INSERT INTO sq.d_orders VALUES (9001, 3, 99), (9002, 3, 5)" \
       -c "DELETE FROM sq.d_orders WHERE id = 9001" -c "INSERT INTO sq.d_orders VALUES (9001, 4, 99)" \
       -c "DELETE FROM sq.d_orders WHERE customer_id = 7" \
       -c "DELETE FROM sq.d_orders WHERE customer_id = 9 AND total = (SELECT max(total) FROM sq.d_orders WHERE customer_id = 9)" \
       -c "UPDATE sq.d_tiers SET name = 'diamond' WHERE id = 2" -c "UPDATE sq.d_customers SET tier_id = 1 WHERE id = 5" \
       -c "INSERT INTO sq.d_customers VALUES (70, 'new', 2)" -c "INSERT INTO sq.d_orders VALUES (9003, 70, 500)" >/dev/null
  eventually "orders moving, leaving, a maximum leaving, a lookup renamed, a new customer" "0" "drift sq_dist" 60
  expect "a customer without a big order is gone" "t" "$($ISO -c "SELECT isomer.get('sq_dist', '7') IS NULL")"
  $ISO <<'SQL' >/dev/null
CREATE OR REPLACE PROCEDURE public.sq_dwriter(seed int, n int) LANGUAGE plpgsql AS $$
DECLARE
    c bigint;
    o bigint;
    t numeric;
BEGIN
    PERFORM setseed(seed / 100.0);
    FOR i IN 1 .. n LOOP
        c := 1 + (random() * 69)::int;
        o := 1 + (random() * 599)::int;
        CASE (random() * 8)::int
            WHEN 0 THEN INSERT INTO sq.d_orders VALUES (200000 + seed * 10000 + i, c, i % 90);
            WHEN 1 THEN UPDATE sq.d_orders SET total = total + 7 WHERE customer_id = c AND id % 3 = i % 3;
            WHEN 2 THEN DELETE FROM sq.d_orders WHERE customer_id = c AND id % 5 = i % 5;
            WHEN 3 THEN UPDATE sq.d_lines SET qty = qty + 1 WHERE order_id = o;
            WHEN 4 THEN INSERT INTO sq.d_lines VALUES (o, 100 + i, 2) ON CONFLICT DO NOTHING;
            WHEN 5 THEN  -- an order moves to another customer, with its lines
                DELETE FROM sq.d_orders WHERE id = o RETURNING total INTO t;
                IF FOUND THEN INSERT INTO sq.d_orders VALUES (o, c, t) ON CONFLICT DO NOTHING; END IF;
            WHEN 6 THEN UPDATE sq.d_marks SET score = (score * 13 + i) % 60 WHERE order_id = o;
            WHEN 7 THEN INSERT INTO sq.d_marks VALUES (o, 1 + i % 3, 100 + i, i % 60) ON CONFLICT DO NOTHING;
            ELSE UPDATE sq.d_customers SET tier_id = 1 + i % 2 WHERE id = c;
        END CASE;
        COMMIT;
    END LOOP;
END $$;
SQL
  pids=()
  for w in 1 2 3 4; do $ISO -c "CALL public.sq_dwriter($w, 100)" >/dev/null & pids+=($!); done
  for pid in "${pids[@]}"; do wait "$pid" || fail "a writer failed" "exit 0" "exit $?"; done
  eventually "after 400 concurrent transactions across the shards, it equals the query" "0" "drift sq_dist" 90
  eventually "and the document with anchored lines" "0" "drift sq_sharded" 90
  eventually "and the one with min and max over them, and marks anchored below them" "0" "drift sq_nest" 90
  DSHARE='SELECT c.id, round((SELECT coalesce(sum(o.total), 0) FROM sq.d_orders o WHERE o.customer_id = c.id)
                         / nullif((SELECT sum(o.total) FROM sq.d_orders o), 0), 6) AS value FROM sq.d_customers c'
  contains "a share of an uncorrelated total, distributed" "its state distributed across the cluster" \
    "$($ISO -c "SELECT isomer.create_projection('sq_dist_share', \$q\$$DSHARE\$q\$, 'cache', 'id', state => 'distributed')")"
  expect "it equals the query" "0" "$(drift sq_dist_share)"
  $ISO -c "INSERT INTO sq.d_orders VALUES (9100, 12, 80)" >/dev/null
  eventually "an order changes every customer's share" "0" "drift sq_dist_share" 60
  contains "back to the coordinator" "state on the coordinator" "$($ISO -c "SELECT isomer.alter_projection('sq_dist', state => 'coordinator')")"
  expect "no state left on the cluster" "0" \
    "$($ISO -c "SELECT count(*) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'isomer_state.${DPFX}%'")"
  expect "it equals the query" "0" "$(drift sq_dist)"
  contains "and distributed again" "state on the cluster" "$($ISO -c "SELECT isomer.alter_projection('sq_dist', state => 'distributed')")"
  $ISO -c "DELETE FROM sq.d_orders WHERE id = 9003" >/dev/null
  eventually "it is maintained again" "0" "drift sq_dist" 60
  expect "repair rebuilds the distributed state" "0" "$($ISO -c "SELECT isomer.repair('sq_dist', ARRAY['3', '4'])" >/dev/null && drift sq_dist)"
fi

echo "== dropping removes the state"
$ISO -c "SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'sq\_%'" -c "DROP PROCEDURE public.sq_writer(int, int)" \
     -c "DROP PROCEDURE IF EXISTS public.sq_dwriter(int, int)" >/dev/null
expect "no state left" "0" "$($ISO -c "SELECT count(*) FROM pg_class WHERE relnamespace = 'isomer_state'::regnamespace")"
if [ "$MULTI" = "t" ]; then
  expect "none on the workers, nor in the cluster's metadata" "0|0" \
    "$($ISO -c "SELECT (SELECT coalesce(sum(result::int), 0) FROM run_command_on_workers(\$\$SELECT count(*) FROM pg_class WHERE relnamespace = 'isomer_state'::regnamespace\$\$)) || '|' || (SELECT count(*) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'isomer\_state.%')")"
fi
echo "subqueries: all checks passed"
