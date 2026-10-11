#!/bin/bash
# Multi-node acceptance test: projections over tables sharded across Citus worker nodes, read
# your writes across nodes, adding a node with an online rebalance, and correctness under load.
#
#   docker compose --profile cluster up -d && ./tests/multinode.sh
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
  $ISO_NOFAIL -c "SELECT 'pending', string_agg(projection || ':' || key || ' token ' || token || ' at ' || enqueued_at, ', ') FROM isomer.pending" \
              -c "SELECT 'node_queue ' || nodename, result FROM run_command_on_workers('SELECT string_agg(projection || '':'' || key, '','') FROM isomer.node_queue')" \
              -c "SELECT command, status, start_time FROM cron.job_run_details WHERE command ~ 'pull|drain' ORDER BY start_time DESC LIMIT 4" \
              -c "SELECT level, left(message, 160) FROM isomer.events ORDER BY id DESC LIMIT 4" \
              -c "SELECT key, expected ->> 'name', actual ->> 'name' FROM isomer.verify('mn_customer') LIMIT 3" 2>&1 | sed 's/^/       /'
  exit 1
}
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=${4:-60} got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 1
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}
mongo_eval() { "${MONGO[@]}" --eval "$1" 2>&1 | tail -1; }
# The node holding a key's shard.
node_of() { $ISO -c "SELECT n.nodename FROM pg_dist_placement p JOIN pg_dist_node n USING (groupid) WHERE p.shardid = get_shard_id_for_distribution_column('mn.orders', $1)"; }

echo "== cluster: coordinator + worker1"
if [ "$($ISO -c "SELECT count(*) FROM pg_dist_node WHERE nodename = 'worker1'")" = "0" ]; then
  contains "add a worker node" "worker1:5432 added" "$($ISO -c "SELECT isomer.add_node('worker1')")"
fi
eventually "rebalance finished" "finished" "$ISO -c \"SELECT coalesce((SELECT state::text FROM citus_rebalance_status()), 'finished')\"" 120

echo "== reset"
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection(name) FROM isomer.projections;
DELETE FROM isomer.events;
DROP SCHEMA IF EXISTS mn CASCADE;
DROP PROCEDURE IF EXISTS public.mn_worker(int, int);
CREATE SCHEMA mn;
CREATE TABLE mn.customers (customer_id bigint PRIMARY KEY, name text NOT NULL, tier text);
CREATE TABLE mn.orders (order_id bigint, customer_id bigint NOT NULL, status text, total numeric, PRIMARY KEY (customer_id, order_id));
CREATE TABLE mn.tiers (tier text PRIMARY KEY, label text);
SELECT isomer.distribute(ARRAY['mn.customers', 'mn.orders']::regclass[], 'customer_id');
SELECT isomer.make_reference(ARRAY['mn.tiers']::regclass[]);
INSERT INTO mn.tiers VALUES ('gold', 'Gold'), ('silver', 'Silver');
INSERT INTO mn.customers SELECT g, 'customer ' || g, CASE WHEN g % 2 = 0 THEN 'gold' ELSE 'silver' END FROM generate_series(1, 300) g;
INSERT INTO mn.orders SELECT g, 1 + g % 300, 'open', g FROM generate_series(1, 3000) g;
SQL
[ "$($ISO -c "SELECT count(DISTINCT nodename) FROM citus_shards WHERE table_name = 'mn.orders'::regclass")" -ge 2 ] \
  && pass "shards live on more than one node" || fail "shards on several nodes" ">=2" "1"

echo "== bounded projection across nodes"
$ISO <<'SQL' >/dev/null
SELECT isomer.create_projection(
  name => 'mn_customer', kind => 'document', key => 'customer_id', database => 'mn', lag => '3 seconds',
  query => $q$ SELECT c.customer_id, jsonb_build_object('name', c.name,
              'tier', (SELECT t.label FROM mn.tiers t WHERE t.tier = c.tier),
              'open', (SELECT count(*) FROM mn.orders o WHERE o.customer_id = c.customer_id AND o.status = 'open')) AS doc
              FROM mn.customers c $q$,
  sources => '{"mn.customers": "customer_id", "mn.orders": "customer_id", "mn.tiers": null}');
SQL
expect "documents built from every node" "300" "$(mongo_eval 'print(db.getSiblingDB("mn").mn_customer.countDocuments())')"
out=$($ISO -c "SELECT isomer.alter_projection('mn_customer', freshness => 'sync')" 2>&1 || true)
contains "sync across nodes is refused with a reason" "distributed across several nodes" "$out"
expect "the certificate checks triggers on every placement, on every node" "true" "$($ISO -c "SELECT isomer.certificate('mn_customer') ->> 'valid'")"

echo "== writes on each node reach the projection, read-your-writes across nodes"
k_coord=""; k_worker=""
for k in $(seq 1 40); do
  n=$(node_of "$k")
  [ "$n" = "db" ] && [ -z "$k_coord" ] && k_coord=$k
  [ "$n" = "worker1" ] && [ -z "$k_worker" ] && k_worker=$k
done
for k in $k_coord $k_worker; do
  read_back=$($ISO <<SQL
UPDATE mn.customers SET name = 'renamed on ' || '$(node_of "$k")' WHERE customer_id = $k;
SELECT isomer.wait_for(isomer.last_write_token(), 'mn_customer', '10 seconds') IS NOT NULL;
SQL
)
  expect "a write to a key on $(node_of "$k") is read back after wait_for" "renamed on $(node_of "$k")" \
    "$(mongo_eval "print(db.getSiblingDB(\"mn\").mn_customer.findOne({_id: $k}).name)")"
done
# Customers with even ids are gold; pick one whose shard is on the worker.
k_gold=""; for k in $(seq 2 2 80); do [ "$(node_of "$k")" = "worker1" ] && { k_gold=$k; break; }; done
$ISO -c "UPDATE mn.tiers SET label = 'GOLD' WHERE tier = 'gold'" -c "SELECT isomer.wait_for(isomer.last_write_token(), 'mn_customer', '15 seconds') IS NOT NULL" >/dev/null
expect "a reference-table change re-derives documents on every node" "GOLD" \
  "$(mongo_eval "print(db.getSiblingDB(\"mn\").mn_customer.findOne({_id: $k_gold}).tier)")"
expect "and the whole projection equals recomputation" "0" "$($ISO -c "SELECT count(*) FROM isomer.verify('mn_customer')")"

# A fresh session whose first statement is a multi-shard INSERT ... SELECT: the capture triggers on
# the coordinator's own shards run while COPYs to the workers are open. Nothing may start a
# subtransaction there (Citus would send SAVEPOINT down those busy connections).
$ISO -c "INSERT INTO mn.customers (customer_id, name, tier) SELECT g, 'bulk ' || g, 'silver' FROM generate_series(5001, 5200) g"
pass "a bulk insert from a fresh session into a table behind a projection succeeds"
$ISO -c "SELECT isomer.wait_for(isomer.current_token(), 'mn_customer', '20 seconds')" >/dev/null
expect "and every new row reaches the projection" "200" \
  "$(mongo_eval 'print(db.getSiblingDB("mn").mn_customer.countDocuments({name: /^bulk /}))')"

echo "== FR-P5 add a node online"
if [ "$($ISO -c "SELECT count(*) FROM pg_dist_node WHERE nodename = 'worker2'")" = "0" ]; then
  contains "worker2 added and rebalancing" "worker2:5432 added" "$($ISO -c "SELECT isomer.add_node('worker2')")"
else
  $ISO -c "SELECT citus_rebalance_start()" >/dev/null 2>&1 || true
  # The job is picked up asynchronously, so its status can still read "finished" for a moment.
  $ISO -c "SELECT citus_rebalance_wait()" >/dev/null 2>&1 || true
  pass "worker2 already in the cluster (rerun): rebalanced"
fi
$ISO -c "SELECT citus_rebalance_wait()" >/dev/null 2>&1 || true
eventually "rebalance finished" "finished" "$ISO -c \"SELECT coalesce((SELECT state::text FROM citus_rebalance_status()), 'finished')\"" 180
expect "shards now on three nodes" "3" "$($ISO -c "SELECT count(DISTINCT nodename) FROM citus_shards WHERE table_name = 'mn.orders'::regclass")"
repaired=$($ISO -c "SELECT count(*) FROM isomer.rebalance_finished()")
pass "capture triggers after the moves: $repaired projection(s) needed repair"
expect "the certificate is valid after the rebalance" "true" "$($ISO -c "SELECT isomer.certificate('mn_customer') ->> 'valid'")"
k_new=""; for k in $(seq 1 60); do [ "$(node_of "$k")" = "worker2" ] && { k_new=$k; break; }; done
$ISO -c "UPDATE mn.orders SET status = 'shipped' WHERE customer_id = $k_new" -c "SELECT isomer.wait_for(isomer.last_write_token(), 'mn_customer', '10 seconds') IS NOT NULL" >/dev/null
expect "a write to a shard moved to worker2 reaches the projection" "0" \
  "$(mongo_eval "print(db.getSiblingDB(\"mn\").mn_customer.findOne({_id: $k_new}).open)")"

echo "== correctness under concurrent writes on every node"
$ISO <<'SQL' >/dev/null
CREATE PROCEDURE public.mn_worker(w int, n int) LANGUAGE plpgsql AS $$
DECLARE i int; c bigint;
BEGIN
    FOR i IN 1..n LOOP
        c := 1 + floor(random() * 300)::bigint;
        CASE floor(random() * 4)::int
        WHEN 0 THEN UPDATE mn.orders SET status = CASE WHEN status = 'open' THEN 'shipped' ELSE 'open' END
                    WHERE customer_id = c AND order_id = (SELECT min(order_id) FROM mn.orders WHERE customer_id = c);
        WHEN 1 THEN INSERT INTO mn.orders VALUES (100000 * w + i, c, 'open', i);
        WHEN 2 THEN DELETE FROM mn.orders WHERE customer_id = c AND order_id = (SELECT max(order_id) FROM mn.orders WHERE customer_id = c);
        ELSE        UPDATE mn.customers SET name = 'w' || w || '-' || i WHERE customer_id = c;
        END CASE;
        IF random() < 0.15 THEN ROLLBACK; ELSE COMMIT; END IF;
    END LOOP;
END $$;
SQL
start=$(python3 -c 'import time; print(int(time.time()*1000))')
for w in 1 2 3; do $ISO -c "CALL public.mn_worker($w, 120)" >/dev/null & done
wait
pass "3 workers ran 360 random transactions across nodes in $(( $(python3 -c 'import time; print(int(time.time()*1000))') - start ))ms"
[ "$($ISO -c "SELECT count(*) FROM mn.orders WHERE order_id >= 100000")" -gt 0 ] \
  && pass "the workers' writes landed ($($ISO -c "SELECT count(*) FROM mn.orders WHERE order_id >= 100000") new orders)" \
  || fail "workers wrote rows" ">0" "0"
$ISO -c "SELECT isomer.wait_for(isomer.current_token(), 'mn_customer', '60 seconds')" >/dev/null
expect "the projection equals recomputation for every key" "0" "$($ISO -c "SELECT count(*) FROM isomer.verify('mn_customer')")"
expect "no pull or drain errors" "0" "$($ISO -c "SELECT count(*) FROM isomer.events WHERE level = 'error'")"

echo "== multi-node passed"
