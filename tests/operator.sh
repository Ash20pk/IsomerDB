#!/bin/bash
# The Kubernetes operator on a local kind cluster: a cluster comes up from one resource, serves
# SQL, MongoDB and HTTP, scales workers up and down online (rebalance, then drain and remove),
# survives pod restarts with its data, and cleans up after itself.
#
#   ./tests/operator.sh          # creates the kind cluster "isomer" if needed, builds and loads images
#
# Uses its own kubeconfig (operator/.kubeconfig-kind); the current kubectl context is never touched.
set -euo pipefail
cd "$(dirname "$0")/.."

export KUBECONFIG=$PWD/operator/.kubeconfig-kind
NS=isomer-test
CL=t1
K="kubectl -n $NS"
PSQL="$K exec -i $CL-coordinator-0 -c postgres -- psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"

pass() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"
  echo "-- state at failure --"
  $K get isomer,pods -o wide 2>&1 | sed 's/^/       /'
  kubectl -n isomerdb-system logs deploy/isomerdb-operator --tail=20 2>&1 | cut -c1-240 | sed 's/^/       /'
  exit 1
}
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=${4:-300} got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 3
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}
sql() { $PSQL -c "$1"; }

echo "== kind cluster and operator"
kind get clusters 2>/dev/null | grep -qx isomer || kind create cluster --name isomer --kubeconfig "$KUBECONFIG"
docker image inspect isomerdb:dev isomer-api:dev >/dev/null
docker build -q -t isomerdb-operator:dev operator >/dev/null
for img in isomerdb:dev isomer-api:dev isomerdb-operator:dev; do kind load docker-image "$img" --name isomer >/dev/null; done
kubectl apply -k operator/config >/dev/null
kubectl -n isomerdb-system rollout restart deploy/isomerdb-operator >/dev/null
kubectl -n isomerdb-system rollout status deploy/isomerdb-operator --timeout=180s >/dev/null
pass "operator running"

echo "== a cluster from one resource"
kubectl delete namespace $NS --ignore-not-found --wait=true >/dev/null
kubectl create namespace $NS >/dev/null
$K apply -f - >/dev/null <<EOF
apiVersion: isomerdb.io/v1alpha1
kind: IsomerCluster
metadata:
  name: $CL
spec:
  image: isomerdb:dev
  workers: 2
  storage: 2Gi
  deleteVolumes: true
  resources:
    requests: { cpu: 200m, memory: 512Mi }
  api:
    enabled: true
    image: isomer-api:dev
EOF
eventually "the cluster is Ready" "Ready" "$K get isomer $CL -o jsonpath='{.status.phase}'" 600
expect "Citus has the coordinator and both workers" "3" "$(sql "SELECT count(*) FROM pg_dist_node WHERE isactive")"
expect "the status reports the isomer version" "0.8.0" "$($K get isomer $CL -o jsonpath='{.status.isomerVersion}')"
expect "credentials were generated" "36" "$($K get secret $CL-credentials -o jsonpath='{.data.postgres-password}' | base64 -d | wc -c | tr -d ' ')"

echo "== data across nodes"
$PSQL >/dev/null <<'SQL'
DROP SCHEMA IF EXISTS shop CASCADE;
CREATE SCHEMA shop;
CREATE TABLE shop.customers (customer_id bigint PRIMARY KEY, name text NOT NULL);
CREATE TABLE shop.orders (order_id bigint, customer_id bigint NOT NULL, status text, PRIMARY KEY (customer_id, order_id));
SELECT isomer.distribute(ARRAY['shop.customers', 'shop.orders']::regclass[], 'customer_id');
INSERT INTO shop.customers SELECT g, 'customer ' || g FROM generate_series(1, 500) g;
INSERT INTO shop.orders SELECT g, 1 + g % 500, 'open' FROM generate_series(1, 5000) g;
SELECT isomer.create_projection(
  name => 'customer_card', kind => 'document', key => 'customer_id', database => 'shop', lag => '2 seconds',
  query => $q$ SELECT c.customer_id, jsonb_build_object('name', c.name,
              'open', (SELECT count(*) FROM shop.orders o WHERE o.customer_id = c.customer_id AND o.status = 'open')) AS doc
              FROM shop.customers c $q$,
  sources => '{"shop.customers": "customer_id", "shop.orders": "customer_id"}');
SELECT isomer.set_history('customer_card', '1 day');
SQL
expect "shards on all three nodes" "3" "$(sql "SELECT count(DISTINCT nodename) FROM citus_shards WHERE table_name = 'shop.orders'::regclass")"
expect "the projection equals recomputation" "0" "$(sql "SELECT count(*) FROM isomer.verify('customer_card')")"

MONGO_PW=$($K get secret $CL-credentials -o jsonpath='{.data.mongo-password}' | base64 -d)
mongo() {
  $K run "mongosh-$RANDOM" --rm -i --restart=Never --quiet --image=mongo:8 --image-pull-policy=IfNotPresent -- \
    mongosh "mongodb://isomer:$MONGO_PW@$CL:10260/?tls=true&tlsAllowInvalidCertificates=true" --quiet --eval "$1" 2>/dev/null | tail -1
}
expect "MongoDB clients read the projection through the gateway" "customer 42" \
  "$(mongo 'print(db.getSiblingDB("shop").customer_card.findOne({_id: 42}).name)')"

$PSQL >/dev/null <<'SQL'
DO $$ BEGIN CREATE ROLE shop_reader; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
GRANT USAGE ON SCHEMA shop TO shop_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO shop_reader;
SQL
KEY=$(sql "SELECT isomer.create_api_key('op-test-' || floor(random() * 1e9)::text, 'shop_reader')")
$K port-forward svc/$CL-api 18080:8080 >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
eventually "the HTTP API answers" "200" "curl -s -o /dev/null -w '%{http_code}' localhost:18080/healthz" 60
expect "and serves the projection by key" "customer 42" \
  "$(curl -s -H "Authorization: Bearer $KEY" localhost:18080/v1/projections/customer_card/docs/42 | python3 -c 'import json,sys; print(json.load(sys.stdin)["name"])')"

echo "== scale up online"
before_rows=$(sql "SELECT count(*) FROM shop.orders")
$K scale isomer $CL --replicas=3 >/dev/null
eventually "a third worker joins and the cluster is Ready again" "Ready 3" \
  "$K get isomer $CL -o jsonpath='{.status.phase} {.status.readyWorkers}'" 600
expect "Citus has four nodes" "4" "$(sql "SELECT count(*) FROM pg_dist_node WHERE isactive")"
expect "shards were rebalanced onto the new worker" "4" "$(sql "SELECT count(DISTINCT nodename) FROM citus_shards WHERE table_name = 'shop.orders'::regclass")"
expect "no row lost" "$before_rows" "$(sql "SELECT count(*) FROM shop.orders")"
k_new=$(sql "SELECT k FROM generate_series(1, 200) k WHERE (SELECT n.nodename FROM pg_dist_placement p JOIN pg_dist_node n USING (groupid)
             WHERE p.shardid = get_shard_id_for_distribution_column('shop.orders', k)) LIKE '$CL-worker-2.%' LIMIT 1")
$PSQL >/dev/null <<SQL
UPDATE shop.orders SET status = 'shipped' WHERE customer_id = $k_new;
SELECT isomer.wait_for(isomer.last_write_token(), 'customer_card', '15 seconds');
SQL
expect "a write to a shard on the new worker reaches the projection" "0" \
  "$(sql "SELECT isomer.get('customer_card', '$k_new') ->> 'open'")"
expect "capture triggers were re-checked after the rebalance" "true" \
  "$(sql "SELECT isomer.certificate('customer_card') ->> 'valid'")"

echo "== scale down online: drain, then remove"
$K scale isomer $CL --replicas=2 >/dev/null
eventually "the third worker is drained, removed, and gone" "Ready 2 3" \
  "echo \$($K get isomer $CL -o jsonpath='{.status.phase} {.status.readyWorkers}') \$(sql 'SELECT count(*) FROM pg_dist_node WHERE isactive')" 600
eventually "its pod is gone" "" "$K get pod $CL-worker-2 -o name 2>/dev/null" 120
expect "no row lost" "$before_rows" "$(sql "SELECT count(*) FROM shop.orders")"
expect "the projection still equals recomputation" "0" "$(sql "SELECT count(*) FROM isomer.verify('customer_card')")"

echo "== restarts keep the data"
# Delete a pod, then wait for its replacement (a new uid) to be ready: the status alone can still
# read Ready from before the deletion.
restart() {
  local pod=$1 old
  old=$($K get pod "$pod" -o jsonpath='{.metadata.uid}')
  $K delete pod "$pod" --wait=true >/dev/null
  eventually "$pod is replaced and ready" "true" \
    "[ \"\$($K get pod $pod -o jsonpath='{.metadata.uid}')\" != '$old' ] && $K get pod $pod -o jsonpath='{.status.containerStatuses[0].ready}'" 300
}
restart $CL-coordinator-0
expect "rows survived (a query over every shard)" "$before_rows" "$(sql "SELECT count(*) FROM shop.orders")"
restart $CL-worker-0
expect "rows survived the worker restart too" "$before_rows" "$(sql "SELECT count(*) FROM shop.orders")"
eventually "the cluster reports Ready with both workers" "Ready 2" "$K get isomer $CL -o jsonpath='{.status.phase} {.status.readyWorkers}'" 120
$PSQL >/dev/null <<'SQL'
UPDATE shop.customers SET name = 'after restarts' WHERE customer_id = 7;
SELECT isomer.wait_for(isomer.last_write_token(), 'customer_card', '15 seconds');
SQL
expect "projections keep being maintained" "after restarts" "$(sql "SELECT isomer.get('customer_card', '7') ->> 'name'")"
expect "and history kept every version" "customer 7,after restarts" \
  "$(sql "SELECT string_agg(doc ->> 'name', ',' ORDER BY id) FROM (SELECT DISTINCT ON (doc ->> 'name') * FROM isomer.history('customer_card', '7') ORDER BY doc ->> 'name', id) h")"

echo "== deleting the resource cleans up"
kill $PF 2>/dev/null || true; wait $PF 2>/dev/null || true
$K delete isomer $CL --wait=true >/dev/null
eventually "pods, services and volumes are gone" "0 0 0" \
  "echo \$($K get pods -o name | wc -l) \$($K get svc -o name | wc -l) \$($K get pvc -o name | wc -l)" 180
kubectl delete namespace $NS --wait=false >/dev/null

echo "== operator passed"
