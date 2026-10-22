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
# SQL on whichever coordinator pod is the primary (it moves with a failover).
primary_of() { $K get pod -l "app.kubernetes.io/instance=$CL,isomerdb.io/node=$1,isomerdb.io/primary=true" -o name | head -1 | cut -d/ -f2; }
PSQL_() { $K exec -i "$(primary_of coordinator)" -c postgres -- psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1 "$@"; }
PSQL=PSQL_

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
  local name=$1 want=$2 cmd=$3 timeout=$(( ${4:-300} * ${ISOMER_TIMEOUT_SCALE:-1} )) got="" start=$SECONDS
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
expect "the status reports the isomer version" "$(sed -n "s/^default_version = '\(.*\)'/\1/p" extensions/isomer/isomer.control)" \
  "$($K get isomer $CL -o jsonpath='{.status.isomerVersion}')"
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

echo "== TLS and certificate authentication between nodes"
W0=$CL-worker-0.$CL-worker.$NS.svc.cluster.local
NODECONN="sslmode=verify-full sslrootcert=/etc/isomerdb/tls/ca.crt sslcert=/etc/isomerdb/tls/client.crt sslkey=/etc/isomerdb/tls/client.key"
from_coordinator() { $K exec -i $CL-coordinator-0 -c postgres -- psql "host=$W0 dbname=postgres $1" -X -tAc "$2" 2>&1 | grep -v "command terminated"; }
contains "a worker refuses a connection without TLS" "no pg_hba.conf entry" "$(from_coordinator "user=postgres sslmode=disable" "SELECT 1")"
contains "and one over TLS without the node certificate" "certificate" "$(from_coordinator "user=postgres sslmode=require" "SELECT 1")"
expect "the node certificate verifies the worker and may act as any role" "isomer" "$(from_coordinator "user=isomer $NODECONN" "SELECT current_user")"
expect "every connection between nodes is encrypted" "0" \
  "$(sql "SELECT sum(result::int) FROM run_command_on_workers(\$\$SELECT count(*) FROM pg_stat_ssl s JOIN pg_stat_activity a USING (pid) WHERE a.client_addr IS NOT NULL AND NOT s.ssl\$\$)")"
expect "and authenticated with the node certificate" "t" \
  "$(sql "SELECT bool_and(result::int > 0) FROM run_command_on_workers(\$\$SELECT count(*) FROM pg_stat_ssl WHERE client_dn = '/CN=isomer-node'\$\$)")"

old_serial=$($K get secret $CL-tls -o jsonpath='{.data.client\.crt}' | base64 -d | openssl x509 -noout -serial | cut -d= -f2)
$K delete secret $CL-tls >/dev/null
eventually "a deleted node certificate is reissued" "changed" \
  "s=\$($K get secret $CL-tls -o jsonpath='{.data.client\\.crt}' | base64 -d | openssl x509 -noout -serial | cut -d= -f2); [ -n \"\$s\" ] && [ \"\$s\" != '$old_serial' ] && echo changed" 60
new_serial=$(python3 -c "print(int('$($K get secret $CL-tls -o jsonpath='{.data.client\.crt}' | base64 -d | openssl x509 -noout -serial | cut -d= -f2)', 16))")
eventually "nodes pick up the new certificate without a restart" "$new_serial" \
  "$K exec -i $CL-coordinator-0 -c postgres -- psql \"host=$W0 dbname=postgres user=postgres $NODECONN\" -X -tAc \"SELECT client_serial FROM pg_stat_ssl WHERE pid = pg_backend_pid()\"" 240
expect "queries across nodes still work" "$(sql "SELECT count(*) FROM shop.customers")" "500"

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

echo "== high availability: a standby for every node, and failover"
$K patch isomer $CL --type merge -p '{"spec":{"ha":{"standbys":1,"failoverAfter":"20s"}}}' >/dev/null
group() { $K get isomer $CL -o jsonpath="{.status.groups[?(@.name=='$1')].$2}"; }
eventually "every node gets a standby that streams" "true true true" \
  "echo \$(group coordinator-0 standbyStreaming) \$(group worker-0 standbyStreaming) \$(group worker-1 standbyStreaming)" 600
expect "standbys are hot: read-only, with the data" "t|$before_rows" \
  "$($K exec -i $CL-worker-replica-0 -c postgres -- psql -U postgres -X -tAc "SELECT pg_is_in_recovery()")|$(sql "SELECT count(*) FROM shop.orders")"

# Freeze a pod's postmaster: it stops answering, as a hung node would, while its container stays up.
# From the kind node: inside the container, PID 1 is its namespace's init and ignores SIGSTOP.
freeze() {
  docker exec isomer-control-plane sh -c "kill -STOP \$(crictl inspect --output go-template --template '{{.info.pid}}' \
    \$(crictl ps --name postgres --state running --pod \$(crictl pods --name $1 --namespace $NS --state ready -q | head -1) -q | head -1))"
}
old_w0=$(group worker-0 primary)
[ -n "$old_w0" ] || fail "the status names worker-0's primary" "a pod" "nothing"
freeze $old_w0
eventually "the frozen worker's standby is promoted" "worker-0 $CL-worker-replica-0" \
  "$K get isomer $CL -o jsonpath='{.status.lastFailover.group} {.status.lastFailover.to}'" 180
expect "Citus now routes that worker's shards to it" "1" \
  "$(sql "SELECT count(*) FROM pg_dist_node WHERE nodename LIKE '$CL-worker-replica-0.%' AND isactive")"
eventually "every row is still readable" "$before_rows" "sql 'SELECT count(*) FROM shop.orders'" 120
$PSQL >/dev/null <<'SQL'
UPDATE shop.customers SET name = 'after worker failover' WHERE customer_id = 11;
SELECT isomer.wait_for(isomer.last_write_token(), 'customer_card', '30 seconds');
SQL
expect "projections keep being maintained" "after worker failover" "$(sql "SELECT isomer.get('customer_card', '11') ->> 'name'")"
eventually "the old primary comes back as the new standby" "$old_w0 true" \
  "echo \$(group worker-0 standby) \$(group worker-0 standbyStreaming)" 600

old_c=$(primary_of coordinator)
[ -n "$old_c" ] || fail "a coordinator pod is labelled primary" "a pod" "nothing"
freeze $old_c
eventually "the frozen coordinator's standby is promoted" "coordinator-0" \
  "$K get isomer $CL -o jsonpath='{.status.lastFailover.group}'" 180
eventually "clients reach the new coordinator through the same Service" "$before_rows" "sql 'SELECT count(*) FROM shop.orders'" 180
eventually "which Citus comes to know as its coordinator" "1" \
  "sql \"SELECT count(*) FROM pg_dist_node WHERE groupid = 0 AND metadatasynced AND nodename LIKE '\$(primary_of coordinator).%'\"" 120
eventually "MongoDB clients too" "after worker failover" \
  "mongo 'print(db.getSiblingDB(\"shop\").customer_card.findOne({_id: 11}).name)'" 180
eventually "the HTTP API too" "after worker failover" \
  "curl -s -H 'Authorization: Bearer $KEY' localhost:18080/v1/projections/customer_card/docs/11 | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"name\"])'" 120
$PSQL >/dev/null <<'SQL'
UPDATE shop.customers SET name = 'after coordinator failover' WHERE customer_id = 12;
SELECT isomer.wait_for(isomer.last_write_token(), 'customer_card', '30 seconds');
SQL
expect "writes and projections carry on" "after coordinator failover" "$(sql "SELECT isomer.get('customer_card', '12') ->> 'name'")"
eventually "the cluster is Ready with every standby streaming again" "Ready True" \
  "$K get isomer $CL -o jsonpath='{.status.phase} {.status.conditions[?(@.type==\"StandbysStreaming\")].status}'" 600
expect "the projection still equals recomputation" "0" "$(sql "SELECT count(*) FROM isomer.verify('customer_card')")"

$K patch isomer $CL --type merge -p '{"spec":{"ha":{"standbys":0}}}' >/dev/null
eventually "turning standbys off moves every primary back and removes the replicas" "0 0" \
  "echo \$($K get pods -o name | grep -c replica) \$(sql \"SELECT count(*) FROM pg_dist_node WHERE nodename LIKE '%replica%'\")" 600
expect "with nothing lost" "$before_rows" "$(sql "SELECT count(*) FROM shop.orders")"
expect "including the writes made after failing over" "after coordinator failover" "$(sql "SELECT isomer.get('customer_card', '12') ->> 'name'")"
eventually "and the cluster is Ready" "Ready" "$K get isomer $CL -o jsonpath='{.status.phase}'" 300

echo "== deleting the resource cleans up"
kill $PF 2>/dev/null || true; wait $PF 2>/dev/null || true
$K delete isomer $CL --wait=true >/dev/null
eventually "pods, services and volumes are gone" "0 0 0" \
  "echo \$($K get pods -o name | wc -l) \$($K get svc -o name | wc -l) \$($K get pvc -o name | wc -l)" 180
kubectl delete namespace $NS --wait=false >/dev/null

echo "== operator passed"
