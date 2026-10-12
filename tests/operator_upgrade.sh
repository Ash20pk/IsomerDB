#!/bin/bash
# Version upgrades with the Kubernetes operator, on kind: a cluster with standbys moves to a new
# image (which carries a newer isomer extension) one node group at a time — standby first, then a
# clean switchover onto it — while a writer keeps writing; then every node's extensions are
# updated. Nothing is lost, and writes carry on through each switchover.
#
#   ./tests/operator_upgrade.sh   # creates the kind cluster "isomer" if needed, builds and loads images
#
# Uses its own kubeconfig (operator/.kubeconfig-kind); the current kubectl context is never touched.
set -euo pipefail
cd "$(dirname "$0")/.."

export KUBECONFIG=$PWD/operator/.kubeconfig-kind
NS=isomer-upgrade
CL=u1
K="kubectl -n $NS"

pass() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"
  echo "-- state at failure --"
  $K get isomer,pods -o wide 2>&1 | sed 's/^/       /'
  kubectl -n isomerdb-system logs deploy/isomerdb-operator --tail=15 2>&1 | cut -c1-240 | sed 's/^/       /'
  exit 1
}
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=${4:-300} got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 3
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}
primary_of() { $K get pod -l "app.kubernetes.io/instance=$CL,isomerdb.io/node=$1,isomerdb.io/primary=true" -o name | head -1 | cut -d/ -f2; }
sql() { $K exec -i "$(primary_of coordinator)" -c postgres -- psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1 -c "$1"; }

CURRENT=$(sed -n "s/^default_version = '\(.*\)'/\1/p" extensions/isomer/isomer.control)
NEXT="$CURRENT.1"

echo "== images: the current one, and a next one with isomer $NEXT"
docker image inspect isomerdb:dev >/dev/null
ctx=$(mktemp -d)
trap 'rm -rf "$ctx"' EXIT
cat > "$ctx/isomer--$CURRENT--$NEXT.sql" <<EOF
-- Test upgrade: no changes beyond the version.
\echo Use "ALTER EXTENSION isomer UPDATE TO '$NEXT'" to load this file. \quit
COMMENT ON SCHEMA isomer IS 'IsomerDB $NEXT';
EOF
cat > "$ctx/Dockerfile" <<EOF
FROM isomerdb:dev
COPY isomer--$CURRENT--$NEXT.sql /usr/share/postgresql/18/extension/
RUN sed -i "s/^default_version = .*/default_version = '$NEXT'/" /usr/share/postgresql/18/extension/isomer.control
EOF
docker build -q -t isomerdb:upgrade-next "$ctx" >/dev/null
pass "built isomerdb:upgrade-next"

echo "== kind cluster and operator"
kind get clusters 2>/dev/null | grep -qx isomer || kind create cluster --name isomer --kubeconfig "$KUBECONFIG"
docker build -q -t isomerdb-operator:dev operator >/dev/null
for img in isomerdb:dev isomerdb:upgrade-next isomerdb-operator:dev; do kind load docker-image "$img" --name isomer >/dev/null; done
kubectl apply -k operator/config >/dev/null
kubectl -n isomerdb-system rollout restart deploy/isomerdb-operator >/dev/null
kubectl -n isomerdb-system rollout status deploy/isomerdb-operator --timeout=180s >/dev/null
kubectl delete namespace $NS --ignore-not-found --wait=true >/dev/null
kubectl create namespace $NS >/dev/null
$K apply -f - >/dev/null <<EOF
apiVersion: isomerdb.io/v1alpha1
kind: IsomerCluster
metadata: { name: $CL }
spec:
  image: isomerdb:dev
  workers: 2
  storage: 2Gi
  deleteVolumes: true
  resources: { requests: { cpu: 200m, memory: 512Mi } }
  api: { enabled: false }
  ha: { standbys: 1, failoverAfter: 30s }
EOF
group() { $K get isomer $CL -o jsonpath="{.status.groups[?(@.name=='$1')].$2}"; }
eventually "the cluster is Ready with every standby streaming" "Ready True" \
  "$K get isomer $CL -o jsonpath='{.status.phase} {.status.conditions[?(@.type==\"StandbysStreaming\")].status}'" 900
expect "it runs isomer $CURRENT" "$CURRENT" "$($K get isomer $CL -o jsonpath='{.status.isomerVersion}')"
$K exec -i "$(primary_of coordinator)" -c postgres -- psql -U postgres -d postgres -X -q -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE SCHEMA shop;
CREATE TABLE shop.events (id bigserial, customer_id bigint NOT NULL, at timestamptz NOT NULL DEFAULT now(),
                          PRIMARY KEY (customer_id, id));
SELECT isomer.distribute(ARRAY['shop.events']::regclass[], 'customer_id');
CREATE TABLE shop.customers (customer_id bigint PRIMARY KEY, name text NOT NULL);
SELECT isomer.distribute(ARRAY['shop.customers']::regclass[], 'customer_id');
INSERT INTO shop.customers SELECT g, 'customer ' || g FROM generate_series(1, 200) g;
SELECT isomer.create_projection(
  name => 'customer_card', kind => 'document', key => 'customer_id', database => 'shop', lag => '2 seconds',
  query => $q$ SELECT c.customer_id, jsonb_build_object('name', c.name,
              'events', (SELECT count(*) FROM shop.events e WHERE e.customer_id = c.customer_id)) AS doc
              FROM shop.customers c $q$,
  sources => '{"shop.customers": "customer_id", "shop.events": "customer_id"}');
SQL

echo "== upgrade, with a writer running"
# The writer commits one event a second through the client Service and records each success.
$K run writer --restart=Never --image=isomerdb:dev --image-pull-policy=IfNotPresent \
  --env="PGPASSWORD=$($K get secret $CL-credentials -o jsonpath='{.data.postgres-password}' | base64 -d)" --command -- \
  sh -c "i=0; while true; do i=\$((i+1)); psql -h $CL -U postgres -d postgres -X -q -t -A -c \
    \"INSERT INTO shop.events (customer_id) VALUES (1 + \$i % 200) RETURNING 'ok'\" 2>/dev/null | grep -q ok && echo ok || echo err; sleep 1; done" >/dev/null
eventually "the writer is writing" "yes" "[ \$($K logs writer 2>/dev/null | grep -c ok) -ge 3 ] && echo yes" 120
before_failovers=$($K get isomer $CL -o jsonpath='{.status.lastFailover.at}')

$K patch isomer $CL --type merge -p '{"spec":{"image":"isomerdb:upgrade-next"}}' >/dev/null
eventually "every database pod runs the new image" "0" \
  "$K get pods -l app.kubernetes.io/instance=$CL,isomerdb.io/node -o jsonpath='{range .items[*]}{.spec.containers[0].image}{\"\\n\"}{end}' | grep -vc upgrade-next" 1200
eventually "the extension is updated on every node" "$NEXT $NEXT $NEXT" \
  "echo \$(sql \"SELECT extversion FROM pg_extension WHERE extname = 'isomer'\") \$(sql \"SELECT string_agg(result, ' ' ORDER BY nodename) FROM run_command_on_workers(\\\$\\\$SELECT extversion FROM pg_extension WHERE extname = 'isomer'\\\$\\\$)\")" 300
eventually "and the cluster is Ready, every standby streaming" "Ready True" \
  "$K get isomer $CL -o jsonpath='{.status.phase} {.status.conditions[?(@.type==\"StandbysStreaming\")].status}'" 600
expect "the status reports isomer $NEXT" "$NEXT" "$($K get isomer $CL -o jsonpath='{.status.isomerVersion}')"
reason=$($K get isomer $CL -o jsonpath='{.status.lastFailover.reason}')
case "$reason" in *upgrade*) pass "primaries moved by planned switchovers ($reason)";;
  *) fail "primaries moved by planned switchovers" "a reason mentioning the upgrade" "$reason";; esac

sleep 5
$K delete pod writer --wait=false >/dev/null
oks=$($K logs writer 2>/dev/null | grep -c ok || true)
errs=$($K logs writer 2>/dev/null | grep -c err || true)
rows=$(sql "SELECT count(*) FROM shop.events")
[ "$rows" -ge "$oks" ] && pass "no acknowledged write was lost ($oks acknowledged, $rows stored)" \
  || fail "no acknowledged write was lost" ">= $oks rows" "$rows"
[ "$errs" -le 30 ] && pass "writes paused only briefly across the switchovers ($errs failed attempts in $((oks + errs)))" \
  || fail "writes paused only briefly" "<= 30 failed attempts" "$errs"
sql "SELECT isomer.wait_for(isomer.current_token(), 'customer_card', '30 seconds')" >/dev/null
expect "the projection equals recomputation" "0" "$(sql "SELECT count(*) FROM isomer.verify('customer_card')")"

echo "== cleaning up"
$K delete isomer $CL --wait=true >/dev/null
kubectl delete namespace $NS --wait=false >/dev/null
echo "== operator upgrade passed"
