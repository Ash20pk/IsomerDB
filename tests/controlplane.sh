#!/bin/bash
# The cloud control plane on a local kind cluster (FR-CL1, FR-CL2): a team signs up, creates a
# cluster in a region on a plan, waits for it, reads its credentials and connects with them,
# scales it within its plan, sees its usage collected and priced on an invoice, can't see another
# team's clusters, and deletes it.
#
#   ./tests/controlplane.sh     # creates the kind cluster "isomer" if needed, builds and loads images
#
# Uses its own kubeconfig (operator/.kubeconfig-kind); the current kubectl context is never touched.
set -euo pipefail
cd "$(dirname "$0")/.."

export KUBECONFIG=$PWD/operator/.kubeconfig-kind
PORT=18095
ADMIN=dev-admin-token

pass() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"
  echo "-- state at failure --"
  kubectl get isomer -A 2>&1 | sed 's/^/       /'
  kubectl -n isomerdb-cloud logs deploy/controlplane --tail=20 2>&1 | cut -c1-240 | sed 's/^/       /'
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
# call METHOD PATH [BODY] [TOKEN]: prints "<status> <body>"
call() {
  local body; body=$(mktemp)
  local args=(-sS -o "$body" -w '%{http_code}' -X "$1" "http://localhost:$PORT$2" -H 'Content-Type: application/json')
  [ -n "${4:-}" ] && args+=(-H "Authorization: Bearer $4")
  [ -n "${3:-}" ] && args+=(-d "$3")
  local code; code=$(curl "${args[@]}")
  echo "$code $(cat "$body")"; rm -f "$body"
}
# Request bodies are built here: escaped quotes nested in "$( )" are mangled by macOS's bash 3.2.
signup() { printf '{"organization": "%s", "email": "%s", "password": "%s"}' "$1" "$2" "$3"; }
login() { printf '{"email": "%s", "password": "%s"}' "$1" "$2"; }
json() { python3 -c "import json,sys; d=json.loads(sys.stdin.read().split(' ',1)[1]); print(eval(sys.argv[1]))" "$1"; }

echo "== kind cluster, operator and control plane"
kind get clusters 2>/dev/null | grep -qx isomer || kind create cluster --name isomer --kubeconfig "$KUBECONFIG"
docker image inspect isomerdb:dev isomer-api:dev >/dev/null
docker build -q -t isomerdb-operator:dev operator >/dev/null
docker build -q -f controlplane/Dockerfile -t isomerdb-controlplane:dev . >/dev/null
# postgres:18.6-bookworm (the control plane's own database) is multi-platform, which kind can't
# load; the node pulls it from the registry instead.
for img in isomerdb:dev isomer-api:dev isomerdb-operator:dev isomerdb-controlplane:dev; do
  kind load docker-image "$img" --name isomer >/dev/null
done
kubectl apply -k operator/config >/dev/null
kubectl -n isomerdb-system rollout restart deploy/isomerdb-operator >/dev/null
kubectl -n isomerdb-system rollout status deploy/isomerdb-operator --timeout=180s >/dev/null
# Clusters a previous run left behind.
for ns in $(kubectl get ns -l isomerdb.io/managed-by=isomerdb-cloud -o name | cut -d/ -f2); do
  kubectl -n "$ns" delete isomer --all --wait=true >/dev/null 2>&1 || true
done
kubectl apply -k controlplane/config >/dev/null
kubectl -n isomerdb-cloud rollout status statefulset/controlplane-postgres --timeout=180s >/dev/null
kubectl -n isomerdb-cloud rollout restart deploy/controlplane >/dev/null
kubectl -n isomerdb-cloud rollout status deploy/controlplane --timeout=180s >/dev/null
kubectl -n isomerdb-cloud port-forward svc/controlplane $PORT:8095 >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do curl -sf "http://localhost:$PORT/healthz" >/dev/null && break; sleep 1; done
pass "control plane serving"

RUN=$(date +%s)
echo "== sign up"
out=$(call POST /v1/signup "$(signup "acme-$RUN" "ops-$RUN@acme.test" "correct horse battery")")
expect "a team signs up and gets an API token" "201" "${out%% *}"
TOKEN=$(echo "$out" | json 'd["token"]')
contains "a second sign-up for the same organisation is refused" "409 " \
  "$(call POST /v1/signup "$(signup "acme-$RUN" "other-$RUN@acme.test" "correct horse battery")")"
expect "a weak password is refused" "400" \
  "$(call POST /v1/signup "$(signup "weak-$RUN" "w-$RUN@acme.test" short)" | cut -d' ' -f1)"
LOGIN=$(call POST /v1/tokens "$(login "ops-$RUN@acme.test" "correct horse battery")" | json 'd["token"]')
expect "logging in gives another token for the same team" "acme-$RUN" "$(call GET /v1/me '' "$LOGIN" | json 'd["organization"]')"
expect "a wrong password is refused" "401" \
  "$(call POST /v1/tokens "$(login "ops-$RUN@acme.test" "wrong wrong wrong")" | cut -d' ' -f1)"
expect "no token, no clusters" "401" "$(call GET /v1/clusters | cut -d' ' -f1)"
expect "logging out revokes that token" "204 401" \
  "$(call DELETE /v1/tokens/current '' "$LOGIN" | cut -d' ' -f1) $(call GET /v1/me '' "$LOGIN" | cut -d' ' -f1)"
page=$(curl -sS -D - "http://localhost:$PORT/")
contains "the sign-up page is served" "Create your account" "$(curl -sS "http://localhost:$PORT/app.js")"
contains "under a content security policy" "default-src 'self'" "$page"
contains "regions are listed" "local-1" "$(call GET /v1/regions)"
contains "plans are listed with prices" "nodeHourUSD" "$(call GET /v1/plans)"

echo "== FR-CL1 a cluster on a plan"
expect "a bad name is refused" "400" "$(call POST /v1/clusters '{"name": "Bad_Name", "region": "local-1", "plan": "dev"}' "$TOKEN" | cut -d' ' -f1)"
expect "an unknown plan is refused" "400" "$(call POST /v1/clusters '{"name": "app", "region": "local-1", "plan": "gold"}' "$TOKEN" | cut -d' ' -f1)"
out=$(call POST /v1/clusters '{"name": "app", "region": "local-1", "plan": "dev"}' "$TOKEN")
expect "a cluster is created" "201" "${out%% *}"
NS=$(kubectl get isomer -A -l "isomerdb.io/org" -o jsonpath="{range .items[?(@.metadata.name=='app')]}{.metadata.namespace}{'\n'}{end}" | tail -1)
expect "as an IsomerCluster in the team's own namespace" "app" "$(kubectl -n "$NS" get isomer app -o jsonpath='{.metadata.name}')"
expect "the same name twice is refused" "409" "$(call POST /v1/clusters '{"name": "app", "region": "local-1", "plan": "dev"}' "$TOKEN" | cut -d' ' -f1)"
eventually "the cluster becomes Ready" "Ready" "call GET /v1/clusters/app '' $TOKEN | json 'd[\"phase\"]'" 600
contains "it reports where to connect" "app.$NS.svc.cluster.local:5432" "$(call GET /v1/clusters/app '' "$TOKEN")"

creds=$(call GET /v1/clusters/app/credentials '' "$TOKEN")
PW=$(echo "$creds" | json 'd["postgres_password"]')
expect "credentials are the cluster's own" "$(kubectl -n "$NS" get secret app-credentials -o jsonpath='{.data.postgres-password}' | base64 -d)" "$PW"
POD=$(kubectl -n "$NS" get pod -l "app.kubernetes.io/instance=app,isomerdb.io/node=coordinator" -o name | head -1)
expect "and connect over the network" "1" \
  "$(kubectl -n "$NS" exec "$POD" -c postgres -- psql "postgres://postgres:$PW@app.$NS.svc.cluster.local:5432/postgres" -tAc 'SELECT 1')"

echo "== teams are kept apart"
OTHER=$(call POST /v1/signup "$(signup "globex-$RUN" "ops-$RUN@globex.test" "correct horse battery")" | json 'd["token"]')
expect "another team can't see the cluster" "404" "$(call GET /v1/clusters/app '' "$OTHER" | cut -d' ' -f1)"
expect "or its credentials" "404" "$(call GET /v1/clusters/app/credentials '' "$OTHER" | cut -d' ' -f1)"
expect "or list it" "200 []" "$(call GET /v1/clusters '' "$OTHER")"

echo "== scaling within the plan"
expect "beyond the plan's limit is refused" "400" "$(call PATCH /v1/clusters/app '{"workers": 5}' "$TOKEN" | cut -d' ' -f1)"
expect "one worker more" "200" "$(call PATCH /v1/clusters/app '{"workers": 1}' "$TOKEN" | cut -d' ' -f1)"
eventually "the worker joins" "1" "call GET /v1/clusters/app '' $TOKEN | json 'd[\"ready_workers\"]'" 600

echo "== FR-CL2 usage, collected and priced"
# The cluster meters itself every minute; collect once it has.
eventually "the cluster has metered itself" "t" \
  "kubectl -n $NS exec $POD -c postgres -- psql -U postgres -tAc \"SELECT count(*) > 0 FROM isomer.usage WHERE metric = 'node_hours'\"" 180
eventually "the control plane collects it" "t" \
  "curl -sS -X POST -H 'Authorization: Bearer $ADMIN' http://localhost:$PORT/v1/admin/collect >/dev/null; call GET /v1/usage '' $TOKEN | json 'any(u[\"metric\"] == \"node_hours\" for u in d[\"clusters\"].get(\"app\", [])) and \"t\" or \"f\"'" 120
expect "only the admin may trigger a collection" "403" "$(call POST /v1/admin/collect '' "$TOKEN" | cut -d' ' -f1)"
MONTH=$(date -u +%Y-%m)
inv=$(call GET "/v1/invoices/$MONTH" '' "$TOKEN")
expect "the invoice prices compute at the plan's node-hour rate" "0.05" \
  "$(echo "$inv" | json '[l["unit_price_usd"] for l in d["lines"] if l["cluster"] == "app" and l["item"] == "compute"][0]')"
expect "for the node-hours the cluster metered" "t" \
  "$(echo "$inv" | json '"t" if [l for l in d["lines"] if l["item"] == "compute"][0]["quantity"] > 0 else "f"')"
expect "storage is priced as GB-months" "t" \
  "$(echo "$inv" | json '"t" if any(l["item"] == "storage" and l["unit"] == "GB-months" for l in d["lines"]) else "f"')"
expect "the other team's invoice is empty" "0" "$(call GET "/v1/invoices/$MONTH" '' "$OTHER" | json 'len(d["lines"])')"

echo "== deleting"
expect "the cluster is deleted" "204" "$(call DELETE /v1/clusters/app '' "$TOKEN" | cut -d' ' -f1)"
eventually "and its IsomerCluster is gone" "" "kubectl -n $NS get isomer -o name" 300
expect "it is no longer listed" "200 []" "$(call GET /v1/clusters '' "$TOKEN")"
expect "but the time it ran is still billed" "t" \
  "$(call GET "/v1/invoices/$MONTH" '' "$TOKEN" | json '"t" if any(l["cluster"] == "app" for l in d["lines"]) else "f"')"
echo "== sign-up and log-in attempts are limited per address"
codes=""
for _ in $(seq 1 12); do codes="$codes $(call POST /v1/tokens "$(login "nobody-$RUN@acme.test" "wrong wrong wrong")" | cut -d' ' -f1)"; done
contains "a burst of failed log-ins is cut off" "429" "$codes"
echo "controlplane: all checks passed"
