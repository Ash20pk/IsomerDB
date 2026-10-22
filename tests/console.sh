#!/bin/bash
# The console and CLI (FR-O3): the admin API they share, who may use it, the CLI against a live
# stack, and the console's static files.
#
#   docker compose up -d && (cd clients && npm ci && npm run build) && ./tests/console.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
API=${ISOMER_API:-http://localhost:8080}
CLI="node clients/cli/dist/isomer.mjs"

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }
status() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
get() { curl -sf -H "Authorization: Bearer $1" "$API$2"; }
post() { curl -sf -X POST -H "Authorization: Bearer $1" -H 'content-type: application/json' -d "${3:-{\}}" "$API$2"; }
jq_() { node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const v=JSON.parse(s);console.log(eval(process.argv[1]))})' "$1"; }

[ -f clients/cli/dist/isomer.mjs ] || (cd clients && npm ci --silent && npm run build --silent >/dev/null)

echo "== reset"
$ISO <<'SQL' >/dev/null
ALTER EXTENSION isomer UPDATE;
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'cx_%';
DELETE FROM isomer.api_keys WHERE name LIKE 'cx_%';
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name IN (SELECT projection FROM isomer.proposals WHERE source_query LIKE '%cx.%');
DELETE FROM isomer.proposals WHERE source_query LIKE '%cx.%';
DROP SCHEMA IF EXISTS cx CASCADE;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cx_ops') THEN DROP OWNED BY cx_ops; DROP ROLE cx_ops; END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cx_reader') THEN DROP OWNED BY cx_reader; DROP ROLE cx_reader; END IF;
END $$;
CREATE ROLE cx_ops;
GRANT isomer_admin TO cx_ops;
CREATE ROLE cx_reader;
CREATE SCHEMA cx;
CREATE TABLE cx.customers (id bigint PRIMARY KEY, name text NOT NULL);
CREATE TABLE cx.orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL, total numeric NOT NULL);
INSERT INTO cx.customers SELECT g, 'customer ' || g FROM generate_series(1, 50) g;
INSERT INTO cx.orders SELECT g, 1 + g % 50, g FROM generate_series(1, 300) g;
GRANT USAGE ON SCHEMA cx TO cx_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA cx TO cx_reader;
SELECT isomer.create_projection(
  name => 'cx_card', kind => 'document', key => 'id', database => 'cx', lag => '2 seconds',
  query => $q$ SELECT c.id, jsonb_build_object('name', c.name,
              'orders', (SELECT count(*) FROM cx.orders o WHERE o.customer_id = c.id)) AS doc FROM cx.customers c $q$,
  sources => '{"cx.customers": "id", "cx.orders": "customer_id"}');
SELECT isomer.create_projection(
  name => 'cx_total', kind => 'cache', key => 'id', freshness => 'sync', target_schema => 'cx',
  query => $q$ SELECT c.id, (SELECT coalesce(sum(o.total), 0) FROM cx.orders o WHERE o.customer_id = c.id) AS value FROM cx.customers c $q$,
  sources => '{"cx.customers": "id", "cx.orders": "customer_id"}');
SQL
OPS=$($ISO -c "SELECT isomer.create_api_key('cx_ops', 'cx_ops')")
READER=$($ISO -c "SELECT isomer.create_api_key('cx_reader', 'cx_reader')")

echo "== who may use the admin API"
expect "no key: 401" "401" "$(status "$API/v1/admin/overview")"
expect "a key whose role is not an admin: 403" "403" "$(status -H "Authorization: Bearer $READER" "$API/v1/admin/overview")"
expect "the same key still reads projections it may read" "200" "$(status -H "Authorization: Bearer $READER" "$API/v1/projections/cx_card/docs/1")"
expect "a member of isomer_admin: 200" "200" "$(status -H "Authorization: Bearer $OPS" "$API/v1/admin/overview")"
expect "admin actions are refused to others too" "403" "$(status -X POST -H "Authorization: Bearer $READER" "$API/v1/admin/audit")"

echo "== overview and detail"
o=$(get "$OPS" /v1/admin/overview)
expect "overview lists both projections" "cx_card,cx_total" "$(echo "$o" | jq_ 'v.projections.map(p=>p.name).filter(n=>n.startsWith("cx_")).join(",")')"
expect "with a lag budget in milliseconds" "2000" "$(echo "$o" | jq_ 'v.projections.find(p=>p.name==="cx_card").lag_budget_ms')"
expect "and the node list" "true" "$(echo "$o" | jq_ 'v.nodes.length >= 1')"
d=$(get "$OPS" /v1/admin/projections/cx_card)
expect "detail has sources" "cx.customers:id,cx.orders:customer_id" "$(echo "$d" | jq_ 'v.sources.map(s=>s.table+":"+s.key_column).join(",")')"
expect "detail has a certificate" "true" "$(echo "$d" | jq_ 'typeof v.certificate === "object" && !v.certificate.error')"
expect "an unknown projection: 404" "404" "$(status -H "Authorization: Bearer $OPS" "$API/v1/admin/projections/nope")"

echo "== lag shows up while a projection is behind"
$ISO -c "SELECT cron.alter_job(jobid, active => false) FROM cron.job WHERE jobname = 'isomer-drain'" >/dev/null
trap '$ISO -c "SELECT cron.alter_job(jobid, active => true) FROM cron.job WHERE jobname = '"'"'isomer-drain'"'"'" >/dev/null' EXIT
sleep 2
$ISO -c "UPDATE cx.customers SET name = name || '!' WHERE id <= 5" >/dev/null
sleep 3
o=$(get "$OPS" /v1/admin/overview)
expect "five keys queued" "5" "$(echo "$o" | jq_ 'v.projections.find(p=>p.name==="cx_card").pending_keys')"
expect "oldest pending exceeds the budget" "true" "$(echo "$o" | jq_ 'v.projections.find(p=>p.name==="cx_card").oldest_pending_ms > 2000')"
contains "a lag_budget alert is raised" "lag_budget" "$(echo "$o" | jq_ 'v.alerts.map(a=>a.alert+":"+a.subject).join(",")')"
out=$($CLI status --url "$API" --key "$OPS" || true)
contains "the CLI shows the projection's backlog" "cx_card" "$out"
contains "and the alert" "lag_budget" "$out"
set +e; $CLI status --url "$API" --key "$OPS" >/dev/null; code=$?; set -e
expect "and exits 1 while there are alerts" "1" "$code"
$ISO -c "SELECT cron.alter_job(jobid, active => true) FROM cron.job WHERE jobname = 'isomer-drain'" >/dev/null
sleep 3
expect "the backlog clears once draining resumes" "0" "$(get "$OPS" /v1/admin/overview | jq_ 'v.projections.find(p=>p.name==="cx_card").pending_keys')"

echo "== actions"
# The audit covers every projection on the stack (other suites plant drift on purpose), so look
# only at this suite's.
r=$(post "$OPS" /v1/admin/audit '{"sample": 20}')
expect "audit: no drift in cx_ projections" "0" "$(echo "$r" | jq_ 'v.report.filter(d=>d.projection.startsWith("cx_")).reduce((n,d)=>n+d.drift,0)')"
expect "and the audit counted checks for them" "true" "$(echo "$r" | jq_ 'v.report.filter(d=>d.projection.startsWith("cx_")).every(d=>d.checks>0)')"
expect "repair has no drifted keys to fix" "0" "$(post "$OPS" /v1/admin/projections/cx_card/repair | jq_ 'v.repaired')"
expect "the CLI audit agrees" "0" "$($CLI audit --json --url "$API" --key "$OPS" | jq_ 'v.report.filter(d=>d.projection.startsWith("cx_")).reduce((n,d)=>n+d.drift,0)' || true)"
expect "drift via the CLI" "0" "$($CLI drift --json --url "$API" --key "$OPS" | jq_ 'v.find(r=>r.projection==="cx_card").drift')"

# Proposals: run a repeated join query, mine the log, accept, then route it.
for i in $(seq 1 60); do echo "SELECT c.id, c.name, o.id AS order_id, o.total FROM cx.customers c JOIN cx.orders o ON o.customer_id = c.id WHERE c.id = $(( 1 + i % 50 ));"; done \
  | docker compose exec -T db psql -U postgres -d postgres -X -q -o /dev/null
n=$(post "$OPS" /v1/admin/propose | jq_ 'v.proposed')
[ "$n" -ge 1 ] && pass "propose finds the repeated join ($n)" || fail "propose finds the repeated join" ">= 1" "$n"
pid=$(get "$OPS" /v1/admin/proposals | jq_ 'v.proposals.find(p=>p.source_query.includes("cx.orders") && p.status==="proposed").id')
contains "the CLI lists it" "FROM cx.customers" "$($CLI proposals --url "$API" --key "$OPS")"
r=$($CLI accept "$pid" --name cx_proposed --url "$API" --key "$OPS")
contains "accepting creates the projection" "cx_proposed" "$r"
expect "the proposal is marked accepted" "accepted" "$(get "$OPS" /v1/admin/proposals | jq_ "v.proposals.find(p=>p.id===$pid).status")"
r=$(post "$OPS" "/v1/admin/proposals/$pid/route")
pass "routing it measures a route: $(echo "$r" | jq_ 'v.result')"
route=$(get "$OPS" /v1/admin/routes | jq_ "JSON.stringify(v.routes.find(r=>r.proposal===$pid))")
qid=$(echo "$route" | jq_ 'v.queryid')
expect "the route is recorded against the proposal" "true" "$(echo "$route" | jq_ 'v.verified_keys > 0')"
expect "routes can be turned off" "false" "$(post "$OPS" "/v1/admin/routes/$qid" '{"enabled": false}' | jq_ 'v.enabled')"
expect "and on again from the CLI" "route $qid enabled" "$($CLI enable-route "$qid" --url "$API" --key "$OPS")"
expect "the change is stored" "t" "$($ISO -c "SELECT enabled FROM isomer.routes WHERE queryid = $qid")"
expect "an unknown route: 404" "404" "$(status -X POST -H "Authorization: Bearer $OPS" -H 'content-type: application/json' -d '{"enabled":true}' "$API/v1/admin/routes/12345")"
expect "events are listed" "true" "$(get "$OPS" '/v1/admin/events?limit=5' | jq_ 'Array.isArray(v.events)')"
expect "the advisor answers" "true" "$(get "$OPS" /v1/admin/advice | jq_ 'Array.isArray(v.advice)')"
expect "attachments answer" "true" "$(get "$OPS" /v1/admin/attachments | jq_ 'Array.isArray(v.attachments)')"

echo "== the console"
expect "/ redirects to the console" "307" "$(status "$API/")"
contains "the console page loads" "IsomerDB console" "$(curl -sf "$API/console/")"
expect "with its script" "200" "$(status "$API/console/app.js")"
expect "and stylesheet" "200" "$(status "$API/console/style.css")"

echo "all console tests passed"
