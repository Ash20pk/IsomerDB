#!/bin/bash
# Alerts reach on-call (FR-K3): drift and lag-budget breaches are delivered to a generic webhook,
# Slack and PagerDuty (a stand-in records them), each channel above its minimum severity; a
# resolved alert is delivered as resolved (PagerDuty: on the same dedup key); a failing channel is
# retried until it accepts.
#
#   docker compose --profile test up -d && ./tests/alerts.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
HOOKS=${ISOMER_MOCKHOOKS:-http://localhost:8090}

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=${4:-60} got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 1
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}
# What the stand-in received, filtered with a Python expression over r (one request).
received() { curl -sf "$HOOKS/received" | python3 -c "import json,sys; rs=json.load(sys.stdin); print(sum(1 for r in rs if $1))"; }
first() { curl -sf "$HOOKS/received" | python3 -c "import json,sys; rs=[r for r in json.load(sys.stdin) if $1]; print($2 if rs else '')"; }

echo "== reset"
docker compose --profile test up -d --wait mockhooks >/dev/null 2>&1
curl -sf -X DELETE "$HOOKS/received" >/dev/null
$ISO <<'SQL' >/dev/null
ALTER EXTENSION isomer UPDATE;
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'al_%';
SELECT isomer.drop_alert_channel(name) FROM isomer.alert_channels;
DELETE FROM isomer.alert_state; DELETE FROM isomer.alert_deliveries;
DELETE FROM isomer.drift_samples; DELETE FROM isomer.audit_stats WHERE projection LIKE 'al_%';
DROP SCHEMA IF EXISTS al CASCADE;
CREATE SCHEMA al;
CREATE TABLE al.customers (id bigint PRIMARY KEY, name text NOT NULL);
INSERT INTO al.customers SELECT g, 'customer ' || g FROM generate_series(1, 50) g;
SELECT isomer.create_projection(name => 'al_card', kind => 'document', key => 'id', database => 'al', lag => '2 seconds',
  query => $q$ SELECT c.id, jsonb_build_object('name', c.name) AS doc FROM al.customers c $q$, sources => '{"al.customers": "id"}');
SELECT isomer.add_alert_channel('al-webhook', 'webhook', 'http://mockhooks:8090/hook', secret_env => 'HOOK_TOKEN', min_severity => 'warning');
SELECT isomer.add_alert_channel('al-slack', 'slack', 'http://mockhooks:8090/slack', min_severity => 'critical');
SELECT isomer.add_alert_channel('al-pagerduty', 'pagerduty', 'http://mockhooks:8090/pd', secret_env => 'PD_ROUTING_KEY', min_severity => 'error');
SELECT isomer.add_alert_channel('al-flaky', 'webhook', 'http://mockhooks:8090/flaky/hook', min_severity => 'critical');
SQL
# Anything already firing on the stack from other suites is delivered first; start from here.
sleep 8
curl -sf -X DELETE "$HOOKS/received" >/dev/null
$ISO -c "DELETE FROM isomer.alert_deliveries" >/dev/null

echo "== drift is critical: every channel pages"
$ISO -c "SELECT documentdb_api.update('al', '{\"update\": \"al_card\", \"updates\": [{\"q\": {\"_id\": 7}, \"u\": {\"\$set\": {\"name\": \"tampered\"}}}]}'::documentdb_core.bson)" >/dev/null
for _ in 1 2 3 4 5; do $ISO -c "SELECT isomer.audit(200)" >/dev/null; sleep 1; done
expect "the auditor raised the alert" "1" "$($ISO -c "SELECT count(*) FROM isomer.alerts WHERE alert = 'drift' AND subject = 'al_card'")"
eventually "the webhook receives it, with its bearer token" "1" \
  "received \"r['path']=='/hook' and r['body']['alert']=='drift' and r['body']['subject']=='al_card' and r['body']['state']=='firing' and r['auth']=='Bearer test-hook-token'\"" 30
eventually "Slack receives it" "1" "received \"r['path']=='/slack' and 'drift on al_card' in r['body']['text']\"" 30
eventually "PagerDuty receives a critical trigger" "1" \
  "received \"r['path']=='/pd' and r['body']['event_action']=='trigger' and r['body']['payload']['component']=='al_card' and r['body']['payload']['severity']=='critical' and r['body']['routing_key']=='test-routing-key'\"" 30
eventually "a failing channel is retried until it accepts" "1" "received \"r['path']=='/flaky/hook' and r['body']['alert']=='drift' and r['body']['subject']=='al_card'\"" 60
expect "and its failed attempts are on record" "2" \
  "$($ISO -c "SELECT count(*) FROM isomer.alert_deliveries WHERE channel = 'al-flaky' AND subject = 'al_card' AND NOT ok")"
sleep 7
expect "nothing is sent twice" "1" "$(received "r['path']=='/hook' and r['body']['alert']=='drift' and r['body']['subject']=='al_card'")"

echo "== a lag-budget breach is an error: not to the critical-only channels"
$ISO -c "SELECT cron.alter_job(jobid, active => false) FROM cron.job WHERE jobname = 'isomer-drain'" >/dev/null
trap '$ISO -c "SELECT cron.alter_job(jobid, active => true) FROM cron.job WHERE jobname = '"'"'isomer-drain'"'"'" >/dev/null' EXIT
sleep 2
$ISO -c "UPDATE al.customers SET name = name || '.' WHERE id <= 3" >/dev/null
eventually "the breach is delivered to the webhook" "1" \
  "received \"r['path']=='/hook' and r['body']['alert']=='lag_budget' and r['body']['subject']=='al_card' and r['body']['state']=='firing'\"" 40
eventually "and to PagerDuty" "1" \
  "received \"r['path']=='/pd' and r['body']['event_action']=='trigger' and r['body']['payload']['class']=='lag_budget'\"" 30
expect "but not to Slack (critical only)" "0" "$(received "r['path']=='/slack' and 'lag budget on al_card' in r['body']['text']")"
dedup=$(first "r['path']=='/pd' and r['body']['payload']['class']=='lag_budget' and r['body']['payload']['component']=='al_card'" "rs[0]['body']['dedup_key']")
$ISO -c "SELECT cron.alter_job(jobid, active => true) FROM cron.job WHERE jobname = 'isomer-drain'" >/dev/null
eventually "once the projection catches up, the webhook hears it resolved" "1" \
  "received \"r['path']=='/hook' and r['body']['alert']=='lag_budget' and r['body']['state']=='resolved'\"" 40
eventually "and PagerDuty resolves the same incident" "1" \
  "received \"r['path']=='/pd' and r['body']['event_action']=='resolve' and r['body']['dedup_key']=='$dedup'\"" 30

echo "all alert tests passed"
