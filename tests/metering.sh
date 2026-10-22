#!/bin/bash
# Usage metering for billing (FR-CL2): isomer.meter() samples storage, node-hours, projection
# writes, embedding tokens and cost, and SQL statements; the HTTP API reports its own requests;
# the admin API serves it rolled up as JSON or CSV, and the CLI prints it.
#
#   docker compose up -d && (cd clients && npm ci && npm run build) && ./tests/metering.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
API=${ISOMER_API:-http://localhost:8080}
CLI="node clients/cli/dist/isomer.mjs"

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
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
usage_of() { $ISO -c "SELECT coalesce(sum(value), 0) FROM isomer.usage WHERE metric = '$1' AND ($2)"; }

[ -f clients/cli/dist/isomer.mjs ] || (cd clients && npm ci --silent && npm run build --silent >/dev/null)

echo "== reset"
$ISO <<'SQL' >/dev/null
ALTER EXTENSION isomer UPDATE;
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'mt_%';
DELETE FROM isomer.api_keys WHERE name LIKE 'mt_%';
DROP SCHEMA IF EXISTS mt CASCADE;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mt_ops') THEN DROP OWNED BY mt_ops; DROP ROLE mt_ops; END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mt_reader') THEN DROP OWNED BY mt_reader; DROP ROLE mt_reader; END IF;
END $$;
CREATE ROLE mt_ops; GRANT isomer_admin TO mt_ops;
CREATE ROLE mt_reader;
CREATE SCHEMA mt;
CREATE TABLE mt.items (id bigint PRIMARY KEY, title text NOT NULL);
GRANT USAGE ON SCHEMA mt TO mt_reader; GRANT SELECT ON mt.items TO mt_reader;
INSERT INTO mt.items SELECT g, 'item ' || g FROM generate_series(1, 50) g;
SELECT isomer.create_projection(name => 'mt_doc', kind => 'document', key => 'id', database => 'mt', freshness => 'sync',
  query => $q$ SELECT i.id, jsonb_build_object('title', i.title) AS doc FROM mt.items i $q$, sources => '{"mt.items": "id"}');
SELECT isomer.create_projection(name => 'mt_vec', kind => 'vector', key => 'id', model => 'local-hash-64', target_schema => 'mt',
  query => $q$ SELECT id, title AS input FROM mt.items $q$, sources => '{"mt.items": "id"}');
SELECT isomer.meter();   -- a first sample: counters start from here
SQL
expect "the meter is scheduled every minute" "* * * * *" "$($ISO -c "SELECT schedule FROM cron.job WHERE jobname = 'isomer-meter'")"
OPS=$($ISO -c "SELECT isomer.create_api_key('mt_ops', 'mt_ops')")
READER=$($ISO -c "SELECT isomer.create_api_key('mt_reader', 'mt_reader')")
HOUR="hour = date_trunc('hour', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'"

echo "== what is metered"
eventually "the vector projection is embedded" "50" "$ISO -c 'SELECT count(*) FROM mt.mt_vec'" 60
docs_before=$(usage_of documents_written "dimension = 'mt_doc'")
tokens_before=$(usage_of embedding_tokens "dimension = 'local-hash-64'")
$ISO -c "UPDATE mt.items SET title = title || ' (new)' WHERE id <= 20" >/dev/null
eventually "re-embedding catches up" "0" "$ISO -c \"SELECT embed_backlog FROM isomer.projection_status WHERE name = 'mt_vec'\"" 60
$ISO -c "SELECT isomer.meter()" >/dev/null
expect "projection writes are counted, per projection" "20" \
  "$( $ISO -c "SELECT $(usage_of documents_written "dimension = 'mt_doc'") - $docs_before" )"
expect "embedding tokens are counted, per model" "t" \
  "$($ISO -c "SELECT $(usage_of embedding_tokens "dimension = 'local-hash-64'") > $tokens_before")"
expect "storage is a gauge, in bytes" "t" "$($ISO -c "SELECT value > 10000000 FROM isomer.usage WHERE metric = 'storage_bytes' AND $HOUR")"
sleep 2
$ISO -c "SELECT isomer.meter()" >/dev/null
expect "node-hours accrue for the time between samples" "t" \
  "$($ISO -c "SELECT sum(value) > 0 FROM isomer.usage WHERE metric = 'node_hours' AND $HOUR")"
expect "SQL statements are counted" "t" "$($ISO -c "SELECT value > 0 FROM isomer.usage WHERE metric = 'sql_statements' AND $HOUR")"
again=$(usage_of documents_written "dimension = 'mt_doc'")
$ISO -c "SELECT isomer.meter(); SELECT isomer.meter()" >/dev/null
expect "sampling twice counts nothing twice" "$again" "$(usage_of documents_written "dimension = 'mt_doc'")"

for i in $(seq 1 12); do curl -sf -o /dev/null -H "Authorization: Bearer $READER" "$API/v1/projections/mt_doc/docs/$i"; done
curl -sf -o /dev/null -H "Authorization: Bearer $OPS" "$API/v1/admin/overview"
eventually "HTTP API data requests are counted per role, admin calls are not" "12|0" \
  "echo \$(usage_of api_requests \"dimension = 'mt_reader' AND $HOUR\" | cut -d. -f1)'|'\$(usage_of api_requests \"dimension = 'mt_ops'\" | cut -d. -f1)" 40
out=$($ISO -c "SELECT isomer.record_usage('storage_bytes', '', 1)" 2>&1 || true)
contains "only counters can be recorded from outside" "not a usage counter" "$out"

echo "== reports"
expect "rolled up by day" "t" \
  "$($ISO -c "SELECT count(*) > 0 AND bool_and(period = date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') FROM isomer.usage_report(now() - interval '1 hour', now() + interval '1 hour', 'day') WHERE metric = 'documents_written'")"
r=$(curl -sf -H "Authorization: Bearer $OPS" "$API/v1/admin/usage?by=hour")
contains "the admin API serves it as JSON" '"metric":"documents_written"' "$r"
csv=$(curl -sf -H "Authorization: Bearer $OPS" "$API/v1/admin/usage?by=day&format=csv")
expect "and as CSV, with a header" "period,metric,dimension,unit,value" "$(echo "$csv" | head -1)"
contains "with a row per metric and dimension" ",documents_written,mt_doc,documents," "$csv"
expect "a role outside isomer_admin cannot read usage" "403" \
  "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $READER" "$API/v1/admin/usage")"
contains "the CLI prints it" "documents_written" "$($CLI usage --by hour --url "$API" --key "$OPS")"
expect "and exports CSV" "period,metric,dimension,unit,value" "$($CLI usage --csv --url "$API" --key "$OPS" | head -1)"
expect "the console has a Usage page" "200" "$(curl -s -o /dev/null -w '%{http_code}' "$API/console/")"

echo "all metering tests passed"
