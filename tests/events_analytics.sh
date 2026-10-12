#!/bin/bash
# Event projections (domain events from row changes, in commit order per row, exactly once) and
# analytics projections (a query kept as a columnar table, rebuilt when its sources change). Runs
# on one node and on the cluster.
#
#   docker compose --profile attach --profile test up -d && ./tests/events_analytics.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
ISO_NOFAIL="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A"
API="curl -sS"   # the HTTP API is published on localhost:8080

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
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
events() { $ISO -c "SELECT coalesce(string_agg(doc ->> 'type' || ':' || key, ' ' ORDER BY id), '') FROM isomer.changes_since('ev_orders', 0) WHERE key = '$1'"; }

echo "== reset"
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'ev\_%';
DELETE FROM isomer.api_keys WHERE name = 'ev-reader';
DROP SCHEMA IF EXISTS ev CASCADE;
DROP ROLE IF EXISTS ev_reader;
CREATE SCHEMA ev;
CREATE TABLE ev.orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL, status text NOT NULL, total numeric NOT NULL);
SELECT create_distributed_table('ev.orders', 'id');
CREATE ROLE ev_reader; GRANT USAGE ON SCHEMA ev TO ev_reader; GRANT SELECT ON ev.orders TO ev_reader;
SQL

echo "== event projections"
contains "rules that don't compile are refused" "do not compile" \
  "$($ISO_NOFAIL -c "SELECT isomer.create_event_projection('ev_bad', 'ev.orders', '{\"x\": {\"when\": \"new.no_such_column = 1\"}}')" 2>&1)"
contains "an event projection over a sharded table" "3 event type(s)" "$($ISO -c "SELECT isomer.create_event_projection('ev_orders', 'ev.orders', '{
    \"order_created\": {\"when\": \"old IS NULL\"},
    \"order_paid\":    {\"when\": \"new.status = ''paid'' AND old.status IS DISTINCT FROM ''paid''\",
                        \"payload\": \"jsonb_build_object(''total'', new.total)\"},
    \"order_deleted\": {\"when\": \"new IS NULL\", \"payload\": \"jsonb_build_object(''id'', old.id)\"}}')")"
$ISO <<'SQL' >/dev/null
INSERT INTO ev.orders VALUES (1, 10, 'new', 5), (2, 10, 'new', 12.50), (3, 11, 'new', 7);
UPDATE ev.orders SET status = 'paid' WHERE id = 2;
UPDATE ev.orders SET status = 'paid' WHERE id = 2;        -- already paid: no event
UPDATE ev.orders SET total = total + 1 WHERE id = 3;      -- no rule matches: no event
DELETE FROM ev.orders WHERE id = 1;
SQL
eventually "each change becomes the events whose rules hold" "order_created:1 order_deleted:1" "events 1"
expect "in order, per row" "order_created:2 order_paid:2" "$(events 2)"
expect "with the rule's payload" '{"total": 12.50}' "$($ISO -c "SELECT doc -> 'payload' FROM isomer.changes_since('ev_orders', 0) WHERE doc ->> 'type' = 'order_paid'")"
expect "a change no rule matches makes no event" "order_created:3" "$(events 3)"
$ISO -c "UPDATE ev.orders SET status = 'paid' WHERE status = 'new'" -c "INSERT INTO ev.orders SELECT g, 1, 'new', g FROM generate_series(100, 199) g" >/dev/null
eventually "a statement across every shard: one event per row" "101" \
  "$ISO -c \"SELECT count(*) FROM isomer.changes_since('ev_orders', 0, 10000) WHERE (doc ->> 'type' = 'order_paid' AND key = '3') OR (doc ->> 'type' = 'order_created' AND key::int >= 100)\""
got=$($ISO <<'SQL'
INSERT INTO ev.orders VALUES (500, 1, 'paid', 99);
SELECT isomer.wait_for(isomer.last_write_token(), 'ev_orders') IS NOT NULL;
SELECT count(*) FROM isomer.changes_since('ev_orders', 0, 100000) WHERE key = '500';
SQL
)
expect "read your writes: after wait_for, the write's events are in the feed (created and paid)" "t 2" "$(echo $got)"
$ISO -c "TRUNCATE ev.orders" -c "INSERT INTO ev.orders VALUES (600, 1, 'new', 1)" >/dev/null
eventually "a TRUNCATE is not a row change" "order_created:600" "events 600"
KEY=$($ISO -c "SELECT isomer.create_api_key('ev-reader', 'ev_reader')")
contains "the feed is served over HTTP, as events" '"op":"event"' \
  "$($API -H "Authorization: Bearer $KEY" "http://localhost:8080/v1/projections/ev_orders/changes?after=0&limit=1")"
expect "the certificate holds" "true" "$($ISO -c "SELECT isomer.certificate('ev_orders') ->> 'valid'")"
before=$($ISO -c "SELECT count(*) FROM isomer.changes WHERE projection = 'ev_orders'")
$ISO -c "UPDATE isomer.changes SET at = now() - interval '30 days' WHERE projection = 'ev_orders' AND key::int < 100" -c "SELECT isomer.prune_history()" >/dev/null
expect "events past retention go, all of them" "0" "$($ISO -c "SELECT count(*) FROM isomer.changes WHERE projection = 'ev_orders' AND key::int < 100")"
expect "and a consumer behind them is told to start again" "IS003" \
  "$($ISO_NOFAIL -c '\set VERBOSITY verbose' -c "SELECT * FROM isomer.changes_since('ev_orders', 1)" 2>&1 | grep -o IS003 | head -1)"

echo "== analytics projections"
$ISO -c "INSERT INTO ev.orders SELECT g, g % 20, CASE WHEN g % 3 = 0 THEN 'paid' ELSE 'new' END, g FROM generate_series(1000, 5999) g" >/dev/null
contains "a query kept as a columnar table" "columnar" \
  "$($ISO -c "SELECT isomer.create_analytics_projection('ev_by_status', 'SELECT o.status, count(*) AS orders, sum(o.total) AS revenue FROM ev.orders o GROUP BY o.status', refresh => '1 second')")"
expect "stored by the columnar access method" "columnar" \
  "$($ISO -c "SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.oid = 'isomer_analytics.ev_by_status_1'::regclass")"
expect "it holds what the query returns" "0" "$($ISO -c "SELECT isomer.analytics_differences('ev_by_status')")"
expect "unchanged sources, no rebuild" "unchanged" "$($ISO -c "SELECT isomer.refresh_analytics('ev_by_status')")"
$ISO -c "UPDATE ev.orders SET status = 'refunded' WHERE id BETWEEN 1000 AND 1099" >/dev/null
eventually "changed sources are noticed and it is rebuilt" "100" \
  "$ISO -c \"SELECT isomer.refresh_analytics_due() >= 0 AND true\" >/dev/null; $ISO -c \"SELECT orders FROM public.ev_by_status WHERE status = 'refunded'\"" 60
expect "the previous generation is gone" "f" "$($ISO -c "SELECT to_regclass('isomer_analytics.ev_by_status_1') IS NOT NULL")"
$ISO -c "TRUNCATE ev.orders" >/dev/null
eventually "a TRUNCATE is noticed too" "0" \
  "$ISO -c \"SELECT isomer.refresh_analytics_due() >= 0 AND true\" >/dev/null; $ISO -c \"SELECT count(*) FROM public.ev_by_status\"" 60
expect "the certificate holds" "true" "$($ISO -c "SELECT isomer.certificate('ev_by_status') ->> 'valid'")"
$ISO -c "SELECT isomer.drop_projection('ev_by_status')" >/dev/null
expect "dropping removes the view and its table" "0" \
  "$($ISO -c "SELECT count(*) FROM pg_class WHERE relnamespace = 'isomer_analytics'::regnamespace AND relname LIKE 'ev\_by\_status%'")"
$ISO -c "SELECT isomer.drop_projection('ev_orders')" >/dev/null
echo "events_analytics: all checks passed"
