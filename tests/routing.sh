#!/bin/bash
# Query routing (FR-M4): a slow keyed read is proposed, routed onto a sync cache projection, and
# from then on planned against it, with no change to the application's SQL.
#
#   docker compose up -d && ./tests/routing.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }

PREP="PREPARE by_customer(bigint) AS SELECT c.id, c.name, v.id AS visit_id, v.page FROM rt.customers c JOIN rt.visits v ON v.customer_id = c.id WHERE c.id = \$1;"

echo "== reset"
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection(name) FROM isomer.projections;
DELETE FROM isomer.routes; DELETE FROM isomer.proposals;
DROP SCHEMA IF EXISTS rt CASCADE;
CREATE SCHEMA rt;
CREATE TABLE rt.customers (id bigint PRIMARY KEY, name text NOT NULL);
CREATE TABLE rt.visits (id bigint PRIMARY KEY, customer_id bigint NOT NULL, page text);   -- no index on customer_id
INSERT INTO rt.customers SELECT g, 'customer ' || g FROM generate_series(1, 200) g;
INSERT INTO rt.visits SELECT g, 1 + g % 200, '/page/' || (g % 37) FROM generate_series(1, 300000) g;
ANALYZE rt.customers, rt.visits;
SQL
sleep 1.5
$ISO -c "SELECT pg_stat_statements_reset()" -c "SELECT isomer.snapshot_activity()" >/dev/null

echo "== the application's slow keyed read"
{ echo "$PREP"; for i in $(seq 1 60); do echo "EXECUTE by_customer($(( (i % 200) + 1 )));"; done; } | $ISO >/dev/null
$ISO -c "SELECT isomer.propose(10)" >/dev/null
pid=$($ISO -c "SELECT id FROM isomer.proposals WHERE source_query LIKE '%rt.visits%'")
[ -n "$pid" ] && pass "the read was proposed (proposal $pid)" || fail "proposal" "an id" "none"

echo "== FR-M4 route it"
out=$($ISO -c "SELECT isomer.route_proposal($pid)")
contains "route verified and enabled" "enabled" "$out"
pass "$out"
expect "the route is backed by a sync projection" "sync" \
  "$($ISO -c "SELECT p.freshness FROM isomer.routes r JOIN isomer.projections p ON p.name = r.projection")"

echo "== the same SQL is now planned against the projection"
plan=$({ echo "$PREP"; echo "EXPLAIN EXECUTE by_customer(7);"; } | $ISO)
contains "the plan reads the cache projection" "jsonb_to_recordset" "$plan"
plan_off=$({ echo "SET isomer.routing = off;"; echo "$PREP"; echo "EXPLAIN EXECUTE by_customer(7);"; } | $ISO)
contains "with routing off it reads the tables" "visits" "$plan_off"
plan_shadow=$({ echo "SET isomer.routing = shadow;"; echo "$PREP"; echo "EXPLAIN EXECUTE by_customer(7);"; } | $ISO)
contains "in shadow mode it reads the tables" "visits" "$plan_shadow"

echo "== same answers"
rows_of() {  # $1 = key, $2 = extra SQL before the statement (e.g. SET isomer.routing = off)
  { echo "$2"; echo "$PREP"; echo "CREATE TEMP TABLE r AS EXECUTE by_customer($1);"
    echo "SELECT coalesce(md5(string_agg(r::text, ',' ORDER BY r::text)), 'none') FROM r;"; } | $ISO | tail -1
}
mismatch=0
for k in $(seq 1 50) 999999; do
  [ "$(rows_of "$k" "")" = "$(rows_of "$k" "SET isomer.routing = off;")" ] || mismatch=$((mismatch + 1))
done
expect "routed and unrouted results are identical on 51 keys" "0" "$mismatch"

echo "== writes are visible on the next routed read (sync)"
$ISO -c "INSERT INTO rt.visits VALUES (900001, 7, '/just-now')" >/dev/null
expect "a new row appears in the routed read" "1" \
  "$({ echo "$PREP"; echo "CREATE TEMP TABLE r AS EXECUTE by_customer(7);"; echo "SELECT count(*) FROM r WHERE page = '/just-now';"; } | $ISO | tail -1)"
$ISO -c "UPDATE rt.customers SET name = 'renamed' WHERE id = 7" >/dev/null
expect "an update appears in the routed read" "renamed" \
  "$({ echo "$PREP"; echo "CREATE TEMP TABLE r AS EXECUTE by_customer(7);"; echo "SELECT DISTINCT name FROM r;"; } | $ISO | tail -1)"

echo "== faster"
t_routed=$({ echo "$PREP"; echo "\\timing on"; for i in $(seq 1 30); do echo "EXECUTE by_customer($(( (i % 200) + 1 )));"; done; } | $ISO 2>&1 | grep -oE 'Time: [0-9.]+' | awk '{s+=$2} END {printf "%.2f", s/NR}')
t_tables=$({ echo "SET isomer.routing = off;"; echo "$PREP"; echo "\\timing on"; for i in $(seq 1 30); do echo "EXECUTE by_customer($(( (i % 200) + 1 )));"; done; } | $ISO 2>&1 | grep -oE 'Time: [0-9.]+' | awk '{s+=$2} END {printf "%.2f", s/NR}')
python3 -c "import sys; sys.exit(0 if $t_routed * 1.2 < $t_tables else 1)" \
  && pass "routed ${t_routed} ms vs tables ${t_tables} ms per read" \
  || fail "routed read is faster" "< ${t_tables} ms" "${t_routed} ms"

echo "== permissions: routing never widens access"
$ISO <<'SQL' >/dev/null
DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rt_reader') THEN DROP OWNED BY rt_reader; DROP ROLE rt_reader; END IF; END $$;
CREATE ROLE rt_reader LOGIN;
GRANT USAGE ON SCHEMA rt TO rt_reader;
GRANT SELECT ON rt.customers TO rt_reader;          -- not rt.visits
SQL
READER="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U rt_reader -d postgres -X -q -t -A"
out=$({ echo "$PREP"; echo "EXECUTE by_customer(7);"; } | $READER 2>&1 || true)
contains "a role that can't read visits still gets permission denied" "permission denied for table visits" "$out"
$ISO -c "GRANT SELECT ON rt.visits TO rt_reader" >/dev/null
plan=$({ echo "$PREP"; echo "EXPLAIN EXECUTE by_customer(7);"; } | $READER 2>&1)
contains "once it can read the tables, its read is routed (no grant on IsomerDB's storage needed)" "jsonb_to_recordset" "$plan"
expect "and returns the rows" "$({ echo "SET isomer.routing = off;"; echo "$PREP"; echo "CREATE TEMP TABLE r AS EXECUTE by_customer(7);"; echo "SELECT count(*) FROM r;"; } | $ISO | tail -1)" \
  "$({ echo "$PREP"; echo "CREATE TEMP TABLE r AS EXECUTE by_customer(7);"; echo "SELECT count(*) FROM r;"; } | $READER | tail -1)"

echo "== disabling the route"
$ISO -c "SELECT isomer.set_route(queryid, false) FROM isomer.routes" >/dev/null
sleep 2.5   # routes are reloaded every two seconds
plan=$({ echo "$PREP"; echo "EXPLAIN EXECUTE by_customer(7);"; } | $ISO)
contains "a disabled route falls back to the tables" "visits" "$plan"

echo "== routing passed"
