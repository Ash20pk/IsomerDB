#!/bin/bash
# Read-your-writes without calling wait_for: SET isomer.read_after on SQL reads (FR-R4), cluster
# time on MongoDB responses and per-read freshness (FR-R4a, FR-R5), and implicit causal reads.
#
#   docker compose up -d && ./tests/freshness.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
APP="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U fr_app -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
NET="$(basename "$PWD" | tr '[:upper:]' '[:lower:]')_default"
MONGO=(docker run --rm --network "$NET" mongo:8 mongosh
       "mongodb://isomer:isomer@gateway:10260/?tls=true&tlsAllowInvalidCertificates=true" --quiet)

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }
mongo_eval() { "${MONGO[@]}" --eval "$1" 2>&1 | tail -1; }
pause_drain() { $ISO -c "SELECT cron.alter_job(jobid, active => false) FROM cron.job WHERE jobname = 'isomer-drain'" >/dev/null; sleep 2; }
resume_drain() { $ISO -c "SELECT cron.alter_job(jobid, active => true) FROM cron.job WHERE jobname = 'isomer-drain'" >/dev/null; }
trap resume_drain EXIT

echo "== reset"
$ISO <<'SQL' >/dev/null
ALTER EXTENSION isomer UPDATE;
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'fr_%';
DROP SCHEMA IF EXISTS fr CASCADE;
DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fr_app') THEN DROP OWNED BY fr_app; DROP ROLE fr_app; END IF; END $$;
CREATE SCHEMA fr;
CREATE TABLE fr.items (id bigint PRIMARY KEY, name text NOT NULL);
CREATE TABLE fr.other (id bigint PRIMARY KEY);
INSERT INTO fr.items SELECT g, 'item ' || g FROM generate_series(1, 20) g;
INSERT INTO fr.other VALUES (1);
SELECT isomer.create_projection(
  name => 'fr_doc', kind => 'document', key => 'id', database => 'fr', lag => '2 seconds',
  query => $q$ SELECT i.id, jsonb_build_object('name', i.name) AS doc FROM fr.items i $q$,
  sources => '{"fr.items": "id"}');
SELECT isomer.create_projection(
  name => 'fr_name', kind => 'cache', key => 'id', lag => '2 seconds', target_schema => 'fr',
  query => $q$ SELECT i.id, to_jsonb(i.name) AS value FROM fr.items i $q$,
  sources => '{"fr.items": "id"}');
CREATE ROLE fr_app LOGIN;
GRANT USAGE ON SCHEMA fr TO fr_app;
GRANT SELECT, UPDATE ON fr.items, fr.other, fr.fr_name TO fr_app;
SQL
expect "extension is at least 0.9.0" "t" "$($ISO -c "SELECT string_to_array(extversion, '.')::int[] >= '{0,9,0}' FROM pg_extension WHERE extname = 'isomer'")"
expect "the router defines isomer.read_after" "" "$($ISO -c "SHOW isomer.read_after")"

echo "== FR-R4 SET isomer.read_after on SQL reads"
pause_drain
tok=$($ISO -c "UPDATE fr.items SET name = 'renamed' WHERE id = 1; SELECT isomer.last_write_token()")
read_doc="SELECT d ->> 'name' FROM documentdb_api.collection('fr', 'fr_doc') c,
          LATERAL isomer._ejson_relax(documentdb_core.bson_to_json_string(c.document)::text::jsonb) d WHERE d ->> '_id' = '1'"
expect "without a token, a read returns the projection as it is" "item 1" "$($ISO -c "$read_doc")"
out=$($ISO -c "SET isomer.read_after_timeout = '300ms'" -c "SET isomer.read_after = '$tok'" -c "$read_doc" 2>&1 || true)
contains "with a token, a read of a lagging projection says not yet" "not yet" "$out"
out=$($ISO -c "SET isomer.read_after_timeout = '300ms'" -c "SET isomer.read_after = '$tok'" -c "SELECT value FROM fr.fr_name WHERE id = 1" 2>&1 || true)
contains "the same holds for a projection's SQL view" "not yet" "$out"
out=$($ISO -c "SET isomer.read_after_timeout = '300ms'" -c "SET isomer.read_after = '$tok'" -c "SELECT isomer.get('fr_doc', '1')" 2>&1 || true)
contains "and for isomer.get" "not yet" "$out"
expect "reads that touch no projection never wait" "renamed|1" \
  "$($ISO -c "SET isomer.read_after_timeout = '300ms'" -c "SET isomer.read_after = '$tok'" -c "SELECT (SELECT name FROM fr.items WHERE id = 1) || '|' || (SELECT count(*) FROM fr.other)")"

# The decisive case: the read starts while the projection is behind, waits, and must then see
# the new value. Under READ COMMITTED its snapshot was taken before the wait; the router replaces it.
( sleep 2; resume_drain ) &
start=$SECONDS
got=$($ISO -c "SET isomer.read_after_timeout = '15s'" -c "SET isomer.read_after = '$tok'" -c "$read_doc")
wait
expect "a read that had to wait sees what it waited for" "renamed" "$got"
(( SECONDS - start >= 1 )) && pass "and it did wait ($(( SECONDS - start ))s)" || fail "it waited" ">= 1s" "$(( SECONDS - start ))s"
expect "isomer.get sees it too" "renamed" "$($ISO -c "SET isomer.read_after = '$tok'" -c "SELECT isomer.get('fr_doc', '1')->>'name'")"

pause_drain
tok=$($ISO -c "UPDATE fr.items SET name = 'second' WHERE id = 2; SELECT isomer.last_write_token()")
( sleep 2; resume_drain ) &
got=$($ISO -c "BEGIN" -c "SET LOCAL isomer.read_after_timeout = '15s'" -c "SET LOCAL isomer.read_after = '$tok'" \
           -c "SELECT value #>> '{}' FROM fr.fr_name WHERE id = 2" -c "COMMIT")
wait
expect "SET LOCAL inside a transaction (PgBouncer transaction mode)" "second" "$got"

out=$($ISO -c "BEGIN ISOLATION LEVEL REPEATABLE READ" -c "SET LOCAL isomer.read_after = '$tok'" -c "SELECT value FROM fr.fr_name WHERE id = 2" 2>&1 || true)
contains "REPEATABLE READ is refused with a reason" "needs READ COMMITTED" "$out"
out=$($ISO -c "SET isomer.read_after = 'yesterday'" -c "SELECT value FROM fr.fr_name WHERE id = 2" 2>&1 || true)
contains "a malformed token is refused" "must be a token" "$out"

pause_drain
tok=$($APP -c "UPDATE fr.items SET name = 'by app' WHERE id = 3" -c "SELECT isomer.last_write_token()" | tail -1)
( sleep 2; resume_drain ) &
got=$($APP -c "SET isomer.read_after_timeout = '15s'" -c "SET isomer.read_after = '$tok'" -c "SELECT value #>> '{}' FROM fr.fr_name WHERE id = 3")
wait
expect "an application role (not superuser) gets the same guarantee" "by app" "$got"

echo "== FR-R4a / FR-R5 cluster time on MongoDB responses"
w=$(mongo_eval 'const r = db.getSiblingDB("fr").runCommand({insert: "plain", documents: [{_id: new ObjectId()}]});
print(r.operationTime instanceof Timestamp, r.$clusterTime !== undefined && r.$clusterTime.clusterTime instanceof Timestamp, Number(r.$clusterTime.signature.keyId))')
expect "a write returns operationTime and \$clusterTime" "true true 0" "$w"
expect "a write's operationTime is the cluster clock after it" "t" \
  "$($ISO -c "SELECT (($(mongo_eval 'const r = db.getSiblingDB("fr").runCommand({insert: "plain", documents: [{_id: new ObjectId()}]}); print(r.operationTime.t + "," + r.operationTime.i)' | sed 's/\(.*\),\(.*\)/\1::bigint * 1000 + (\2::bigint >> 22)) << 22 | (\2::bigint \& 4194303)/')) <= isomer.current_token()")"
pause_drain
tok=$($ISO -c "UPDATE fr.items SET name = 'behind' WHERE id = 4; SELECT isomer.last_write_token()")
r=$(mongo_eval 'const r = db.getSiblingDB("fr").runCommand({find: "fr_doc", filter: {_id: 4}}); print(r.operationTime.t + "," + r.operationTime.i + "," + (r.$clusterTime === undefined))')
fresh=$($ISO -c "SELECT isomer.freshness('fr_doc')")
expect "a projection read reports its freshness as operationTime, and gossips no \$clusterTime" \
  "$($ISO -c "SELECT t || ',' || i || ',true' FROM isomer.token_to_mongo_timestamp($fresh)")" "$r"
expect "that freshness is older than the pending write" "t" "$($ISO -c "SELECT $fresh < $tok")"

echo "== FR-R4a implicit causal reads (ISOMER_IMPLICIT_CAUSAL)"
ts=$($ISO -c "SELECT t || ',' || i FROM isomer.token_to_mongo_timestamp($tok)")
# Drivers gossip \$clusterTime only after a response has carried one: this client's write does that.
implicit_read='db.getSiblingDB("fr").plain.insertOne({});
const s = db.getMongo().startSession({causalConsistency: false});
s.advanceClusterTime({clusterTime: Timestamp({t: __T__, i: __I__}), signature: {hash: BinData(0, "AAAAAAAAAAAAAAAAAAAAAAAAAAA="), keyId: NumberLong(0)}});
try { print(s.getDatabase("fr").fr_doc.findOne({_id: 4}).name) } catch (e) { print("ERR " + e.message) }'
implicit_read=${implicit_read//__T__/${ts%,*}}; implicit_read=${implicit_read//__I__/${ts#*,}}
expect "off by default: a gossiped \$clusterTime does not make a read wait" "item 4" "$(mongo_eval "$implicit_read")"
ISOMER_IMPLICIT_CAUSAL=1 docker compose up -d --wait gateway >/dev/null 2>&1
contains "on: the same read waits for the client's own cluster time" "not yet" "$(mongo_eval "$implicit_read")"
resume_drain
expect "and returns the write once the projection catches up" "behind" "$(mongo_eval "$implicit_read")"
docker compose up -d --wait gateway >/dev/null 2>&1

echo "all freshness tests passed"
