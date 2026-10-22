#!/bin/bash
# Edge replicas: a projection served by a replica fed by the origin's change feed. It takes the
# snapshot, follows every change, never lets a key read more than the origin allows, answers a
# read that must see a write only once it has, and resumes where it stopped when the origin's API
# goes away and comes back. It keeps its copy on disk across a restart (and drops it when the
# projection was recreated meanwhile), and serves MongoDB over TLS to an API key.
#
#   docker compose --profile attach --profile test up -d && ./tests/edge.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
NET="$(basename "$PWD" | tr '[:upper:]' '[:lower:]')_default"
EDGE=isomerdb-edge-test
EDGE_DATA=isomerdb-edge-test-data
PORT=18081
CERTS=$(mktemp -d)

pass() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"
  echo "-- edge logs --"; docker logs --tail 15 $EDGE 2>&1 | sed 's/^/       /'
  exit 1
}
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=$(( ${4:-30} * ${ISOMER_TIMEOUT_SCALE:-1} )) got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 0.5
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}
# edge KEY PATH [extra curl args...]: prints "<status> <body>"
edge() {
  local key=$1 path=$2; shift 2
  local body; body=$(mktemp)
  local code; code=$(curl -sS -o "$body" -w '%{http_code}' ${key:+-H "Authorization: Bearer $key"} "$@" "http://localhost:$PORT$path")
  echo "$code $(cat "$body")"; rm -f "$body"
}
field() { python3 -c "import json,sys; print(json.loads(sys.stdin.read().split(' ',1)[1])$1)"; }

echo "== reset"
docker rm -f $EDGE >/dev/null 2>&1 || true
docker volume rm $EDGE_DATA >/dev/null 2>&1 || true
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'ed\_%';
DELETE FROM isomer.api_keys WHERE name LIKE 'edge-%';
DROP SCHEMA IF EXISTS ed CASCADE;
DROP ROLE IF EXISTS edge_reader; DROP ROLE IF EXISTS edge_stranger;
CREATE SCHEMA ed;
CREATE TABLE ed.customers (id int PRIMARY KEY, name text NOT NULL, tier text);
INSERT INTO ed.customers SELECT g, 'customer ' || g, CASE WHEN g % 2 = 0 THEN 'gold' ELSE 'silver' END FROM generate_series(1, 200) g;
CREATE ROLE edge_reader; GRANT USAGE ON SCHEMA ed TO edge_reader; GRANT SELECT ON ed.customers TO edge_reader;
CREATE ROLE edge_stranger;
SELECT isomer.create_projection(name => 'ed_cards', kind => 'document', key => 'id', database => 'ed',
  query => 'SELECT c.id, jsonb_build_object(''name'', c.name, ''tier'', c.tier) AS doc FROM ed.customers c',
  sources => '{"ed.customers": "id"}');
SELECT isomer.set_history('ed_cards', '1 day');
SQL
READER=$($ISO -c "SELECT isomer.create_api_key('edge-reader', 'edge_reader')")
STRANGER=$($ISO -c "SELECT isomer.create_api_key('edge-stranger', 'edge_stranger')")
docker build -q -f services/edge/Dockerfile -t isomer-edge:dev . >/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=$EDGE" -keyout "$CERTS/key.pem" -out "$CERTS/cert.pem" 2>/dev/null
chmod 644 "$CERTS"/*.pem
docker run -d --name $EDGE --network "$NET" -p $PORT:8081 -v $EDGE_DATA:/var/lib/isomer-edge -v "$CERTS":/certs:ro \
  -e ISOMER_ORIGIN=http://api:8080 -e ISOMER_ORIGIN_KEY="$READER" -e ISOMER_EDGE_PROJECTIONS=ed_cards \
  -e ISOMER_EDGE_DATA_DIR=/var/lib/isomer-edge -e ISOMER_EDGE_MONGO_LISTEN=0.0.0.0:27017 \
  -e ISOMER_EDGE_TLS_CERT=/certs/cert.pem -e ISOMER_EDGE_TLS_KEY=/certs/key.pem \
  -e ISOMER_EDGE_WAIT_MS=5000 isomer-edge:dev >/dev/null
trap 'docker rm -f $EDGE >/dev/null 2>&1 || true; docker volume rm $EDGE_DATA >/dev/null 2>&1 || true; rm -rf "$CERTS"' EXIT
# mongo_edge KEY SCRIPT: mongosh against the edge, over TLS, the API key as the password.
mongo_edge() {
  docker run --rm --network "$NET" mongo:8 mongosh --quiet \
    "mongodb://edge:$1@$EDGE:27017/ed?authMechanism=PLAIN&authSource=%24external&tls=true&tlsAllowInvalidCertificates=true" \
    --eval "$2" 2>&1 | tail -1
}

echo "== the edge takes the snapshot and serves it"
eventually "every document is replicated" "200" "edge '' /v1/edge/status | field \"['ed_cards']['documents']\""
expect "a document reads the same as at the origin" \
  "$($ISO -c "SELECT isomer.get('ed_cards', '42') ->> 'name'")" "$(edge "$READER" /v1/projections/ed_cards/docs/42 | field "['name']")"

echo "== access is the origin's"
expect "no key: 401" "401" "$(edge '' /v1/projections/ed_cards/docs/42 | cut -d' ' -f1)"
expect "a key whose role can't read the tables: 404, as at the origin" "404" "$(edge "$STRANGER" /v1/projections/ed_cards/docs/42 | cut -d' ' -f1)"
expect "an unknown key: 401" "401" "$(edge 'iso_not_a_key' /v1/projections/ed_cards/docs/42 | cut -d' ' -f1)"
expect "a projection this edge doesn't serve: 404" "404" "$(edge "$READER" /v1/projections/other/docs/1 | cut -d' ' -f1)"

echo "== changes follow"
$ISO -c "UPDATE ed.customers SET tier = 'platinum' WHERE id = 7" >/dev/null
eventually "an update reaches the edge" "platinum" "edge '$READER' /v1/projections/ed_cards/docs/7 | field \"['tier']\""
$ISO -c "DELETE FROM ed.customers WHERE id = 8" >/dev/null
eventually "a delete reaches the edge" "404" "edge '$READER' /v1/projections/ed_cards/docs/8 | cut -d' ' -f1"

echo "== read your writes at the edge"
for i in 1 2 3 4 5; do
  TOKEN=$($ISO -c "UPDATE ed.customers SET name = 'renamed $i' WHERE id = 5" -c "SELECT isomer.last_write_token()" | tail -1)
  got=$(edge "$READER" /v1/projections/ed_cards/docs/5 -H "X-Isomer-After: $TOKEN" | field "['name']")
  [ "$got" = "renamed $i" ] || fail "a read after a write sees it ($i)" "renamed $i" "$got"
done
pass "five writes, each read back at once through the edge with its token"
expect "a token that isn't one: 400" "400" "$(edge "$READER" /v1/projections/ed_cards/docs/5 -H "X-Isomer-After: soon" | cut -d' ' -f1)"

echo "== the origin's API goes away and comes back"
CURSOR=$(edge '' /v1/edge/status | field "['ed_cards']['cursor']")
docker compose stop api >/dev/null
$ISO -c "UPDATE ed.customers SET tier = 'while away' WHERE id IN (11, 12, 13)" >/dev/null
docker compose start api >/dev/null
eventually "the changes made meanwhile arrive" "while away" "edge '$READER' /v1/projections/ed_cards/docs/13 | field \"['tier']\"" 60
expect "resumed from its cursor, not from a new snapshot" "t" \
  "$(docker logs $EDGE 2>&1 | grep -c 'snapshot applied' | awk '{print ($1 == 1) ? "t" : "f"}')"
expect "the edge is still complete" "199" "$(edge '' /v1/edge/status | field "['ed_cards']['documents']")"
[ "$(edge '' /v1/edge/status | field "['ed_cards']['cursor']")" -gt "$CURSOR" ] && pass "its cursor moved on" || fail "cursor moved on" "> $CURSOR" "$(edge '' /v1/edge/status)"

echo "== MongoDB at the edge"
expect "a document by _id reads the same as at the origin" \
  "$($ISO -c "SELECT isomer.get('ed_cards', '42') ->> 'name'")" "$(mongo_edge "$READER" 'print(db.ed_cards.findOne({_id: 42}).name)')"
expect "a filter, as at the origin" "$($ISO -c "SELECT count(*) FROM ed.customers WHERE tier = 'gold'")" \
  "$(mongo_edge "$READER" 'print(db.ed_cards.countDocuments({tier: "gold"}))')"
expect "sort, skip, limit and projection" '[{"_id":198,"name":"customer 198"},{"_id":197,"name":"customer 197"}]' \
  "$(mongo_edge "$READER" 'print(JSON.stringify(db.ed_cards.find({}, {name: 1}).sort({_id: -1}).skip(2).limit(2).toArray()))')"
expect "every document through cursors" "199" "$(mongo_edge "$READER" 'print(db.ed_cards.find().batchSize(7).toArray().length)')"
expect "an aggregation" "$($ISO -c "SELECT string_agg(tier || '=' || n, ',' ORDER BY tier) FROM (SELECT tier, count(*) n FROM ed.customers GROUP BY tier) t")" \
  "$(mongo_edge "$READER" 'print(db.ed_cards.aggregate([{$group: {_id: "$tier", n: {$sum: 1}}}, {$sort: {_id: 1}}]).toArray().map(g => g._id + "=" + g.n).join(","))')"
expect "the collection is listed" "ed_cards" "$(mongo_edge "$READER" 'print(db.getCollectionNames().join(","))')"
contains "a write is refused, naming the origin" "read-only: send insert to the origin" \
  "$(mongo_edge "$READER" 'try { db.ed_cards.insertOne({_id: 1000}) } catch (e) { print(e.message) }')"
contains "a stage the edge can't run is refused, not answered wrongly" "send this query to the origin" \
  "$(mongo_edge "$READER" 'try { db.ed_cards.aggregate([{$lookup: {from: "x", localField: "a", foreignField: "b", as: "c"}}]).toArray() } catch (e) { print(e.message) }')"
contains "an unknown key can't log in" "unknown or revoked API key" "$(mongo_edge iso_not_a_key 'print(1)')"
contains "a key whose role can't read the tables doesn't see the collection" "not a projection this edge replicates" \
  "$(mongo_edge "$STRANGER" 'try { db.ed_cards.findOne() } catch (e) { print(e.message) }')"
contains "SCRAM is refused with what to use instead" "authMechanism=PLAIN" \
  "$(docker run --rm --network "$NET" mongo:8 mongosh --quiet "mongodb://edge:$READER@$EDGE:27017/ed?tls=true&tlsAllowInvalidCertificates=true" --eval 'print(1)' 2>&1 | tail -3)"
for i in 1 2 3; do
  ts=$($ISO -c "UPDATE ed.customers SET name = 'mongo $i' WHERE id = 6" -c "SELECT t || ',' || i FROM isomer.token_to_mongo_timestamp(isomer.last_write_token())" | tail -1)
  got=$(mongo_edge "$READER" "s = db.getMongo().startSession({causalConsistency: true}); s.advanceOperationTime(Timestamp({t: ${ts%,*}, i: ${ts#*,}})); print(s.getDatabase('ed').ed_cards.findOne({_id: 6}).name)")
  [ "$got" = "mongo $i" ] || fail "a causal read after a write sees it ($i)" "mongo $i" "$got"
done
pass "three writes, each read back at once through a causally consistent session at the edge"
contains "without TLS the listener refuses to start" "needs ISOMER_EDGE_TLS_CERT" \
  "$(docker run --rm -e ISOMER_ORIGIN=http://api:8080 -e ISOMER_ORIGIN_KEY=x -e ISOMER_EDGE_PROJECTIONS=ed_cards \
       -e ISOMER_EDGE_MONGO_LISTEN=0.0.0.0:27017 isomer-edge:dev 2>&1 | tail -1)"

echo "== the copy survives a restart"
docker stop $EDGE >/dev/null
$ISO -c "UPDATE ed.customers SET tier = 'while down' WHERE id = 21" >/dev/null
docker start $EDGE >/dev/null
eventually "a change made while it was down arrives" "while down" "edge '$READER' /v1/projections/ed_cards/docs/21 | field \"['tier']\"" 60
contains "it restored the copy on disk" "restored the copy on disk" "$(docker logs $EDGE 2>&1 | grep 'restored the copy')"
expect "and took no new snapshot" "1" "$(docker logs $EDGE 2>&1 | grep -c 'snapshot applied')"
expect "it is still complete" "199" "$(edge '' /v1/edge/status | field "['ed_cards']['documents']")"
expect "including what it had before" "while away" "$(edge "$READER" /v1/projections/ed_cards/docs/13 | field "['tier']")"

echo "== a projection recreated while the edge was down"
docker stop $EDGE >/dev/null
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection('ed_cards');
DELETE FROM ed.customers WHERE id > 100;
SELECT isomer.create_projection(name => 'ed_cards', kind => 'document', key => 'id', database => 'ed',
  query => 'SELECT c.id, jsonb_build_object(''name'', c.name, ''tier'', c.tier) AS doc FROM ed.customers c',
  sources => '{"ed.customers": "id"}');
SELECT isomer.set_history('ed_cards', '1 day');
SQL
docker start $EDGE >/dev/null
eventually "the old copy is dropped for a new snapshot" "99" "edge '' /v1/edge/status | field \"['ed_cards']['documents']\"" 60
contains "because it came from another incarnation" "starting afresh" "$(docker logs $EDGE 2>&1 | grep 'starting afresh')"
expect "a key the new projection no longer has is gone" "404" "$(edge "$READER" /v1/projections/ed_cards/docs/150 | cut -d' ' -f1)"

$ISO -c "SELECT isomer.drop_projection('ed_cards')" >/dev/null
echo "edge: all checks passed"
