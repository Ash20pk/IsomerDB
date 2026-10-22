#!/bin/bash
# Projection history: time travel (as_of), the change feed (gap-free, in commit order, under
# concurrent writers), retention, and the HTTP API that serves all of it, including live streams.
#
#   docker compose up -d && ./tests/history.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
ISO_NOFAIL="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A"
API=${ISOMER_API:-http://localhost:8080}

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=$(( ${4:-20} * ${ISOMER_TIMEOUT_SCALE:-1} )) got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name"; return; }
    sleep 0.5
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}

echo "== reset"
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'hx_%';
DELETE FROM isomer.api_keys WHERE name LIKE 'hx_%';
DROP SCHEMA IF EXISTS hx CASCADE;
CREATE SCHEMA hx;
CREATE TABLE hx.accounts (id bigint PRIMARY KEY, owner text NOT NULL, balance numeric NOT NULL);
CREATE TABLE hx.notes (id bigint PRIMARY KEY, account_id bigint NOT NULL, body text);
INSERT INTO hx.accounts SELECT g, 'owner ' || g, 100 FROM generate_series(1, 200) g;
INSERT INTO hx.notes SELECT g, 1 + g % 200, 'note ' || g FROM generate_series(1, 400) g;
SELECT isomer.create_projection(
  name => 'hx_account', kind => 'document', key => 'id', database => 'hx', freshness => 'sync',
  query => $q$ SELECT a.id, jsonb_build_object('owner', a.owner, 'balance', a.balance,
                      'notes', (SELECT count(*) FROM hx.notes n WHERE n.account_id = a.id)) AS doc
              FROM hx.accounts a $q$,
  sources => '{"hx.accounts": "id", "hx.notes": "account_id"}');
SQL

echo "== history on"
contains "the baseline holds every current document" "200 documents in the baseline" \
  "$($ISO -c "SELECT isomer.set_history('hx_account', '1 hour')")"
t0=$($ISO -c "SELECT clock_timestamp()")
c0=$($ISO -c "SELECT max(id) FROM isomer.changes WHERE projection = 'hx_account'")
$ISO -c "UPDATE hx.accounts SET balance = 250 WHERE id = 7" >/dev/null
$ISO -c "UPDATE hx.accounts SET balance = 250 WHERE id = 7" >/dev/null
$ISO -c "INSERT INTO hx.notes VALUES (10001, 7, 'hello')" >/dev/null
$ISO -c "DELETE FROM hx.notes WHERE account_id = 9" -c "DELETE FROM hx.accounts WHERE id = 9" >/dev/null
$ISO -c "INSERT INTO hx.accounts VALUES (5000, 'newcomer', 1)" >/dev/null
expect "an update that changes nothing is not logged; every real change is, in order" "7:upsert,7:upsert,9:upsert,9:delete,5000:upsert" \
  "$($ISO -c "SELECT string_agg(key || ':' || op, ',' ORDER BY id) FROM isomer.changes_since('hx_account', $c0)")"

echo "== time travel"
expect "as_of before the writes: the old balance" "100" \
  "$($ISO -c "SELECT isomer.as_of('hx_account', '7', '$t0') ->> 'balance'")"
expect "now: the new balance and note count" "250 3" \
  "$($ISO -c "SELECT (isomer.get('hx_account', '7') ->> 'balance') || ' ' || (isomer.get('hx_account', '7') ->> 'notes')")"
expect "a deleted document existed before" "owner 9" "$($ISO -c "SELECT isomer.as_of('hx_account', '9', '$t0') ->> 'owner'")"
expect "and is absent now" "" "$($ISO -c "SELECT isomer.as_of('hx_account', '9', clock_timestamp())")"
expect "a new document did not exist before" "" "$($ISO -c "SELECT isomer.as_of('hx_account', '5000', '$t0')")"
expect "the whole projection as of then" "200 no-5000" \
  "$($ISO -c "SELECT count(*) || ' ' || CASE WHEN bool_or(key = '5000') THEN 'has-5000' ELSE 'no-5000' END FROM isomer.as_of('hx_account', '$t0'::timestamptz)")"
expect "the whole projection as of now equals what it holds" "0" \
  "$($ISO -c "SELECT count(*) FROM isomer.as_of('hx_account', clock_timestamp()) h WHERE h.doc IS DISTINCT FROM isomer.get('hx_account', h.key)")"
out=$($ISO_NOFAIL -c "SELECT isomer.as_of('hx_account', '7', now() - interval '1 day')" 2>&1 || true)
contains "a moment before history began is refused" "is before the history of hx_account" "$out"
expect "history of one key, oldest first" "100,250" \
  "$($ISO -c "SELECT string_agg(doc ->> 'balance', ',' ORDER BY id) FROM (SELECT DISTINCT ON (doc ->> 'balance') * FROM isomer.history('hx_account', '7') ORDER BY doc ->> 'balance', id) h")"

echo "== the feed under concurrent writers"
$ISO <<'SQL' >/dev/null
CREATE OR REPLACE PROCEDURE hx.writer(w int, n int) LANGUAGE plpgsql AS $$
DECLARE i int; k bigint;
BEGIN
    FOR i IN 1..n LOOP
        k := 1 + floor(random() * 40)::bigint;   -- a small key range, so writers collide
        CASE floor(random() * 3)::int
        WHEN 0 THEN UPDATE hx.accounts SET balance = balance + 1 WHERE id = k;
        WHEN 1 THEN INSERT INTO hx.notes VALUES (100000 * w + i, k, 'w' || w);
        ELSE        DELETE FROM hx.notes WHERE id = (SELECT max(id) FROM hx.notes WHERE account_id = k);
        END CASE;
        IF random() < 0.1 THEN ROLLBACK; ELSE COMMIT; END IF;
    END LOOP;
END $$;
SQL
c1=$($ISO -c "SELECT max(id) FROM isomer.changes WHERE projection = 'hx_account'")
for w in 1 2 3 4; do $ISO -c "CALL hx.writer($w, 150)" >/dev/null & done; wait
pass "4 writers ran 600 transactions on 40 keys"
# Replay the feed from before the writers over the as_of state at that cursor: it must land on
# exactly what the projection holds now.
expect "replaying the feed reproduces the projection exactly" "0" "$($ISO <<SQL
WITH base AS (SELECT key, doc FROM isomer.changes WHERE projection = 'hx_account' AND id <= $c1
              AND id IN (SELECT max(id) FROM isomer.changes WHERE projection = 'hx_account' AND id <= $c1 GROUP BY key)
              AND op = 'u'),
     last AS (SELECT DISTINCT ON (key) key, op, doc FROM isomer.changes_since('hx_account', $c1, 100000) ORDER BY key, id DESC),
     replayed AS (SELECT key, doc FROM base WHERE key NOT IN (SELECT key FROM last)
                  UNION ALL SELECT key, doc FROM last WHERE op = 'upsert')
SELECT count(*) FROM (
  (SELECT key, doc FROM replayed EXCEPT SELECT key, isomer.get('hx_account', key) FROM replayed)
  UNION ALL
  (SELECT k::text, isomer.get('hx_account', k::text) FROM generate_series(1, 200) k
   WHERE isomer.get('hx_account', k::text) IS NOT NULL AND k::text NOT IN (SELECT key FROM replayed))) d
SQL
)"
expect "and what it holds equals recomputation" "0" "$($ISO -c "SELECT count(*) FROM isomer.verify('hx_account')")"
expect "per key, the feed's last version is the current document" "0" \
  "$($ISO -c "SELECT count(*) FROM (SELECT DISTINCT ON (key) key, doc FROM isomer.changes WHERE projection = 'hx_account' ORDER BY key, id DESC) l WHERE l.doc IS DISTINCT FROM isomer.get('hx_account', l.key)")"

echo "== bounded projections keep history too"
$ISO <<'SQL' >/dev/null
SELECT isomer.create_projection(
  name => 'hx_notes_cache', kind => 'cache', key => 'id', freshness => 'bounded', lag => '2 seconds',
  query => $q$ SELECT a.id, (SELECT count(*) FROM hx.notes n WHERE n.account_id = a.id) AS value FROM hx.accounts a $q$,
  sources => '{"hx.accounts": "id", "hx.notes": "account_id"}');
SELECT isomer.set_history('hx_notes_cache', '1 hour');
SQL
c2=$($ISO -c "SELECT coalesce(max(id), 0) FROM isomer.changes WHERE projection = 'hx_notes_cache'")
$ISO -c "INSERT INTO hx.notes VALUES (20001, 3, 'x'), (20002, 3, 'y')" >/dev/null
eventually "a bounded write reaches the feed once drained" "3" \
  "$ISO -c \"SELECT string_agg(key, ',') FROM isomer.changes_since('hx_notes_cache', $c2)\""

echo "== retention"
$ISO <<'SQL' >/dev/null
SELECT isomer.set_history('hx_account', '1 minute');
-- Age everything for key 7 but its newest version past the cutoff, and its newest just inside.
UPDATE isomer.changes SET at = at - interval '10 minutes'
WHERE projection = 'hx_account' AND key = '7';
UPDATE isomer.projections SET history_since = history_since - interval '20 minutes' WHERE name = 'hx_account';
SQL
before=$($ISO -c "SELECT isomer.as_of('hx_account', '7', now() - interval '59 seconds')")
pruned=$($ISO -c "SELECT isomer.prune_history()")
[ "$pruned" -gt 0 ] && pass "pruning removed $pruned superseded versions" || fail "pruning removed versions" ">0" "$pruned"
expect "the version as of the horizon survives pruning" "$before" \
  "$($ISO -c "SELECT isomer.as_of('hx_account', '7', now() - interval '59 seconds')")"
out=$($ISO_NOFAIL -c "SELECT count(*) FROM isomer.changes_since('hx_account', 1)" 2>&1 || true)
contains "a feed cursor older than what is kept is refused (re-snapshot)" "older than its history" "$out"
$ISO -c "SELECT isomer.set_history('hx_account', '1 hour')" >/dev/null

echo "== HTTP API"
$ISO <<'SQL' >/dev/null
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hx_app') THEN DROP OWNED BY hx_app; DROP ROLE hx_app; END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hx_outsider') THEN DROP OWNED BY hx_outsider; DROP ROLE hx_outsider; END IF;
END $$;
CREATE ROLE hx_app; CREATE ROLE hx_outsider;
GRANT USAGE ON SCHEMA hx TO hx_app, hx_outsider;
GRANT SELECT ON hx.accounts, hx.notes TO hx_app;
GRANT SELECT ON hx.accounts TO hx_outsider;       -- not hx.notes
SQL
KEY=$($ISO -c "SELECT isomer.create_api_key('hx_app_key', 'hx_app')")
OUT_KEY=$($ISO -c "SELECT isomer.create_api_key('hx_outsider_key', 'hx_outsider')")
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
expect "no key: 401" "401" "$(code "$API/v1/projections/hx_account/docs/7")"
expect "a role that can't read every table behind it: 404" "404" \
  "$(code -H "Authorization: Bearer $OUT_KEY" "$API/v1/projections/hx_account/docs/7")"
contains "the projection list shows what the role may read" '"hx_account"' \
  "$(curl -s -H "Authorization: Bearer $KEY" "$API/v1/projections")"
expect "and hides the rest" "[]" "$(curl -s -H "Authorization: Bearer $OUT_KEY" "$API/v1/projections" | python3 -c 'import json,sys; print(json.load(sys.stdin)["projections"])')"
want=$($ISO -c "SELECT isomer.get('hx_account', '7')")
expect "GET a document: the same document the projection holds" "same" \
  "$(curl -s -H "Authorization: Bearer $KEY" "$API/v1/projections/hx_account/docs/7" | python3 -c "import json,sys; a=json.load(sys.stdin); b=json.loads(sys.argv[1]); print('same' if a == b else (a, b))" "$want")"
expect "a missing key: 404" "404" "$(code -H "Authorization: Bearer $KEY" "$API/v1/projections/hx_account/docs/9")"
expect "a key that is not the key's type: 400" "400" "$(code -H "Authorization: Bearer $KEY" "$API/v1/projections/hx_account/docs/abc")"
t_enc=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$t0")
expect "time travel over HTTP: ?as_of" "owner 9" \
  "$(curl -s -H "Authorization: Bearer $KEY" "$API/v1/projections/hx_account/docs/9?as_of=$t_enc" | python3 -c 'import json,sys; print(json.load(sys.stdin)["owner"])')"
c3=$($ISO -c "SELECT max(id) FROM isomer.changes WHERE projection = 'hx_account'")
$ISO -c "UPDATE hx.accounts SET owner = 'paged a' WHERE id = 21" -c "UPDATE hx.accounts SET owner = 'paged b' WHERE id = 22" \
     -c "UPDATE hx.accounts SET owner = 'paged c' WHERE id = 23" >/dev/null
expect "the change feed pages with a cursor" "21,22 then 23" \
  "$(p1=$(curl -s -H "Authorization: Bearer $KEY" "$API/v1/projections/hx_account/changes?after=$c3&limit=2")
     next=$(echo "$p1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["cursor"])')
     p2=$(curl -s -H "Authorization: Bearer $KEY" "$API/v1/projections/hx_account/changes?after=$next")
     echo "$(echo "$p1" | python3 -c 'import json,sys; print(",".join(c["key"] for c in json.load(sys.stdin)["changes"]))') then $(echo "$p2" | python3 -c 'import json,sys; print(",".join(c["key"] for c in json.load(sys.stdin)["changes"]))')")"
expect "a pruned cursor: 410 Gone" "410" "$(code -H "Authorization: Bearer $KEY" "$API/v1/projections/hx_account/changes?after=1")"

echo "== read your writes over HTTP"
tok=$($ISO <<'SQL' | tail -1
UPDATE hx.accounts SET owner = 'via api' WHERE id = 11;
SELECT isomer.last_write_token();
SQL
)
expect "X-Isomer-After returns the write" "via api" \
  "$(curl -s -H "Authorization: Bearer $KEY" -H "X-Isomer-After: $tok" "$API/v1/projections/hx_account/docs/11" | python3 -c 'import json,sys; print(json.load(sys.stdin)["owner"])')"
contains "and the response carries a token" "x-isomer-token" \
  "$(curl -s -D - -o /dev/null -H "Authorization: Bearer $KEY" "$API/v1/projections/hx_account/docs/11" | tr 'A-Z' 'a-z')"

echo "== live stream (server-sent events)"
LIVE=$(mktemp)
curl -s -N -H "Authorization: Bearer $KEY" "$API/v1/projections/hx_account/live" > "$LIVE" &
cpid=$!
eventually "the stream opens with a snapshot" "1" "grep -c '^event: snapshot' $LIVE"
$ISO -c "UPDATE hx.accounts SET owner = 'live one' WHERE id = 12" >/dev/null
start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
eventually "a write arrives as a change event" "1" "grep -c 'live one' $LIVE" 10
pass "delivered in $(( $(python3 -c 'import time; print(int(time.time()*1000))') - start_ms ))ms (or less)"
last_id=$(grep '^id: ' "$LIVE" | tail -1 | cut -d' ' -f2)
kill $cpid 2>/dev/null || true
$ISO -c "UPDATE hx.accounts SET owner = 'while away' WHERE id = 13" >/dev/null
curl -s -N -m 3 -H "Authorization: Bearer $KEY" -H "Last-Event-ID: $last_id" "$API/v1/projections/hx_account/live" > "$LIVE" || true
contains "reconnecting with Last-Event-ID resumes with what was missed" "while away" "$(cat "$LIVE")"
expect "and does not resend the snapshot" "0" "$(grep -c '^event: snapshot' "$LIVE" || true)"
rm -f "$LIVE"

$ISO -c "SELECT isomer.revoke_api_key('hx_app_key')" >/dev/null
expect "a revoked key: 401" "401" "$(code -H "Authorization: Bearer $KEY" "$API/v1/projections/hx_account/docs/7")"

echo "== history passed"
