#!/bin/bash
# Phase 3 acceptance test (single-node parts): the proposer, certificates, the shard advisor,
# unused-projection detection, and a randomized concurrency check of projection correctness
# and read-your-writes.
#
#   docker compose --profile attach up -d && ./tests/phase3.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
ISO_NOFAIL="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A"

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
contains() { case "$3" in *"$2"*) pass "$1";; *) fail "$1" "...$2..." "$3";; esac; }

echo "== reset"
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection(name) FROM isomer.projections;
DELETE FROM isomer.proposals; DELETE FROM isomer.events; DELETE FROM isomer.read_counters;
SELECT isomer.set_setting('proposer.auto_accept_max_write_ms_per_s', NULL);
SELECT isomer.set_setting('unused.quiet_period', '7 days');
DROP SCHEMA IF EXISTS p3, p3dist CASCADE;
DROP PROCEDURE IF EXISTS public.p3_worker(int, int);
CREATE SCHEMA p3;
CREATE TABLE p3.customers (id bigint PRIMARY KEY, name text NOT NULL, region text);
CREATE TABLE p3.orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL, status text NOT NULL, total numeric);
CREATE TABLE p3.regions (code text PRIMARY KEY, label text);
INSERT INTO p3.regions VALUES ('eu', 'Europe'), ('us', 'Americas');
INSERT INTO p3.customers SELECT g, 'customer ' || g, CASE WHEN g % 2 = 0 THEN 'eu' ELSE 'us' END FROM generate_series(1, 200) g;
INSERT INTO p3.orders SELECT g, 1 + g % 200, 'open', g FROM generate_series(1, 1000) g;
-- A big table read by customer without an index: the kind of read a projection pays off for.
CREATE TABLE p3.visits (id bigint PRIMARY KEY, customer_id bigint NOT NULL, page text);
INSERT INTO p3.visits SELECT g, 1 + g % 200, '/page/' || (g % 37) FROM generate_series(1, 200000) g;
ANALYZE p3.customers, p3.orders, p3.visits;
SQL
# Table statistics are flushed by each session about once a second: take the write baseline in a
# fresh session after the bulk load has been flushed, so the load doesn't count as recent writes.
sleep 1.5
$ISO -c "SELECT pg_stat_statements_reset()" -c "SELECT isomer.snapshot_activity()" >/dev/null

echo "== FR-M1/M2 the proposer finds a repeated keyed join"
{
  echo "PREPARE by_customer(bigint) AS SELECT c.id, c.name, v.id AS visit_id, v.page FROM p3.customers c JOIN p3.visits v ON v.customer_id = c.id WHERE c.id = \$1;"
  for i in $(seq 1 120); do echo "EXECUTE by_customer($(( (i % 200) + 1 )));"; done
  echo "PREPARE by_region(text) AS SELECT c.id, r.label FROM p3.customers c JOIN p3.regions r ON r.code = c.region WHERE c.name = \$1;"
  for i in $(seq 1 60); do echo "EXECUTE by_region('customer $i');"; done
  for i in $(seq 1 30); do echo "UPDATE p3.visits SET page = '/updated' WHERE id = $i;"; done
} | $ISO >/dev/null
expect "propose records proposals" "2" "$($ISO -c "SELECT isomer.propose(10)")"
good=$($ISO -c "SELECT id FROM isomer.proposals WHERE source_query LIKE '%v.customer_id = c.id%'")
expect "the keyed join is proposed cleanly" "proposed" "$($ISO -c "SELECT status FROM isomer.proposals WHERE id = $good")"
expect "sources are mapped through the join" '{"p3.visits": "customer_id", "p3.customers": "id"}' \
  "$($ISO -c "SELECT draft -> 'sources' FROM isomer.proposals WHERE id = $good")"
expect "a slow keyed read shows a net benefit" "t" "$($ISO -c "SELECT benefit_ms_per_s > cost_ms_per_s AND est_speedup > 1 FROM isomer.proposals WHERE id = $good")"
contains "a lookup keyed on a non-key column needs review" "cannot map" \
  "$($ISO -c "SELECT status || ': ' || array_to_string(problems, '; ') FROM isomer.proposals WHERE source_query LIKE '%r.code = c.region%'")"

echo "== FR-M3 accepting a proposal"
contains "accept creates the projection" "document projection proposed_$good" "$($ISO -c "SELECT isomer.accept_proposal($good)")"
expect "the accepted projection equals recomputation" "0" "$($ISO -c "SELECT count(*) FROM isomer.verify('proposed_$good')")"
expect "a document groups the key's rows" "$($ISO -c "SELECT count(*) FROM p3.visits WHERE customer_id = 7")" \
  "$($ISO -c "SELECT jsonb_array_length(expected -> 'rows') FROM isomer._audit_pairs((SELECT p FROM isomer.projections p WHERE name = 'proposed_$good'), ARRAY['7'])")"
$ISO -c "SELECT isomer.set_setting('proposer.auto_accept_max_write_ms_per_s', '1000')" >/dev/null
{
  echo "PREPARE by_visitor(bigint) AS SELECT v.id, v.page, c.name FROM p3.visits v JOIN p3.customers c ON c.id = v.customer_id WHERE v.customer_id = \$1;"
  for i in $(seq 1 80); do echo "EXECUTE by_visitor($i);"; done
} | $ISO >/dev/null
$ISO -c "SELECT isomer.propose(10)" >/dev/null
expect "with a cost threshold set, a cheap win is auto-accepted" "auto_accepted" \
  "$($ISO -c "SELECT status FROM isomer.proposals WHERE source_query LIKE '%WHERE v.customer_id = \$1%'")"

echo "== FR-S5 certificates"
expect "certificate is valid" "true" "$($ISO -c "SELECT isomer.certificate('proposed_$good') ->> 'valid'")"
contains "certificate states the rule" "key-recompute" "$($ISO -c "SELECT isomer.certificate('proposed_$good') -> 'rules'")"
$ISO -c "ALTER TABLE p3.visits DISABLE TRIGGER isomer_proposed_$good" >/dev/null
expect "a disabled capture trigger invalidates the certificate" "false" "$($ISO -c "SELECT isomer.certificate('proposed_$good') ->> 'valid'")"
$ISO -c "SELECT isomer._create_source_triggers('proposed_$good')" >/dev/null
expect "recreating the triggers restores it" "true" "$($ISO -c "SELECT isomer.certificate('proposed_$good') ->> 'valid'")"

echo "== FR-M6/M7 the shard advisor"
$ISO <<'SQL' >/dev/null
CREATE SCHEMA p3dist;
CREATE TABLE p3dist.accounts (account_id bigint PRIMARY KEY, name text, owner_id bigint);
CREATE TABLE p3dist.events (event_id bigint, account_id bigint NOT NULL, kind text, PRIMARY KEY (account_id, event_id));
CREATE TABLE p3dist.owners (owner_id bigint PRIMARY KEY, name text);
SELECT isomer.distribute(ARRAY['p3dist.accounts', 'p3dist.events']::regclass[], 'account_id');
SELECT isomer.distribute(ARRAY['p3dist.owners']::regclass[], 'owner_id');
INSERT INTO p3dist.owners SELECT g, 'owner ' || g FROM generate_series(1, 10) g;
INSERT INTO p3dist.accounts SELECT g, 'account ' || g, 1 + g % 10 FROM generate_series(1, 100) g;
INSERT INTO p3dist.events SELECT g, 1 + g % 100, 'login' FROM generate_series(1, 500) g;
SELECT isomer.create_projection(
  name => 'account_events', kind => 'document', key => 'account_id', database => 'p3',
  query => $q$ SELECT a.account_id, jsonb_build_object('name', a.name,
              'events', (SELECT count(*) FROM p3dist.events e WHERE e.account_id = a.account_id)) AS doc
              FROM p3dist.accounts a $q$,
  sources => '{"p3dist.accounts": "account_id", "p3dist.events": "account_id"}');
SELECT isomer.create_projection(
  name => 'owner_accounts', kind => 'document', key => 'owner_id', database => 'p3',
  query => $q$ SELECT o.owner_id, jsonb_build_object('name', o.name, 'accounts', count(*)) AS doc
              FROM p3dist.owners o JOIN p3dist.accounts a ON a.owner_id = o.owner_id
              GROUP BY o.owner_id, o.name $q$,
  sources => '{"p3dist.owners": "owner_id", "p3dist.accounts": "owner_id"}');
SQL
for _ in $(seq 1 20); do $ISO -c "SELECT count(*) FROM p3dist.accounts a JOIN p3dist.owners o ON o.owner_id = a.owner_id" >/dev/null; done
advice=$($ISO -c "SELECT string_agg(advice || ':' || subject, ' | ' ORDER BY advice, subject) FROM isomer.shard_advice()")
contains "a lone small table is a reference candidate" "reference_candidate:p3dist.owners" "$advice"
contains "a co-located bounded projection is sync-ready" "sync_ready:account_events" "$advice"
contains "a projection blocked by co-location is reported" "sync_blocked_by_colocation:owner_accounts" "$advice"
contains "a fan-out join is reported" "cross_shard_query" "$advice"
contains "a reshard plan is generated" "alter_distributed_table" "$($ISO -c "SELECT string_agg(s, ' ') FROM isomer.reshard_plan('p3dist.accounts', 'owner_id', 'p3dist.owners') s")"

echo "== FR-M5 unused projections"
$ISO -c "SELECT isomer.set_setting('unused.quiet_period', '1 second'); SELECT isomer.track_reads();" >/dev/null
sleep 2
$ISO -c "SELECT count(*) FROM p3dist.events" >/dev/null   # not a projection read
$ISO -c "SELECT isomer.track_reads()" >/dev/null
contains "a projection nobody reads is flagged" "account_events" "$($ISO -c "SELECT string_agg(name, ',') FROM isomer.unused_projections")"
contains "and alerted" "unused" "$($ISO -c "SELECT string_agg(DISTINCT alert, ',') FROM isomer.alerts")"
$ISO -c "SELECT isomer.set_setting('unused.quiet_period', '7 days')" >/dev/null

echo "== randomized concurrency: correctness and read-your-writes under load"
$ISO <<'SQL' >/dev/null
SELECT isomer.alter_projection('account_events', freshness => 'sync');
SELECT isomer.create_projection(
  name => 'customer_orders3', kind => 'document', key => 'id', database => 'p3', lag => '2 seconds',
  query => $q$ SELECT c.id, jsonb_build_object('name', c.name, 'region', (SELECT r.label FROM p3.regions r WHERE r.code = c.region),
              'orders', coalesce((SELECT jsonb_agg(jsonb_build_object('id', o.id, 'status', o.status, 'total', o.total) ORDER BY o.id)
                                  FROM p3.orders o WHERE o.customer_id = c.id), '[]'::jsonb)) AS doc
              FROM p3.customers c $q$,
  sources => '{"p3.customers": "id", "p3.orders": "customer_id", "p3.regions": null}');
CREATE TABLE IF NOT EXISTS public.p3_violations (worker int, iteration int, detail text);
TRUNCATE public.p3_violations;
-- One worker: random transactions (some rolled back), including moving orders between
-- customers (a key change), and after each committed rename, a read-your-writes check.
CREATE PROCEDURE public.p3_worker(w int, n int) LANGUAGE plpgsql AS $$
DECLARE
    i int; c bigint; o bigint; a bigint; v text; got text; coll text;
BEGIN
    coll := isomer._collection_table('p3', 'customer_orders3');
    FOR i IN 1..n LOOP
        c := 1 + floor(random() * 200)::bigint;
        o := 1 + floor(random() * 1000)::bigint;
        a := 1 + floor(random() * 100)::bigint;
        CASE floor(random() * 6)::int
        WHEN 0 THEN UPDATE p3.orders SET customer_id = c WHERE id = o;                 -- moves between documents
        WHEN 1 THEN UPDATE p3.orders SET status = 'shipped', total = total + 1 WHERE id = o;
        WHEN 2 THEN INSERT INTO p3.orders VALUES (100000 * w + i, c, 'open', i);
        WHEN 3 THEN DELETE FROM p3.orders WHERE id = o;
        WHEN 4 THEN INSERT INTO p3dist.events VALUES (100000 * w + i, a, 'click');
        ELSE        UPDATE p3dist.accounts SET name = 'w' || w || '-' || i WHERE account_id = a;
        END CASE;
        IF random() < 0.15 THEN ROLLBACK; ELSE COMMIT; END IF;

        IF i % 10 = 0 THEN
            v := 'w' || w || '-i' || i;
            UPDATE p3.customers SET name = v WHERE id = c;
            COMMIT;
            PERFORM isomer.wait_for(isomer.last_write_token(), 'customer_orders3', '10 seconds');
            EXECUTE format('SELECT isomer._ejson_relax(documentdb_core.bson_to_json_string(document)::text::jsonb) ->> ''name'' FROM %s '
                           'WHERE isomer._ejson_relax(documentdb_core.bson_to_json_string(document)::text::jsonb) ->> ''_id'' = $1', coll)
            INTO got USING c::text;
            -- Another worker may rename the same customer in between; only a stale value is wrong.
            IF got IS DISTINCT FROM v AND NOT EXISTS (SELECT 1 FROM p3.customers WHERE id = c AND name <> v) THEN
                INSERT INTO public.p3_violations VALUES (w, i, format('customer %s: wrote %s, read back %s', c, v, got));
            END IF;
            COMMIT;
        END IF;
    END LOOP;
END $$;
SQL
start=$SECONDS
for w in 1 2 3 4; do $ISO_NOFAIL -c "CALL public.p3_worker($w, 150)" > /dev/null 2>&1 & done
wait
pass "4 workers ran 600 random transactions in $(( SECONDS - start ))s"
expect "every read-your-writes check saw its own write" "0" "$($ISO -c "SELECT count(*) FROM public.p3_violations")"
$ISO -c "SELECT isomer.wait_for(isomer.current_token(), NULL, '30 seconds')" >/dev/null
expect "the bounded projection equals recomputation for every key" "0" "$($ISO -c "SELECT count(*) FROM isomer.verify('customer_orders3')")"
expect "the sync projection over shards equals recomputation for every key" "0" "$($ISO -c "SELECT count(*) FROM isomer.verify('account_events')")"
expect "the accepted proposal equals recomputation" "0" "$($ISO -c "SELECT count(*) FROM isomer.verify('proposed_$good')")"
expect "no errors were logged" "0" "$($ISO -c "SELECT count(*) FROM isomer.events WHERE level = 'error'")"

echo "== phase 3 (single node) passed"
