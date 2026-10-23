#!/bin/bash
# Phase 1 acceptance test: vector projections. Embeddings are computed by the embedder service
# against a mock OpenAI-compatible API (tests/mockai), which rate-limits every 3rd request.
#
#   docker compose --profile attach up -d && ./tests/phase1.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
ISO_DIAG="docker compose exec -T db psql -U postgres -d postgres -X -q -t -A"   # every query runs, even after one fails
MOCK() { docker compose exec -T mockai python -c "import urllib.request,sys; r=urllib.request.Request('http://localhost:8080$1', method='$2'); print(urllib.request.urlopen(r).read().decode())"; }
stat() { MOCK /stats GET | python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }

pass() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"
  echo "-- state at failure --"
  $ISO_DIAG -c "SELECT 'alert', alert, subject, left(detail, 200) FROM isomer.alerts" \
            -c "SELECT 'status', name, kind, status, oldest_pending, search_staleness FROM isomer.projection_status" 2>&1 | sed 's/^/       /'
  exit 1
}
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=$(( ${4:-20} * ${ISOMER_TIMEOUT_SCALE:-1} )) got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 0.5
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}

echo "== reset"
$ISO <<'SQL'
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE kind = 'vector';
DELETE FROM isomer.embedding_models WHERE name LIKE 'mock-%';
DELETE FROM isomer.embedding_usage; DELETE FROM isomer.events;
DROP TABLE IF EXISTS public.products;
CREATE TABLE public.products (sku text PRIMARY KEY, title text NOT NULL, description text, price numeric);
INSERT INTO public.products VALUES
  ('KB-01', 'Quiet mechanical keyboard', 'Tactile switches, dampened for open offices', 89),
  ('KB-09', 'Ergonomic split keyboard', 'Tented halves, low-profile mechanical switches', 149),
  ('MS-02', 'Wireless gaming mouse', 'Lightweight shell, 26k DPI optical sensor', 59),
  ('HP-03', 'Noise-cancelling headphones', 'Over-ear, 40 hour battery', 199),
  ('CB-07', 'USB-C cable', 'Braided, 2 metres', 12),
  ('XX-00', 'FAIL', NULL, 1);
SELECT isomer.add_embedding_model('mock-8', 'openai', 'mock-8', 8, endpoint => 'http://mockai:8080/v1',
                                  api_key_env => 'MOCKAI_KEY', price_per_million => 0.02, max_batch_items => 2);
SELECT isomer.add_embedding_model('mock-16', 'openai', 'mock-16', 16, endpoint => 'http://mockai:8080/v1',
                                  api_key_env => 'MOCKAI_KEY', price_per_million => 0.06, max_batch_items => 4);
SQL
MOCK /reset POST >/dev/null

echo "== FR-V2 cost estimate before creating"
expect "estimate covers every row" "6" \
  "$($ISO -c "SELECT rows FROM isomer.estimate_embedding_cost(\$\$SELECT sku, title || coalesce(' ' || description, '') AS input FROM public.products\$\$, 'mock-8')")"

echo "== FR-V1 create a vector projection"
$ISO <<'SQL'
SELECT isomer.create_projection(
  name => 'product_vectors', kind => 'vector', key => 'sku', model => 'mock-8', lag => '10 seconds',
  query => $q$ SELECT sku, title || coalesce(' ' || description, '') AS input FROM products $q$,
  sources => '{"products": "sku"}');
SQL
out=$($ISO -c "SELECT isomer.create_projection(name => 'v2', kind => 'vector', key => 'sku', model => 'mock-8', freshness => 'sync',
  query => 'SELECT sku, title AS input FROM products', sources => '{\"products\": \"sku\"}')" 2>&1 || true)
case "$out" in *"cannot be sync"*) pass "sync vector projection refused with a reason";; *) fail "sync refused" "cannot be sync" "$out";; esac

eventually "backfill embeds every good row" "5" "$ISO -c \"SELECT count(*) FROM public.product_vectors\"" 40
eventually "the bad input lands in the dead letter, alone" "1" \
  "$ISO -c \"SELECT count(*) FROM isomer.embed_dead_letter WHERE projection = 'product_vectors' AND key = 'XX-00'\"" 30
expect "each text sent to the provider once despite retries" "5" "$(stat "d['inputs_total']")"
[ "$(stat "d['rate_limited']")" -ge 1 ] && pass "the provider's 429s were retried" || fail "429s" ">=1" "$(stat "d['rate_limited']")"
expect "embedding has the model's dimensions" "8" "$($ISO -c "SELECT DISTINCT vector_dims(embedding) FROM public.product_vectors")"

echo "== FR-V4 query-time embedding"
expect "exact text finds its own row first" "MS-02" \
  "$($ISO -c "SELECT sku FROM public.product_vectors ORDER BY embedding <=> isomer.embed_for('product_vectors', 'Wireless gaming mouse Lightweight shell, 26k DPI optical sensor') LIMIT 1")"
expect "the HNSW index is used" "1" \
  "$($ISO -c "SET enable_seqscan = off; EXPLAIN SELECT sku FROM public.product_vectors ORDER BY embedding <=> '[1,0,0,0,0,0,0,0]' LIMIT 3" | grep -c 'hnsw' )"

echo "== FR-V1 only changed text is re-embedded"
before=$(stat "d['inputs_total']")
$ISO -c "UPDATE products SET price = 79 WHERE sku = 'KB-01'"
sleep 4
expect "a price change (not in the text) embeds nothing" "$before" "$(stat "d['inputs_total']")"
$ISO -c "UPDATE products SET description = 'Silent linear switches, dampened for open offices' WHERE sku = 'KB-01'"
eventually "a description change re-embeds that one row" "$((before + 1))" "stat \"d['inputs_total']\"" 20
$ISO -c "DELETE FROM products WHERE sku = 'CB-07'"
eventually "a deleted row's embedding is removed" "0" "$ISO -c \"SELECT count(*) FROM public.product_vectors WHERE sku = 'CB-07'\"" 10
$ISO -c "UPDATE products SET title = 'Bad input fixed' WHERE sku = 'XX-00'"
eventually "fixing the bad row embeds it and clears its dead letter" "1|0" \
  "$ISO -c \"SELECT (SELECT count(*) FROM public.product_vectors WHERE sku = 'XX-00') || '|' || (SELECT count(*) FROM isomer.embed_dead_letter WHERE key = 'XX-00')\"" 20
expect "steady-state embeddings within the lag budget" "0" \
  "$($ISO -c "SELECT budget_breaches FROM isomer.projection_status WHERE name = 'product_vectors'")"

echo "== FR-V2 spend is tracked and capped"
expect "usage recorded for backfill, steady and query" "backfill,query,steady" \
  "$($ISO -c "SELECT string_agg(DISTINCT phase, ',' ORDER BY phase) FROM isomer.embedding_usage WHERE tokens > 0")"
$ISO -c "SELECT isomer.set_embedding_cap('product_vectors', 0.0000001)"
$ISO -c "UPDATE products SET description = 'Over-ear, 50 hour battery' WHERE sku = 'HP-03'"
eventually "reaching the cap pauses the projection" "paused_budget" \
  "$ISO -c \"SELECT status FROM isomer.projections WHERE name = 'product_vectors'\"" 20
$ISO -c "UPDATE products SET description = 'Over-ear, 60 hour battery' WHERE sku = 'HP-03'"
sleep 3
expect "a paused projection's work waits" "1" "$($ISO -c "SELECT count(*) FROM isomer.pending WHERE projection = 'product_vectors'")"
expect "the pause shows up as an alert" "1" "$($ISO -c "SELECT count(*) FROM isomer.alerts WHERE alert = 'budget_paused'")"
$ISO -c "SELECT isomer.set_embedding_cap('product_vectors', 10)"
eventually "raising the cap resumes and catches up" "active|0|0" \
  "$ISO -c \"SELECT status || '|' || pending_keys || '|' || embed_backlog FROM isomer.projection_status WHERE name = 'product_vectors'\"" 20

echo "== FR-V3 model upgrade with an atomic swap"
$ISO -c "SELECT isomer.set_projection_model('product_vectors', 'mock-16')"
expect "reads stay on the old model while re-embedding" "8" "$($ISO -c "SELECT DISTINCT vector_dims(embedding) FROM public.product_vectors")"
eventually "swaps to the new model when complete" "mock-16||16" \
  "$ISO -c \"SELECT p.model || '|' || coalesce(p.pending_model, '') || '|' || (SELECT DISTINCT vector_dims(embedding) FROM public.product_vectors) FROM isomer.projections p WHERE name = 'product_vectors'\"" 40
expect "old embeddings are removed" "0" "$($ISO -c "SELECT count(*) FROM isomer_vectors.product_vectors WHERE model = 'mock-8'")"

echo "== local test embedder (no network)"
$ISO <<'SQL'
SELECT isomer.create_projection(
  name => 'product_vectors_local', kind => 'vector', key => 'sku', model => 'local-hash-64',
  query => $q$ SELECT sku, title || coalesce(' ' || description, '') AS input FROM products $q$,
  sources => '{"products": "sku"}');
SQL
eventually "local embeddings built" "5" "$ISO -c \"SELECT count(*) FROM public.product_vectors_local\"" 30
expect "word overlap ranks the keyboards first" "KB-01,KB-09" \
  "$($ISO -c "SELECT string_agg(sku, ',' ORDER BY sku) FROM (SELECT sku FROM public.product_vectors_local ORDER BY embedding <=> isomer.embed('mechanical keyboard switches', 'local-hash-64') LIMIT 2) s")"

echo "== FR-O1 status and alerts"
# Alerts are the state now: a projection behind its lag budget clears once it catches up.
eventually "no open alerts (lag, backlog, dead letters, caps)" "0" "$ISO -c \"SELECT count(*) FROM isomer.alerts WHERE alert NOT IN ('warning', 'error')\""
$ISO -c "SELECT name, model, embed_backlog, embed_dead_letters, embed_cost_this_month, max_lag_ms FROM isomer.projection_status WHERE kind = 'vector'" | sed 's/^/       /'

echo "== phase 1 passed"
