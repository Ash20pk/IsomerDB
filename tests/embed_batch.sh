#!/bin/bash
# Embedding backfills through a provider's Batch API (FR-V1a), and bundled models billed at list
# price plus a markup after a free allowance (FR-V1b), against the mock provider (tests/mockai).
#
#   docker compose --profile attach --profile test up -d && ./tests/embed_batch.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"
MOCK() { docker compose exec -T mockai python -c "import urllib.request,sys; r=urllib.request.Request('http://localhost:8080$1', method='$2'); print(urllib.request.urlopen(r).read().decode())"; }
stat() { MOCK /stats GET | python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }

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

echo "== reset"
$ISO <<'SQL' >/dev/null
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE kind = 'vector';
DELETE FROM isomer.embed_batches;
DELETE FROM isomer.embedding_models WHERE name LIKE 'mock-%';
DELETE FROM isomer.embedding_usage; DELETE FROM isomer.events; DELETE FROM isomer.usage; DELETE FROM isomer.usage_cursor;
UPDATE isomer.bundled_allowance SET used_tokens = 0;
SELECT isomer.set_setting('bundled.free_tokens', '100');
DROP TABLE IF EXISTS public.eb_docs, public.eb_note_rows;
CREATE TABLE public.eb_docs (id int PRIMARY KEY, body text NOT NULL);
INSERT INTO public.eb_docs SELECT g, 'document number ' || g || ' about ' || (ARRAY['keyboards', 'mice', 'cables', 'screens'])[1 + g % 4]
FROM generate_series(1, 300) g;
INSERT INTO public.eb_docs VALUES (999, 'FAIL');
CREATE TABLE public.eb_note_rows (id int PRIMARY KEY, body text NOT NULL);
INSERT INTO public.eb_note_rows SELECT g, 'a short note, number ' || g FROM generate_series(1, 40) g;
-- Batch backfills: at least 100 jobs waiting go through the Batch API, at half the price.
SELECT isomer.add_embedding_model('mock-batch-8', 'openai', 'mock-8', 8, endpoint => 'http://mockai:8080/v1',
                                  api_key_env => 'MOCKAI_KEY', price_per_million => 0.10, max_batch_items => 16,
                                  batch_api => true, batch_min_items => 100);
-- A bundled model: IsomerDB's key, list price $1 per million tokens plus 20%.
SELECT isomer.add_embedding_model('mock-bundled-8', 'openai', 'mock-8', 8, endpoint => 'http://mockai:8080/v1',
                                  api_key_env => 'MOCKAI_KEY', price_per_million => 1.00, max_batch_items => 16,
                                  bundled => true, markup => 0.2);
SQL
MOCK /reset POST >/dev/null
# The meter's first sample after a reset is only a baseline: take it now, before any usage.
$ISO -c "SELECT isomer.meter()" >/dev/null

echo "== FR-V1a a backfill goes through the provider's Batch API"
$ISO -c "SELECT isomer.create_projection(name => 'eb_vectors', kind => 'vector', key => 'id', model => 'mock-batch-8', lag => '30 seconds',
  query => 'SELECT id, body AS input FROM public.eb_docs', sources => '{\"public.eb_docs\": \"id\"}')" >/dev/null
# Rows that change while the batch is out: their batched (old) text must not be written.
sleep 3
$ISO -c "UPDATE public.eb_docs SET body = 'changed text ' || id WHERE id BETWEEN 10 AND 19" >/dev/null
eventually "every document is embedded" "300" "$ISO -c \"SELECT count(*) FROM isomer_vectors.eb_vectors\"" 90
eventually "every stored embedding is of its row's current text (only the rejected row is missing)" "999" \
  "$ISO -c \"SELECT string_agg(key, ',') FROM isomer.verify('eb_vectors')\"" 60
expect "in one provider batch" "1" "$(stat 'd["batches"]')"
expect "each text went to the provider exactly once, through the batch" "300 0" \
  "$(stat 'd["batch_inputs_total"]') $(stat 'sum(v for k, v in d["by_input"].items() if k.startswith("document"))')"
expect "the batch is recorded as completed, with every item" "completed 301" \
  "$($ISO -c "SELECT status || ' ' || items FROM isomer.embed_batches")"
eventually "the input the provider rejected left the batch and ends in the dead letter" "1" \
  "$ISO -c \"SELECT count(*) FROM isomer.embed_dead_letter WHERE projection = 'eb_vectors' AND key = '999'\"" 60
# The batch is billed at half price; the rows that changed mid-batch went by ordinary request, at
# full price.
expect "backfill tokens are billed at the batch price, the rest at full price" "t" \
  "$($ISO -c "SELECT tokens > 0 AND cost_usd >= tokens * 0.10 * 0.5 / 1000000 AND cost_usd < tokens * 0.10 / 1000000 FROM isomer.embedding_usage WHERE projection = 'eb_vectors' AND phase = 'backfill'")"

echo "== steady-state changes don't wait for a batch"
$ISO -c "UPDATE public.eb_docs SET body = body || ' (revised)' WHERE id IN (1, 2, 3)" >/dev/null
eventually "three revised documents are re-embedded through ordinary requests" "3" \
  "stat 'sum(v for k, v in d[\"by_input\"].items() if k.endswith(\"(revised)\"))'" 30
expect "and no second batch" "1" "$(stat 'd["batches"]')"

echo "== FR-V1b bundled models: list price plus markup, after the free allowance"
$ISO -c "SELECT isomer.create_projection(name => 'eb_notes', kind => 'vector', key => 'id', model => 'mock-bundled-8', lag => '30 seconds',
  query => 'SELECT id, body AS input FROM public.eb_note_rows', sources => '{\"public.eb_note_rows\": \"id\"}')" >/dev/null
eventually "every note is embedded" "40" "$ISO -c \"SELECT count(*) FROM isomer_vectors.eb_notes\"" 60
expect "the first 100 tokens are free, the rest billed at list price plus 20%" "t" \
  "$($ISO -c "SELECT sum(tokens) > 100 AND sum(billed_usd) = (sum(tokens) - 100) * 1.00 * 1.2 / 1000000 FROM isomer.embedding_usage WHERE model = 'mock-bundled-8'")"
expect "what the provider charges is kept apart" "t" \
  "$($ISO -c "SELECT sum(cost_usd) = sum(tokens) * 1.00 / 1000000 FROM isomer.embedding_usage WHERE model = 'mock-bundled-8'")"
expect "the allowance is used up once, for the whole cluster" "100" "$($ISO -c "SELECT used_tokens FROM isomer.bundled_allowance")"
expect "a model on the customer's own key bills nothing" "0" \
  "$($ISO -c "SELECT sum(billed_usd) FROM isomer.embedding_usage WHERE model = 'mock-batch-8'")"
$ISO -c "SELECT isomer.meter()" >/dev/null
expect "metered for billing as embedding_billed_usd" "t" \
  "$($ISO -c "SELECT coalesce(sum(value), 0) > 0 FROM isomer.usage WHERE metric = 'embedding_billed_usd' AND dimension = 'mock-bundled-8'")"

echo "== a monthly cap counts what the customer pays"
$ISO -c "SELECT isomer.set_embedding_cap('eb_notes', 0.00001)" >/dev/null
$ISO -c "UPDATE public.eb_note_rows SET body = body || ' and more words to embed' WHERE id <= 20" >/dev/null
eventually "past the cap, the projection pauses" "paused_budget" "$ISO -c \"SELECT status FROM isomer.projections WHERE name = 'eb_notes'\"" 30

$ISO -c "SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'eb\_%'" -c "SELECT isomer.set_setting('bundled.free_tokens', '1000000')" >/dev/null
echo "embed_batch: all checks passed"
