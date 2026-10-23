#!/bin/bash
# The in-cluster CPU embedding model (FR-V1b): a vector projection embedded by bge-small-en-v1.5 on
# the embedder's own CPU, with no API key and no network, ranking by meaning rather than words.
#
#   docker compose up -d && ./tests/local_model.sh
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"

pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n       expected: %s\n       got:      %s\n' "$1" "$2" "$3"; exit 1; }
expect() { [ "$3" = "$2" ] && pass "$1" || fail "$1" "$2" "$3"; }
eventually() {
  local name=$1 want=$2 cmd=$3 timeout=$(( ${4:-60} * ${ISOMER_TIMEOUT_SCALE:-1} )) got="" start=$SECONDS
  while (( SECONDS - start < timeout )); do
    got=$(eval "$cmd" 2>/dev/null || true)
    [ "$got" = "$want" ] && { pass "$name ($(( SECONDS - start ))s)"; return; }
    sleep 1
  done
  fail "$name (after ${timeout}s)" "$want" "$got"
}

echo "== reset"
$ISO <<'SQL' >/dev/null
ALTER EXTENSION isomer UPDATE;
SELECT isomer.drop_projection(name) FROM isomer.projections WHERE name LIKE 'lm_%';
DROP SCHEMA IF EXISTS lm CASCADE;
CREATE SCHEMA lm;
CREATE TABLE lm.listings (id text PRIMARY KEY, title text NOT NULL);
-- Each query below shares no content word with the listing it should find.
INSERT INTO lm.listings VALUES
  ('kb',    'Silent mechanical switches for late-night writing'),
  ('pan',   'Cast iron skillet, pre-seasoned, for searing steak'),
  ('tent',  'Two-person backpacking shelter, waterproof fly'),
  ('lamp',  'Dimmable LED reading light with a clamp'),
  ('shoes', 'Trail running footwear with a grippy outsole');
SQL
expect "the CPU model is registered" "local|384|0" \
  "$($ISO -c "SELECT provider || '|' || dimensions || '|' || price_per_million FROM isomer.embedding_models WHERE name = 'bge-small-en-v1.5'")"

echo "== FR-V1b embeddings computed on the embedder, no key, no network"
$ISO <<'SQL' >/dev/null
SELECT isomer.create_projection(
  name => 'lm_listing_vectors', kind => 'vector', key => 'id', model => 'bge-small-en-v1.5', target_schema => 'lm',
  query => $q$ SELECT id, title AS input FROM lm.listings $q$, sources => '{"lm.listings": "id"}');
SQL
eventually "every listing is embedded" "5|384" \
  "$ISO -c \"SELECT count(*) || '|' || min(vector_dims(embedding)) FROM lm.lm_listing_vectors\""
nearest() {
  $ISO -c "SELECT id FROM lm.lm_listing_vectors ORDER BY embedding <=> isomer.embed('$1', 'bge-small-en-v1.5') LIMIT 1"
}
expect "a quiet keyboard for typing at night  -> the silent switches" "kb" "$(nearest 'a quiet keyboard for typing at night')"
expect "something to cook a ribeye in         -> the skillet" "pan" "$(nearest 'something to cook a ribeye in')"
expect "where to sleep on a hike in the rain  -> the shelter" "tent" "$(nearest 'where to sleep on a hike in the rain')"
expect "a lamp for my bed                     -> the reading light" "lamp" "$(nearest 'a lamp for my bed')"
expect "sneakers for muddy paths              -> the trail shoes" "shoes" "$(nearest 'sneakers for muddy paths')"

echo "== only changed text is re-embedded, at no cost"
before=$($ISO -c "SELECT embedded_at FROM isomer_vectors.lm_listing_vectors WHERE key = 'pan'")
kb_before=$($ISO -c "SELECT embedded_at FROM isomer_vectors.lm_listing_vectors WHERE key = 'kb'")
$ISO -c "UPDATE lm.listings SET title = 'Carbon steel wok for stir-frying' WHERE id = 'kb'" >/dev/null
eventually "the edited listing is re-embedded" "t" \
  "$ISO -c \"SELECT embedded_at > '$kb_before' FROM isomer_vectors.lm_listing_vectors WHERE key = 'kb'\""
expect "and now answers for its new meaning" "kb" "$(nearest 'a pan for stir fry')"
expect "an unchanged one is not" "$before" "$($ISO -c "SELECT embedded_at FROM isomer_vectors.lm_listing_vectors WHERE key = 'pan'")"
expect "tokens are counted and cost nothing" "true|0" \
  "$($ISO -c "SELECT (sum(tokens) > 0) || '|' || sum(cost_usd)::int FROM isomer.embedding_usage WHERE projection = 'lm_listing_vectors'")"

echo "all local model tests passed"
