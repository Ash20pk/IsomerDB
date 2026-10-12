#!/bin/bash
# Every acceptance suite, on fresh stacks: single-node first, then the three-node cluster.
# Destroys this compose project's volumes.
#
#   ./scripts/test-all.sh            # everything
#   ./scripts/test-all.sh single     # single-node suites only
set -uo pipefail
cd "$(dirname "$0")/.."

failed=()
run() {
  echo "### $1"
  if "./$1"; then echo "### $1: passed"; else echo "### $1: FAILED"; failed+=("$1"); fi
}
healthy() {
  for _ in $(seq 1 120); do
    docker compose exec -T db pg_isready -U postgres -q 2>/dev/null \
      && docker compose exec -T db psql -U postgres -tAc "SELECT 1 FROM pg_extension WHERE extname = 'isomer'" 2>/dev/null | grep -q 1 \
      && return 0
    sleep 2
  done
  echo "the stack did not come up"; exit 1
}

docker compose --profile attach --profile test --profile cluster down -v --remove-orphans
(cd clients && npm ci --silent && npm run build --silent >/dev/null && npm test --silent) || { echo "### clients: FAILED"; exit 1; }
docker compose --profile attach --profile test build
docker compose --profile attach --profile test up -d
healthy
sleep 5
for t in scripts/smoke.sh tests/phase0.sh tests/phase1.sh tests/phase2.sh tests/phase3.sh tests/native.sh \
         tests/routing.sh tests/history.sh tests/freshness.sh tests/console.sh tests/local_model.sh tests/metering.sh tests/alerts.sh tests/cutover_plan.sh \
         tests/incremental.sh tests/embed_batch.sh tests/managed_source.sh tests/edge.sh; do
  run "$t"
done

if [ "${1:-all}" = "all" ]; then
  docker compose --profile attach --profile test down -v --remove-orphans
  docker compose --profile cluster up -d
  healthy
  sleep 5
  run tests/multinode.sh
  run tests/routing.sh
  run tests/history.sh
  run tests/freshness.sh
  run tests/incremental.sh
fi

if [ ${#failed[@]} -eq 0 ]; then echo "### all checks passed"; else echo "### failed: ${failed[*]}"; exit 1; fi
