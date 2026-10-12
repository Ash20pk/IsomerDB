#!/bin/bash
# Jepsen against the three-node compose cluster (coordinator db, workers worker1 and worker2), with
# workers paused, killed and cut off from the network while clients run:
#
#   append  Elle list-append over a distributed table: no G0/G1 anomalies, nothing lost
#   ryw     read-your-writes through a projection: never a stale answer, nothing lost, no drift
#
#   ./tests/jepsen.sh                      # both workloads, 120s each
#   ./tests/jepsen.sh ryw --time-limit 300 # one workload, more time (extra args go to Jepsen)
#
# Results (history, Elle's anomaly explanations, timelines, latency plots) are in jepsen/store/.
set -euo pipefail
cd "$(dirname "$0")/.."

workloads=(append ryw)
if [ $# -gt 0 ] && [[ "$1" != -* ]]; then workloads=("$1"); shift; fi

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"

echo "== cluster"
docker compose --profile cluster up -d --wait >/dev/null
for w in worker1 worker2; do
  if [ "$($ISO -c "SELECT count(*) FROM pg_dist_node WHERE nodename = '$w'")" = "0" ]; then
    $ISO -c "SELECT isomer.add_node('$w')" >/dev/null
  fi
done
echo "   $($ISO -c "SELECT string_agg(nodename, ', ' ORDER BY groupid) FROM pg_dist_node WHERE isactive")"

echo "== control node"
docker build -q -t isomerdb-jepsen:dev jepsen >/dev/null
mkdir -p jepsen/store

failed=()
for w in "${workloads[@]}"; do
  echo "== $w"
  if docker run --rm --network isomerdb_default \
       -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD/jepsen/store:/jepsen/store" \
       isomerdb-jepsen:dev -- test --no-ssh --nodes worker1,worker2 --concurrency 10 \
       --workload "$w" --time-limit 120 "$@" 2>&1 | tee "jepsen/store/$w-latest.log" | grep -E '^(Everything looks good|Analysis invalid|Errors occurred)|:valid\? |INFO jepsen.core - Run complete' ; then
    :
  fi
  # Jepsen prints its verdict last.
  if grep -q "Everything looks good" "jepsen/store/$w-latest.log"; then
    echo "   $w: valid"
  else
    echo "   $w: INVALID (see jepsen/store/$w-latest.log and jepsen/store/isomerdb-$w-*/latest/)"
    failed+=("$w")
  fi
done

# Leave the cluster whole.
for c in isomerdb-worker1-1 isomerdb-worker2-1; do
  docker unpause "$c" >/dev/null 2>&1 || true
  docker start "$c" >/dev/null 2>&1 || true
  n=${c#isomerdb-}; docker network connect --alias "${n%-1}" isomerdb_default "$c" >/dev/null 2>&1 || true
done

if [ ${#failed[@]} -eq 0 ]; then echo "== jepsen passed"; else echo "== jepsen failed: ${failed[*]}"; exit 1; fi
