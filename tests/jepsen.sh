#!/bin/bash
# Jepsen against the three-node compose cluster (coordinator db, workers worker1 and worker2), while
# clients run and a nemesis pauses, kills and cuts off nodes, or skews their clocks:
#
#   append       Elle list-append over a distributed table: no G0/G1 anomalies, nothing lost
#   ryw          read-your-writes through a projection over SQL: never stale, nothing lost, no drift
#   mongo        the same through the MongoDB gateway, with a causally consistent driver session
#   incremental  an incrementally maintained projection: equal to recomputation at the end, nothing
#                left queued on the workers, no delta pull left prepared
#
#   ./tests/jepsen.sh                         # every workload, faults on the workers, 120s each
#   ./tests/jepsen.sh ryw --time-limit 300    # one workload, more time (extra args go to Jepsen)
#   FAULT_NODES=db ./tests/jepsen.sh          # faults on the coordinator instead
#   SKEW=1 ./tests/jepsen.sh                  # clock skew on every node (libfaketime image)
#   ./tests/jepsen.sh report                  # the release matrix, written to docs/jepsen/<version>.md
#
# Results (history, Elle's anomaly explanations, timelines, latency plots) are in jepsen/store/.
set -euo pipefail
cd "$(dirname "$0")/.."

ISO="docker compose exec -T -e PGOPTIONS=-cclient_min_messages=warning db psql -U postgres -d postgres -X -q -t -A -v ON_ERROR_STOP=1"

cluster() {   # $1: 1 to run the nodes on the libfaketime image
  local files=(-f docker-compose.yml)
  if [ "$1" = "1" ]; then
    docker build -q -f jepsen/skew.Dockerfile -t isomerdb:skew . >/dev/null
    files+=(-f jepsen/compose.skew.yml)
  fi
  docker compose "${files[@]}" --profile cluster up -d --wait >/dev/null
  for w in worker1 worker2; do
    if [ "$($ISO -c "SELECT count(*) FROM pg_dist_node WHERE nodename = '$w'")" = "0" ]; then
      $ISO -c "SELECT isomer.add_node('$w')" >/dev/null
    fi
  done
  echo "   $($ISO -c "SELECT string_agg(nodename, ', ' ORDER BY groupid) FROM pg_dist_node WHERE isactive")"
}

# Leave the cluster whole.
heal() {
  for c in isomerdb-db-1 isomerdb-worker1-1 isomerdb-worker2-1; do
    docker unpause "$c" >/dev/null 2>&1 || true
    docker start "$c" >/dev/null 2>&1 || true
    n=${c#isomerdb-}; docker network connect --alias "${n%-1}" isomerdb_default "$c" >/dev/null 2>&1 || true
    docker exec "$c" sh -c 'echo +0 > /tmp/isomer-faketime' >/dev/null 2>&1 || true
  done
}

# run <workload> <fault nodes> <faults> <skew 0|1> [jepsen args...]; sets $verdict
run() {
  local w=$1 nodes=$2 faults=$3 skew=$4; shift 4
  local log="jepsen/store/$w-${nodes//,/+}-${faults//,/+}-latest.log"
  local args=(test --no-ssh --nodes worker1,worker2 --concurrency 10 --workload "$w" --time-limit 120
              --fault-nodes "$nodes" --faults "$faults")
  [ "$skew" = "1" ] && args+=(--skew)
  echo "== $w: $faults on $nodes"
  docker run --rm --network isomerdb_default \
    -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD/jepsen/store:/jepsen/store" \
    isomerdb-jepsen:dev -- "${args[@]}" "$@" > "$log" 2>&1 || true
  heal
  # Jepsen prints its verdict last.
  if grep -q "Everything looks good" "$log"; then verdict=valid; else verdict=INVALID; fi
  echo "   $w: $verdict ($log)"
  last_log=$log
}

echo "== control node"
docker build -q -t isomerdb-jepsen:dev jepsen >/dev/null
mkdir -p jepsen/store
failed=()

if [ "${1:-}" = "report" ]; then
  shift
  version=$($ISO -c "SELECT extversion FROM pg_extension WHERE extname = 'isomer'" 2>/dev/null || echo unknown)
  out="docs/jepsen/isomer-$version.md"
  mkdir -p docs/jepsen
  rows=()
  for scenario in "worker1,worker2|pause,kill,partition|0" "db|pause,kill,partition|0" "db,worker1,worker2|skew|1"; do
    IFS='|' read -r nodes faults skew <<< "$scenario"
    echo "== cluster ($([ "$skew" = 1 ] && echo libfaketime || echo plain) image)"
    cluster "$skew"
    for w in append ryw mongo incremental; do
      run "$w" "$nodes" "$faults" "$skew" "$@"
      [ "$verdict" = valid ] || failed+=("$w on $nodes ($faults)")
      rows+=("| $w | ${faults//,/, } | ${nodes//,/, } | $verdict | $(python3 tests/jepsen_summary.py "$last_log") |")
    done
  done
  cluster 0
  {
    echo "# Jepsen: isomer $version"
    echo
    echo "Run $(date -u '+%Y-%m-%d %H:%M UTC') by \`tests/jepsen.sh report\` against the three-node compose cluster"
    echo "(a coordinator and two workers), 120 seconds per run, 10 clients, a fault every 10 seconds"
    echo "healed 10 seconds later. What each workload checks is in \`jepsen/src/isomerdb/jepsen.clj\`."
    echo
    echo "| Workload | Faults | On | Verdict | Summary |"
    echo "|---|---|---|---|---|"
    printf '%s\n' "${rows[@]}"
    echo
    echo "Full histories, Elle's explanations and timelines are not kept in the repository; rerun to"
    echo "reproduce (\`jepsen/store/\`)."
  } > "$out"
  echo "== report: $out"
else
  workloads=(append ryw mongo incremental)
  if [ $# -gt 0 ] && [[ "$1" != -* ]]; then workloads=("$1"); shift; fi
  nodes=${FAULT_NODES:-worker1,worker2}
  skew=${SKEW:-0}
  faults=$([ "$skew" = 1 ] && echo pause,kill,partition,skew || echo pause,kill,partition)
  echo "== cluster"
  cluster "$skew"
  for w in "${workloads[@]}"; do
    run "$w" "$nodes" "$faults" "$skew" "$@"
    [ "$verdict" = valid ] || failed+=("$w")
  done
  [ "$skew" = 1 ] && cluster 0
fi

if [ ${#failed[@]} -eq 0 ]; then echo "== jepsen passed"; else printf '== jepsen failed: %s\n' "${failed[@]}"; exit 1; fi
