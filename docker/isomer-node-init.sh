#!/bin/bash
# Prepares this pod's data directory for its place in its node group (Kubernetes operator, spec.ha).
#
# A node group is a primary and its streaming standby: pod i of <cluster>-worker and pod i of
# <cluster>-worker-replica, or the two coordinator pods. The operator records which member is the
# group's primary in the topology ConfigMap (one file per group, mounted at $TOPOLOGY). Then:
#
#   * the primary keeps its data directory (a new one is initialised by the image's entrypoint);
#   * a standby with no data is cloned from the primary (pg_basebackup, with a replication slot);
#   * a former primary (after a failover) is rewound onto the new primary (pg_rewind), or cloned
#     afresh if it cannot be rewound;
#   * an existing standby is pointed at the current primary.
#
# Runs as an init container, as the postgres user, with the data volume and the node TLS files.
set -euo pipefail

: "${ISOMER_CLUSTER:?}" "${POD_NAMESPACE:?}"
PGDATA=${PGDATA:-/var/lib/postgresql/18/docker}
TOPOLOGY=${TOPOLOGY:-/etc/isomerdb/topology}
TLS=${TLS:-/etc/isomerdb/tls}

me=$(hostname)                               # e.g. demo-worker-replica-1
ordinal=${me##*-}
set_name=${me%-*}                            # demo-worker-replica
kind=${set_name#"$ISOMER_CLUSTER"-}          # worker-replica
kind=${kind%-replica}                        # worker
group="$kind-$ordinal"                       # worker-1, coordinator-0
primary=$(cat "$TOPOLOGY/$group" 2>/dev/null || echo "$ISOMER_CLUSTER-$kind-$ordinal")

log() { echo "isomer-node-init [$me, group $group]: $*"; }

if [ "$primary" = "$me" ]; then
  log "primary; keeping the data directory"
  exit 0
fi

primary_host="$primary.${primary%-*}.$POD_NAMESPACE.svc.cluster.local"
conn="host=$primary_host port=5432 user=postgres dbname=postgres sslmode=verify-full sslrootcert=$TLS/ca.crt sslcert=$TLS/client.crt sslkey=$TLS/client.key"
slot=$(echo "$me" | tr -- '-' '_')
log "standby of $primary"

# Wait for the primary to be up and out of recovery: during a switchover it is still being promoted.
is_primary() { [ "$(psql -X -tAq -d "$conn" -c 'SELECT NOT pg_is_in_recovery()' 2>/dev/null)" = "t" ]; }
for _ in $(seq 1 120); do
  is_primary && break
  sleep 5
done
is_primary || { log "primary $primary_host is not reachable, or not a primary yet"; exit 1; }
psql -X -q -v ON_ERROR_STOP=1 -d "$conn" -c \
  "SELECT pg_create_physical_replication_slot('$slot') WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '$slot')"

clone() {
  log "cloning from $primary_host"
  find "$PGDATA" -mindepth 1 -delete 2>/dev/null || true
  mkdir -p "$PGDATA"
  chmod 700 "$PGDATA"
  pg_basebackup -d "$conn" -D "$PGDATA" -X stream -S "$slot" -R -c fast
}

follow() {
  # primary_conninfo and primary_slot_name in postgresql.auto.conf, replacing any older ones.
  local auto="$PGDATA/postgresql.auto.conf"
  touch "$auto"
  sed -i -e '/^primary_conninfo/d' -e '/^primary_slot_name/d' "$auto"
  printf "primary_conninfo = '%s'\nprimary_slot_name = '%s'\n" "$conn application_name=$slot" "$slot" >> "$auto"
  touch "$PGDATA/standby.signal"
}

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  clone
elif [ -f "$PGDATA/standby.signal" ]; then
  log "already a standby"
else
  log "former primary: rewinding onto $primary_host"
  if ! pg_rewind --target-pgdata="$PGDATA" --source-server="$conn" --progress; then
    log "rewind failed; cloning instead"
    clone
  fi
fi
follow
log "ready to follow $primary_host"
