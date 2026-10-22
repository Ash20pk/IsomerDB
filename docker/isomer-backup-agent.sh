#!/bin/bash
# Takes base backups of this pod's node, while it is its group's primary (Kubernetes operator,
# spec.backup). Runs beside Postgres as a sidecar sharing its data volume and socket. WAL is
# archived continuously by Postgres itself (archive_command); this adds a full base backup every
# ISOMER_BACKUP_FULL_EVERY seconds and keeps the newest ISOMER_BACKUP_RETAIN of them.
set -uo pipefail

PGDATA=${PGDATA:-/var/lib/postgresql/18/docker}
every=${ISOMER_BACKUP_FULL_EVERY:-86400}
retain=${ISOMER_BACKUP_RETAIN:-7}
log() { echo "isomer-backup-agent [$(hostname)]: $*"; }

is_primary() {
  [ "$(psql -X -tAq -h /var/run/postgresql -U postgres -d postgres -c 'SELECT NOT pg_is_in_recovery()' 2>/dev/null)" = "t" ]
}

# Seconds since the group's newest base backup ("" if there is none).
newest_age() {
  local newest
  newest=$(isomer-walg backup-list --json 2>/dev/null | tr '{' '\n' | sed -n 's/.*"time":"\([^"]*\)".*/\1/p' | sort | tail -1)
  [ -n "$newest" ] || return 0
  echo $(( $(date +%s) - $(date -d "$newest" +%s) ))
}

log "started: a full backup every ${every}s, keeping $retain"
while true; do
  if is_primary; then
    age=$(newest_age)
    if [ -z "$age" ] || [ "$age" -ge "$every" ]; then
      log "taking a base backup (newest is ${age:-none} seconds old)"
      if isomer-walg backup-push "$PGDATA"; then
        isomer-walg delete retain FULL "$retain" --confirm || log "pruning old backups failed"
        log "base backup done"
      else
        log "base backup failed; retrying in a minute"
      fi
    fi
  fi
  sleep 60
done
