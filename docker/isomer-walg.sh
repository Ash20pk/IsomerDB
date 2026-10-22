#!/bin/bash
# Runs WAL-G against this pod's node group's area of the backup store (Kubernetes operator,
# spec.backup / spec.restore).
#
#   isomer-walg wal-push %p                 archive_command
#   isomer-walg backup-push $PGDATA         a base backup
#   isomer-walg --source wal-fetch %f %p    restore_command while restoring from another cluster
#
# Each node group (coordinator-0, worker-3, ...) keeps its own WAL and base backups under
# <prefix>/<group>: both pods of a group (primary and standby) share it, across failovers.
# ISOMER_BACKUP_PREFIX (s3://bucket/path) is this cluster's store; ISOMER_RESTORE_PREFIX the source
# cluster's. Credentials and endpoint come from the AWS_* variables WAL-G reads.
set -euo pipefail

: "${ISOMER_CLUSTER:?}"
prefix=${ISOMER_BACKUP_PREFIX:-}
if [ "${1:-}" = "--source" ]; then
  shift
  prefix=${ISOMER_RESTORE_PREFIX:?}
  # The source store has its own credentials and endpoint (ISOMER_RESTORE_AWS_*).
  unset AWS_ENDPOINT AWS_S3_FORCE_PATH_STYLE
  for v in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION AWS_ENDPOINT AWS_S3_FORCE_PATH_STYLE; do
    src="ISOMER_RESTORE_$v"
    if [ -n "${!src:-}" ]; then export "$v=${!src}"; fi
  done
fi
: "${prefix:?no backup store configured}"

me=$(hostname)
ordinal=${me##*-}
kind=${me%-*}
kind=${kind#"$ISOMER_CLUSTER"-}
kind=${kind%-replica}
export WALG_S3_PREFIX="${prefix%/}/$kind-$ordinal"
export PGHOST=${PGHOST:-/var/run/postgresql} PGUSER=${PGUSER:-postgres} PGDATABASE=${PGDATABASE:-postgres}
exec wal-g "$@"
