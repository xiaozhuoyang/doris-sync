#!/bin/sh
set -eu

mode=${SYNC_MODE:-overwrite}
case "$mode" in
  overwrite)
    exec /opt/doris-partition-sync/doris-partition-sync \
      --config /etc/doris-partition-sync/partition-sync.json --sync-mode overwrite
    ;;
  time-window)
    [ -n "${TIME_FIELD:-}" ] || { echo 'TIME_FIELD is required for time-window mode.' >&2; exit 2; }
    exec /opt/doris-partition-sync/doris-partition-sync \
      --config /etc/doris-partition-sync/partition-sync.json --sync-mode time-window \
      --time-field "$TIME_FIELD"
    ;;
  *) echo "Invalid SYNC_MODE: $mode" >&2; exit 2 ;;
esac
