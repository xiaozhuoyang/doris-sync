#!/bin/sh
set -eu

usage() {
  echo "Usage: sudo ./install.sh --config /path/partition-sync.json --env /path/partition-sync.env" >&2
  exit 2
}

config=
envfile=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --config) [ "$#" -ge 2 ] || usage; config=$2; shift 2 ;;
    --env) [ "$#" -ge 2 ] || usage; envfile=$2; shift 2 ;;
    *) usage ;;
  esac
done
[ -n "$config" ] && [ -f "$config" ] && [ -n "$envfile" ] && [ -f "$envfile" ] || usage
[ "$(id -u)" -eq 0 ] || { echo 'Run as root (sudo).' >&2; exit 1; }
command -v systemctl >/dev/null 2>&1 || { echo 'systemd is required.' >&2; exit 1; }
command -v useradd >/dev/null 2>&1 || { echo 'useradd is required.' >&2; exit 1; }
if grep -q 'replace_me' "$envfile" || grep -q 'example.com\|example-backup-bucket' "$config"; then
  echo 'Replace all example endpoints and credentials before installation.' >&2
  exit 1
fi

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
[ -x "$here/doris-partition-sync" ] && [ -f "$here/run.sh" ] || { echo 'Run install.sh from the extracted package.' >&2; exit 1; }

user=doris-partition-sync
home=/var/lib/doris-partition-sync
etcdir=/etc/doris-partition-sync
bindir=/opt/doris-partition-sync
if ! id "$user" >/dev/null 2>&1; then
  useradd --system --home-dir "$home" --shell "$(command -v nologin || printf /sbin/nologin)" "$user"
fi
group=$(id -gn "$user")
install -d -m 0700 -o "$user" -g "$group" "$home"
install -d -m 0750 -o root -g "$group" "$etcdir"
install -d -m 0755 -o root -g root "$bindir"
install -m 0755 -o root -g root "$here/doris-partition-sync" "$bindir/doris-partition-sync"
install -m 0755 -o root -g root "$here/run.sh" "$bindir/run.sh"
install -m 0640 -o root -g "$group" "$config" "$etcdir/partition-sync.json"
install -m 0640 -o root -g "$group" "$envfile" "$etcdir/partition-sync.env"

service=/etc/systemd/system/doris-partition-sync.service
printf '%s\n' \
  '[Unit]' \
  'Description=Doris / SelectDB partition synchronization' \
  'Wants=network-online.target' \
  'After=network-online.target' \
  '' \
  '[Service]' \
  'Type=simple' \
  "User=$user" \
  "Group=$group" \
  "WorkingDirectory=$home" \
  "EnvironmentFile=$etcdir/partition-sync.env" \
  "ExecStart=$bindir/run.sh" \
  'Restart=on-failure' \
  'RestartSec=10s' \
  'NoNewPrivileges=true' \
  'ProtectSystem=strict' \
  'ProtectHome=true' \
  "ReadWritePaths=$home" \
  '' \
  '[Install]' \
  'WantedBy=multi-user.target' > "$service"
chmod 0644 "$service"
systemctl daemon-reload
systemctl enable doris-partition-sync.service
systemctl restart doris-partition-sync.service
systemctl --no-pager --full status doris-partition-sync.service || true
printf 'Installed. State: %s/partition-sync-state.db; logs: journalctl -u doris-partition-sync -f\n' "$home"
