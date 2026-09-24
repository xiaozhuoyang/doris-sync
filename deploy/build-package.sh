#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
version=${1:-$(date -u +%Y%m%dT%H%M%SZ)}
case "$version" in
  *[!A-Za-z0-9._-]*|'') echo "Invalid version: $version" >&2; exit 2 ;;
esac

mkdir -p "$root/dist"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT HUP INT TERM
name="doris-partition-sync-${version}-linux-amd64"
mkdir -p "$stage/$name"

(
  cd "$root"
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' \
    -o "$stage/$name/doris-partition-sync" .
)
cp "$root/deploy/install.sh" "$root/deploy/run.sh" "$root/partition-sync.example.json" "$root/deploy/partition-sync.env.example" "$root/README.md" "$stage/$name/"
chmod 755 "$stage/$name/install.sh" "$stage/$name/run.sh" "$stage/$name/doris-partition-sync"
(
  cd "$stage/$name"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum doris-partition-sync > SHA256SUMS
  else
    shasum -a 256 doris-partition-sync > SHA256SUMS
  fi
)
tar -C "$stage" -czf "$root/dist/$name.tar.gz" "$name"
cp "$stage/$name/doris-partition-sync" "$root/dist/doris-partition-sync-linux-amd64"
printf 'Package: %s\nBinary: %s\n' "$root/dist/$name.tar.gz" "$root/dist/doris-partition-sync-linux-amd64"
