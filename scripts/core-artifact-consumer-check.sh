#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$tmp/artifact"
(
  cd "$root"
  git ls-files -co --exclude-standard -z -- \
    ':(top,glob)*.go' ':(top,glob)internal/**' \
    go.mod go.sum LICENSE NOTICE README.md \
    | tar --null -T - -cf "$tmp/lazily-go.tar"
)
tar -xf "$tmp/lazily-go.tar" -C "$tmp/artifact"
mkdir -p "$tmp/consumer"
cp "$root/tests/core-artifact-consumer/go.mod" "$tmp/consumer/go.mod"
cp "$root/tests/core-artifact-consumer/main.go" "$tmp/consumer/main.go"
sed -i "s|github.com/lazily-hub/lazily-go => ../..|github.com/lazily-hub/lazily-go => $tmp/artifact|" "$tmp/consumer/go.mod"

(cd "$tmp/consumer" && go run .)
if grep -F "$root" "$tmp/consumer/go.sum" 2>/dev/null; then
  echo "consumer metadata references the source checkout" >&2
  exit 1
fi
