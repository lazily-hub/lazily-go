#!/usr/bin/env bash
set -euo pipefail

# PostgreSQL projection-maintenance barrier suite (#lzprojectionbarrier).
#
# Two modules, on purpose (#lzgooptionalpgx). The integration tests need a real
# driver, and Go has no optional dependencies, so they live in their own module
# under integration/postgres and the root module's `require` block stays empty.
# `go test ./...` does NOT descend into a nested module, so running the root
# alone would silently stop exercising every test that needs a database — which
# is exactly the regression scripts/check-module-dependencies.sh refuses.
#
# The root leg still runs: two tests matching this name pattern are pure unit
# tests that never open a connection, and they stay in the root package.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run_suites() {
	go test -count=1 -run '^(TestPostgresProjectionBarrier|TestSimProjectionMaintenancePostgres)' "$repo_root"
	(cd "$repo_root/integration/postgres" && go test -count=1 ./...)
}

if [[ -n "${LAZILY_POSTGRES_URL:-}" ]]; then
	run_suites
	exit
fi

for command in initdb pg_ctl psql; do
	if ! command -v "$command" >/dev/null 2>&1; then
		echo "missing PostgreSQL test command: $command" >&2
		exit 1
	fi
done

test_root="$(mktemp -d)"
data_dir="$test_root/data"
log_file="$test_root/postgres.log"
port="$(python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)"

cleanup() {
	status=$?
	pg_ctl -D "$data_dir" -m fast stop >/dev/null 2>&1 || true
	if [[ $status -ne 0 && -s "$log_file" ]]; then
		cat "$log_file" >&2
	fi
	rm -rf "$test_root"
	exit "$status"
}
trap cleanup EXIT

initdb -D "$data_dir" -A trust -U postgres --no-locale >/dev/null
pg_ctl -D "$data_dir" -l "$log_file" -o "-F -h 127.0.0.1 -k $test_root -p $port" start >/dev/null
export LAZILY_POSTGRES_URL="postgresql://postgres@127.0.0.1:$port/postgres"

run_suites
