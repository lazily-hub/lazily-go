#!/usr/bin/env bash
set -euo pipefail

# Published-module dependency floor (#lzgooptionalpgx).
#
# WHAT THIS PROVES. Two things, and it needs both to prove either:
#
#   A. The root module's `require` set is EMPTY, so a consumer of
#      github.com/lazily-hub/lazily-go resolves no third-party module. Go has no
#      optional dependencies: anything the root module's packages OR TESTS import
#      lands in this block and travels to every consumer. That is how
#      jackc/pgx/v5 plus six indirects came to be required by a reactive-signals
#      library for the sake of one integration suite.
#
#   B. That suite still EXISTS, still BUILDS, and still declares the tests it is
#      supposed to. This half is the control that keeps half A honest, and it is
#      the whole reason this script is not three lines long: DELETING the
#      integration tests satisfies half A perfectly. An emptiness check alone
#      cannot tell a dependency that was correctly relocated from one that was
#      dropped along with its coverage, so measuring only A would report green on
#      the one regression that matters most.
#
# Both sets are compared for EQUALITY, in both directions. A subset check in
# either half is satisfiable by accident: pinning tests the module no longer has
# fails one direction, and a module that grew a test nobody pinned fails the
# other, which is the prompt to update this pin deliberately rather than let
# coverage drift in unannounced.
#
# WHAT IT DOES NOT PROVE. That the tests PASS, or that they touched a database.
# `go test -list` compiles the package and asks it to name its tests; it runs
# none of them. Passing is `make test-postgres-projection-barrier`'s job, which
# boots a real PostgreSQL and runs both modules. This guard is the static half:
# it fails when the dependency creeps back or the suite evaporates, neither of
# which a passing run would notice, because a deleted test cannot fail.
#
# MEASUREMENT DISCIPLINE. Half A reads `go mod edit -json`, the structured form,
# not the text of go.mod. A grep for `^require` is defeated by a single-line
# `require github.com/x v1` outside a block, by a `// require` comment, and by
# any reformatting `go mod tidy` chooses; the JSON says what the go command
# itself resolved. Every measurement below refuses (exit 2) when it cannot be
# taken, rather than treating "nothing measured" as "nothing wrong".

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
nested_rel="integration/postgres"
nested_dir="$repo_root/$nested_rel"

# The tests that must live in the NESTED module: every one that opens a database.
EXPECTED_NESTED_TESTS=(
	"TestPostgresProjectionBarrierBoundsWaitAndHonorsCancellation"
	"TestPostgresProjectionBarrierRetriesCompleteSerializableTransaction"
	"TestPostgresProjectionBarrierTwoConnectionMultiHostNoAcceptedWriteLost"
	"TestSimProjectionMaintenancePostgresRaceCorpus"
)

# The tests matching the same name pattern that must STAY in the root module:
# pure unit tests that never open a connection. Pinned so that "move the
# database tests out" cannot quietly become "move everything out", which would
# take root coverage with it and still leave both halves above green.
EXPECTED_ROOT_TESTS=(
	"TestPostgresProjectionBarrierAdvertisesDurableCrossProcessScope"
	"TestPostgresProjectionBarrierRejectsUnboundedOrMissingWork"
)

TEST_NAME_PATTERN='^(TestPostgresProjectionBarrier|TestSimProjectionMaintenancePostgres)'

# A pin that names nothing pins nothing, and an empty array is what a bad merge
# leaves behind (#lzvacuousrun). Checked BEFORE anything is measured, so the
# pins cannot be emptied to make a comparison trivially succeed.
if [ "${#EXPECTED_NESTED_TESTS[@]}" -eq 0 ]; then
	echo "check-module-dependencies: EXPECTED_NESTED_TESTS is empty — a pin that names no test pins nothing" >&2
	exit 2
fi
if [ "${#EXPECTED_ROOT_TESTS[@]}" -eq 0 ]; then
	echo "check-module-dependencies: EXPECTED_ROOT_TESTS is empty — a pin that names no test pins nothing" >&2
	exit 2
fi

status=0

# ---------------------------------------------------------------- half A: empty
if [ ! -f "$repo_root/go.mod" ]; then
	echo "check-module-dependencies: $repo_root/go.mod is missing — cannot measure the published requirement set" >&2
	exit 2
fi

root_mod_json="$(cd "$repo_root" && go mod edit -json)" || {
	echo "check-module-dependencies: \`go mod edit -json\` failed in the root module — requirement set unmeasurable" >&2
	exit 2
}
if [ -z "$root_mod_json" ]; then
	echo "check-module-dependencies: \`go mod edit -json\` produced no output — refusing to read an empty measurement as an empty require block" >&2
	exit 2
fi

root_requires="$(printf '%s' "$root_mod_json" | python3 -c '
import json, sys
try:
    doc = json.load(sys.stdin)
except Exception as exc:
    print("UNPARSEABLE: %s" % exc)
    raise SystemExit(3)
if not isinstance(doc, dict) or "Module" not in doc:
    print("UNPARSEABLE: go mod edit -json did not describe a module")
    raise SystemExit(3)
for req in doc.get("Require") or []:
    print("%s %s%s" % (req.get("Path"), req.get("Version"), " // indirect" if req.get("Indirect") else ""))
')" || {
	echo "check-module-dependencies: could not parse \`go mod edit -json\` — requirement set unmeasurable" >&2
	printf '%s\n' "$root_requires" >&2
	exit 2
}

if [ -n "$root_requires" ]; then
	echo "check-module-dependencies: the root module requires $(printf '%s\n' "$root_requires" | grep -c .) module(s); the published graph must require NOTHING (#lzgooptionalpgx)" >&2
	printf '  %s\n' $(printf '%s\n' "$root_requires" | cut -d' ' -f1) >&2
	echo "  a test-only dependency belongs in $nested_rel, whose own go.mod no consumer reads" >&2
	status=1
else
	echo "check-module-dependencies: root require block empty"
fi

# ------------------------------------------- half B: the suite still exists
if [ ! -f "$nested_dir/go.mod" ]; then
	echo "check-module-dependencies: $nested_rel/go.mod is missing — the relocated suite is gone, so half A above is vacuous" >&2
	exit 2
fi

list_tests() { # $1 = dir, $2 = package pattern
	(cd "$1" && go test -list "$TEST_NAME_PATTERN" "$2" 2>&1) || return 1
}

nested_raw="$(list_tests "$nested_dir" "./...")" || {
	echo "check-module-dependencies: the relocated suite in $nested_rel does not build — cannot confirm it still exists" >&2
	printf '%s\n' "$nested_raw" >&2
	exit 2
}
root_raw="$(list_tests "$repo_root" ".")" || {
	echo "check-module-dependencies: the root module's test binary does not build — cannot confirm which tests stayed" >&2
	printf '%s\n' "$root_raw" >&2
	exit 2
}

# `go test -list` prints one test name per line plus a trailing `ok <pkg>` /
# `no test files` summary. Keep only lines that look like a Go test identifier.
only_tests() { grep -E '^Test[A-Za-z0-9_]*$' || true; }

nested_seen="$(printf '%s\n' "$nested_raw" | only_tests | sort -u)"
root_seen="$(printf '%s\n' "$root_raw" | only_tests | sort -u)"

if [ -z "$nested_seen" ]; then
	echo "check-module-dependencies: $nested_rel named ZERO matching tests — refusing to read an empty listing as a relocated suite" >&2
	exit 2
fi
if [ -z "$root_seen" ]; then
	echo "check-module-dependencies: the root module named ZERO matching tests — refusing an empty listing as proof the unit tests stayed" >&2
	exit 2
fi

compare_sets() { # $1 = label, $2 = pinned (newline list), $3 = observed
	local label="$1" want="$2" got="$3" missing extra
	missing="$(comm -23 <(printf '%s\n' "$want") <(printf '%s\n' "$got"))"
	extra="$(comm -13 <(printf '%s\n' "$want") <(printf '%s\n' "$got"))"
	if [ -n "$missing" ]; then
		echo "check-module-dependencies: $label is missing pinned test(s):" >&2
		printf '  %s\n' $missing >&2
		return 1
	fi
	if [ -n "$extra" ]; then
		echo "check-module-dependencies: $label declares test(s) this pin does not name:" >&2
		printf '  %s\n' $extra >&2
		echo "  add them here deliberately — an unpinned test is coverage this guard cannot vouch for" >&2
		return 1
	fi
	echo "check-module-dependencies: $label matches its pin ($(printf '%s\n' "$got" | grep -c .) test(s))"
	return 0
}

want_nested="$(printf '%s\n' "${EXPECTED_NESTED_TESTS[@]}" | sort -u)"
want_root="$(printf '%s\n' "${EXPECTED_ROOT_TESTS[@]}" | sort -u)"

compare_sets "$nested_rel" "$want_nested" "$nested_seen" || status=1
compare_sets "the root module" "$want_root" "$root_seen" || status=1

if [ "$status" -eq 0 ]; then
	echo "check-module-dependencies: OK — published graph requires nothing, relocated suite intact"
fi
exit "$status"
