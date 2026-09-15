#!/usr/bin/env bash
# Attack the scenario parser's error channel with otherwise-current evidence
# (#lazilycheckconformance). The aggregate equality is not an independent oracle
# when both totals consume the same parser output.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
  echo "test-conformance-coverage-guard: $*" >&2
  exit 1
}

manifest="${LAZILY_CONFORMANCE_MANIFEST:-build/conformance-fixtures-loaded.txt}"
scenarios="${LAZILY_CONFORMANCE_SCENARIOS:-build/conformance-scenarios-replayed.txt}"
spec_dir="${LAZILY_SPEC_CONFORMANCE_DIR:-../lazily-spec/conformance}"

[[ -f "$manifest" && -f "$scenarios" ]] || fail \
  "current manifest and scenario ledger are required; run 'make test' first"
run_id="$(awk 'index($0, "# lazily-run-id ") == 1 { sub(/^# lazily-run-id /, ""); print; exit }' "$manifest")"
[[ -n "$run_id" ]] || fail "manifest carries no run-id stamp"
fixture="$(awk '!/^#/ && NF { print; exit }' "$manifest")"
[[ -n "$fixture" && -f "$spec_dir/$fixture" ]] || fail \
  "manifest carries no opened fixture that resolves in the corpus"

scratch_dir="$(mktemp -d "${TMPDIR:-/tmp}/lazily-go-conformance-guard.XXXXXX")"
trap 'rm -rf "$scratch_dir"' EXIT
real_jq="$(command -v jq)"
cat > "$scratch_dir/jq" <<'JQPROBE'
#!/usr/bin/env bash
last=""
for last in "$@"; do :; done
if [[ "$last" == "$LAZILY_TEST_MALFORMED_FIXTURE" ]]; then
  echo "jq: simulated parse error for $last" >&2
  exit 4
fi
exec "$LAZILY_TEST_REAL_JQ" "$@"
JQPROBE
chmod +x "$scratch_dir/jq"

if output="$(PATH="$scratch_dir:$PATH" \
    LAZILY_TEST_REAL_JQ="$real_jq" \
    LAZILY_TEST_MALFORMED_FIXTURE="$spec_dir/$fixture" \
    LAZILY_CONFORMANCE_RUN_ID="$run_id" \
    LAZILY_CONFORMANCE_MANIFEST="$manifest" \
    LAZILY_CONFORMANCE_SCENARIOS="$scenarios" \
    LAZILY_SPEC_CONFORMANCE_DIR="$spec_dir" \
    bash ./scripts/check-conformance-coverage.sh --probe-run 2>&1)"; then
  fail "a jq parse failure for '$fixture' was accepted: $output"
fi
case "$output" in
  *"cannot parse canonical fixture '$fixture'"*"jq exited 4"*) ;;
  *) fail "parse failure lacked the fixture-specific diagnostic: $output" ;;
esac
case "$output" in
  *"scenario coverage OK"*) fail "guard reported scenario success after a parse failure" ;;
esac

echo "test-conformance-coverage-guard: OK"
