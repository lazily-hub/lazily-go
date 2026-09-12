#!/usr/bin/env bash
# Conformance-coverage guard (#portconformancecoverage).
#
# Fails the build when the canonical corpus in ../lazily-spec/conformance/ grows a
# fixture that no test in this repo even mentions. That is the drift this guard
# exists for: a fixture lands upstream, every binding stays green, and nobody
# learns that one of them is not replaying it.
#
# This binding uses the RUNTIME manifest (#lazilyupgradeconformance), not the
# static grep it started with. The test run records every file it actually reads
# from the conformance corpus, so a fixture named in a comment but hand-transcribed
# — the drift found in lazily-cpp's queue tests — is caught here. A source grep
# cannot see that case at all.
#
# A missing manifest is missing EVIDENCE and fails. It does not mean "no fixtures
# were read"; it means the suite ran without the recorder attached, and passing in
# that state is the vacuous green this guard exists to prevent.
#
# A manifest that exists is not automatically evidence about THIS run either: the
# file is written by a separate process, `go test` serves a cached package without
# running its binary, and a file nobody rewrote still parses perfectly. So every
# evidence file read here must carry this invocation's LAZILY_CONFORMANCE_RUN_ID
# stamp, and this guard refuses rather than skips when it cannot check that
# (#lzstalemanifest, and see the block above the check itself).
set -euo pipefail

SPEC_DIR="${LAZILY_SPEC_CONFORMANCE_DIR:-../lazily-spec/conformance}"

# A missing corpus is a legitimate local state (no sibling checkout) and an
# illegitimate CI state (#lzvacuousrun). Skipping under CI is the vacuous green
# this guard exists to prevent: every rung below reasons about fixtures the run
# OPENED, so an absent corpus reports OK over nothing at all — zero opened
# fixtures means zero uncovered fixtures, zero unreplayed scenarios, and zero
# stale excuses, so nothing further down can contradict it. Under CI this is
# missing EVIDENCE, exactly as a missing manifest is, and it fails the same way.
# Locally it stays a skip, because a contributor without the sibling checkout is
# not making a false claim.
if [ ! -d "$SPEC_DIR" ]; then
  if [ -n "${CI:-}" ]; then
    echo "ERROR: canonical corpus not found at $SPEC_DIR, and CI is set." >&2
    echo "       Under CI this is missing EVIDENCE, not evidence of absence: the" >&2
    echo "       checkout is wrong, not the corpus. Exiting 0 here would report" >&2
    echo "       conformance OK having examined zero fixtures (#lzvacuousrun)." >&2
    exit 1
  fi
  echo "SKIP: canonical corpus not found at $SPEC_DIR (clone the lazily-spec sibling)" >&2
  echo "      Local checkout only — this would be a hard failure under CI." >&2
  exit 0
fi

# Fixtures deliberately not covered by this binding yet. Each entry is a claim that
# someone looked; shrinking this list is the work. Adding to it silently is how the
# guard rots, so keep a reason with any new entry.
KNOWN_UNCOVERED=(
  # Register CRDTs (LWW / MV / PnCounter + the CellCrdt projection bit) are
  # implemented here, but this binding has no canonical replay for the new
  # registers corpus yet; the Registers coverage row is `~` until it does.
  "collections/registers_convergence.json"
  # Reactive egress is currently Rust-only; Go has no egress replay runner.
  "egress/egress_generation_fence.json"
  "egress/egress_inflight_window.json"
  "egress/egress_ordered_ack.json"
  "egress/egress_retry_budget.json"
  # Experimental protobuf-v1 generation is piloted in Rust/Kotlin/TypeScript;
  # Go must negotiate the capability before replaying this typed trace.
  "protobuf/graph_boundary_traces.json"
  "reliable-sync/coalesce_bounds_outbox.json"
  "reliable-sync/liveness_lease_eviction.json"
  # The canonical journal-decoder trace has no Go replay runner yet.
  "reliable-sync/outbox_journal_decode.json"
)

# Per-scenario replay accounting (#lzscenariocoverage) — rung 4.
#
# KNOWN_UNCOVERED above is about whole FILES. A fixture with four scenarios of
# which the suite replays three is "opened", so it counts as covered here and
# nothing notices the missing quarter. That is not a hypothetical: this binding
# opened reliable-sync/liveness_orset_lww.json and replayed 3 of its 4 scenarios,
# green, for as long as the fixture existed.
#
# The key guards cannot see it either. They only bind blocks a runner actually
# reaches, so a scenario nobody replays contributes no unconsumed key and no
# unasserted key. Skipping a whole scenario is invisible to a guard that only
# inspects the scenarios you ran.
#
# So the evidence is a RUNTIME ledger, exactly like the fixture manifest above:
# the suite records each scenario id at the point of replay (see
# conformance_scenario_ledger_test.go) and this script joins that ledger against
# the ids the fixtures on disk actually carry. A hand-authored "scenarios this
# runner covers" list would be a claim, and a claim rots.
#
# excuse_scenario is the escape hatch and it lives HERE, beside KNOWN_UNCOVERED,
# so there is one place to read what this binding does not prove. It carries the
# same both-directions rule: an excuse for a scenario the run DID replay, or for
# an id the fixture does not carry, fails as stale.
SCENARIO_EXCUSES=()
excuse_scenario() {
  local fixture="$1" id="$2" reason="$3"
  if [ -z "$reason" ]; then
    echo "ERROR: excuse_scenario('$fixture', '$id') has no reason — an excuse without a reason is a silent skip." >&2
    exit 1
  fi
  SCENARIO_EXCUSES+=("$fixture|$id|$reason")
}

# Nothing is excused. lazily-go replays every scenario of every fixture it opens.
# `derived_live_doc_aggregate_converges_under_retry` used to be the one gap; it
# is implemented rather than excused, which is the point of this rung.

SCENARIOS="${LAZILY_CONFORMANCE_SCENARIOS:-build/conformance-scenarios-replayed.txt}"
MANIFEST="${LAZILY_CONFORMANCE_MANIFEST:-build/conformance-fixtures-loaded.txt}"

# ---------------------------------------------------------------------------
# Evidence freshness (#lzstalemanifest)
# ---------------------------------------------------------------------------
#
# Every rung below reasons from two files this script did not write. Until now it
# had no freshness check of any kind — no run identifier, no mtime ordering
# against the test step — so "these bytes were really read" meant "some run really
# read them", and the run it described could be any run that ever wrote the file.
#
# `go test` makes that reachable. A package whose inputs have not changed is
# served from the test cache, the binary never executes, neither recorder flushes,
# and the previous file survives untouched. Measured here: a second `go test .`
# prints `ok ... (cached)` and appends ZERO lines, with both evidence env vars set
# and even when their VALUES differ from the cached run's — the cache does not key
# on them.
#
# `make test` truncating both files first is what has kept `make check` honest:
# a cached run leaves them empty and the reads below fail as missing evidence.
# That is a fail-closed accident of recipe ORDER inside one make invocation, not a
# property of this guard, and it covers none of the ways this script actually gets
# run — by hand after an earlier suite, as an individual CI step, or under
# `make -j` alongside the test step that is still appending.
#
# So the writers stamp `# lazily-run-id <value>` and this reader requires it to be
# the CURRENT invocation's. `-count=1` in the Makefile and in CI is not a
# substitute: it prevents stale evidence from being PRODUCED, which keeps the
# ordinary case from going falsely red, while this requires stale evidence to be
# REFUSED. One is a flag one edit away from being dropped with nothing downstream
# noticing; the other is the noticing.
RUN_ID_PREFIX='# lazily-run-id '
RUN_ID="${LAZILY_CONFORMANCE_RUN_ID:-}"

# Absent, the guard REFUSES. Skipping instead would accept unstamped evidence
# whenever the variable happens to be unset, which is the original hole with an
# extra step in front of it. There is deliberately no opt-out: `make check` and
# CI both supply an id (the Makefile generates one per invocation and exports it;
# the workflow derives one from the run and attempt), and a hand-run audit of an
# earlier suite's files is exactly the thing being refused.
if [ -z "$RUN_ID" ]; then
  echo "FAIL: LAZILY_CONFORMANCE_RUN_ID is unset, so this guard cannot tell whether" >&2
  echo "      $MANIFEST and $SCENARIOS describe THIS run or an" >&2
  echo "      earlier one (#lzstalemanifest). Run the suite and this guard from one" >&2
  echo "      \`make check\` — the Makefile generates one id per invocation and" >&2
  echo "      exports it to both — or set the variable and re-run the suite under it." >&2
  echo "      Accepting unstamped evidence here would be the stale-manifest hole with" >&2
  echo "      one more step in front of it." >&2
  exit 1
fi
case "$RUN_ID" in
*[[:space:]]*)
  echo "FAIL: LAZILY_CONFORMANCE_RUN_ID='$RUN_ID' contains whitespace. The stamp is one" >&2
  echo "      line with a fixed prefix, so whitespace either splits it into a data line" >&2
  echo "      this guard would try to resolve against the corpus, or makes the written" >&2
  echo "      and compared ids differ invisibly (#lzstalemanifest)." >&2
  exit 1
  ;;
esac

# The stamps an evidence file carries. One per contributing test binary: appending
# is how several binaries share one file, and none of them knows whether it is
# first, so the rule is that EVERY stamp must be the current id rather than the
# first one.
evidence_stamps() {
  awk -v n="${#RUN_ID_PREFIX}" 'index($0, "'"$RUN_ID_PREFIX"'") == 1 { print substr($0, n + 1) }' "$1"
}

# The data lines: everything that is not a comment. Written as a filter so the two
# readers below cannot drift into stripping different things.
evidence_data() {
  grep -v '^#' -- "$1" || true
}

require_fresh_evidence() {
  local file="$1" label="$2" stamps stale
  stamps="$(evidence_stamps "$file")"
  if [ -z "$stamps" ]; then
    echo "FAIL: $label at $file carries no '${RUN_ID_PREFIX}' line." >&2
    echo "      Unstamped evidence is evidence about an unknown run: an older file" >&2
    echo "      predating #lzstalemanifest has no stamp, and so does one written by a" >&2
    echo "      suite that was never told which run it was recording. Re-run the suite" >&2
    echo "      with LAZILY_CONFORMANCE_RUN_ID='$RUN_ID' set." >&2
    return 1
  fi
  stale="$(printf '%s\n' "$stamps" | grep -vxF "$RUN_ID" || true)"
  if [ -n "$stale" ]; then
    echo "FAIL: $label at $file was written by a DIFFERENT run." >&2
    echo "      found:  $(printf '%s' "$stale" | tr '\n' ' ')" >&2
    echo "      wanted: $RUN_ID" >&2
    echo "      The test step did not write this file during this invocation — a cached" >&2
    echo "      \`go test\` (which does not run the binary, so no recorder flushes), a" >&2
    echo "      guard run by hand after an earlier suite, or a build/ directory left" >&2
    echo "      over from a previous checkout. The numbers below would describe that" >&2
    echo "      other run (#lzstalemanifest)." >&2
    return 1
  fi
  if [ -z "$(evidence_data "$file")" ]; then
    echo "FAIL: $label at $file carries this run's stamp and NO data lines." >&2
    echo "      The recorder attached and recorded nothing. Reporting coverage from an" >&2
    echo "      empty evidence file is the vacuous green this guard exists to prevent." >&2
    return 1
  fi
  return 0
}
TEST_DIRS=(".")
EXTS=(".go")

collect_sources() {
  for d in "${TEST_DIRS[@]}"; do
    [ -d "$d" ] || continue
    for e in "${EXTS[@]}"; do
      find "$d" -type f -name "*$e" -print0
    done
  done
}

if [ ! -s "$MANIFEST" ]; then
  echo "FAIL: no conformance manifest at $MANIFEST." >&2
  echo "      Run the suite with LAZILY_CONFORMANCE_MANIFEST set so the recorder" >&2
  echo "      attaches. An absent manifest is missing evidence, not evidence of" >&2
  echo "      absence." >&2
  exit 1
fi
require_fresh_evidence "$MANIFEST" "the conformance manifest" || exit 1
OPENED="$(evidence_data "$MANIFEST" | sort -u)"

missing=0
total=0
covered=0
while IFS= read -r fixture; do
  total=$((total + 1))
  name="$(basename "$fixture")"
  # Here-string, NOT a pipe. With `set -o pipefail`, `printf ... | grep -q` reports
  # FAILURE when grep matches: grep -q exits immediately on the first hit, printf
  # takes SIGPIPE writing the rest, and pipefail surfaces printf's death as the
  # pipeline's status. The check then inverts — every covered fixture is reported
  # missing. That is exactly how it behaved before this line changed.
  if grep -qxF "$fixture" <<< "$OPENED"; then
    covered=$((covered + 1))
    continue
  fi
  excused=0
  for known in "${KNOWN_UNCOVERED[@]:-}"; do
    if [ "$known" = "$fixture" ]; then excused=1; break; fi
  done
  if [ "$excused" -eq 0 ]; then
    echo "ERROR: canonical fixture '$fixture' was NOT opened by the suite." >&2
    echo "       A runner may still name it in source while no longer reading it —" >&2
    echo "       that is the drift this manifest exists to catch. Replay it, or add" >&2
    echo "       it to KNOWN_UNCOVERED with a reason." >&2
    missing=$((missing + 1))
  fi
done < <(cd "$SPEC_DIR" && find . -name '*.json' | sed 's|^\./||' | sort)

# The evidence channel guards itself. Every recorded id must resolve against the
# corpus root; otherwise the manifest was truncated or interleaved in transit,
# and coverage computed from it cannot be trusted.
while IFS= read -r id; do
  [ -n "$id" ] || continue
  if [ ! -f "$SPEC_DIR/$id" ]; then
    echo "ERROR: manifest records '$id', which names no file in $SPEC_DIR." >&2
    echo "       The recorder is dropping or interleaving writes; coverage computed" >&2
    echo "       from this manifest cannot be trusted." >&2
    missing=$((missing + 1))
  fi
done <<< "$OPENED"

# A stale allowlist is its own drift, in two directions.
#
# 1. An entry naming a fixture that no longer exists means the corpus moved and
#    nobody updated the excuse.
# 2. An entry naming a fixture the suite DOES open is a stale excuse: the gap it
#    claims was closed, and the excuse outlived it. That rot understates coverage,
#    which is the direction nobody files a bug about — you do not report missing
#    coverage you have been told you lack — and it buries the real gaps in noise.
#
# The open test below uses the SAME `grep -qxF ... <<< "$OPENED"` comparison as the
# covered-check above, deliberately: if the two ever disagreed, a fixture could be
# both counted as covered and excused as uncovered in one run.
for known in "${KNOWN_UNCOVERED[@]:-}"; do
  if [ ! -f "$SPEC_DIR/$known" ]; then
    echo "ERROR: KNOWN_UNCOVERED lists '$known', which is not in the canonical corpus." >&2
    missing=$((missing + 1))
    continue
  fi
  if grep -qxF "$known" <<< "$OPENED"; then
    echo "ERROR: KNOWN_UNCOVERED lists '$known', but the suite DID open it." >&2
    echo "       The excuse is stale — the gap it claims no longer exists. Delete" >&2
    echo "       this entry from KNOWN_UNCOVERED. Leaving it there understates this" >&2
    echo "       binding's coverage and hides the fixtures that are really missing." >&2
    missing=$((missing + 1))
  fi
done

# ---------------------------------------------------------------------------
# Rung 4: per-scenario replay accounting (#lzscenariocoverage)
# ---------------------------------------------------------------------------

if ! command -v jq >/dev/null 2>&1; then
  echo "FAIL: jq is required to read the corpus's scenario ids." >&2
  echo "      Skipping this check on a missing tool would report the same green" >&2
  echo "      as a fully replayed corpus, which is the failure mode this file" >&2
  echo "      exists to prevent." >&2
  exit 1
fi

if [ ! -s "$SCENARIOS" ]; then
  echo "FAIL: no scenario ledger at $SCENARIOS." >&2
  echo "      Run the suite with LAZILY_CONFORMANCE_SCENARIOS set so the recorder" >&2
  echo "      attaches. An absent ledger is missing evidence, not evidence that" >&2
  echo "      every scenario ran." >&2
  exit 1
fi
require_fresh_evidence "$SCENARIOS" "the scenario ledger" || exit 1
REPLAYED="$(evidence_data "$SCENARIOS" | sort -u)"

# scenario_ids prints a fixture's scenario ids in the ONE resolution order every
# binding uses: `id`, else `name`. There is no third step (#lzspecscenarioids) --
# a positional `#<n>` id silently rebinds to a different scenario when the corpus
# array is reordered, so an unidentified scenario is marked and reported rather
# than given an invented id. The runtime ledger resolves identically (scenarioKey
# in conformance_scenario_ledger_test.go); if the two ever drifted apart, every
# scenario of the affected fixture would read as unreplayed at once.
scenario_ids() {
  jq -r '
    if (.scenarios | type) == "array" then
      .scenarios | to_entries[] |
        if ((.value.id? // "") | tostring | gsub("\\s"; "")) != "" then (.value.id | tostring)
        elif ((.value.name? // "") | tostring | gsub("\\s"; "")) != "" then (.value.name | tostring)
        else "!UNIDENTIFIED!\(.key)" end
    else empty end' "$SPEC_DIR/$1"
}

SCENARIO_TOTAL=0
SCENARIO_REPLAYED=0

while IFS= read -r fixture; do
  # Only fixtures the manifest says were OPENED. A fixture nobody opened is
  # already reported (or excused) by the file-level check above; re-reporting
  # each of its scenarios would bury that one finding under n copies.
  grep -qxF "$fixture" <<< "$OPENED" || continue
  while IFS= read -r id; do
    [ -n "$id" ] || continue
    SCENARIO_TOTAL=$((SCENARIO_TOTAL + 1))
    # An unidentified scenario is a corpus defect, not an id to invent
    # (#lzspecscenarioids). Booking it by POSITION would silently rebind that
    # ledger entry to a different scenario on any corpus reorder.
    case "$id" in
      '!UNIDENTIFIED!'*)
        echo "ERROR: '$fixture' scenario at index ${id#!UNIDENTIFIED!} carries neither" >&2
        echo "       \`id\` nor \`name\`. The ledger would record it by POSITION, which" >&2
        echo "       silently rebinds on a corpus reorder. Give it a stable id upstream" >&2
        echo "       in lazily-spec (#lzspecscenarioids)." >&2
        missing=$((missing + 1))
        continue
        ;;
    esac
    key="$(printf '%s\t%s' "$fixture" "$id")"
    if grep -qxF "$key" <<< "$REPLAYED"; then
      SCENARIO_REPLAYED=$((SCENARIO_REPLAYED + 1))
      continue
    fi
    excused=0
    for entry in "${SCENARIO_EXCUSES[@]:-}"; do
      [ -n "$entry" ] || continue
      rest="${entry#*|}"
      if [ "${entry%%|*}" = "$fixture" ] && [ "${rest%%|*}" = "$id" ]; then
        excused=1
        break
      fi
    done
    if [ "$excused" -eq 0 ]; then
      echo "ERROR: '$fixture' scenario '$id' was OPENED but never REPLAYED." >&2
      echo "       The file-level manifest counts this fixture as covered — one" >&2
      echo "       scenario is enough to open it — and the key guards never see" >&2
      echo "       a block this run did not reach. Implement the scenario, or" >&2
      echo "       excuse_scenario '$fixture' '$id' '<why this binding cannot express it>'." >&2
      missing=$((missing + 1))
    fi
  done < <(scenario_ids "$fixture")
done < <(cd "$SPEC_DIR" && find . -name '*.json' | sed 's|^\./||' | sort)

# The ledger guards itself, same as the manifest does. Every entry must name a
# corpus fixture, one the manifest agrees was opened, and an id that fixture
# really carries — otherwise the recorder is mislabelling replays and the
# coverage computed from it cannot be trusted.
while IFS=$'\t' read -r fixture id; do
  [ -n "$fixture" ] || continue
  if [ ! -f "$SPEC_DIR/$fixture" ]; then
    echo "ERROR: scenario ledger records '$fixture', which names no file in $SPEC_DIR." >&2
    missing=$((missing + 1))
    continue
  fi
  if ! grep -qxF "$fixture" <<< "$OPENED"; then
    echo "ERROR: scenario ledger records a replay of '$fixture [$id]', but the" >&2
    echo "       fixture manifest never saw that file opened. The two recorders" >&2
    echo "       disagree; one of them is mislabelling." >&2
    missing=$((missing + 1))
    continue
  fi
  if ! grep -qxF "$id" <<< "$(scenario_ids "$fixture")"; then
    echo "ERROR: scenario ledger records '$fixture [$id]', which is not a scenario" >&2
    echo "       that fixture carries. The runner is recording an id it invented." >&2
    missing=$((missing + 1))
  fi
done <<< "$REPLAYED"

# A stale scenario excuse, in the same two directions as KNOWN_UNCOVERED.
for entry in "${SCENARIO_EXCUSES[@]:-}"; do
  [ -n "$entry" ] || continue
  fixture="${entry%%|*}"
  rest="${entry#*|}"
  id="${rest%%|*}"
  if [ ! -f "$SPEC_DIR/$fixture" ]; then
    echo "ERROR: excuse_scenario names '$fixture', which is not in the canonical corpus." >&2
    missing=$((missing + 1))
    continue
  fi
  if ! grep -qxF "$id" <<< "$(scenario_ids "$fixture")"; then
    echo "ERROR: excuse_scenario '$fixture' '$id' names a scenario that fixture does" >&2
    echo "       not carry — the excuse is stale. The corpus renamed or dropped it;" >&2
    echo "       delete the excuse or point it at the id that replaced it." >&2
    missing=$((missing + 1))
    continue
  fi
  key="$(printf '%s\t%s' "$fixture" "$id")"
  if grep -qxF "$key" <<< "$REPLAYED"; then
    echo "ERROR: excuse_scenario '$fixture' '$id', but the suite DID replay it." >&2
    echo "       The excuse is stale — the gap it claims no longer exists. Delete" >&2
    echo "       it. Leaving it there understates this binding's coverage and hides" >&2
    echo "       the scenarios that are really missing." >&2
    missing=$((missing + 1))
  fi
done

if [ "$missing" -gt 0 ]; then
  echo "conformance coverage FAILED: $missing problem(s)" >&2
  exit 1
fi

# ---- Positive-evidence floor (#lzvacuousrun) ----
# Everything above reasons about fixtures this run OPENED, so all of it is
# vacuously satisfied by an empty population: zero fixtures means zero uncovered
# fixtures and zero stale excuses, and `missing` stays 0. The loops cannot
# distinguish "nothing is wrong" from "nothing was examined", so assert the
# magnitude explicitly before reporting OK. Do not lower these to fix a red run —
# a drop here means the corpus or the recorder shrank, which IS the finding.
#
# DERIVED, and an EQUALITY (#lzgotypedfloors). Both magnitudes used to be numbers
# typed into this file and compared with `-lt`, re-pinned by hand from a completed
# CI run, and both halves were wrong in the way the assertion-block magnitude in
# conformance_unbound_block_test.go already was (#lzblocksitepin):
#
#   * a TYPED number is re-pinned by hand, so it drifts by hand. The comment that
#     used to sit here told the next reader to re-read the coverage lines from a
#     COMPLETED CI run and set the number to that total — a ritual nobody performs
#     on a green run, which is how lazily-dart's hand-typed block floor sat 3
#     below reality for an afternoon;
#   * a FLOOR cannot see a shrink that stays above it, and a shrink is exactly
#     what a detached recorder looks like.
#
# They were also not what catches a SHRINKING CORPUS, which is what a floor looks
# like it is for. `lazily-spec/corpus-counts.json` pins the fixture total and
# `lazily-spec/scripts/check-corpus-floors.mjs` asserts it EQUAL for all ten
# bindings, so a corpus that loses a fixture is refused centrally, at one edit
# site — verified rather than assumed, by pointing that audit at a scratch copy
# of the corpus with one fixture removed: "the corpus carries 155 fixtures;
# corpus-counts.json pins 156", exit 1. Ten hand-typed floors were the weaker
# half of that pair, not the load-bearing one.
#
# The expectation below is the corpus listing minus this binding's own
# KNOWN_UNCOVERED. The two directions above already enforce that composition
# fixture by fixture — every corpus fixture is opened or excused, and every excuse
# names a fixture in the corpus that the run did NOT open — so
# `corpus \ KNOWN_UNCOVERED` IS the opened set, and its CARDINALITY is checkable
# without a second source of truth. What the equality adds over those two loops is
# the ARITHMETIC: a duplicated excuse entry passes both directions while making
# the count disagree, which is why the duplicate check comes first.
#
# Do not re-spell either retired constant in its assignment form anywhere under
# scripts/. check-corpus-floors.mjs greps this directory for that literal shape,
# so a commented example keeps the audit comparing a number that no longer runs —
# the trap lazily-py fell into, and one lazily-rs is in right now: its
# `#lzrstypedfloors` comments quote both retired spellings, and the central audit
# still reads them as rs's declared floors of 150 and 166.
uniq_known=0
if [ "${#KNOWN_UNCOVERED[@]}" -gt 0 ]; then
  dupes="$(printf '%s\n' "${KNOWN_UNCOVERED[@]}" | grep -v '^$' | sort | uniq -d || true)"
  if [ -n "$dupes" ]; then
    echo "ERROR: KNOWN_UNCOVERED lists the same fixture more than once:" >&2
    printf '         %s\n' $dupes >&2
    echo "       Both directions above still pass on a duplicate — the fixture is in" >&2
    echo "       the corpus and the suite does not open it, once per copy — while the" >&2
    echo "       derived opened count below silently drops by one per repeat. Delete" >&2
    echo "       the duplicates." >&2
    exit 1
  fi
  uniq_known="$(printf '%s\n' "${KNOWN_UNCOVERED[@]}" | grep -v '^$' | sort -u | wc -l)"
fi

if [ "$total" -eq 0 ]; then
  echo "ERROR: the corpus at $SPEC_DIR listed ZERO fixtures." >&2
  echo "       Every check above is vacuously green over an empty population." >&2
  exit 1
fi
expected_opened=$((total - uniq_known))
if [ "$expected_opened" -le 0 ]; then
  echo "ERROR: the corpus at $SPEC_DIR minus KNOWN_UNCOVERED derives $expected_opened" >&2
  echo "       fixtures to open. An expectation of zero is a green badge over an" >&2
  echo "       empty comparison (#lzvacuousrun)." >&2
  exit 1
fi
if [ "$covered" -ne "$expected_opened" ]; then
  if [ "$covered" -lt "$expected_opened" ]; then
    echo "ERROR: only $covered distinct canonical fixtures were OPENED; the corpus at" >&2
    echo "       $SPEC_DIR minus KNOWN_UNCOVERED derives $expected_opened." >&2
    echo "       A replay was removed, renamed, or short-circuited, or the recorder" >&2
    echo "       detached mid-run. There is no number to lower here — the expectation" >&2
    echo "       is computed from the corpus, not typed." >&2
  else
    echo "ERROR: $covered distinct fixtures were OPENED but the corpus minus" >&2
    echo "       KNOWN_UNCOVERED derives only $expected_opened, so the manifest and the" >&2
    echo "       corpus this guard walked are not the same tree: a leftover manifest, a" >&2
    echo "       corpus that shrank underneath it, or LAZILY_SPEC_CONFORMANCE_DIR" >&2
    echo "       pointing the two halves at different trees." >&2
  fi
  exit 1
fi

echo "conformance coverage OK: $covered/$total canonical fixtures OPENED by the suite" \
     "($uniq_known listed as known-uncovered; $expected_opened DERIVED from the corpus" \
     "listing minus that ledger and asserted EQUAL; runtime manifest stamped $RUN_ID —" \
     "these bytes were really read, by THIS run)"

# The same treatment for rung 4. Its loop walks the scenarios of OPENED fixtures,
# so zero opened fixtures means zero scenarios, which means zero unreplayed
# scenarios — OK reported having compared nothing.
#
# `SCENARIO_TOTAL` is NOT the independent witness. It walks only fixtures the
# MANIFEST says were opened, so a detached recorder shrinks the manifest,
# SCENARIO_TOTAL and SCENARIO_REPLAYED together, and an expectation derived from
# it follows them into the ditch. The walk below starts from the CORPUS LISTING
# minus KNOWN_UNCOVERED instead — the same composition the fixture rung just
# asserted the cardinality of — and resolves ids through the SAME `scenario_ids`
# the rung above uses, so there is one id-resolution rule with two callers rather
# than two rules that can disagree about what a scenario is.
#
# Unidentified ids are counted here exactly as the rung above counts them, so the
# two halves stay comparable. They cannot survive to this point anyway: the rung
# above books each one into `missing`, and `missing` exits before the floor.
DERIVED_OPENED=""
derived_scenario_total=0
while IFS= read -r fixture; do
  is_known=0
  for known in "${KNOWN_UNCOVERED[@]:-}"; do
    [ -n "$known" ] || continue
    if [ "$known" = "$fixture" ]; then is_known=1; break; fi
  done
  [ "$is_known" -eq 0 ] || continue
  DERIVED_OPENED+="$fixture"$'\n'
  while IFS= read -r id; do
    [ -n "$id" ] || continue
    derived_scenario_total=$((derived_scenario_total + 1))
  done < <(scenario_ids "$fixture")
done < <(cd "$SPEC_DIR" && find . -name '*.json' | sed 's|^\./||' | sort)

# An excuse only subtracts when its fixture is one the corpus composition says is
# OPENED. An excuse naming an uncovered fixture contributes no scenario to
# `derived_scenario_total`, so counting it would take the expectation one below
# reality. Duplicates are refused first, for the same arithmetic reason
# KNOWN_UNCOVERED duplicates are.
excuse_keys=""
for entry in "${SCENARIO_EXCUSES[@]:-}"; do
  [ -n "$entry" ] || continue
  fixture="${entry%%|*}"
  rest="${entry#*|}"
  excuse_keys+="$fixture|${rest%%|*}"$'\n'
done
if [ -n "$excuse_keys" ]; then
  dupes="$(printf '%s' "$excuse_keys" | grep -v '^$' | sort | uniq -d || true)"
  if [ -n "$dupes" ]; then
    echo "ERROR: excuse_scenario names the same fixture and scenario more than once:" >&2
    printf '         %s\n' $dupes >&2
    echo "       Both directions above still pass on a duplicate, while the derived" >&2
    echo "       replay count below silently drops by one per repeat. Delete the" >&2
    echo "       duplicates." >&2
    exit 1
  fi
fi
derived_excused=0
while IFS= read -r key; do
  [ -n "$key" ] || continue
  if grep -qxF "${key%%|*}" <<< "$DERIVED_OPENED"; then
    derived_excused=$((derived_excused + 1))
  fi
done <<< "$(printf '%s' "$excuse_keys" | grep -v '^$' | sort -u || true)"

if [ "$SCENARIO_TOTAL" -eq 0 ] || [ "$derived_scenario_total" -eq 0 ]; then
  echo "ERROR: ZERO scenarios were found across the opened fixtures." >&2
  echo "       The rung above is vacuously green over an empty population." >&2
  exit 1
fi
expected_replayed=$((derived_scenario_total - derived_excused))
if [ "$expected_replayed" -le 0 ]; then
  echo "ERROR: the corpus at $SPEC_DIR minus KNOWN_UNCOVERED carries" >&2
  echo "       $derived_scenario_total scenario(s) and excuse_scenario excuses" >&2
  echo "       $derived_excused of them, deriving an expectation of $expected_replayed." >&2
  echo "       An expectation of zero is a green badge over an empty comparison" >&2
  echo "       (#lzvacuousrun)." >&2
  exit 1
fi
if [ "$SCENARIO_REPLAYED" -ne "$expected_replayed" ]; then
  if [ "$SCENARIO_REPLAYED" -lt "$expected_replayed" ]; then
    echo "ERROR: only $SCENARIO_REPLAYED distinct scenarios were REPLAYED; the corpus at" >&2
    echo "       $SPEC_DIR minus KNOWN_UNCOVERED carries $derived_scenario_total, less" >&2
    echo "       $derived_excused excused, deriving $expected_replayed." >&2
    echo "       A scenario dispatch stopped matching, or the ledger detached. There is" >&2
    echo "       no number to lower here — the expectation is computed from the corpus," >&2
    echo "       not typed." >&2
  else
    echo "ERROR: $SCENARIO_REPLAYED distinct scenarios were REPLAYED but the corpus at" >&2
    echo "       $SPEC_DIR minus KNOWN_UNCOVERED derives only $expected_replayed" >&2
    echo "       ($derived_scenario_total carried, less $derived_excused excused)." >&2
    echo "       The ledger and the corpus this guard walked are not the same tree: a" >&2
    echo "       leftover ledger, a corpus that shrank underneath it, or" >&2
    echo "       LAZILY_SPEC_CONFORMANCE_DIR pointing the two halves apart." >&2
  fi
  exit 1
fi

echo "scenario coverage OK: $SCENARIO_REPLAYED/$SCENARIO_TOTAL scenarios of those fixtures REPLAYED" \
     "($derived_excused excused; $expected_replayed DERIVED from the corpus listing minus" \
     "KNOWN_UNCOVERED and asserted EQUAL; runtime ledger stamped $RUN_ID — recorded at the" \
     "point of replay, during THIS run)"
