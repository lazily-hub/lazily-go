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
  "arena_blob.json"
  "reactive-graph/exact_fold_paths_stay_exact.json"
  "reactive-graph/feedback_drain_bound_reports_exhaustion.json"
  "reactive-graph/merge_cell_acquires_no_dependency_edge.json"
  "reactive-graph/merge_feed_through_a_formula_coalesces.json"
  "reactive-graph/merge_folds_synchronously_in_batch.json"
  "reactive-graph/merge_per_settled_cone_not_per_write.json"
  "reliable-sync/coalesce_bounds_outbox.json"
  "reliable-sync/liveness_lease_eviction.json"
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
OPENED="$(sort -u "$MANIFEST")"

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
REPLAYED="$(sort -u "$SCENARIOS")"

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
MIN_FIXTURES="${MIN_FIXTURES:-130}"
if [ "$total" -eq 0 ]; then
  echo "ERROR: the corpus at $SPEC_DIR listed ZERO fixtures." >&2
  echo "       Every check above is vacuously green over an empty population." >&2
  exit 1
fi
if [ "$covered" -lt "$MIN_FIXTURES" ]; then
  echo "ERROR: only $covered distinct canonical fixtures were OPENED, expected >= $MIN_FIXTURES." >&2
  echo "       A replay was removed, renamed, or short-circuited, or the recorder" >&2
  echo "       detached mid-run. Do not lower MIN_FIXTURES to fix this." >&2
  exit 1
fi

echo "conformance coverage OK: $covered/$total canonical fixtures OPENED by the suite" \
     "(${#KNOWN_UNCOVERED[@]} listed as known-uncovered; runtime manifest — these bytes were really read)"

# The same floor for rung 4. Its loop walks the scenarios of OPENED fixtures, so
# zero opened fixtures means zero scenarios, which means zero unreplayed
# scenarios — OK reported having compared nothing.
MIN_SCENARIOS="${MIN_SCENARIOS:-126}"
if [ "$SCENARIO_TOTAL" -eq 0 ]; then
  echo "ERROR: ZERO scenarios were found across the opened fixtures." >&2
  echo "       The rung above is vacuously green over an empty population." >&2
  exit 1
fi
if [ "$SCENARIO_REPLAYED" -lt "$MIN_SCENARIOS" ]; then
  echo "ERROR: only $SCENARIO_REPLAYED distinct scenarios were REPLAYED, expected >= $MIN_SCENARIOS." >&2
  echo "       A scenario dispatch stopped matching, or the ledger detached." >&2
  echo "       Do not lower MIN_SCENARIOS to fix this." >&2
  exit 1
fi

echo "scenario coverage OK: $SCENARIO_REPLAYED/$SCENARIO_TOTAL scenarios of those fixtures REPLAYED" \
     "(${#SCENARIO_EXCUSES[@]} excused; runtime ledger — recorded at the point of replay)"

