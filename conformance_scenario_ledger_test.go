package lazily

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Runtime scenario ledger (#lzscenariocoverage) — rung 4.
//
// The rungs below this one all stop short of the scenario:
//
//  1. the runtime fixture manifest proves the fixture was OPENED;
//  2. consumeKeys/strictJSON prove every key of a block the runner REACHED was
//     read;
//  3. the assertion tracker proves every read key reached a COMPARISON.
//
// None of them can see a whole scenario that was never replayed. A fixture with
// four scenarios of which a runner replays three opens the file (rung 1 green),
// binds every key of the three blocks it reaches (rung 2 green), and asserts
// them (rung 3 green). The fourth scenario contributes no unconsumed key and no
// unasserted key, because a guard that only inspects the scenarios you ran
// cannot inspect the one you skipped. That is exactly the state this binding was
// in: `reliable-sync/liveness_orset_lww.json` carries four scenarios and this
// runner replayed three, green, for as long as the fixture has existed.
//
// So the evidence has to come from the replay itself, the same way the fixture
// manifest does. recordScenario is called at the point of replay — inside the
// loop body, after any `continue`, so a skipped scenario cannot record itself —
// and the ledger it builds is flushed beside the manifest. Verification is NOT
// here: it lives in scripts/check-conformance-coverage.sh, next to the
// KNOWN_UNCOVERED fixture allowlist and the excuse list, so there is one place
// to read what this binding does not prove rather than a third parallel guard.
//
// A hand-authored "scenarios this runner covers" list is the thing being
// guarded against. It is a claim, and a claim rots.
var (
	scenarioLedgerMu sync.Mutex
	scenarioLedger   = map[string]map[string]struct{}{}
)

// scenarioKey resolves a scenario's identity in the one order every binding
// uses: `id`, else `name`, else the 0-based positional index spelled `#<n>`.
//
// The corpus is not uniform — the three stdlib fixtures carry `id`, 28 others
// carry `name`, and collections/mergecell_algebra.json carries neither, its
// scenarios distinguished only by `policy`. The positional fallback exists so
// this guard is not blocked on a shared-corpus edit; it is reported by the
// verifier rather than silently accepted, and that visibility is what makes the
// corpus gap fixable upstream later.
func scenarioKey(id, name string, index int) string {
	if s := strings.TrimSpace(id); s != "" {
		return s
	}
	if s := strings.TrimSpace(name); s != "" {
		return s
	}
	return "#" + strconv.Itoa(index)
}

// recordScenario records that the run replayed `id` of `fixture`.
//
// fixture is the corpus-relative path ("reliable-sync/liveness_orset_lww.json").
// A filesystem path that passes through the corpus root is accepted too and
// normalized to the same id the fixture manifest uses, so the two evidence
// channels are keyed identically and the verifier can join them.
func recordScenario(fixture, id string) {
	f := conformanceFixtureID(fixture)
	if f == "" || id == "" {
		return
	}
	scenarioLedgerMu.Lock()
	defer scenarioLedgerMu.Unlock()
	ids, ok := scenarioLedger[f]
	if !ok {
		ids = map[string]struct{}{}
		scenarioLedger[f] = ids
	}
	ids[id] = struct{}{}
}

// recordScenarioAt is recordScenario for the common loop body: resolve the id
// from the scenario's own fields and its position, record it, and hand it back
// for use as a subtest label.
func recordScenarioAt(fixture string, index int, id, name string) string {
	key := scenarioKey(id, name, index)
	recordScenario(fixture, key)
	return key
}

// recordScenarioMap is recordScenarioAt for the runners that decode a scenario
// into map[string]any: it applies the same id -> name -> #<n> resolution to the
// decoded object, tolerating a fixture family that carries neither key.
func recordScenarioMap(fixture string, index int, scenario map[string]any) string {
	return recordScenarioAt(fixture, index, scenarioField(scenario, "id"), scenarioField(scenario, "name"))
}

func scenarioField(scenario map[string]any, key string) string {
	if s, ok := scenario[key].(string); ok {
		return s
	}
	return ""
}

// conformanceFixtureID normalizes a fixture reference to the corpus-relative,
// slash-separated id the manifest records.
func conformanceFixtureID(name string) string {
	if abs, err := filepath.Abs(name); err == nil {
		if idx := strings.Index(abs, conformanceMarker); idx != -1 {
			return filepath.ToSlash(abs[idx+len(conformanceMarker):])
		}
	}
	return filepath.ToSlash(strings.TrimPrefix(name, "./"))
}

// flushConformanceScenarios appends this binary's ledger to the path in
// LAZILY_CONFORMANCE_SCENARIOS, one `<fixture>\t<scenario id>` line per entry.
// Append, not truncate, for the same reason the manifest appends: `go test ./...`
// runs one binary per package and each must contribute. A no-op when the
// variable is unset, so a plain `go test` is unaffected.
func flushConformanceScenarios() {
	out := os.Getenv("LAZILY_CONFORMANCE_SCENARIOS")
	if out == "" {
		return
	}
	scenarioLedgerMu.Lock()
	defer scenarioLedgerMu.Unlock()
	if len(scenarioLedger) == 0 {
		return
	}
	lines := make([]string, 0, len(scenarioLedger))
	for fixture, ids := range scenarioLedger {
		for id := range ids {
			lines = append(lines, fixture+"\t"+id)
		}
	}
	sort.Strings(lines)
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		// A ledger we cannot write surfaces downstream as missing evidence,
		// which is correct. Never fail a suite over bookkeeping.
		return
	}
	defer f.Close()
	_, _ = f.WriteString(strings.Join(lines, "\n") + "\n")
}

// TestScenarioKeyResolutionOrder pins the resolution order the whole corpus is
// read through. Getting it wrong does not fail loudly — it renames every
// scenario of a fixture at once, so the verifier reports the entire fixture
// unreplayed and the diagnosis points at the runner instead of at this
// function.
func TestScenarioKeyResolutionOrder(t *testing.T) {
	if got := scenarioKey("fires_at_deadline", "ignored name", 3); got != "fires_at_deadline" {
		t.Fatalf("id must win: %q", got)
	}
	if got := scenarioKey("", "repair_converges", 3); got != "repair_converges" {
		t.Fatalf("name must be the fallback: %q", got)
	}
	if got := scenarioKey("", "", 3); got != "#3" {
		t.Fatalf("positional fallback must be #<index>: %q", got)
	}
	if got := scenarioKey("  ", "  ", 0); got != "#0" {
		t.Fatalf("blank id/name must fall through to the position: %q", got)
	}
}

// TestConformanceFixtureIDNormalization pins the join key between the ledger and
// the fixture manifest. If these two ever disagreed, every scenario of every
// fixture would look unreplayed.
func TestConformanceFixtureIDNormalization(t *testing.T) {
	const want = "reliable-sync/liveness_orset_lww.json"
	if got := conformanceFixtureID(want); got != want {
		t.Fatalf("corpus-relative id rewritten: %q", got)
	}
	got := conformanceFixtureID(filepath.Join("..", "lazily-spec", "conformance", "reliable-sync", "liveness_orset_lww.json"))
	if got != want {
		t.Fatalf("sibling path not normalized: %q", got)
	}
}

// TestRecordScenarioLedgersByFixture pins that the ledger keys by fixture and
// deduplicates — the reactive-graph corpus is replayed once per execution model,
// so a scenario legitimately records twice.
func TestRecordScenarioLedgersByFixture(t *testing.T) {
	const probe = "conformance-ledger-self-test/probe.json"
	defer func() {
		scenarioLedgerMu.Lock()
		delete(scenarioLedger, probe)
		scenarioLedgerMu.Unlock()
	}()
	recordScenario(probe, "one")
	recordScenario(probe, "one")
	recordScenario(probe, "two")
	scenarioLedgerMu.Lock()
	n := len(scenarioLedger[probe])
	scenarioLedgerMu.Unlock()
	if n != 2 {
		t.Fatalf("ledger holds %d ids, want 2 (deduplicated)", n)
	}
}
