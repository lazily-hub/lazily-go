package lazily

import (
	"encoding/json"
	"fmt"
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
// uses: `id`, else `name`. There is no third option.
//
// The positional `#<n>` fallback is GONE (#lzspecscenarioids). It let the ledger
// record a scenario BY POSITION, where inserting one ahead of it silently rebinds
// that entry — and any excuse naming it — to a different scenario, with nothing
// turning red: the verifier compares "index 1 was replayed" against whatever now
// sits at index 1 and agrees with itself.
//
// It was load-bearing for exactly one fixture,
// collections/mergecell_algebra.json, whose scenarios were distinguished only by
// `policy`. They carry ids now, and lazily-spec's scenario-identity-check keeps
// every scenario identified — so this is a hole with no users, which is one
// waiting to become load-bearing again.
//
// A blank identifier is refused for the same reason: it would file every
// blank-id scenario under one ledger entry, which reads as "replayed" the moment
// any one of them runs.
//
// `ok` is false for an unidentified scenario. The caller turns that into a
// failure rather than inventing an id — a test helper cannot panic usefully from
// inside a subtest, so refusal is a return value here rather than a panic.
func scenarioKey(id, name string, index int) (string, bool) {
	if s := strings.TrimSpace(id); s != "" {
		return s, true
	}
	if s := strings.TrimSpace(name); s != "" {
		return s, true
	}
	return "#" + strconv.Itoa(index), false
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
	key, ok := scenarioKey(id, name, index)
	if !ok {
		panic(fmt.Sprintf(
			"%s: scenario at index %d carries neither `id` nor `name`. The replay "+
				"ledger would have to record it by POSITION, where inserting a scenario "+
				"ahead of it silently rebinds that entry to a different scenario. Give it "+
				"a stable id upstream in lazily-spec (#lzspecscenarioids).",
			fixture, index))
	}
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
	if got, ok := scenarioKey("fires_at_deadline", "ignored name", 3); got != "fires_at_deadline" || !ok {
		t.Fatalf("id must win: %q (ok=%v)", got, ok)
	}
	if got, ok := scenarioKey("", "repair_converges", 3); got != "repair_converges" || !ok {
		t.Fatalf("name must be the fallback: %q (ok=%v)", got, ok)
	}
	// #lzspecscenarioids: there is no third rung. A positional id silently
	// rebinds to a different scenario on a corpus reorder, so an unidentified
	// scenario is refused rather than booked.
	if _, ok := scenarioKey("", "", 3); ok {
		t.Fatal("an unidentified scenario must be refused, not booked by position")
	}
	if _, ok := scenarioKey("  ", "  ", 0); ok {
		t.Fatal("a blank id/name must be refused, not booked by position")
	}
}

// TestRecordScenarioAtPanicsOnAnUnidentifiedScenario pins the refusal at the
// call site runners actually use. Returning an id here would put the whole
// fixture's ledger back on POSITION, which is the drift #lzspecscenarioids
// closed.
func TestRecordScenarioAtPanicsOnAnUnidentifiedScenario(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("recordScenarioAt accepted a scenario with neither `id` nor `name`")
		}
		if !strings.Contains(fmt.Sprint(r), "carries neither `id` nor `name`") {
			t.Fatalf("panic does not name the defect: %v", r)
		}
	}()
	recordScenarioAt("conformance-ledger-self-test/probe.json", 1, "", "")
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

// ---------------------------------------------------------------------------
// Yielding is not replaying (#lzscenariobodyskip)
// ---------------------------------------------------------------------------
//
// Everything above books a scenario at the moment a runner is HANDED one. That
// cannot tell a loop body that ran from one that skipped: `continue`, an
// unmatched dispatch arm, an early `return` inside `t.Run`, and a body someone
// commented out all look identical from here — the loop simply comes back for
// the next item. So a skipped scenario booked itself and rung 4 stayed silent
// about the exact defect it exists for. lazily-py proved that against the
// contract's own probe; lazily-js, lazily-rs, lazily-cs and lazily-kt carry the
// fix. This is the Go seam.
//
// The rule is the same in every binding: book on the first read of the
// scenario's PAYLOAD, and stay silent for its LABEL. A dispatch chain that reads
// `id`, matches no arm, and falls through has replayed nothing, so reading `id`
// must not book. Decoding the steps/ops/frames/expect a replay is about to run
// is only ever done by a runner about to run it, so that read books.
//
// Go has no property interception, so the seam is explicit and comes in two
// shapes, one per decode style already in use here:
//
//   - scenarioView, for the runners that decode into map[string]any. Label() and
//     ID() are silent; Map() books and hands over the payload. Call Map() at the
//     point of replay — inside the subtest, inside the matched dispatch arm —
//     not next to the loop header.
//   - rawScenarioView, for the runners that decode into typed structs. Its
//     Decode is the booking read, which is exactly the operation only a runner
//     about to replay performs.
//
// Peek() is the escape hatch for a runner that legitimately needs to look
// without replaying. It is deliberately ugly to reach for.

// scenarioLabelKeys are the keys that IDENTIFY or narrate a scenario rather than
// drive one. Reading only these is looking at the label, not replaying. The list
// is shared with every other binding.
var scenarioLabelKeys = []string{
	"comment", "description", "id", "label", "name", "note", "notes", "reason", "why",
}

// scenarioView is one scenario, handed over unbooked.
type scenarioView struct {
	fixture string
	index   int
	id      string
	name    string
	raw     any
	booked  bool
}

// Label is the ledger key, for use as a subtest name. Reading it does NOT book:
// naming a scenario in test output is not replaying it.
func (v *scenarioView) Label() string {
	key, ok := scenarioKey(v.id, v.name, v.index)
	if !ok {
		panic(fmt.Sprintf(
			"%s: scenario at index %d carries neither `id` nor `name`. The replay "+
				"ledger would have to record it by POSITION, where inserting a scenario "+
				"ahead of it silently rebinds that entry to a different scenario. Give it "+
				"a stable id upstream in lazily-spec (#lzspecscenarioids).",
			v.fixture, v.index))
	}
	return key
}

// ID is the canonical scenario identity, for dispatch. Silent, by design: a
// chain that reads it and matches no arm has replayed nothing.
func (v *scenarioView) ID() string { return v.id }

// Map books this scenario as REPLAYED and hands over its payload.
func (v *scenarioView) Map() map[string]any {
	v.book()
	return jsMap(v.raw)
}

// Peek hands over the payload WITHOUT booking. For a runner that must inspect a
// scenario it is not replaying.
func (v *scenarioView) Peek() map[string]any { return jsMap(v.raw) }

func (v *scenarioView) book() {
	if v.booked {
		return
	}
	v.booked = true
	recordScenario(v.fixture, v.Label())
}

// scenarioViews wraps a fixture's decoded `scenarios` array. Iterating does not
// book; each view books when its payload is read.
func scenarioViews(fixture string, list []any) []*scenarioView {
	out := make([]*scenarioView, 0, len(list))
	for index, raw := range list {
		scenario, _ := raw.(map[string]any)
		out = append(out, &scenarioView{
			fixture: fixture,
			index:   index,
			id:      scenarioField(scenario, "id"),
			name:    scenarioField(scenario, "name"),
			raw:     raw,
		})
	}
	return out
}

// rawScenarioView is scenarioView for the typed-struct runners: the scenario
// stays as undecoded bytes until a runner decodes it, and THAT is the booking
// read. A scenario the loop skips is never decoded and so never booked.
type rawScenarioView struct {
	fixture string
	index   int
	id      string
	name    string
	raw     json.RawMessage
	booked  bool
}

func (v *rawScenarioView) Label() string {
	key, ok := scenarioKey(v.id, v.name, v.index)
	if !ok {
		panic(fmt.Sprintf(
			"%s: scenario at index %d carries neither `id` nor `name` (#lzspecscenarioids).",
			v.fixture, v.index))
	}
	return key
}

func (v *rawScenarioView) ID() string { return v.id }

// Decode books this scenario as REPLAYED and decodes its payload into target.
func (v *rawScenarioView) Decode(t *testing.T, target any) {
	t.Helper()
	v.book()
	if err := json.Unmarshal(v.raw, target); err != nil {
		t.Fatalf("%s: decode scenario %s: %v", v.fixture, v.Label(), err)
	}
}

// Peek decodes WITHOUT booking.
func (v *rawScenarioView) Peek(t *testing.T, target any) {
	t.Helper()
	if err := json.Unmarshal(v.raw, target); err != nil {
		t.Fatalf("%s: decode scenario %s: %v", v.fixture, v.Label(), err)
	}
}

func (v *rawScenarioView) book() {
	if v.booked {
		return
	}
	v.booked = true
	recordScenario(v.fixture, v.Label())
}

// rawScenarioViews wraps a fixture's undecoded `scenarios` array. The label is
// read out of each element up front — that is a label read, which never books.
func rawScenarioViews(t *testing.T, fixture string, list []json.RawMessage) []*rawScenarioView {
	t.Helper()
	out := make([]*rawScenarioView, 0, len(list))
	for index, raw := range list {
		var head struct {
			Id   string `json:"id"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &head); err != nil {
			t.Fatalf("%s: decode scenario head at index %d: %v", fixture, index, err)
		}
		out = append(out, &rawScenarioView{
			fixture: fixture,
			index:   index,
			id:      head.Id,
			name:    head.Name,
			raw:     raw,
		})
	}
	return out
}

// typedScenarioView is scenarioView for the runners that decode the WHOLE
// fixture into typed structs up front (mustStrictJSON, which is what keeps
// their unknown-field strictness). Those cannot defer the decode without giving
// up that strictness, so the booking rides on the handoff instead: Value() is
// the payload read, Label()/ID() are label reads and stay silent.
//
// A subtest that returns before calling Value() books nothing, which is the
// no-op-body case the loop header could not see.
type typedScenarioView[T any] struct {
	fixture  string
	index    int
	id       string
	name     string
	scenario T
	booked   bool
}

func (v *typedScenarioView[T]) Label() string {
	key, ok := scenarioKey(v.id, v.name, v.index)
	if !ok {
		panic(fmt.Sprintf(
			"%s: scenario at index %d carries neither `id` nor `name` (#lzspecscenarioids).",
			v.fixture, v.index))
	}
	return key
}

func (v *typedScenarioView[T]) ID() string { return v.id }

// Value books this scenario as REPLAYED and hands over its payload.
func (v *typedScenarioView[T]) Value() T {
	if !v.booked {
		v.booked = true
		recordScenario(v.fixture, v.Label())
	}
	return v.scenario
}

// Peek hands over the payload WITHOUT booking.
func (v *typedScenarioView[T]) Peek() T { return v.scenario }

// typedScenarioViews wraps a decoded scenario slice. `identity` reads the label
// off each element — a label read, which never books.
func typedScenarioViews[T any](fixture string, list []T, identity func(T) (string, string)) []*typedScenarioView[T] {
	out := make([]*typedScenarioView[T], 0, len(list))
	for index, scenario := range list {
		id, name := identity(scenario)
		out = append(out, &typedScenarioView[T]{
			fixture:  fixture,
			index:    index,
			id:       id,
			name:     name,
			scenario: scenario,
		})
	}
	return out
}
