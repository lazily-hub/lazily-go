package lazily

// Fixture-driven conformance runner for the reactive-graph observer fixtures
// (#lzdartobservercow, #lzspecconf).
//
// This executes the canonical JSON at
// ../lazily-spec/conformance/reactive-graph/observer_*.json DIRECTLY. It is
// deliberately NOT a hand transcription: transcriptions drift from the spec
// (lazily-kt bundles copies under src/test/resources/conformance/ and those have
// already drifted), so a transcribed copy cannot serve as the conformance gate.
// Fixtures are never copied into this repo — the sibling checkout is the only
// source.
//
// Normative text: lazily-spec docs/reactive-graph.md, "Firing order is
// registration order" through the "Known divergences (migration required)"
// table.
//
// Path resolution mirrors lazily-rs (#lzspecconf, tests/collections_conformance.rs):
// a single sibling-relative constant, and a skip — never a failure — when the
// sibling is absent. The skip is only safe because CI clones the sibling and
// then ASSERTS the directory exists; see .github/workflows/ci.yml, the
// "conformance (canonical lazily-spec fixtures)" step. Without that guard a
// skip-if-absent runner reports green while testing nothing.
//
// lazily-go is a single-package module rooted at the repo root, so `go test`
// runs with the package directory (= repo root) as its working directory and the
// sibling-relative constant resolves for `go test ./...`, `make check`, and
// `make conformance` alike.
//
// The runner is table-driven over the fixture directory, so a fixture added to
// lazily-spec is picked up without editing this file. The same directory holds
// the disposal/teardown fixtures (dispose_*.json, scope_*.json, churn_*.json,
// ...); widening observerFixtureGlob is the extension point once lazily-go grows
// the ops those need.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// observerSpecDir is the canonical fixture directory, relative to the package
// directory. Mirrors lazily-rs's `const SPEC_DIR: &str = "../lazily-spec/conformance/..."`.
const observerSpecDir = "../lazily-spec/conformance/reactive-graph"

// observerFixtureGlob selects the fixtures this runner's op vocabulary covers.
const observerFixtureGlob = "observer_*.json"

// ---------------------------------------------------------------------------
// Fixture schema
// ---------------------------------------------------------------------------

// fixtureOp is one op in a fixture step, or one nested op under `on_notify`.
// Nested ops use only {type, id, id_prefix, cell}.
type fixtureOp struct {
	Type         string      `json:"type"`
	ID           string      `json:"id"`
	IDPrefix     string      `json:"id_prefix"`
	Cell         string      `json:"cell"`
	Callback     string      `json:"callback"`
	Value        *int        `json:"value"`
	Times        *int        `json:"times"`
	OnNotifyOnce bool        `json:"on_notify_once"`
	OnNotify     []fixtureOp `json:"on_notify"`
}

// fixtureExpect is a step's assertion block. Pointer/map/RawMessage fields
// distinguish "key absent, assert nothing" from "key present and empty", which
// matters: `"observed_order": []` asserts that NOTHING fired.
type fixtureExpect struct {
	ObservedOrder  *[]string       `json:"observed_order"`
	ObservedCount  *int            `json:"observed_count"`
	ObservedCounts map[string]int  `json:"observed_counts"`
	Readable       map[string]bool `json:"readable"`
	Error          json.RawMessage `json:"error"`
	Note           string          `json:"note"`
}

type fixtureStep struct {
	Op     fixtureOp      `json:"op"`
	Expect *fixtureExpect `json:"expect"`
}

type observerFixture struct {
	Description string        `json:"description"`
	Kind        string        `json:"kind"`
	Model       string        `json:"model"`
	Steps       []fixtureStep `json:"steps"`
}

// ---------------------------------------------------------------------------
// Runner state
// ---------------------------------------------------------------------------

// registration is one `subscribe` op: a fixture id bound to the disposer
// lazily-go's Cell.Subscribe returned.
//
// This is where the fixture's registration-id vocabulary meets Go. The fixture
// unsubscribes by REGISTRATION id (obs_x1 vs obs_x2), never by callback, and Go
// func values are not comparable — there is no identity or equality to key on
// even if a binding wanted to. So a registration is keyed by the disposer
// closure Subscribe handed back (which internally holds its own *observerSlot),
// and two subscribes of one shared callback necessarily yield two independent
// registrations with two distinct disposers. The no-dedup clause is structurally
// unfalsifiable in Go; the runner still exercises it, and sharedCallback below
// checks the stronger property that actually has content here.
type registration struct {
	id      string
	cell    string
	label   string
	dispose func()
	live    bool
}

// sharedCallback models the fixture's `callback` field: registrations carrying
// the same label share ONE callback body. runs counts how many times that body
// executed, which the runner cross-checks against the number of registrations
// that fired — the real content of "every registration is independent".
type sharedCallback struct {
	label string
	runs  int
}

type observerRunner struct {
	t     *testing.T
	ctx   *Context
	cells map[string]*Cell[int]
	// torn records cells retired by a `dispose` op (see applyOp).
	torn      map[string]bool
	regs      map[string]*registration
	cellRegs  map[string][]*registration
	callbacks map[string]*sharedCallback
	spawnSeq  map[string]int

	// observation log for the step in flight
	order  []string
	counts map[string]int
}

func newObserverRunner(t *testing.T) *observerRunner {
	return &observerRunner{
		t:         t,
		ctx:       NewContext(),
		cells:     map[string]*Cell[int]{},
		torn:      map[string]bool{},
		regs:      map[string]*registration{},
		cellRegs:  map[string][]*registration{},
		callbacks: map[string]*sharedCallback{},
		spawnSeq:  map[string]int{},
		counts:    map[string]int{},
	}
}

// fire records one observer invocation under its registration id.
func (r *observerRunner) fire(id string) {
	r.order = append(r.order, id)
	r.counts[id]++
}

// resetLog clears the observation log so each step asserts only its own
// notifications.
func (r *observerRunner) resetLog() {
	r.order = nil
	r.counts = map[string]int{}
}

func (r *observerRunner) cell(id string) *Cell[int] {
	c, ok := r.cells[id]
	if !ok {
		r.t.Fatalf("op references unknown cell %q", id)
	}
	return c
}

func (r *observerRunner) reg(id string) *registration {
	reg, ok := r.regs[id]
	if !ok {
		r.t.Fatalf("op references unknown registration %q", id)
	}
	return reg
}

// applyOp executes one fixture op against lazily-go.
func (r *observerRunner) applyOp(op fixtureOp) {
	switch op.Type {
	case "cell":
		if op.Value == nil {
			r.t.Fatalf("cell %q has no value", op.ID)
		}
		r.cells[op.ID] = NewCell(r.ctx, *op.Value)

	case "set_cell":
		if op.Value == nil {
			r.t.Fatalf("set_cell %q has no value", op.ID)
		}
		r.cell(op.ID).Set(*op.Value)

	case "subscribe":
		r.applySubscribe(op)

	case "unsubscribe":
		times := 1
		if op.Times != nil {
			times = *op.Times
		}
		reg := r.reg(op.ID)
		for i := 0; i < times; i++ {
			reg.dispose()
		}
		reg.live = false

	case "dispose":
		// Cell teardown. lazily-go has no Cell.Dispose: a Cell is reclaimed by
		// the GC once unreachable, and reclamation issues no notification. The
		// op is modeled as retiring every live registration on the cell through
		// its real disposer, which genuinely exercises the normative half of the
		// clause — "tearing down the cell MUST drop its observers WITHOUT
		// invoking them", asserted by the step's `observed_order: []` — and
		// leaves the follow-up `unsubscribe` genuinely testing that a disposer
		// outliving its cell is a no-op. The `readable` half is harness-tracked
		// rather than observed, since lazily-go has no disposed-cell read to
		// probe. This is a PARTIAL mapping and the one place the runner models
		// rather than measures.
		for _, reg := range r.cellRegs[op.ID] {
			if reg.live {
				reg.dispose()
				reg.live = false
			}
		}
		r.torn[op.ID] = true

	default:
		r.t.Fatalf("unsupported op type %q — extend the runner before widening %s",
			op.Type, observerFixtureGlob)
	}
}

func (r *observerRunner) applySubscribe(op fixtureOp) {
	id := op.ID
	if id == "" && op.IDPrefix != "" {
		// `id_prefix` under on_notify: each invocation registers a fresh
		// observer named <prefix>_<n>, n counting from 0 per prefix.
		n := r.spawnSeq[op.IDPrefix]
		r.spawnSeq[op.IDPrefix]++
		id = fmt.Sprintf("%s_%d", op.IDPrefix, n)
	}
	if id == "" {
		r.t.Fatalf("subscribe op has neither id nor id_prefix")
	}
	if _, dup := r.regs[id]; dup {
		r.t.Fatalf("duplicate registration id %q", id)
	}

	// Registrations sharing a `callback` label share one callback body.
	label := op.Callback
	var shared *sharedCallback
	if label != "" {
		shared = r.callbacks[label]
		if shared == nil {
			shared = &sharedCallback{label: label}
			r.callbacks[label] = shared
		}
	}

	reg := &registration{id: id, cell: op.Cell, label: label, live: true}
	actions := op.OnNotify
	once := op.OnNotifyOnce
	fired := false

	observer := func(int) {
		r.fire(reg.id)
		if shared != nil {
			shared.runs++
		}
		first := !fired
		fired = true
		if len(actions) > 0 && (!once || first) {
			for _, action := range actions {
				r.applyOp(action)
			}
		}
	}

	reg.dispose = r.cell(op.Cell).Subscribe(observer)
	r.regs[id] = reg
	r.cellRegs[op.Cell] = append(r.cellRegs[op.Cell], reg)
}

// checkExpect asserts a step's `expect` block against the observation log.
func (r *observerRunner) checkExpect(step int, op fixtureOp, ex *fixtureExpect, recovered any) {
	t := r.t
	t.Helper()
	where := fmt.Sprintf("step %d (%s %s)", step, op.Type, op.ID)
	if ex.Note != "" {
		where += ": " + ex.Note
	}

	// `"error": null` — the op must complete without an error. Go's analogue of
	// a raised error here is a panic.
	if ex.Error != nil {
		if string(ex.Error) == "null" {
			if recovered != nil {
				t.Fatalf("%s\n  expected no error, but the op panicked: %v", where, recovered)
			}
		} else if recovered == nil {
			t.Fatalf("%s\n  fixture expects error %s, but the op succeeded", where, string(ex.Error))
		}
	}

	if ex.ObservedOrder != nil {
		want := *ex.ObservedOrder
		if !equalStrings(r.order, want) {
			t.Fatalf("%s\n  observed_order = %v\n  want           = %v", where, r.order, want)
		}
	}
	if ex.ObservedCount != nil {
		if len(r.order) != *ex.ObservedCount {
			t.Fatalf("%s\n  observed_count = %d, want %d (order %v)", where, len(r.order), *ex.ObservedCount, r.order)
		}
	}
	for id, want := range ex.ObservedCounts {
		if r.counts[id] != want {
			t.Fatalf("%s\n  observed_counts[%q] = %d, want %d (order %v)", where, id, r.counts[id], want, r.order)
		}
	}
	for cellID, want := range ex.Readable {
		if got := !r.torn[cellID]; got != want {
			t.Fatalf("%s\n  readable[%q] = %v, want %v", where, cellID, got, want)
		}
	}
}

// checkSharedCallbackRuns cross-checks, fixture-independently, that a callback
// body shared by N registrations ran once per firing registration rather than
// once per callback — the no-dedup clause stated as an invariant instead of as
// an expectation copied out of one fixture.
func (r *observerRunner) checkSharedCallbackRuns(total map[string]int) {
	for label, shared := range r.callbacks {
		want := 0
		for id, n := range total {
			if reg, ok := r.regs[id]; ok && reg.label == label {
				want += n
			}
		}
		if shared.runs != want {
			r.t.Fatalf("callback %q body ran %d times, want %d (once per firing registration — a dedup by callback identity or equality collapses these)",
				label, shared.runs, want)
		}
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

// TestReactiveGraphObserverConformance replays every observer_*.json fixture in
// the canonical lazily-spec reactive-graph directory.
func TestReactiveGraphObserverConformance(t *testing.T) {
	// Skip — never fail — when the sibling checkout is absent, mirroring
	// lazily-rs. CI clones the sibling and asserts this directory exists, so the
	// skip cannot silently pass there.
	if info, statErr := os.Stat(observerSpecDir); statErr != nil || !info.IsDir() {
		t.Skipf("skipping: %s absent - run with the lazily-spec sibling", observerSpecDir)
	}
	paths, err := filepath.Glob(filepath.Join(observerSpecDir, observerFixtureGlob))
	if err != nil {
		t.Fatalf("glob %s: %v", observerSpecDir, err)
	}
	if len(paths) == 0 {
		t.Fatalf("%s exists but contains no %s — the fixture set cannot be empty",
			observerSpecDir, observerFixtureGlob)
	}
	sort.Strings(paths)

	for _, path := range paths {
		t.Run(fixtureName(path), func(t *testing.T) {
			runObserverFixture(t, path)
		})
	}
}

// fixtureName turns a fixture path into a subtest name.
func fixtureName(path string) string {
	base := filepath.Base(path)
	return base[:len(base)-len(filepath.Ext(base))]
}

func runObserverFixture(t *testing.T, path string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var fx observerFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if fx.Kind != "ReactiveGraph" {
		t.Fatalf("%s: kind = %q, want ReactiveGraph", path, fx.Kind)
	}
	if fx.Model != "Context" {
		t.Fatalf("%s: model = %q, want Context", path, fx.Model)
	}
	if len(fx.Steps) == 0 {
		t.Fatalf("%s: fixture has no steps", path)
	}

	r := newObserverRunner(t)
	total := map[string]int{}

	for i, step := range fx.Steps {
		r.resetLog()

		recovered := func() (rec any) {
			defer func() { rec = recover() }()
			r.applyOp(step.Op)
			return nil
		}()

		for id, n := range r.counts {
			total[id] += n
		}

		if step.Expect != nil {
			r.checkExpect(i, step.Op, step.Expect, recovered)
		} else if recovered != nil {
			t.Fatalf("step %d (%s %s) panicked with no expectation: %v",
				i, step.Op.Type, step.Op.ID, recovered)
		}
	}

	r.checkSharedCallbackRuns(total)
}

// TestObserverRegistrationsAreNotKeyedByCallbackIdentity is the Go-specific
// companion to observer_duplicate_registrations_are_independent.json.
//
// The fixture expresses "the same callback, subscribed twice" through a shared
// `callback` label, which the runner honours with a shared callback body. This
// test states the strongest form the clause can take in Go: the IDENTICAL func
// value is passed to Subscribe twice. Go func values are not comparable, so a
// binding here cannot dedup by identity or equality even by accident — which is
// exactly the spec's argument for why keying by callback is not portable
// ("inexpressible where callbacks are neither hashable nor comparable").
func TestObserverRegistrationsAreNotKeyedByCallbackIdentity(t *testing.T) {
	ctx := NewContext()
	c := NewCell(ctx, 0)

	runs := 0
	shared := func(int) { runs++ }

	first := c.Subscribe(shared)
	c.Subscribe(shared)

	c.Set(1)
	if runs != 2 {
		t.Fatalf("shared callback ran %d times, want 2 (one per registration)", runs)
	}

	// Each disposer removes exactly one registration; the other keeps firing.
	first()
	c.Set(2)
	if runs != 3 {
		t.Fatalf("shared callback ran %d times, want 3 (the surviving registration still fires)", runs)
	}

	// The spent disposer is latched and must not reach the survivor.
	first()
	c.Set(3)
	if runs != 4 {
		t.Fatalf("shared callback ran %d times, want 4 (a repeat disposal must not remove the other registration)", runs)
	}
}
