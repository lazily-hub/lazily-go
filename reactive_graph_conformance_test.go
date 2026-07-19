package lazily

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// Cross-language conformance for the reactive-graph plane (#lzspecconf,
// #lzspecedgeindex) — see ../lazily-spec/conformance/reactive-graph/*.json.
//
// Why this runner exists, in this repo, specifically: an invalidation-cascade
// defect shipped in this binding's AsyncContext (a chain cell -> a -> b served a
// stale b forever) and was fixed in bdfdbce. A fixture encoding exactly that
// property — transitive_invalidation_reaches_depth.json — already existed in the
// shared spec, and lazily-go replayed nothing from that corpus. The defect was
// correct synchronously and broken asynchronously, so the runner below is
// parameterised over the *execution model* rather than hardcoding the default
// context: a single-context replay would have missed it exactly as the
// pre-existing hand-written tests did.
//
// Fixtures are never vendored here. The corpus is resolved through one
// sibling-relative path; when the checkout is absent the whole runner t.Skip()s
// with an explicit message, and CI guards that the checkout is present so
// "green" and "ran nothing" cannot be confused.

const reactiveGraphSpecDir = "../lazily-spec/conformance/reactive-graph"

// reactiveGraphReplayed is the set of fixtures this binding can execute today.
// Asserted against the on-disk listing together with the unsupported ledger, so
// an upstream addition or rename fails loudly instead of going unrun.
var reactiveGraphReplayed = []string{
	"transitive_invalidation_reaches_depth.json",
}

// reactiveGraphUnsupported names every fixture this binding cannot execute and
// the exact op or assertion that blocks it. These are findings against
// lazily-go's public surface, not relaxations of the fixtures: nothing here is
// skipped silently, and a fixture becoming executable fails the ledger
// assertion until it is promoted into reactiveGraphReplayed.
//
// The three missing capabilities, all of them public-API gaps:
//
//   - disposal of a Slot or a Cell. Only *Effect and *Signal expose Dispose();
//     a derived slot or an input cell cannot be torn down, so no fixture that
//     detaches a node mid-graph can run.
//   - scope/teardown (`begin_scope`, `end_scope`, `disarm`). lazily-go ships no
//     TeardownScope equivalent — cf. lazily-rs `ctx.scope()`.
//   - edge-degree introspection (`dependents_of`, `dependencies_of`). The
//     dependent/dependency sets are unexported context state with no accessor.
var reactiveGraphUnsupported = map[string]string{
	"churn_returns_to_baseline.json": `op "fanout"/"churn"/"dispose_fanout" and assertion "dependents_of": ` +
		`no bulk fanout construction and no edge-degree introspection`,
	"cross_scope_teardown_hazard.json": `op "begin_scope"/"end_scope"/"dispose" (of a Slot): ` +
		`no teardown scopes and no Slot.Dispose`,
	"disarm_disposes_nothing.json": `op "begin_scope"/"end_scope"/"disarm": ` +
		`no teardown scopes and no disarm()`,
	"dispose_detaches_edges_both_directions.json": `op "dispose" (of a Slot) and assertions ` +
		`"dependents_of"/"dependencies_of"/"readable": no Slot.Dispose and no edge-degree introspection`,
	"read_after_dispose_is_an_error.json": `op "dispose" (of a Slot) and assertion "readable": ` +
		`no Slot.Dispose, so a read can never be an error`,
	"recycled_id_inherits_nothing.json": `op "fanout"/"dispose_fanout"/"dispose_stale_handle": ` +
		`no bulk fanout construction and no stale-handle disposal`,
	"scope_teardown_equals_fold_of_disposals.json": `op "begin_scope"/"end_scope" and assertion ` +
		`"cleanup_order": no teardown scopes`,
	"scoping_bounds_teardown_not_visibility.json": `op "begin_scope"/"end_scope" and assertion ` +
		`"dependencies_of": no teardown scopes and no edge-degree introspection`,
}

// ---------------------------------------------------------------------------
// Execution models
// ---------------------------------------------------------------------------

// graphModel abstracts one execution model so the same fixture op stream can be
// replayed against every context lazily-go ships.
type graphModel interface {
	name() string
	cell(t *testing.T, id string, value int)
	computed(t *testing.T, id string, reads []string, offset int)
	read(t *testing.T, id string) int
	setCell(t *testing.T, id string, value int)
	close()
}

// syncModel replays against the default single-threaded *Context.
type syncModel struct {
	ctx   *Context
	cells map[string]*Cell[int]
	slots map[string]*Slot[int]
}

func newSyncModel() graphModel {
	return &syncModel{
		ctx:   NewContext(),
		cells: map[string]*Cell[int]{},
		slots: map[string]*Slot[int]{},
	}
}

func (m *syncModel) name() string { return "Context" }
func (m *syncModel) close()       {}

func (m *syncModel) cell(t *testing.T, id string, value int) {
	t.Helper()
	m.cells[id] = NewCell(m.ctx, value)
}

func (m *syncModel) computed(t *testing.T, id string, reads []string, offset int) {
	t.Helper()
	deps := append([]string(nil), reads...)
	m.slots[id] = NewSlot(m.ctx, func(*Context) int {
		sum := offset
		for _, d := range deps {
			sum += m.readNode(d)
		}
		return sum
	})
}

func (m *syncModel) readNode(id string) int {
	if c, ok := m.cells[id]; ok {
		return c.Get()
	}
	if s, ok := m.slots[id]; ok {
		return s.Get()
	}
	panic(fmt.Sprintf("reactive-graph: unknown node %q", id))
}

func (m *syncModel) read(t *testing.T, id string) int {
	t.Helper()
	if _, ok := m.cells[id]; !ok {
		if _, ok := m.slots[id]; !ok {
			t.Fatalf("read of unknown node %q", id)
		}
	}
	return m.readNode(id)
}

func (m *syncModel) setCell(t *testing.T, id string, value int) {
	t.Helper()
	c, ok := m.cells[id]
	if !ok {
		t.Fatalf("set_cell on unknown cell %q", id)
	}
	c.Set(value)
}

// asyncModel replays against *AsyncContext — the context whose cascade defect
// (bdfdbce) this corpus pins.
type asyncModel struct {
	ctx   *AsyncContext
	cells map[string]*AsyncCellHandle[int]
	slots map[string]*AsyncSlotHandle[int]
}

func newAsyncModel() graphModel {
	return &asyncModel{
		ctx:   NewAsyncContext(),
		cells: map[string]*AsyncCellHandle[int]{},
		slots: map[string]*AsyncSlotHandle[int]{},
	}
}

func (m *asyncModel) name() string { return "AsyncContext" }
func (m *asyncModel) close()       { _ = m.ctx.Close() }

func (m *asyncModel) cell(t *testing.T, id string, value int) {
	t.Helper()
	m.cells[id] = NewAsyncCell(m.ctx, value)
}

func (m *asyncModel) computed(t *testing.T, id string, reads []string, offset int) {
	t.Helper()
	deps := append([]string(nil), reads...)
	m.slots[id] = NewAsyncSlot(m.ctx, func(cc *AsyncComputeContext) (int, error) {
		sum := offset
		for _, d := range deps {
			if c, ok := m.cells[d]; ok {
				sum += TrackCell(cc, c)
				continue
			}
			s, ok := m.slots[d]
			if !ok {
				return 0, fmt.Errorf("reactive-graph: unknown node %q", d)
			}
			v, err := TrackAsync(cc, s)
			if err != nil {
				return 0, err
			}
			sum += v
		}
		return sum, nil
	})
}

func (m *asyncModel) read(t *testing.T, id string) int {
	t.Helper()
	if c, ok := m.cells[id]; ok {
		return c.Get()
	}
	s, ok := m.slots[id]
	if !ok {
		t.Fatalf("read of unknown node %q", id)
	}
	v, err := s.GetAsync(context.Background())
	if err != nil {
		t.Fatalf("GetAsync(%q) failed: %v", id, err)
	}
	return v
}

func (m *asyncModel) setCell(t *testing.T, id string, value int) {
	t.Helper()
	c, ok := m.cells[id]
	if !ok {
		t.Fatalf("set_cell on unknown cell %q", id)
	}
	c.Set(value)
}

// ---------------------------------------------------------------------------
// Fixture shape
// ---------------------------------------------------------------------------

type reactiveGraphFixture struct {
	Shape string `json:"shape"`
	Steps []struct {
		Op struct {
			Type   string   `json:"type"`
			ID     string   `json:"id"`
			Value  *int     `json:"value"`
			Reads  []string `json:"reads"`
			Offset int      `json:"offset"`
		} `json:"op"`
		Expect map[string]json.RawMessage `json:"expect"`
	} `json:"steps"`
}

// replayReport counts what actually executed, so "passed" cannot mean "did
// nothing".
type replayReport struct {
	ops    int
	checks int
}

func replayReactiveGraph(t *testing.T, m graphModel, name string, fx *reactiveGraphFixture) replayReport {
	t.Helper()
	var rep replayReport

	for i, step := range fx.Steps {
		op := step.Op
		var lastRead *int

		switch op.Type {
		case "cell":
			if op.Value == nil {
				t.Fatalf("%s#%d: cell op has no value", name, i)
			}
			m.cell(t, op.ID, *op.Value)
		case "computed":
			m.computed(t, op.ID, op.Reads, op.Offset)
		case "read":
			v := m.read(t, op.ID)
			lastRead = &v
		case "set_cell":
			if op.Value == nil {
				t.Fatalf("%s#%d: set_cell op has no value", name, i)
			}
			m.setCell(t, op.ID, *op.Value)
		default:
			// Never silent: a fixture in the replayed set must not contain an
			// op this runner does not implement.
			t.Fatalf("%s#%d: unsupported op %q in a fixture listed as replayable — "+
				"move it to reactiveGraphUnsupported with the op named", name, i, op.Type)
		}
		rep.ops++

		for _, key := range reactiveGraphRawKeys(step.Expect) {
			raw := step.Expect[key]
			switch key {
			case "note":
				// Prose for humans, not an assertion.
			case "value":
				var want int
				mustUnmarshal(t, name, i, key, raw, &want)
				if lastRead == nil {
					t.Fatalf("%s#%d: `value` assertion on a non-read op %q", name, i, op.Type)
				}
				if *lastRead != want {
					t.Errorf("%s[%s]#%d: read %q = %d, want %d",
						name, m.name(), i, op.ID, *lastRead, want)
				}
				rep.checks++
			case "read":
				var want map[string]int
				mustUnmarshal(t, name, i, key, raw, &want)
				ids := make([]string, 0, len(want))
				for id := range want {
					ids = append(ids, id)
				}
				sort.Strings(ids)
				for _, id := range ids {
					got := m.read(t, id)
					if got != want[id] {
						t.Errorf("%s[%s]#%d: read %q = %d, want %d",
							name, m.name(), i, id, got, want[id])
					}
					rep.checks++
				}
			default:
				t.Fatalf("%s#%d: unsupported assertion %q in a fixture listed as "+
					"replayable — move it to reactiveGraphUnsupported with the key named",
					name, i, key)
			}
		}
	}
	return rep
}

func reactiveGraphRawKeys(m map[string]json.RawMessage) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func mustUnmarshal(t *testing.T, name string, step int, key string, raw json.RawMessage, out any) {
	t.Helper()
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("%s#%d: assertion %q is malformed: %v", name, step, key, err)
	}
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

func TestReactiveGraphConformance(t *testing.T) {
	if _, err := os.Stat(reactiveGraphSpecDir); err != nil {
		t.Skipf("%s not found — clone lazily-spec as a sibling checkout to replay the "+
			"reactive-graph (#lzspecedgeindex) fixtures", reactiveGraphSpecDir)
	}

	// The fixture set on disk must be exactly what this runner accounts for,
	// either as replayed or as explicitly unsupported. An upstream addition
	// cannot arrive unexecuted and unreported.
	entries, err := filepath.Glob(filepath.Join(reactiveGraphSpecDir, "*.json"))
	if err != nil {
		t.Fatalf("listing %s: %v", reactiveGraphSpecDir, err)
	}
	onDisk := map[string]bool{}
	for _, e := range entries {
		onDisk[filepath.Base(e)] = true
	}
	accounted := map[string]bool{}
	for _, f := range reactiveGraphReplayed {
		accounted[f] = true
	}
	for f := range reactiveGraphUnsupported {
		if accounted[f] {
			t.Fatalf("%s is listed as both replayed and unsupported", f)
		}
		accounted[f] = true
	}
	for f := range onDisk {
		if !accounted[f] {
			t.Errorf("fixture %s is on disk but neither replayed nor listed as unsupported — "+
				"the corpus drifted and this fixture would go unrun", f)
		}
	}
	for f := range accounted {
		if !onDisk[f] {
			t.Errorf("fixture %s is accounted for but missing from %s — stale ledger entry",
				f, reactiveGraphSpecDir)
		}
	}

	// Report the gaps loudly, once, rather than skipping them in silence.
	for _, f := range reactiveGraphLedgerKeys(reactiveGraphUnsupported) {
		t.Logf("UNSUPPORTED reactive-graph/%s — %s", f, reactiveGraphUnsupported[f])
	}

	models := []struct {
		name string
		make func() graphModel
	}{
		{"Context", newSyncModel},
		{"AsyncContext", newAsyncModel},
	}

	// Positive assertion (#lzspecconf): count distinct fixtures actually
	// replayed, per model, and fail loudly at zero. A runner that executes
	// nothing must not be able to report green.
	replayedPerModel := map[string]map[string]bool{}

	for _, mdl := range models {
		mdl := mdl
		replayedPerModel[mdl.name] = map[string]bool{}
		t.Run(mdl.name, func(t *testing.T) {
			for _, name := range reactiveGraphReplayed {
				name := name
				t.Run(name, func(t *testing.T) {
					raw, err := os.ReadFile(filepath.Join(reactiveGraphSpecDir, name))
					if err != nil {
						t.Fatalf("reading fixture %s: %v", name, err)
					}
					var fx reactiveGraphFixture
					if err := json.Unmarshal(raw, &fx); err != nil {
						t.Fatalf("parsing fixture %s: %v", name, err)
					}
					if fx.Shape != "steps" {
						t.Fatalf("%s: unsupported fixture shape %q", name, fx.Shape)
					}

					m := mdl.make()
					defer m.close()

					rep := replayReactiveGraph(t, m, name, &fx)
					if rep.ops == 0 {
						t.Fatalf("%s[%s]: replayed zero ops", name, mdl.name)
					}
					if rep.checks == 0 {
						t.Fatalf("%s[%s]: replayed zero assertions", name, mdl.name)
					}
					t.Logf("reactive-graph[%s] %s: %d ops, %d assertions",
						mdl.name, name, rep.ops, rep.checks)
					replayedPerModel[mdl.name][name] = true
				})
			}
		})
	}

	for _, mdl := range models {
		got := len(replayedPerModel[mdl.name])
		if got == 0 {
			t.Fatalf("%s: replayed zero reactive-graph fixtures — the runner executed nothing",
				mdl.name)
		}
		if got != len(reactiveGraphReplayed) {
			t.Errorf("%s: replayed %d of %d reactive-graph fixtures",
				mdl.name, got, len(reactiveGraphReplayed))
		}
		t.Logf("reactive-graph[%s]: %d fixtures replayed, %d unsupported",
			mdl.name, got, len(reactiveGraphUnsupported))
	}
}

func reactiveGraphLedgerKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
