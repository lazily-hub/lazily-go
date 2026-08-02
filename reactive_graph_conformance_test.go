package lazily

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
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
// The same reasoning now carries the disposal/teardown half of the corpus.
// #lzspecedgeindex is about per-node graph memory, and a leak on the async path
// is the hardest kind to notice and to reproduce by hand — so disposal, scope
// teardown, and edge-degree assertions are replayed against both contexts too.
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
	"churn_returns_to_baseline.json",
	"cross_scope_teardown_hazard.json",
	"disarm_disposes_nothing.json",
	"disposal_does_not_run_surviving_effects.json",
	"dispose_detaches_edges_both_directions.json",
	"dispose_signal_reverts_to_lazy.json",
	"failed_compute_is_never_cached.json",
	"read_after_dispose_is_an_error.json",
	"recycled_id_inherits_nothing.json",
	"scope_teardown_equals_fold_of_disposals.json",
	"scoping_bounds_teardown_not_visibility.json",
	"signal_materializes_once_per_batch.json",
	"signal_materializes_without_a_read.json",
	"teardown_runs_members_in_reverse_creation_order.json",
	"transitive_invalidation_reaches_depth.json",
}

// reactiveGraphUnsupported names every fixture this binding cannot execute and
// the exact op or assertion that blocks it. Entries are findings against
// lazily-go's public surface, never relaxations of the fixtures: nothing here is
// skipped silently, and a fixture becoming executable fails the ledger
// assertion until it is promoted into reactiveGraphReplayed.
//
// The merge-feed fixtures landed on spec main (#lzmergefeed, Step 3): they drive
// the `merge_cell` op — the accumulate/fold write surface (RelayCell / merge
// policy) — which this reactive-graph runner does not model. The runner has no
// `merge_cell` node kind, so replaying them would Fatalf on an unsupported op;
// they are accounted-for skips, not silent gaps, and each stays here until the
// op is implemented and the fixture promoted into reactiveGraphReplayed.
//
// feedback_drain_bound_reports_exhaustion uses only supported ops (cell / effect
// / read / set_cell) but asserts the novel `drain_exhausted` / `writes_own_cone`
// keys — the bounded-feedback drain semantics carried forward under #lzmergefeed.
// It is parked here rather than mis-replayed against expectation keys the runner
// cannot check (they would Fatalf in the assertion switch's default arm).
//
// merge ops are NOT implemented and the drift assertion is NOT loosened: these
// entries are exactly what makes the on-disk corpus drift check pass without
// running fixtures this binding cannot honestly execute.
var reactiveGraphUnsupported = map[string]string{
	"exact_fold_paths_stay_exact.json":             "merge_cell op (#lzmergefeed) — no merge/fold node kind in this runner",
	"merge_cell_acquires_no_dependency_edge.json":  "merge_cell op (#lzmergefeed) — no merge/fold node kind in this runner",
	"merge_feed_through_a_formula_coalesces.json":  "merge_cell op (#lzmergefeed) — no merge/fold node kind in this runner",
	"merge_folds_synchronously_in_batch.json":      "merge_cell op (#lzmergefeed) — no merge/fold node kind in this runner",
	"merge_per_settled_cone_not_per_write.json":    "merge_cell op (#lzmergefeed) — no merge/fold node kind in this runner",
	"feedback_drain_bound_reports_exhaustion.json": "drain_exhausted/writes_own_cone assertion keys (#lzmergefeed) — bounded-feedback drain not modeled",
}

// reactiveGraphModelUnsupported names fixture/model pairs one execution model
// cannot run, keyed "<model>/<fixture>". Same discipline as the ledger above:
// each entry is a gap in lazily-go's public surface, reported loudly, and the
// per-model completeness assertion subtracts exactly these — so a pair that
// becomes runnable is not silently left unrun, and one that stops being runnable
// cannot be hidden by adding an entry without also stating why.
var reactiveGraphModelUnsupported = map[string]string{
	"AsyncContext/dispose_signal_reverts_to_lazy.json": "AsyncContext ships no signal constructor " +
		"(no NewAsyncSignal), so the `signal` op has nothing to build on that plane",
	"AsyncContext/signal_materializes_once_per_batch.json": "AsyncContext ships no signal constructor " +
		"(no NewAsyncSignal), so the `signal` op has nothing to build on that plane",
	"AsyncContext/signal_materializes_without_a_read.json": "AsyncContext ships no signal constructor " +
		"(no NewAsyncSignal), so the `signal` op has nothing to build on that plane",
}

// reactiveGraphDivergences records fixture assertions an execution model does
// not satisfy, keyed `<model>/<fixture><label>#<step>:<key>`.
//
// Each entry would be a finding against the implementation, never a relaxation
// of a fixture: the runner asserts the observed set equals this one exactly, so
// a new divergence fails the build and a fixed one fails it until its entry is
// deleted. Both directions are load-bearing.
var reactiveGraphDivergences = map[string]bool{}

// ---------------------------------------------------------------------------
// Node references
// ---------------------------------------------------------------------------

type nodeKind int

const (
	kindCell nodeKind = iota
	kindSlot
	kindEffect
	kindSignal
)

// nodeRef names one node in whichever graph is under test. The handle is opaque
// to the engine; only the model knows how to read or tear it down.
type nodeRef struct {
	kind nodeKind
	id   string
	h    any
}

// ---------------------------------------------------------------------------
// Execution models
// ---------------------------------------------------------------------------

// nodeFactory is the constructor surface shared by a context and a scope, so
// the engine can dispatch a scoped op without a second code path.
type nodeFactory interface {
	cell(id string, value int) nodeRef
	computed(id string, reads []nodeRef, offset int) nodeRef
	effect(id string, reads []nodeRef) nodeRef
}

// scopeModel is one teardown scope in whichever graph is under test.
type scopeModel interface {
	nodeFactory
	owned() int
	disarm()
	closeScope()
}

// graphModel abstracts one execution model so the same fixture op stream can be
// replayed against every context lazily-go ships.
type graphModel interface {
	nodeFactory
	name() string
	// read returns the corpus's `read_after_dispose` as a non-nil error.
	read(n nodeRef) (int, error)
	setCell(n nodeRef, value int)
	dispose(n nodeRef)
	dependentCount(n nodeRef) int
	dependencyCount(n nodeRef) int
	isEffectActive(n nodeRef) bool
	// signal builds the derived eager construct (#lzsignaleager). Separate from
	// nodeFactory because the corpus never creates one inside a teardown scope.
	signal(id string, reads []nodeRef, offset int) nodeRef
	// disposeSignal removes only the eager puller — the narrower operation the
	// corpus spells `dispose_signal`, not a graph teardown.
	disposeSignal(n nodeRef)
	batch(run func())
	// failNext arms the next `count` computes of an existing node to fail, so a
	// fixture can assert on computesOf that a failed compute is never cached:
	// the node re-runs per read instead of replaying a stored error. It creates
	// nothing and does not touch the node's dependency set.
	failNext(id string, count int)
	// computesOf is the cumulative number of times a node's compute body ran.
	// It is the only observable that separates an eager signal from the lazy
	// slot it is built on: their values are identical for every read sequence.
	computesOf(id string) int
	scope() scopeModel
	// settle drives the model to quiescence before assertions are evaluated.
	// Synchronous models are already quiescent when an op returns; async
	// effects and computes are spawned, so `observed_by`, `observed_count`, and
	// every degree assertion are meaningless until the runtime has run them.
	// This changes *when* assertions are evaluated, never *what* they assert:
	// an effect that never runs still fails.
	settle()
	runLog() []string
	cleanupLog() []string
	close()
}

// --- effect log, shared by both models --------------------------------------

type effectLog struct {
	mu       sync.Mutex
	runs     []string
	cleanups []string
}

func (l *effectLog) run(id string) {
	l.mu.Lock()
	l.runs = append(l.runs, id)
	l.mu.Unlock()
}

func (l *effectLog) cleanup(id string) {
	l.mu.Lock()
	l.cleanups = append(l.cleanups, id)
	l.mu.Unlock()
}

func (l *effectLog) snapshotRuns() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.runs...)
}

func (l *effectLog) snapshotCleanups() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.cleanups...)
}

// --- compute counter, shared by both models ---------------------------------

// computeLog counts compute-body executions per node id. Async computes are
// spawned, so the counter is mutex-guarded like effectLog.
type computeLog struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *computeLog) tick(id string) {
	c.mu.Lock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[id]++
	c.mu.Unlock()
}

func (c *computeLog) count(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[id]
}

// --- sync model -------------------------------------------------------------

// errComputeFailed is the failure a `fail_next`-armed compute body raises. It is
// a runner-owned sentinel, not a library error: the contract under test is that
// the library does not CACHE it, so what it is matters less than that the same
// body raises it once per armed run and the node still re-runs afterwards.
var errComputeFailed = errors.New("reactive-graph: compute_failed (fail_next)")

// armedFailures counts, per node id, how many upcoming compute bodies must fail.
type armedFailures map[string]int

func (a *armedFailures) arm(id string, count int) {
	if *a == nil {
		*a = armedFailures{}
	}
	if count <= 0 {
		count = 1
	}
	(*a)[id] += count
}

// take reports whether this run is armed to fail, consuming one arming. Called
// from inside the compute body, after the compute counter ticks, so an armed run
// is counted exactly like a successful one.
func (a *armedFailures) take(id string) bool {
	if *a == nil || (*a)[id] <= 0 {
		return false
	}
	(*a)[id]--
	return true
}

// syncModel replays against the default single-threaded *Context.
type syncModel struct {
	ctx      *Context
	log      effectLog
	computes computeLog
	armed    armedFailures
}

func newSyncModel() graphModel { return &syncModel{ctx: NewContext()} }

func (m *syncModel) name() string         { return "Context" }
func (m *syncModel) close()               {}
func (m *syncModel) settle()              {}
func (m *syncModel) runLog() []string     { return m.log.snapshotRuns() }
func (m *syncModel) cleanupLog() []string { return m.log.snapshotCleanups() }
func (m *syncModel) scope() scopeModel    { return &syncScope{m: m, s: m.ctx.Scope()} }

func (m *syncModel) cell(id string, value int) nodeRef {
	return nodeRef{kind: kindCell, id: id, h: NewSource(m.ctx, value)}
}

// trackRead is the read used *inside* a compute: it panics on a disposed node,
// which the top-level TryGet turns into `read_after_dispose`.
func (m *syncModel) trackRead(c *Compute, r nodeRef) int {
	switch r.kind {
	case kindCell:
		return Get[int](c, r.h.(*Source[int]))
	case kindSlot:
		return Get[int](c, r.h.(*Computed[int]))
	case kindSignal:
		return Get[int](c, r.h.(*Computed[int]))
	}
	panic(fmt.Sprintf("reactive-graph: cannot read effect %q", r.id))
}

func (m *syncModel) computed(id string, reads []nodeRef, offset int) nodeRef {
	deps := append([]nodeRef(nil), reads...)
	s := NewNamedSlot(m.ctx, id, func(c *Compute) int {
		m.computes.tick(id)
		if m.armed.take(id) {
			panic(errComputeFailed)
		}
		sum := offset
		for _, d := range deps {
			sum += m.trackRead(c, d)
		}
		return sum
	})
	return nodeRef{kind: kindSlot, id: id, h: s}
}

func (m *syncModel) signal(id string, reads []nodeRef, offset int) nodeRef {
	deps := append([]nodeRef(nil), reads...)
	// The eager construction is a driven Computed — NewComputed(f).Eager() — not a
	// bespoke Signal. Drive attaches a puller Effect that materializes the value
	// now and re-pulls after every invalidation, coalescing per batch flush.
	s := NewComputed(m.ctx, func(c *Compute) int {
		m.computes.tick(id)
		sum := offset
		for _, d := range deps {
			sum += m.trackRead(c, d)
		}
		return sum
	}).Eager()
	return nodeRef{kind: kindSignal, id: id, h: s}
}

// disposeSignal is the former dispose_signal op: undrive the formula (remove the
// puller) while leaving the node in the graph, readable as a lazy value.
func (m *syncModel) disposeSignal(r nodeRef) { r.h.(*Computed[int]).Lazy() }

func (m *syncModel) batch(run func()) { m.ctx.Batch(run) }

func (m *syncModel) computesOf(id string) int { return m.computes.count(id) }

func (m *syncModel) failNext(id string, count int) { m.armed.arm(id, count) }

func (m *syncModel) effect(id string, reads []nodeRef) nodeRef {
	deps := append([]nodeRef(nil), reads...)
	e := NewEffect(m.ctx, func(c *Compute) func() {
		m.log.run(id)
		// The body must not unwind on a disposed-node read: catch the
		// *DisposedError so the effect body returns normally. Dependency tracking
		// is value-threaded through c (no ambient stack), so a panic strands no
		// frame — nothing to repair, just swallow the sentinel.
		func() {
			defer func() {
				if r := recover(); r != nil {
					if _, ok := r.(*DisposedError); !ok {
						panic(r)
					}
				}
			}()
			for _, d := range deps {
				m.trackRead(c, d)
			}
		}()
		return func() { m.log.cleanup(id) }
	})
	return nodeRef{kind: kindEffect, id: id, h: e}
}

func (m *syncModel) read(r nodeRef) (v int, err error) {
	// A `fail_next`-armed compute body panics; the corpus observes that as a
	// failed read, exactly like the disposed-node error TryGet already returns.
	defer func() {
		if rec := recover(); rec != nil {
			if rec == errComputeFailed {
				v, err = 0, errComputeFailed
				return
			}
			panic(rec)
		}
	}()
	switch r.kind {
	case kindCell:
		return r.h.(*Source[int]).TryGet()
	case kindSlot:
		return r.h.(*Computed[int]).TryGet()
	case kindSignal:
		return r.h.(*Computed[int]).TryGet()
	}
	return 0, fmt.Errorf("reactive-graph: cannot read effect %q", r.id)
}

func (m *syncModel) setCell(r nodeRef, value int) { r.h.(*Source[int]).Set(value) }

func (m *syncModel) dispose(r nodeRef) {
	switch r.kind {
	case kindCell:
		r.h.(*Source[int]).Dispose()
	case kindSlot:
		r.h.(*Computed[int]).Dispose()
	case kindSignal:
		// `dispose` is the graph teardown (tears down the puller too);
		// `dispose_signal` is the narrower puller removal (Undrive).
		r.h.(*Computed[int]).Dispose()
	case kindEffect:
		r.h.(*Effect).Dispose()
	}
}

func (m *syncModel) graphNode(r nodeRef) GraphNode {
	switch r.kind {
	case kindCell:
		return r.h.(*Source[int])
	case kindSlot:
		return r.h.(*Computed[int])
	case kindSignal:
		return r.h.(*Computed[int])
	}
	return r.h.(*Effect)
}

func (m *syncModel) dependentCount(r nodeRef) int  { return m.ctx.DependentCount(m.graphNode(r)) }
func (m *syncModel) dependencyCount(r nodeRef) int { return m.ctx.DependencyCount(m.graphNode(r)) }

func (m *syncModel) isEffectActive(r nodeRef) bool { return r.h.(*Effect).IsActive() }

type syncScope struct {
	m *syncModel
	s *TeardownScope
}

func (sc *syncScope) cell(id string, value int) nodeRef {
	r := sc.m.cell(id, value)
	Own(sc.s, r.h.(*Source[int]))
	return r
}

func (sc *syncScope) computed(id string, reads []nodeRef, offset int) nodeRef {
	r := sc.m.computed(id, reads, offset)
	Own(sc.s, r.h.(*Computed[int]))
	return r
}

func (sc *syncScope) effect(id string, reads []nodeRef) nodeRef {
	r := sc.m.effect(id, reads)
	Own(sc.s, r.h.(*Effect))
	return r
}

func (sc *syncScope) owned() int  { return sc.s.Len() }
func (sc *syncScope) disarm()     { sc.s.Disarm() }
func (sc *syncScope) closeScope() { sc.s.Close() }

// --- async model ------------------------------------------------------------

// asyncModel replays against *AsyncContext — the context whose cascade defect
// (bdfdbce) this corpus pins, and the plane where a disposal leak is hardest to
// reproduce by hand.
type asyncModel struct {
	ctx      *AsyncContext
	log      effectLog
	computes computeLog
	armed    armedFailures
}

func newAsyncModel() graphModel { return &asyncModel{ctx: NewAsyncContext()} }

func (m *asyncModel) name() string         { return "AsyncContext" }
func (m *asyncModel) close()               { _ = m.ctx.Close() }
func (m *asyncModel) runLog() []string     { return m.log.snapshotRuns() }
func (m *asyncModel) cleanupLog() []string { return m.log.snapshotCleanups() }
func (m *asyncModel) scope() scopeModel    { return &asyncScope{m: m, s: m.ctx.Scope()} }

// settle waits until no compute and no effect body is in flight. Every op the
// engine issues is synchronous from the caller's point of view except effect
// bodies and slot computes, which are spawned; without this the corpus would be
// asserting against a graph still mid-cascade.
func (m *asyncModel) settle() {
	deadline := time.Now().Add(30 * time.Second)
	for {
		idle := true
		m.ctx.do(func() {
			if len(m.ctx.computing) != 0 {
				idle = false
				return
			}
			for e := range m.ctx.effects {
				if e.running {
					idle = false
					return
				}
			}
		})
		if idle || time.Now().After(deadline) {
			return
		}
		time.Sleep(50 * time.Microsecond)
	}
}

func (m *asyncModel) cell(id string, value int) nodeRef {
	return nodeRef{kind: kindCell, id: id, h: NewAsyncSource(m.ctx, value)}
}

// trackRead is the read used inside an async compute or effect body. A disposed
// cell panics (TrackSource returns a bare T and has no error channel); a disposed
// slot returns an error. Both surface here as an error.
func (m *asyncModel) trackRead(cc *AsyncComputeContext, r nodeRef) (v int, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			de, ok := rec.(*DisposedError)
			if !ok {
				panic(rec)
			}
			v, err = 0, de
		}
	}()
	switch r.kind {
	case kindCell:
		return TrackSource(cc, r.h.(*AsyncSource[int])), nil
	case kindSlot:
		return TrackComputed(cc, r.h.(*AsyncComputed[int]))
	}
	return 0, fmt.Errorf("reactive-graph: cannot read effect %q", r.id)
}

func (m *asyncModel) computed(id string, reads []nodeRef, offset int) nodeRef {
	deps := append([]nodeRef(nil), reads...)
	s := NewAsyncComputed(m.ctx, func(cc *AsyncComputeContext) (int, error) {
		m.computes.tick(id)
		if m.armed.take(id) {
			return 0, errComputeFailed
		}
		sum := offset
		for _, d := range deps {
			v, err := m.trackRead(cc, d)
			if err != nil {
				return 0, err
			}
			sum += v
		}
		return sum, nil
	})
	return nodeRef{kind: kindSlot, id: id, h: s}
}

// signal and disposeSignal are unreachable on this plane: AsyncContext ships no
// signal constructor, so every signal fixture is gated out per-model in
// reactiveGraphModelUnsupported rather than quietly degraded to a slot here.
// Degrading would be the worst option — the fixtures assert compute counts, and
// a slot standing in for a signal reports plausible-looking numbers that pass
// two of the three assertions.
func (m *asyncModel) signal(id string, reads []nodeRef, offset int) nodeRef {
	panic("reactive-graph: AsyncContext ships no signal — this fixture must be gated " +
		"in reactiveGraphModelUnsupported")
}

func (m *asyncModel) disposeSignal(r nodeRef) {
	panic("reactive-graph: AsyncContext ships no signal — this fixture must be gated " +
		"in reactiveGraphModelUnsupported")
}

func (m *asyncModel) batch(run func()) { m.ctx.Batch(run) }

func (m *asyncModel) computesOf(id string) int { return m.computes.count(id) }

func (m *asyncModel) failNext(id string, count int) { m.armed.arm(id, count) }

func (m *asyncModel) effect(id string, reads []nodeRef) nodeRef {
	deps := append([]nodeRef(nil), reads...)
	e := m.ctx.EffectAsync(func(cc *AsyncComputeContext) func() {
		m.log.run(id)
		for _, d := range deps {
			// A read that fails because a dependency is gone is the contract,
			// not a test failure: the effect simply observes the error.
			_, _ = m.trackRead(cc, d)
		}
		return func() { m.log.cleanup(id) }
	})
	return nodeRef{kind: kindEffect, id: id, h: e}
}

func (m *asyncModel) read(r nodeRef) (int, error) {
	switch r.kind {
	case kindCell:
		return r.h.(*AsyncSource[int]).TryGet()
	case kindSlot:
		return r.h.(*AsyncComputed[int]).GetAsync(context.Background())
	}
	return 0, fmt.Errorf("reactive-graph: cannot read effect %q", r.id)
}

func (m *asyncModel) setCell(r nodeRef, value int) { r.h.(*AsyncSource[int]).Set(value) }

func (m *asyncModel) dispose(r nodeRef) {
	switch r.kind {
	case kindCell:
		r.h.(*AsyncSource[int]).DisposeAsync()
	case kindSlot:
		r.h.(*AsyncComputed[int]).DisposeAsync()
	case kindEffect:
		r.h.(*AsyncEffectHandle).DisposeAsync()
	}
}

func (m *asyncModel) graphNode(r nodeRef) AsyncGraphNode {
	switch r.kind {
	case kindCell:
		return r.h.(*AsyncSource[int])
	case kindSlot:
		return r.h.(*AsyncComputed[int])
	}
	return r.h.(*AsyncEffectHandle)
}

func (m *asyncModel) dependentCount(r nodeRef) int  { return m.ctx.DependentCount(m.graphNode(r)) }
func (m *asyncModel) dependencyCount(r nodeRef) int { return m.ctx.DependencyCount(m.graphNode(r)) }

func (m *asyncModel) isEffectActive(r nodeRef) bool { return r.h.(*AsyncEffectHandle).IsActive() }

type asyncScope struct {
	m *asyncModel
	s *AsyncTeardownScope
}

func (sc *asyncScope) cell(id string, value int) nodeRef {
	r := sc.m.cell(id, value)
	OwnAsync(sc.s, r.h.(*AsyncSource[int]))
	return r
}

func (sc *asyncScope) computed(id string, reads []nodeRef, offset int) nodeRef {
	r := sc.m.computed(id, reads, offset)
	OwnAsync(sc.s, r.h.(*AsyncComputed[int]))
	return r
}

func (sc *asyncScope) effect(id string, reads []nodeRef) nodeRef {
	r := sc.m.effect(id, reads)
	OwnAsync(sc.s, r.h.(*AsyncEffectHandle))
	return r
}

func (sc *asyncScope) owned() int  { return sc.s.Len() }
func (sc *asyncScope) disarm()     { sc.s.Disarm() }
func (sc *asyncScope) closeScope() { sc.s.Close() }

// ---------------------------------------------------------------------------
// Fixture shape
// ---------------------------------------------------------------------------

type reactiveGraphOp struct {
	Type       string   `json:"type"`
	ID         string   `json:"id"`
	Value      *int     `json:"value"`
	Reads      []string `json:"reads"`
	Offset     int      `json:"offset"`
	Scope      string   `json:"scope"`
	IDPrefix   string   `json:"id_prefix"`
	Count      int      `json:"count"`
	Source     string   `json:"source"`
	LiveWidth  int      `json:"live_width"`
	Cycles     int      `json:"cycles"`
	Mode       string   `json:"mode"`
	HandleOf   string   `json:"handle_of"`
	HandleKind string   `json:"handle_kind"`
	// ReadEach is the corpus's instruction to pull every generated subscriber
	// once so its dependency edge is actually registered. This binding builds
	// subscribers as effects, which register eagerly, so it was tempting to call
	// the key satisfied by construction and never read it — which is precisely
	// the vacuous pass the fixture's `why_read_each` note warns about. The read
	// is performed, and the edge it is supposed to register is asserted.
	ReadEach bool `json:"read_each"`
	// Writes is the batch op's write list: an ordered sequence of cell writes
	// applied inside one batch, so the corpus can pin coalescing.
	Writes []reactiveGraphWrite `json:"writes"`
}

// reactiveGraphWrite is one cell write inside a `batch` op.
type reactiveGraphWrite struct {
	ID    string `json:"id"`
	Value int    `json:"value"`
}

type reactiveGraphStep struct {
	Op     reactiveGraphOp            `json:"op"`
	Expect map[string]json.RawMessage `json:"expect"`
}

type reactiveGraphScenario struct {
	Id    string              `json:"id"`
	Name  string              `json:"name"`
	Steps []reactiveGraphStep `json:"steps"`
}

type reactiveGraphFixture struct {
	conformanceMeta
	Shape     string                  `json:"shape"`
	Steps     []reactiveGraphStep     `json:"steps"`
	Scenarios []reactiveGraphScenario `json:"scenarios"`
	Expected  struct {
		ObservationallyEqual []string `json:"observationally_equal"`
		FinalState           struct {
			DependentsOf map[string]int  `json:"dependents_of"`
			Readable     map[string]bool `json:"readable"`
			Read         map[string]int  `json:"read"`
		} `json:"final_state"`
		AfterPublish struct {
			Op           *reactiveGraphOp `json:"op"`
			ObservedBy   []string         `json:"observed_by"`
			Read         map[string]int   `json:"read"`
			DependentsOf map[string]int   `json:"dependents_of"`
		} `json:"after_publish"`
	} `json:"expected"`
}

// observation is everything a scenario leaves behind that
// `observationally_equal` compares.
type observation struct {
	CleanupOrder      []string        `json:"cleanup_order"`
	Readable          map[string]bool `json:"readable"`
	Reads             map[string]int  `json:"reads"`
	AfterPublishRuns  []string        `json:"after_publish_runs"`
	AfterPublishReads map[string]int  `json:"after_publish_reads"`
	Degrees           map[string]int  `json:"degrees"`
}

// replayReport counts what actually executed, so "passed" cannot mean "did
// nothing".
type replayReport struct {
	ops         int
	checks      int
	divergences []string
	obs         observation
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

type replayEngine struct {
	t       *testing.T
	m       graphModel
	fixture string
	label   string // "" for a steps-shaped fixture, "[<scenario>]" otherwise

	nodes map[string]nodeRef
	// stale keeps every handle ever minted so `dispose_stale_handle` can tear
	// down through a handle whose node is long gone.
	stale    map[string]nodeRef
	scopes   map[string]scopeModel
	poisoned map[string]bool

	step int
	rep  replayReport

	// readEachPending holds the subscribers a  op created this step,
	// checked after settle because the async plane registers an effect's edges
	// when it first runs rather than when it is constructed.
	readEachPending []readEachClaim
}

func newReplayEngine(t *testing.T, m graphModel, fixture, label string) *replayEngine {
	return &replayEngine{
		t:        t,
		m:        m,
		fixture:  fixture,
		label:    label,
		nodes:    map[string]nodeRef{},
		stale:    map[string]nodeRef{},
		scopes:   map[string]scopeModel{},
		poisoned: map[string]bool{},
	}
}

func (e *replayEngine) node(id string) nodeRef {
	n, ok := e.nodes[id]
	if !ok {
		e.t.Fatalf("%s: op names unknown node %q", e.fixture, id)
	}
	return n
}

func (e *replayEngine) refs(ids []string) []nodeRef {
	out := make([]nodeRef, 0, len(ids))
	for _, id := range ids {
		out = append(out, e.node(id))
	}
	return out
}

func (e *replayEngine) put(id string, r nodeRef) {
	e.nodes[id] = r
	e.stale[id] = r
	delete(e.poisoned, id)
}

// readID is a top-level read. A non-nil error is the corpus's
// `read_after_dispose`; once a node has errored it stays poisoned, matching a
// live reader that names a disposed dependency and does not recover.
// readID reads through the model, latching disposal so a torn-down id stays
// unreadable without re-entering the graph.
//
// The latch is deliberately NOT applied to a `fail_next` compute failure. Those
// are two different things the corpus must be able to tell apart: disposal is
// permanent by contract, a failed compute is recoverable by contract (the next
// read re-runs the body). Latching both was safe while disposal was the only
// error a read could produce; with `failed_compute_is_never_cached.json` it
// would make the engine report the very defect that fixture exists to catch,
// against bindings that are correct.
func (e *replayEngine) readID(id string) (int, error) {
	if e.poisoned[id] {
		return 0, ErrDisposed
	}
	v, err := e.m.read(e.node(id))
	if err != nil {
		if !errors.Is(err, errComputeFailed) {
			e.poisoned[id] = true
		}
		return 0, err
	}
	return v, nil
}

func (e *replayEngine) check(key string, got, want any) {
	e.rep.checks++
	if fmt.Sprint(got) == fmt.Sprint(want) {
		return
	}
	step := fmt.Sprint(e.step)
	if e.step < 0 {
		step = "expected"
	}
	entry := fmt.Sprintf("%s/%s%s#%s:%s", e.m.name(), e.fixture, e.label, step, key)
	e.t.Logf("  DIVERGENCE %s — got %v, want %v", entry, got, want)
	e.rep.divergences = append(e.rep.divergences, entry)
}

func (e *replayEngine) scope(name string) scopeModel {
	sc, ok := e.scopes[name]
	if !ok {
		e.t.Fatalf("%s: op names unknown scope %q", e.fixture, name)
	}
	return sc
}

// create dispatches a node constructor to the context or to a named scope.
func (e *replayEngine) create(op reactiveGraphOp) nodeRef {
	var target nodeFactory = e.m
	if op.Scope != "" {
		target = e.scope(op.Scope)
	}
	switch op.Type {
	case "cell":
		if op.Value == nil {
			e.t.Fatalf("%s#%d: cell op has no value", e.fixture, e.step)
		}
		return target.cell(op.ID, *op.Value)
	case "computed":
		return target.computed(op.ID, e.refs(op.Reads), op.Offset)
	default:
		return target.effect(op.ID, e.refs(op.Reads))
	}
}

func (e *replayEngine) runOp(op reactiveGraphOp) (opValue *int, opError bool) {
	switch op.Type {
	case "cell", "computed", "effect":
		e.put(op.ID, e.create(op))
	case "read":
		v, err := e.readID(op.ID)
		if err != nil {
			return nil, true
		}
		return &v, false
	case "fail_next":
		e.m.failNext(op.ID, op.Count)
	case "set_cell":
		if op.Value == nil {
			e.t.Fatalf("%s#%d: set_cell op has no value", e.fixture, e.step)
		}
		n := e.node(op.ID)
		if n.kind != kindCell {
			e.t.Fatalf("%s#%d: set_cell on non-cell %q", e.fixture, e.step, op.ID)
		}
		e.m.setCell(n, *op.Value)
	case "signal", "drive":
		// `drive` is the Cell-kernel spelling of `signal`: the eager construction
		// is formula().drive(). Dual-accepted so a corpus emitting either op — the
		// old `signal`/`dispose_signal` or the new `drive`/`undrive` — replays the
		// same driven Computed (design §9.3, spec step 3).
		r := e.m.signal(op.ID, e.refs(op.Reads), op.Offset)
		e.put(op.ID, r)
		// Creation materializes the value (clause 1), so the op has a value to
		// assert on without a separate read step — and reading it here must not
		// itself compute, which is what the fixture's `computes_of` pins.
		v, err := e.m.read(r)
		if err != nil {
			return nil, true
		}
		return &v, false
	case "dispose_signal", "undrive":
		e.m.disposeSignal(e.node(op.ID))
	case "batch":
		e.m.batch(func() {
			for _, w := range op.Writes {
				n := e.node(w.ID)
				if n.kind != kindCell {
					e.t.Fatalf("%s#%d: batch writes non-cell %q", e.fixture, e.step, w.ID)
				}
				e.m.setCell(n, w.Value)
			}
		})
	case "dispose":
		// The entry stays in the map: a disposed id remains readable-as-an-
		// error, and disposing it again must be a no-op.
		e.m.dispose(e.node(op.ID))
	case "fanout":
		// Subscribers are effects, not derived slots: the corpus asserts
		// `observed_count` on a publish, and in a lazy binding only an eager
		// reader observes a publish without being pulled.
		base := e.refs(op.Reads)
		for i := 0; i < op.Count; i++ {
			id := fmt.Sprintf("%s_%d", op.IDPrefix, i)
			e.put(id, e.m.effect(id, base))
			e.readEach(op, id, base)
		}
	case "dispose_fanout":
		for i := 0; i < op.Count; i++ {
			if n, ok := e.nodes[fmt.Sprintf("%s_%d", op.IDPrefix, i)]; ok {
				e.m.dispose(n)
			}
		}
	case "churn":
		e.runChurn(op)
	case "begin_scope":
		e.scopes[op.Scope] = e.m.scope()
	case "end_scope":
		sc := e.scope(op.Scope)
		delete(e.scopes, op.Scope)
		sc.closeScope()
	case "disarm":
		e.scope(op.Scope).disarm()
		// A disarmed scope owns nothing; keeping it in the map is what makes a
		// later `scope_owned_count` and `end_scope` no-ops rather than errors.
	case "dispose_stale_handle":
		h, ok := e.stale[op.HandleOf]
		if !ok {
			e.t.Fatalf("%s#%d: no recorded handle for %q", e.fixture, e.step, op.HandleOf)
		}
		wantKind, known := map[string]nodeKind{
			"cell": kindCell, "slot": kindSlot, "effect": kindEffect,
		}[op.HandleKind]
		if !known || h.kind != wantKind {
			e.t.Fatalf("%s#%d: handle_kind %q does not match the recorded handle",
				e.fixture, e.step, op.HandleKind)
		}
		// In this binding a handle is a pointer to the node, not an index into
		// an arena, so there is no id to recycle and a stale handle can only
		// ever name the node it was minted for. Disposing it again is the
		// idempotence case; the "must not tear down whatever now owns that id"
		// hazard is unreachable by construction rather than guarded against.
		e.m.dispose(h)
	default:
		e.t.Fatalf("%s#%d: unsupported op %q — the fixture is listed as replayable, so "+
			"either implement the op or move the fixture to reactiveGraphUnsupported "+
			"with the op named", e.fixture, e.step, op.Type)
	}
	return nil, false
}

// readEach discharges the corpus's `read_each` instruction.
//
// The key says: pull every generated subscriber once so its dependency edge is
// actually registered, because "a lazy binding that never pulls the slot
// registers no dependency and would pass a weaker version of this fixture
// vacuously". This binding builds subscribers as effects, which run — and so
// register — at construction, and an effect is not readable through the model's
// read seam. The instruction therefore cannot be discharged as a literal pull;
// what it exists to establish can be, and is: the subscriber is subscribed. That
// is asserted here rather than assumed, so the eager construction is checked
// instead of being taken as a reason to skip the key.
func (e *replayEngine) readEach(op reactiveGraphOp, id string, reads []nodeRef) {
	if !op.ReadEach {
		return
	}
	// Churn reuses a fixed set of ids across its cycles, so claims are deduped by
	// id: 500 cycles over 8 live subscribers is 8 distinct claims, not 500
	// repetitions of the same check.
	for i, c := range e.readEachPending {
		if c.id == id {
			if len(reads) > c.deps {
				e.readEachPending[i].deps = len(reads)
			}
			return
		}
	}
	e.readEachPending = append(e.readEachPending, readEachClaim{id: id, deps: len(reads)})
}

// readEachClaim is one `read_each` subscriber awaiting its post-settle check.
type readEachClaim struct {
	id   string
	deps int
}

// checkReadEach evaluates the pending `read_each` claims. It runs after settle
// because the async plane registers an effect's edges when it first runs, not
// when it is constructed.
func (e *replayEngine) checkReadEach() {
	pending := e.readEachPending
	e.readEachPending = nil
	for _, claim := range pending {
		n, ok := e.nodes[claim.id]
		if !ok {
			e.t.Fatalf("%s#%d: read_each: no node %q", e.fixture, e.step, claim.id)
		}
		if n.kind != kindEffect {
			if _, err := e.m.read(n); err != nil {
				e.t.Fatalf("%s#%d: read_each: reading %q failed: %v", e.fixture, e.step, claim.id, err)
			}
		} else if !e.m.isEffectActive(n) {
			e.t.Fatalf("%s#%d: read_each: subscriber %q is not active, so it pulled nothing",
				e.fixture, e.step, claim.id)
		}
		if got := e.m.dependencyCount(n); got < claim.deps {
			e.t.Fatalf("%s#%d: read_each: %q holds %d dependencies, want at least %d "+
				"— a subscriber that pulls nothing subscribes to nothing",
				e.fixture, e.step, claim.id, got, claim.deps)
		}
	}
}

func (e *replayEngine) runChurn(op reactiveGraphOp) {
	source := e.node(op.Source)
	switch op.Mode {
	case "dispose_then_create":
		// Hold live_width subscribers; each cycle disposes one and creates its
		// replacement, so the live count is invariant.
		for c := 0; c < op.Cycles; c++ {
			id := fmt.Sprintf("%s_%d", op.IDPrefix, c%op.LiveWidth)
			if n, ok := e.nodes[id]; ok {
				e.m.dispose(n)
			}
			e.put(id, e.m.effect(id, []nodeRef{source}))
			e.readEach(op, id, []nodeRef{source})
		}
	case "scope_per_cycle":
		// One teardown scope per cycle; its subscriber is gone by the end of
		// its own cycle.
		name := op.IDPrefix + "_scoped"
		for c := 0; c < op.Cycles; c++ {
			sc := e.m.scope()
			sc.effect(name, []nodeRef{source})
			sc.closeScope()
		}
	default:
		e.t.Fatalf("%s#%d: unknown churn mode %q", e.fixture, e.step, op.Mode)
	}
}

func (e *replayEngine) replay(steps []reactiveGraphStep) {
	for i, step := range steps {
		e.step = i
		runsBefore := len(e.m.runLog())

		opValue, opError := e.runOp(step.Op)
		e.rep.ops++
		e.m.settle()
		e.checkReadEach()

		observed := e.m.runLog()[runsBefore:]
		// `cleanup_order` is cumulative, not per-step: the individual-disposal
		// scenario spreads three disposals over three steps and pins the whole
		// order on the last one.
		cleaned := e.m.cleanupLog()

		// Assertion keys are evaluated in sorted order, matching lazily-rs's
		// BTreeMap iteration, so both bindings agree on when a degree is
		// sampled relative to a read that re-registers the edge it counts.
		for _, key := range sortedRawKeys(step.Expect) {
			raw := step.Expect[key]
			switch key {
			case "note":
				// Prose for humans, not an assertion.
			case "value":
				if _, hasErr := step.Expect["error"]; hasErr {
					continue // an errored read has no value
				}
				var want int
				e.unmarshal(key, raw, &want)
				if opValue == nil {
					e.check("value", "<no value>", want)
					continue
				}
				e.check("value", *opValue, want)
			case "error":
				var want *string
				e.unmarshal(key, raw, &want)
				// Any non-null error code means "this op must fail"; null means
				// "must not". The runner does not model error identity — the
				// fixtures carry the code so the contract is legible, and each
				// binding's own tests pin which error type it raises.
				e.check("error", opError, want != nil)
			case "read":
				var want map[string]int
				e.unmarshal(key, raw, &want)
				for _, id := range sortedIntKeys(want) {
					got, err := e.readID(id)
					if err != nil {
						e.check("read."+id, "error:"+err.Error(), want[id])
						continue
					}
					e.check("read."+id, got, want[id])
				}
			case "readable":
				var want map[string]bool
				e.unmarshal(key, raw, &want)
				for _, id := range sortedBoolKeys(want) {
					e.check("readable."+id, e.readable(id), want[id])
				}
			case "computes_of":
				var want map[string]int
				e.unmarshal(key, raw, &want)
				for _, id := range sortedIntKeys(want) {
					e.check("computes_of."+id, e.m.computesOf(id), want[id])
				}
			case "dependents_of":
				var want map[string]int
				e.unmarshal(key, raw, &want)
				for _, id := range sortedIntKeys(want) {
					e.check("dependents_of."+id, e.m.dependentCount(e.node(id)), want[id])
				}
			case "dependencies_of":
				var want map[string]int
				e.unmarshal(key, raw, &want)
				for _, id := range sortedIntKeys(want) {
					e.check("dependencies_of."+id, e.m.dependencyCount(e.node(id)), want[id])
				}
			case "observed_by":
				var want []string
				e.unmarshal(key, raw, &want)
				e.check("observed_by", normalizeList(observed), normalizeList(want))
			case "observed_count":
				var want int
				e.unmarshal(key, raw, &want)
				e.check("observed_count", len(observed), want)
			case "cleanup_order":
				var want []string
				e.unmarshal(key, raw, &want)
				// Only effects run a cleanup callback, so the expected order is
				// projected onto its effect entries.
				proj := make([]string, 0, len(want))
				for _, id := range want {
					if h, ok := e.stale[id]; ok && h.kind == kindEffect {
						proj = append(proj, id)
					}
				}
				e.check("cleanup_order", normalizeList(cleaned), normalizeList(proj))
			case "scope_owned_count":
				var want map[string]int
				e.unmarshal(key, raw, &want)
				for _, name := range sortedIntKeys(want) {
					e.check("scope_owned_count."+name, e.scope(name).owned(), want[name])
				}
			default:
				e.t.Fatalf("%s#%d: unsupported assertion %q — the fixture is listed as "+
					"replayable, so either implement the assertion or move the fixture to "+
					"reactiveGraphUnsupported with the key named", e.fixture, i, key)
			}
		}
	}
}

// readable mirrors the corpus's notion of liveness: an effect is readable while
// it is active, any other node while a read of it succeeds.
func (e *replayEngine) readable(id string) bool {
	n, ok := e.nodes[id]
	if !ok {
		return false
	}
	if n.kind == kindEffect {
		return e.m.isEffectActive(n)
	}
	_, err := e.readID(id)
	return err == nil
}

// evaluateTail checks the `scenarios` shape's `expected` block against the final
// world state and records the observation the equality relation compares.
//
// The key order here is fixed rather than sorted, and deliberately so: the
// after-publish `read` must run before its `dependents_of`, because this
// binding's cascade consumes the reverse edge and the read is what re-registers
// it. Sampling the degree first would measure the middle of a cascade.
func (e *replayEngine) evaluateTail(fx *reactiveGraphFixture) {
	e.step = -1
	fin := fx.Expected.FinalState
	e.rep.obs.Degrees = map[string]int{}
	e.rep.obs.Readable = map[string]bool{}
	e.rep.obs.Reads = map[string]int{}
	e.rep.obs.AfterPublishReads = map[string]int{}
	e.rep.obs.AfterPublishRuns = []string{}

	for _, id := range sortedIntKeys(fin.DependentsOf) {
		got := e.m.dependentCount(e.node(id))
		e.check("final.dependents_of."+id, got, fin.DependentsOf[id])
		e.rep.obs.Degrees[id] = got
	}
	for _, id := range sortedBoolKeys(fin.Readable) {
		got := e.readable(id)
		e.check("final.readable."+id, got, fin.Readable[id])
		e.rep.obs.Readable[id] = got
	}
	for _, id := range sortedIntKeys(fin.Read) {
		got, err := e.readID(id)
		if err != nil {
			e.check("final.read."+id, "error:"+err.Error(), fin.Read[id])
			continue
		}
		e.check("final.read."+id, got, fin.Read[id])
		e.rep.obs.Reads[id] = got
	}

	pub := fx.Expected.AfterPublish
	if pub.Op != nil {
		n := e.node(pub.Op.ID)
		if n.kind != kindCell || pub.Op.Value == nil {
			e.t.Fatalf("%s: after_publish must be a set_cell on a cell", e.fixture)
		}
		before := len(e.m.runLog())
		e.m.setCell(n, *pub.Op.Value)
		e.m.settle()
		e.rep.obs.AfterPublishRuns = normalizeList(e.m.runLog()[before:])
		e.check("after_publish.observed_by", e.rep.obs.AfterPublishRuns, normalizeList(pub.ObservedBy))
		for _, id := range sortedIntKeys(pub.Read) {
			got, err := e.readID(id)
			if err != nil {
				e.check("after_publish.read."+id, "error:"+err.Error(), pub.Read[id])
				continue
			}
			e.check("after_publish.read."+id, got, pub.Read[id])
			e.rep.obs.AfterPublishReads[id] = got
		}
		for _, id := range sortedIntKeys(pub.DependentsOf) {
			e.check("after_publish.dependents_of."+id,
				e.m.dependentCount(e.node(id)), pub.DependentsOf[id])
		}
	}
	e.rep.obs.CleanupOrder = normalizeList(e.m.cleanupLog())
}

func (e *replayEngine) unmarshal(key string, raw json.RawMessage, out any) {
	if err := json.Unmarshal(raw, out); err != nil {
		e.t.Fatalf("%s#%d: assertion %q is malformed: %v", e.fixture, e.step, key, err)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func normalizeList(s []string) []string {
	if len(s) == 0 {
		return []string{}
	}
	return s
}

func sortedRawKeys(m map[string]json.RawMessage) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func sortedIntKeys(m map[string]int) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func sortedBoolKeys(m map[string]bool) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func reactiveGraphLedgerKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
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

	// Report any remaining gaps loudly, once, rather than skipping in silence.
	for _, f := range reactiveGraphLedgerKeys(reactiveGraphUnsupported) {
		t.Logf("UNSUPPORTED reactive-graph/%s — %s", f, reactiveGraphUnsupported[f])
	}
	for _, k := range reactiveGraphLedgerKeys(reactiveGraphModelUnsupported) {
		t.Logf("UNSUPPORTED %s — %s", k, reactiveGraphModelUnsupported[k])
		if f := k[strings.Index(k, "/")+1:]; !onDisk[f] {
			t.Errorf("per-model ledger entry %s names a fixture missing from %s — stale entry",
				k, reactiveGraphSpecDir)
		}
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
	observedDivergences := map[string]bool{}

	for _, mdl := range models {
		replayedPerModel[mdl.name] = map[string]bool{}
		t.Run(mdl.name, func(t *testing.T) {
			totalOps, totalChecks := 0, 0
			for _, name := range reactiveGraphReplayed {
				if _, gated := reactiveGraphModelUnsupported[mdl.name+"/"+name]; gated {
					continue
				}
				t.Run(name, func(t *testing.T) {
					raw, err := specReadFile(filepath.Join(reactiveGraphSpecDir, name))
					if err != nil {
						t.Fatalf("reading fixture %s: %v", name, err)
					}
					var fx reactiveGraphFixture
					mustStrictJSON(t, name, raw, &fx)

					// Dispatch on the fixture's declared `shape`, never on its
					// filename: a filename special case goes stale silently the
					// moment a second scenarios-shaped fixture appears.
					var reports []replayReport
					switch fx.Shape {
					case "steps":
						m := mdl.make()
						defer m.close()
						e := newReplayEngine(t, m, name, "")
						e.replay(fx.Steps)
						reports = append(reports, e.rep)
					case "scenarios":
						// `observationally_equal` is a relation between two op
						// streams, which a single `steps` array cannot express.
						// Each scenario is replayed in its own context and the
						// resulting observations are compared.
						for index, sc := range fx.Scenarios {
							// Rung 4 (#lzscenariocoverage): record at the point
							// of replay. The corpus is replayed once per
							// execution model, so each id records twice; the
							// ledger deduplicates.
							recordScenarioAt(filepath.Join(reactiveGraphSpecDir, name), index, sc.Id, sc.Name)
							m := mdl.make()
							e := newReplayEngine(t, m, name, "["+sc.Name+"]")
							e.replay(sc.Steps)
							e.evaluateTail(&fx)
							reports = append(reports, e.rep)
							m.close()
						}
						reports = append(reports,
							checkObservationalEquality(t, mdl.name, name, &fx, reports))
					case "":
						t.Fatalf("%s: fixture declares no `shape`", name)
					default:
						t.Fatalf("%s: unknown fixture shape %q", name, fx.Shape)
					}

					ops, checks := 0, 0
					for _, r := range reports {
						ops += r.ops
						checks += r.checks
						for _, d := range r.divergences {
							observedDivergences[d] = true
						}
					}
					if ops == 0 {
						t.Fatalf("%s[%s]: replayed zero ops", name, mdl.name)
					}
					if checks == 0 {
						t.Fatalf("%s[%s]: replayed zero assertions", name, mdl.name)
					}
					t.Logf("reactive-graph[%s] %s: %d ops, %d assertions", mdl.name, name, ops, checks)
					totalOps += ops
					totalChecks += checks
					replayedPerModel[mdl.name][name] = true
				})
			}
			t.Logf("reactive-graph[%s]: %d fixtures, %d ops, %d assertions",
				mdl.name, len(replayedPerModel[mdl.name]), totalOps, totalChecks)
		})
	}

	for _, mdl := range models {
		got := len(replayedPerModel[mdl.name])
		if got == 0 {
			t.Fatalf("%s: replayed zero reactive-graph fixtures — the runner executed nothing",
				mdl.name)
		}
		gated := 0
		for k := range reactiveGraphModelUnsupported {
			if strings.HasPrefix(k, mdl.name+"/") {
				gated++
			}
		}
		if want := len(reactiveGraphReplayed) - gated; got != want {
			t.Errorf("%s: replayed %d of %d reactive-graph fixtures (%d gated per-model)",
				mdl.name, got, want, gated)
		}
	}

	// Divergence ledger: the observed set must equal the documented one, so a
	// new divergence fails the build and a fixed one forces its entry deleted.
	// Neither direction can pass unnoticed.
	for d := range observedDivergences {
		if !reactiveGraphDivergences[d] {
			t.Errorf("undocumented divergence %s — this is a finding against lazily-go; "+
				"fix it or record it in reactiveGraphDivergences (never edit the fixture)", d)
		}
	}
	for d := range reactiveGraphDivergences {
		if !observedDivergences[d] {
			t.Errorf("documented divergence %s no longer reproduces — delete the stale "+
				"reactiveGraphDivergences entry", d)
		}
	}
}

// checkObservationalEquality asserts that the scenarios named by
// `observationally_equal` agree on every observable, not merely that each
// satisfies `expected` independently. That relation is the whole point of the
// `scenarios` shape and cannot be expressed as a step assertion.
func checkObservationalEquality(
	t *testing.T, model, fixture string, fx *reactiveGraphFixture, reports []replayReport,
) replayReport {
	t.Helper()
	var rep replayReport
	if len(fx.Expected.ObservationallyEqual) < 2 {
		return rep
	}
	byName := map[string]int{}
	for i, sc := range fx.Scenarios {
		byName[sc.Name] = i
	}
	first := -1
	for _, want := range fx.Expected.ObservationallyEqual {
		idx, ok := byName[want]
		if !ok {
			t.Fatalf("%s: `observationally_equal` names unknown scenario %q", fixture, want)
		}
		if first < 0 {
			first = idx
			continue
		}
		rep.checks++
		a, b := mustJSON(t, reports[first].obs), mustJSON(t, reports[idx].obs)
		if a != b {
			t.Errorf("%s[%s]: scenarios %q and %q are not observationally equal\n  %s\n  %s",
				fixture, model, fx.Scenarios[first].Name, fx.Scenarios[idx].Name, a, b)
		}
	}
	return rep
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshalling observation: %v", err)
	}
	return string(b)
}
