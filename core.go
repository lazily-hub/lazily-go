// Package lazily provides lazy reactive primitives for Go: Slot -> Cell ->
// Signal, plus the lazily-spec wire protocol, CRDT collection types, keyed cell
// collections, state machines/charts, and the distributed CRDT plane.
//
// A Go port of the lazily reactive family (lazily-rs, lazily-py, lazily-kt,
// lazily-js, lazily-dart, lazily-zig), conformant with lazily-spec and
// lazily-formal.
//
// The reactive family:
//
//   - Cell[T]        — the read genus (an interface): every value-bearing node
//     satisfies it. Go cannot restrict methods by type parameter, so the genus is
//     an interface and the two kinds below are structs (design §4).
//   - SourceCell[T]  — a value written from outside; the only kind with Set/Merge.
//     Folds writes under a MergePolicy (KeepLatest by default = a plain cell;
//     Sum/Max = the former MergeCell). Constructors: NewSourceCell / source(v).
//   - FormulaCell[T] — a value computed from upstream; lazily cached and
//     dependency-tracking, with neither Set nor Merge. Formula(f) is guarded by
//     default. formula.Drive() makes it eager (the former Signal), returning the
//     same handle.
//
// Values are lazy by default: dependents are marked dirty on invalidation but
// only recompute when read. For eager push-style semantics, Drive a FormulaCell.
//
// A Context is the shared scope: the computation stack used for automatic
// dependency tracking (cached slot values live on the nodes themselves, not in
// a shared Context table, so reads stay O(1) regardless of graph size). All
// reactives that
// react to each other must share a Context. Context is not safe for concurrent
// use by multiple goroutines; use ThreadSafeContext (see thread_safe.go) or the
// channel-serialized AsyncContext (see async_context.go) for concurrent access.
package lazily

// reactiveNode is the internal interface implemented by every node that
// participates in dependency tracking (Slot, Cell, Signal, Effect, Memo).
type reactiveNode interface {
	// node returns the embedded dependency-tracking state.
	node() *reactiveBase
	// onInvalidate is the hook run when this node is marked stale by the
	// frontier walk, before the walk descends to its dependents.
	onInvalidate()
	// invalidate marks this node and its transitive dependent cone stale. It
	// does not consume edges and computes nothing; see markCone.
	invalidate()
	// refresh brings a stale node up to date at pull time and reports whether
	// its value actually CHANGED. Non-value-bearing nodes (Cell, Effect,
	// Signal) are never stale and always report false.
	refresh() bool
}

// reactiveBase holds the bidirectional dependency edges shared by every
// reactive node. Edges are refreshed on every recompute:
//
//   - dependents   — nodes that read this one (downstream).
//   - dependencies — nodes this one read during its last computation (upstream).
//
// self is the concrete reactiveNode embedding this base, used to route
// polymorphic calls (onInvalidate) and to key the edge sets/cache by node
// identity.
type reactiveBase struct {
	dependents   map[reactiveNode]struct{}
	dependencies map[reactiveNode]struct{}
	self         reactiveNode

	// disposed marks a node torn down by Dispose (or by a TeardownScope).
	// A disposed node has no edges in either direction, reads as an error,
	// and reports zero degree. See disposal.go.
	disposed bool

	// dirty and forceRecompute are the two staleness levels of the
	// mark-frontier walk, mirroring lazily-rs's SlotNode.{dirty,force_recompute}.
	//
	//   dirty          — "check me": something in my ancestry changed, but it
	//                    may recompute to an equal value, so my own cached
	//                    value is not yet known to be wrong.
	//   forceRecompute — "I am definitely wrong": I read the node that was
	//                    actually written, so I must recompute.
	//
	// Direct dependents of a written node become force roots; every deeper
	// node is merely dirty. A dirty-but-not-forced node is resolved at pull
	// time by refreshing its dependencies: if none of them report a changed
	// value, it is marked clean WITHOUT recomputing (the equality guard's
	// downstream suppression). Only value-bearing nodes (Slot, Memo) carry
	// these; a Cell is a source and an Effect is a sink.
	dirty          bool
	forceRecompute bool
	// slotIndex is this node's position in Context.slots, or -1 when the node
	// is not value-bearing. Disposal swap-removes by this index so a
	// subscribe/unsubscribe workload does not grow the registry without bound
	// (#lzspecedgeindex).
	slotIndex int
}

func newReactiveBase() reactiveBase {
	return reactiveBase{
		dependents:   map[reactiveNode]struct{}{},
		dependencies: map[reactiveNode]struct{}{},
		slotIndex:    -1,
	}
}

func (b *reactiveBase) node() *reactiveBase { return b }

// onInvalidate is a no-op default; concrete nodes override it.
func (b *reactiveBase) onInvalidate() {}

// track registers the currently-computing node (if any) as a dependent of this
// node, and records the reverse edge. Called whenever this node is read.
func (b *reactiveBase) track(ctx *Context) {
	parent := ctx.current()
	if parent != nil && parent != b.self {
		if _, ok := b.dependents[parent]; !ok {
			b.dependents[parent] = struct{}{}
			parent.node().dependencies[b.self] = struct{}{}
		}
	}
}

// detachUpstream detaches this node from all of its current upstream
// dependencies. Called before a recompute so edges reflect only the most recent
// computation.
func (b *reactiveBase) detachUpstream() {
	for dep := range b.dependencies {
		delete(dep.node().dependents, b.self)
	}
	clear(b.dependencies)
}

// refresh is the default pull-time hook: a node that bears no value is never
// stale and never reports a change.
func (b *reactiveBase) refresh() bool { return false }

// markEntry is one node on the frontier walk's explicit stack, paired with the
// staleness level to apply to it.
type markEntry struct {
	node  reactiveNode
	force bool
}

// invalidate marks this node's transitive dependent cone stale.
//
// This is a NON-CONSUMING mark-frontier walk (the lazily-rs model, see
// Context::mark_frontier_locked). It reads `dependents` and leaves every edge
// in place; nothing is recomputed and no value is produced. It is what makes
// pull-time suppression possible: a dependent can be marked clean again
// WITHOUT recomputing and still be reachable from its source, so the next
// genuine change is not lost at depth two.
//
// The walk terminates on a staleness short-circuit rather than on edge
// consumption: a node already at or above the level being applied does not
// need its cone re-walked, because that cone was marked when it was first
// reached and nothing has cleaned it since (a dependent can only be cleaned
// through refresh, which refreshes — and so cleans — all of its dependencies).
//
// A source (Cell) carries no staleness of its own, so its DIRECT dependents
// are the force roots. A value-bearing node invalidated directly (a teardown
// survivor, Cell.Invalidate) is itself a force root.
func (b *reactiveBase) invalidate() {
	if _, markable := b.self.(cacheable); markable {
		markCone([]markEntry{{node: b.self, force: true}}, true)
		return
	}
	b.self.onInvalidate()
	b.markDependents(true)
}

// markDependents force-marks this node's direct dependents and dirties
// everything below them, leaving this node itself alone. It is the walk used
// when a node's own value has just been established as changed: by a Cell
// write, or by a pull-time recompute that did not compare equal.
//
// scheduleEffects is false on the pull-time path. An effect reached there was
// already reached by the write's own cascade — a node only becomes stale
// through that cascade — so re-scheduling it would run an effect a second time
// merely because one of its dependencies happened to recompute while the
// effect itself was mid-flush.
func (b *reactiveBase) markDependents(scheduleEffects bool) {
	if len(b.dependents) == 0 {
		return
	}
	stack := make([]markEntry, 0, len(b.dependents))
	for d := range b.dependents {
		stack = append(stack, markEntry{node: d, force: true})
	}
	markCone(stack, scheduleEffects)
}

// markCone is the iterative frontier walk shared by every invalidation path.
// Roots arrive at the level their caller chose; every node reached from a root
// is merely dirty, never forced.
func markCone(stack []markEntry, scheduleEffects bool) {
	for len(stack) > 0 {
		entry := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n := entry.node
		nb := n.node()
		if nb.disposed {
			continue
		}
		if eff, isEffect := n.(*Effect); isEffect {
			// Effects are sinks: they are scheduled (or, during teardown,
			// deliberately not), never marked, and never descended through.
			if scheduleEffects {
				eff.markStale(entry.force)
			}
			continue
		}
		propagate := true
		if _, markable := n.(cacheable); markable {
			// Re-walk only when this node learns something new: it was clean,
			// or it is being raised from dirty to forced.
			propagate = !nb.dirty || (entry.force && !nb.forceRecompute)
			if entry.force {
				nb.forceRecompute = true
			}
		}
		n.onInvalidate()
		if !propagate {
			continue
		}
		for d := range nb.dependents {
			stack = append(stack, markEntry{node: d, force: false})
		}
	}
}

// cacheable is a slot-like node that stores its own memoized value and can be
// asked to drop it. Cached values live ON the node (see Slot), not in a shared
// Context-owned map — so a read touches only the node it reads, and read
// latency is independent of the total graph size (no probing a whole-graph hash
// table). The Context keeps a registry of these purely so Clear/Size stay O(1)
// on the hot path and correct; the registry is never touched by a read.
type cacheable interface {
	clearCache()
	cachedNow() bool
	// node is what makes this interface unexported-sealed and lets the
	// registry swap-remove a disposed entry in O(1).
	node() *reactiveBase
}

// Context is a reactive scope: the computation stack for automatic dependency
// tracking plus batch/effect scheduling. Cached slot values are stored on the
// nodes themselves, not here. All Slots, Cells, and Signals that should react to
// each other must be created with (and thus share) the same Context.
//
// Context is not safe for concurrent use. Wrap it with ThreadSafeContext for
// lock-backed concurrency, or drive it from a single goroutine via AsyncContext.
type Context struct {
	stack []reactiveNode

	slots       []cacheable // registry of value-bearing slots (for Clear/Size)
	cachedCount int         // number of slots currently holding a cached value

	batchDepth       int
	batchedCells     map[reactiveNode]struct{}
	pendingEffects   []*Effect
	effectsHead      int
	scheduledEffects map[*Effect]struct{}
	flushingEffects  bool

	// disposing is the teardown depth. While it is non-zero the invalidation
	// cascade marks nodes dirty but must not *run* anything: an effect rerun or
	// an eager (driven FormulaCell) recompute during teardown re-enters a compute
	// that reads the node being disposed, which breaks teardown idempotence. The
	// contract is "errors on next recompute", not "errors during the dispose
	// call". See disposal.go.
	disposing int

	// drivenBy is the eager side table (design §9.3.3): it maps a driven
	// FormulaCell's node to the puller Effect that keeps it materialized. Off the
	// node so a lazy formula costs nothing; one entry per driven formula. Keyed by
	// node identity (a pointer), and cleared on undrive/dispose so it never
	// aliases a stale node — Go's pointer identity means there is no recycled-id
	// hazard here (cf. the generation-tagged SlotId the arena bindings need).
	drivenBy map[reactiveNode]*Effect
}

// NewContext creates an empty reactive scope.
func NewContext() *Context {
	return &Context{
		batchedCells:     map[reactiveNode]struct{}{},
		scheduledEffects: map[*Effect]struct{}{},
		drivenBy:         map[reactiveNode]*Effect{},
	}
}

// registerSlot records a value-bearing slot so Clear can reset it. Called once
// per slot at construction; not on any read path.
func (c *Context) registerSlot(s cacheable) {
	s.node().slotIndex = len(c.slots)
	c.slots = append(c.slots, s)
}

// unregisterSlot swap-removes a disposed slot from the registry in O(1).
func (c *Context) unregisterSlot(b *reactiveBase) {
	i := b.slotIndex
	if i < 0 || i >= len(c.slots) {
		return
	}
	last := len(c.slots) - 1
	moved := c.slots[last]
	c.slots[i] = moved
	moved.node().slotIndex = i
	c.slots[last] = nil
	c.slots = c.slots[:last]
	b.slotIndex = -1
}

// Size reports the number of slots currently holding a cached value.
func (c *Context) Size() int { return c.cachedCount }

// Clear drops every cached slot value. Dependency edges are re-established
// lazily as slots are read again. Cell values are unaffected.
func (c *Context) Clear() {
	for _, s := range c.slots {
		s.clearCache()
	}
	c.cachedCount = 0
}

func (c *Context) current() reactiveNode {
	if len(c.stack) == 0 {
		return nil
	}
	return c.stack[len(c.stack)-1]
}

func (c *Context) push(n reactiveNode) { c.stack = append(c.stack, n) }
func (c *Context) pop()                { c.stack = c.stack[:len(c.stack)-1] }

// IsBatching reports whether a Batch is currently active.
func (c *Context) IsBatching() bool { return c.batchDepth > 0 }

// Batch runs fn inside a batch. Cell writes inside the batch defer their
// invalidation cascades until the outermost batch exits, at which point a single
// coalesced cascade fires and pending Effects flush once. Re-entrant.
func (c *Context) Batch(fn func()) {
	c.batchDepth++
	defer func() {
		c.batchDepth--
		if c.batchDepth == 0 {
			c.flushBatch()
		}
	}()
	fn()
}

func (c *Context) cellChanged(cell reactiveNode) {
	if c.batchDepth > 0 {
		c.batchedCells[cell] = struct{}{}
	} else {
		cell.invalidate()
		c.flushEffects()
	}
}

func (c *Context) flushBatch() {
	if len(c.batchedCells) == 0 {
		c.flushEffects()
		return
	}
	cells := make([]reactiveNode, 0, len(c.batchedCells))
	for cell := range c.batchedCells {
		cells = append(cells, cell)
	}
	clear(c.batchedCells)
	for _, cell := range cells {
		cell.invalidate()
	}
	c.flushEffects()
}

func (c *Context) scheduleEffect(e *Effect) {
	if _, ok := c.scheduledEffects[e]; !ok {
		c.scheduledEffects[e] = struct{}{}
		c.pendingEffects = append(c.pendingEffects, e)
	}
}

func (c *Context) removePendingEffect(e *Effect) {
	delete(c.scheduledEffects, e)
	for i := c.effectsHead; i < len(c.pendingEffects); i++ {
		if c.pendingEffects[i] == e {
			c.pendingEffects[i] = nil
			return
		}
	}
}

func (c *Context) flushEffects() {
	if c.flushingEffects {
		return
	}
	c.flushingEffects = true
	defer func() { c.flushingEffects = false }()
	for c.effectsHead < len(c.pendingEffects) {
		e := c.pendingEffects[c.effectsHead]
		c.pendingEffects[c.effectsHead] = nil
		c.effectsHead++
		if e == nil {
			continue
		}
		delete(c.scheduledEffects, e)
		force := e.forceRun
		e.forceRun = false
		if !force && e.active && !e.dependenciesChanged() {
			// Every dependency recomputed to an equal value (a Memo guard
			// upstream suppressed the change), so there is nothing new to
			// react to.
			continue
		}
		e.rerun()
	}
	c.pendingEffects = c.pendingEffects[:0]
	c.effectsHead = 0
}

// FormulaCell is a lazy, cached, dependency-tracking computation.
//
// Get returns the cached value if present; otherwise it computes the value
// (tracking every Cell, Signal, or FormulaCell read during computation as a
// dependency), caches it, and returns it. When any dependency changes, the
// cached value is invalidated and the next Get recomputes.
//
// The cached value lives on the FormulaCell itself (value/cached fields), not in a
// shared Context map — so a Get is a direct field read on the node you already
// hold, and read latency does not grow with the total number of nodes.
// The cache is three-state, which pull-time checking requires and a lone
// `cached bool` cannot express:
//
//	hasValue=false                — no value has ever been computed.
//	hasValue=true,  cached=false  — a PREVIOUS value is still held, but it is
//	                                stale. The value is kept precisely so a
//	                                recompute can compare against it (the
//	                                equality guard) instead of blindly
//	                                cascading.
//	hasValue=true,  cached=true   — the value is current.
//
// `cached` keeps its original meaning — "this value is current" — so Peek,
// cachedNow, Context.Size, and every collection layer built on them are
// unchanged. `hasValue` is the added state.
type FormulaCell[T any] struct {
	reactiveBase
	ctx     *Context
	compute func(ctx *Context) T
	value   T    // last computed value (valid iff hasValue)
	cached  bool // whether value is current
	// hasValue reports whether value holds a previous computation, current or
	// not. Invariant: cached implies hasValue.
	hasValue bool
	// equals is the optional equality guard. When set (Memo) and a recompute
	// yields a value equal to the previous one, the node reports "unchanged"
	// and its dependents are left holding their caches. nil (plain Slot) means
	// every recompute counts as a change. Mirrors lazily-rs SlotNode.equals.
	equals func(prev, next T) bool
	// refreshing guards the pull-time dependency walk against a cyclic graph
	// re-entering the same node.
	refreshing bool
	// driven is the eager bit (design §9.3.3): a driven FormulaCell has a puller
	// Effect keeping it materialized. The bit alone makes Drive idempotent with
	// no lookup; the puller itself lives in Context.drivenBy, off the node, so an
	// undriven formula costs nothing. This replaces the old Signal type — eager
	// is a state a formula is in, not a separate kind.
	driven bool
	Name   string
}

// NewFormulaCell creates a lazy slot bound to ctx.
func NewFormulaCell[T any](ctx *Context, compute func(ctx *Context) T) *FormulaCell[T] {
	s := &FormulaCell[T]{reactiveBase: newReactiveBase(), ctx: ctx, compute: compute}
	s.self = s
	ctx.registerSlot(s)
	return s
}

// NewNamedFormulaCell creates a lazy slot with a debug name.
func NewNamedFormulaCell[T any](ctx *Context, name string, compute func(ctx *Context) T) *FormulaCell[T] {
	s := NewFormulaCell(ctx, compute)
	s.Name = name
	return s
}

// Get reads (and caches if needed) the value.
//
// Panics with a *DisposedError if this slot has been disposed. Use TryGet for
// the checked form; see disposal.go for why a read of a torn-down node is a
// panic rather than a returned error.
func (s *FormulaCell[T]) Get() T {
	if s.disposed {
		panic(&DisposedError{Name: s.Name, Kind: "slot"})
	}
	s.track(s.ctx)
	if s.cached {
		return s.value
	}
	s.refresh()
	return s.value
}

// refresh brings this slot up to date and reports whether its value CHANGED.
//
// This is the pull half of the model (lazily-rs Context::refresh_slot). A
// stale slot first refreshes its dependencies. If none of them actually
// changed value, the slot is marked clean WITHOUT recomputing — that is the
// suppression a Memo's equality guard buys its whole downstream cone, and it
// is decided per-dependency, so a slot with a second, genuinely-changed
// dependency still recomputes.
//
// Only a forced slot (one that directly read the written node) skips the
// dependency check, because for it "something upstream changed" is already
// known.
func (s *FormulaCell[T]) refresh() bool {
	if s.disposed || s.cached {
		return false
	}
	if s.refreshing {
		return false // cyclic graph: treat the back-edge as unchanged
	}
	dependencyChanged := false
	if s.hasValue && !s.forceRecompute {
		deps := make([]reactiveNode, 0, len(s.dependencies))
		for d := range s.dependencies {
			deps = append(deps, d)
		}
		s.refreshing = true
		for _, d := range deps {
			if d.refresh() {
				dependencyChanged = true
			}
		}
		s.refreshing = false
		if !dependencyChanged {
			// Nothing upstream moved: the cached value is still correct.
			s.markClean()
			return false
		}
	}
	return s.recomputeNow()
}

// markClean records that this slot's existing value is current again, without
// running its compute.
func (s *FormulaCell[T]) markClean() {
	s.dirty = false
	s.forceRecompute = false
	if !s.cached {
		s.cached = true
		s.ctx.cachedCount++
	}
}

// recomputeNow runs the compute, refreshes the dependency edges, and reports
// whether the value changed. A first-ever computation reports false: nothing
// downstream can be holding a stale value of a slot that never had one.
func (s *FormulaCell[T]) recomputeNow() bool {
	s.detachUpstream()
	s.ctx.push(s.self)
	v := s.compute(s.ctx)
	s.ctx.pop()

	hadValue := s.hasValue
	unchanged := hadValue && s.equals != nil && s.equals(s.value, v)
	s.markClean()
	if unchanged {
		return false
	}
	s.value = v
	s.hasValue = true
	if !hadValue {
		// First computation: nothing downstream can be holding a stale value.
		return false
	}
	// The value really moved, so every dependent must recompute rather than
	// merely re-check. Raising them from dirty to forced here is what makes
	// the pull walk order-independent: a dependent that refreshes its
	// dependencies in an unlucky order — reaching this slot after some other
	// path already refreshed and cleaned it — would otherwise see "no
	// dependency changed" and keep a stale cache. lazily-rs raises them the
	// same way (notify_slot_value_changed from recompute_slot_now).
	s.markDependents(false)
	return true
}

// Peek returns the cached value without recomputing, and whether it was cached.
func (s *FormulaCell[T]) Peek() (T, bool) {
	if s.cached {
		return s.value, true
	}
	var zero T
	return zero, false
}

// onInvalidate marks this slot stale. The previous value is deliberately KEPT
// (hasValue stays true) so a later recompute can compare against it; only its
// currency is dropped.
func (s *FormulaCell[T]) onInvalidate() {
	s.dirty = true
	if s.cached {
		s.cached = false
		s.ctx.cachedCount--
	}
}

// clearCache drops the memoized value outright (used by Context.Clear and by
// teardown); does not touch cachedCount, which Clear resets wholesale. Unlike
// invalidation this discards the value itself, so the next read must recompute
// rather than compare.
func (s *FormulaCell[T]) clearCache() {
	s.cached = false
	s.hasValue = false
	s.dirty = true
	s.forceRecompute = true
	var zero T
	s.value = zero
}

// cachedNow reports whether this slot currently holds a cached value.
func (s *FormulaCell[T]) cachedNow() bool { return s.cached }

// SourceCell is a value written from outside the graph; it invalidates its
// dependents when it changes. It is the source kind of the Cell genus (Cell[T]):
// the only kind that carries Set/Merge. A FormulaCell computes from upstream and
// has neither, so `formulaCell.Set(…)` does not compile — the write protection
// the Cell kernel design (§3/§4) puts in the type rather than a runtime gate.
//
// A SourceCell folds writes under a MergePolicy M. The default policy is
// KeepLatest, so a plain SourceCell is exactly the old plain Cell; a SourceCell
// with M ≠ KeepLatest is the old MergeCell. One kind, the policy in a field —
// the Go analogue of the design's SourceCell<T, M>.
//
// Reading Get inside a FormulaCell/Effect computation registers a dependency.
// Set triggers a cascade only when the new value is not equal to the old one —
// the ==-guard. SourceCell uses Go == for equality, so T must be comparable.
type SourceCell[T comparable] struct {
	reactiveBase
	ctx    *Context
	value  T
	policy MergePolicy[T]
}

// NewSourceCell creates a mutable source cell bound to ctx under the default
// KeepLatest policy (the plain cell). This is the design's source(v).
func NewSourceCell[T comparable](ctx *Context, initial T) *SourceCell[T] {
	return NewSourceCellWithPolicy(ctx, initial, KeepLatest[T]())
}

// NewSourceCellWithPolicy creates a source cell whose Merge folds under policy —
// the design's source::<M>(v). With KeepLatest it is a plain cell; with Sum/Max
// it is the former MergeCell.
func NewSourceCellWithPolicy[T comparable](ctx *Context, initial T, policy MergePolicy[T]) *SourceCell[T] {
	c := &SourceCell[T]{reactiveBase: newReactiveBase(), ctx: ctx, value: initial, policy: policy}
	c.self = c
	return c
}

// Get reads the value. Reading inside a computation subscribes the reader.
//
// Panics with a *DisposedError if this cell has been disposed; use TryGet for
// the checked form.
func (c *SourceCell[T]) Get() T {
	if c.disposed {
		panic(&DisposedError{Kind: "cell"})
	}
	c.track(c.ctx)
	return c.value
}

// Peek returns the current value without registering a dependency.
func (c *SourceCell[T]) Peek() T { return c.value }

// Set assigns a new value. If newValue != old, dependents are invalidated.
// Writing a disposed cell is a no-op: it has no dependents left to notify.
func (c *SourceCell[T]) Set(newValue T) {
	if c.disposed {
		return
	}
	if newValue != c.value {
		c.value = newValue
		c.ctx.cellChanged(c.self)
	}
}

// Merge folds op into the current value under this cell's policy and writes the
// result through the ==-guarded Set, so an idempotent policy's no-op merge fires
// no cascade (free dedup). Reads the current value untracked (Peek). Merge, like
// Set, exists only on SourceCell — the write half of the Cell genus.
func (c *SourceCell[T]) Merge(op T) { c.Set(c.policy.Merge(c.Peek(), op)) }

// Policy returns this cell's merge policy.
func (c *SourceCell[T]) Policy() MergePolicy[T] { return c.policy }

// Invalidate force-invalidates this cell's dependents without changing the
// value. Used by collection layers when an entry is removed.
func (c *SourceCell[T]) Invalidate() {
	c.invalidate()
	c.ctx.flushEffects()
}

func (c *SourceCell[T]) onInvalidate() {} // cells hold their value directly

// Formula creates a guarded FormulaCell bound to ctx — the design's formula(f),
// guarded by default (§9.3). "Guarded" means a recompute yielding a value equal
// (==) to the previous one suppresses the downstream cascade; it is exactly the
// former Memo, which is now what a formula is unless you opt out with
// NewFormulaCell. The guard is a pull-time check (see FormulaCell.refresh), so it
// recomputes nothing during invalidation.
func Formula[T comparable](ctx *Context, compute func(ctx *Context) T) *FormulaCell[T] {
	s := NewFormulaCell(ctx, compute)
	s.equals = func(prev, next T) bool { return prev == next }
	return s
}

// Drive makes this FormulaCell eager and returns the same handle (design §9.3.1).
//
// Eager is a driven FormulaCell, not a separate kind: Drive attaches a puller
// Effect that reads the formula now — materializing its value and its dependency
// edges immediately — and again after every invalidation, from inside the
// invalidating write's effect flush. Because the puller is an ordinary Effect and
// effects are scheduled rather than inline, N invalidations inside a Batch
// coalesce into a single scheduled pull at the flush: the value re-materializes
// once at batch exit, not once per write (#lzsignaleager clause 3). The former
// Signal built the same slot+puller pair as a bespoke type that could, and in
// lazily-go once did, weld a per-write puller into invalidation. Composing it out
// of formula().Drive() makes that bug structurally unwritable.
//
// Drive is idempotent — the driven bit short-circuits a second call, so
// f.Drive().Drive() attaches exactly one puller. It returns f itself (mutated),
// not a driver handle, so the caller holds the thing it reads via ordinary Get.
func (s *FormulaCell[T]) Drive() *FormulaCell[T] {
	if s.disposed || s.driven {
		return s
	}
	s.driven = true
	puller := NewEffect(s.ctx, func(*Context) func() {
		s.Get()
		return nil
	})
	s.ctx.drivenBy[s.self] = puller
	return s
}

// Undrive reverts an eager FormulaCell to lazy: it disposes the puller Effect and
// clears the driven bit and side-table entry. The value remains readable and
// recomputes on demand. Idempotent; a no-op on a lazy formula. This is the
// reverse transition that replaces the old dispose_signal.
func (s *FormulaCell[T]) Undrive() {
	if !s.driven {
		return
	}
	if p, ok := s.ctx.drivenBy[s.self]; ok {
		delete(s.ctx.drivenBy, s.self)
		p.Dispose()
	}
	s.driven = false
}

// IsDriven reports whether this FormulaCell is eager (has a live puller).
func (s *FormulaCell[T]) IsDriven() bool { return s.driven }

// EffectRun is a side-effect function that may return a cleanup callback. The
// cleanup (if non-nil) is invoked before the next rerun and on Dispose.
type EffectRun func(ctx *Context) (cleanup func())

// Effect is a side-effect observer that reruns whenever a tracked dependency
// changes. It is the eager-push primitive for side effects (logging, I/O). Any
// Cell, Slot, or Signal read inside run becomes a dependency; when any changes,
// the effect is scheduled and reruns after the current cascade (or at Batch
// exit).
type Effect struct {
	reactiveBase
	ctx     *Context
	run     EffectRun
	cleanup func()
	active  bool
	running bool
	// forceRun marks a scheduled effect that must rerun without a dependency
	// freshness check, because it read the node that was actually written.
	// An effect scheduled only transitively — reached through a Memo, say —
	// clears its check at flush time instead: if every dependency refreshes to
	// an unchanged value, it does not rerun. That is how the equality guard
	// reaches effects, and it mirrors lazily-rs's EffectNode.force_run.
	forceRun bool
}

// NewEffect creates and immediately runs a side-effect observer.
func NewEffect(ctx *Context, run EffectRun) *Effect {
	e := &Effect{reactiveBase: newReactiveBase(), ctx: ctx, run: run, active: true}
	e.self = e
	e.rerun()
	return e
}

// Dispose removes the eager observer. Invokes the last cleanup, then
// unsubscribes from all dependencies. Idempotent.
func (e *Effect) Dispose() {
	if !e.active {
		return
	}
	e.active = false
	e.disposed = true
	e.ctx.removePendingEffect(e)
	e.detachUpstream()
	c := e.cleanup
	e.cleanup = nil
	if c != nil {
		c()
	}
}

// IsActive reports whether the effect is still active (not disposed).
func (e *Effect) IsActive() bool { return e.active }

func (e *Effect) rerun() {
	if !e.active || e.running {
		return
	}
	e.running = true
	defer func() { e.running = false }()
	e.detachUpstream()
	prev := e.cleanup
	e.cleanup = nil
	if prev != nil {
		prev()
	}
	e.ctx.push(e.self)
	defer e.ctx.pop()
	e.cleanup = e.run(e.ctx)
}

func (e *Effect) onInvalidate() { e.markStale(true) }

// markStale schedules this effect to rerun after the current cascade. force
// reports whether it read the written node directly; see Effect.forceRun.
func (e *Effect) markStale(force bool) {
	if e.ctx.disposing > 0 {
		// Teardown, not a publish: mark only, never schedule. Running the
		// effect here would re-enter a compute that reads the node being
		// disposed. The contract is that the effect errors on its *next*
		// recompute (spec: reactive-graph/read_after_dispose_is_an_error).
		//
		// Nothing needs re-attaching here. The frontier walk that reached us
		// does not consume edges, so an effect that deliberately does not
		// rerun keeps every edge to its surviving dependencies — the same
		// reason lazily-rs has nothing to do on this path.
		return
	}
	if force {
		e.forceRun = true
	}
	e.ctx.scheduleEffect(e)
}

// dependenciesChanged refreshes every node this effect read on its last run and
// reports whether any of them produced a different value. Used at flush time to
// decide whether a transitively-scheduled effect actually needs to rerun.
func (e *Effect) dependenciesChanged() bool {
	deps := make([]reactiveNode, 0, len(e.dependencies))
	for d := range e.dependencies {
		deps = append(deps, d)
	}
	changed := false
	for _, d := range deps {
		if d.refresh() {
			changed = true
		}
	}
	return changed
}

// Memo is a lazy, cached, dependency-tracking computation with an equality
// guard. It behaves like Slot but suppresses downstream recomputation when a
// recompute yields a value equal (==) to the previous one.
//
// The guard is a PULL-TIME check, matching the rest of the family:
// invalidation marks the dependent cone stale and computes nothing; a read
// recomputes the memo, compares against the previous value, and if they are
// equal leaves its dependents holding their caches (they are marked clean
// without recomputing — see Slot.refresh). Because the invalidation walk does
// not consume edges, a dependent suppressed this way is still reachable from
// its source, so the next genuine change still arrives.
//
// Structurally a Memo is exactly "a Slot whose equals is set", which is how
// lazily-rs models it too; it overrides no behavior of its own.
type Memo[T comparable] struct {
	FormulaCell[T]
}

// NewMemo creates a memoized slot with an equality guard bound to ctx.
func NewMemo[T comparable](ctx *Context, compute func(ctx *Context) T) *Memo[T] {
	m := &Memo[T]{FormulaCell: FormulaCell[T]{
		reactiveBase: newReactiveBase(),
		ctx:          ctx,
		compute:      compute,
		equals:       func(prev, next T) bool { return prev == next },
	}}
	m.self = m
	ctx.registerSlot(m)
	return m
}
