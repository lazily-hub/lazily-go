// Package lazily provides lazy reactive primitives for Go — the Cell kernel
// (#lzcellkernel) — plus the lazily-spec wire protocol, CRDT collection types,
// keyed cell collections, state machines/charts, and the distributed CRDT plane.
//
// A Go port of the lazily reactive family (lazily-rs, lazily-py, lazily-kt,
// lazily-js, lazily-dart, lazily-zig), conformant with lazily-spec and
// lazily-formal.
//
// The reactive family (v2 kernel). "Cell" is a conceptual word for a
// value-bearing reactive node, not a Go type — the two kinds are two concrete
// handle structs, and write protection lives in the type (design §3/§4):
//
//   - Source[T]   — a value written from outside; the only kind with Set/Merge.
//     Folds writes under a MergePolicy (KeepLatest by default = a plain cell;
//     Sum/Max = the former MergeCell). Constructors: NewSource / NewSourceWithPolicy.
//   - Computed[T] — a value computed from upstream; lazily cached and
//     dependency-tracking, with neither Set nor Merge (so computed.Set(…) does not
//     compile). NewComputed(f) is GUARDED by default: a recompute yielding an equal
//     value suppresses the downstream cascade. NewSlot(f) is the bound-free
//     storage-sense primitive (T any, no guard) for non-comparable values.
//     computed.Eager() makes it eager (the former Signal), returning the same handle;
//     computed.Lazy() reverses it. The former Memo is removed — a Computed IS the
//     guarded form.
//   - Effect       — a side-effect sink (ctx.effect); outside the Cell hierarchy.
//
// The v1 `Cell[T]` read-genus interface is dropped: no Go generic code used it as
// a bound, and v2 no longer needs a genus for write protection.
//
// Values are lazy by default: dependents are marked dirty on invalidation but
// only recompute when read. For eager push-style semantics, call Eager on a Computed.
//
// A Context is the shared scope. Dependency tracking is value-threaded through a
// per-recompute Compute view — there is no ambient recompute stack — and cached
// slot values live on the nodes themselves, not in a shared Context table, so
// reads stay O(1) regardless of graph size. All reactives that
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

// trackTo registers parent (the recomputing node, if any) as a dependent of
// this node, and records the reverse edge. This is the value-threaded tracking
// core (#lzcellkernel): the identity to attribute to is passed in as a value,
// never read from ambient state. A nil parent (a top-level or explicitly
// untracked read) forms no edge.
func (b *reactiveBase) trackTo(parent reactiveNode) {
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

// Context is a reactive scope: batch/effect scheduling plus the node registry.
// Dependency tracking is value-threaded through the per-recompute Compute view
// (there is no ambient recompute stack); a read attributes to the recomputing
// node only when it goes through that view (Get(c, handle)). Cached slot values
// are stored on the nodes themselves, not here. All Slots, Cells, and Signals
// that should react to each other must be created with (and thus share) the same
// Context.
//
// Context is not safe for concurrent use. Wrap it with ThreadSafeContext for
// lock-backed concurrency, or drive it from a single goroutine via AsyncContext.
type Context struct {
	// computeGen is a monotonic counter stamped onto each Compute view minted by
	// newCompute. It is the generation half of the compute-view fortification
	// guard (see Compute): a view is dead the moment its recompute returns, and
	// the stamp lets a superseded view be told apart from the live one.
	computeGen uint64

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
	// an eager (Computed) recompute during teardown re-enters a compute
	// that reads the node being disposed, which breaks teardown idempotence. The
	// contract is "errors on next recompute", not "errors during the dispose
	// call". See disposal.go.
	disposing int

	// eagerBy is the eager side table (design §9.3.3): it maps an eager
	// Computed's node to the puller Effect that keeps it materialized. Off the
	// node so a lazy computed costs nothing; one entry per eager computed. Keyed by
	// node identity (a pointer), and cleared on lazy/dispose so it never
	// aliases a stale node — Go's pointer identity means there is no recycled-id
	// hazard here (cf. the generation-tagged SlotId the arena bindings need).
	eagerBy map[reactiveNode]*Effect
}

// NewContext creates an empty reactive scope.
func NewContext() *Context {
	return &Context{
		batchedCells:     map[reactiveNode]struct{}{},
		scheduledEffects: map[*Effect]struct{}{},
		eagerBy:          map[reactiveNode]*Effect{},
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

// ComputeOps is the compute-time operations subset shared by the two read
// surfaces (#lzcellkernel). It is the Go analogue of lazily-rs's `ComputeOps`
// trait, implemented by exactly two types:
//
//   - *Context — the owning scope, whose reads are UNTRACKED (trackNode is nil).
//   - *Compute — the per-recompute view handed to a compute/effect closure,
//     whose reads register a dependency edge against the recomputing node.
//
// Go methods cannot be generic, so the value-carrying operations of the rs
// trait (get/source/cell/computed/slot) are expressed as free generic functions
// that take a ComputeOps — Get(c, handle) for tracked reads, and the New*C
// constructors for building nodes. This interface carries only the non-generic
// operations plus the tracking identity itself; it mirrors the same split the
// async surface already uses (AsyncComputeContext + free TrackSource/TrackComputed).
//
// There is deliberately no GetRc: Rc handles are a rust ownership device with no
// Go analogue (the runtime is garbage-collected), exactly as on the async side.
type ComputeOps interface {
	// trackNode returns the node a tracked read should attribute to, or nil for
	// an untracked surface. It is unexported so ComputeOps cannot be implemented
	// outside this package: *Context and *Compute are the only inhabitants.
	trackNode() reactiveNode
	// context returns the owning reactive scope.
	context() *Context
	// Batch runs fn inside a coalescing batch on the owning scope.
	Batch(fn func())
	// Untracked returns the untracked read surface (the owning *Context). A read
	// through it — Get(c.Untracked(), handle) — forms no dependency edge. It is
	// the sole, explicit escape from tracking, mirroring rs Compute::untracked.
	Untracked() *Context
}

// trackNode on *Context is nil: a read through the owning context is untracked.
func (c *Context) trackNode() reactiveNode { return nil }

// context on *Context returns itself.
func (c *Context) context() *Context { return c }

// Untracked on *Context returns itself — the context is already the untracked
// surface, so this is idempotent and lets *Context satisfy ComputeOps uniformly.
func (c *Context) Untracked() *Context { return c }

// Compute is the per-recompute view handed to a value-threaded compute/effect
// closure. It carries the recomputing node id AS A VALUE (node), so a tracked
// read — Get(c, handle) — attributes the edge to that node by construction,
// never to ambient state. It is the sole tracking surface: reading a handle
// through the owning Context (or Compute.Untracked()) forms no edge.
//
// Fortification, and its Go limits. lazily-rs makes the view non-escapable by
// construction — a lifetime binds it to the recompute and !Send stops it moving
// to another thread — so it is impossible to store and replay against the wrong
// node. Go has neither lifetimes nor a compile-time move check, so
// non-escapability is by convention, backed by a RUNTIME guard: each Compute is
// generation-stamped (gen) and marked dead (live=false) the instant its
// recompute returns. Any trackNode() on a dead or superseded Compute panics
// rather than silently registering an edge against a node that is no longer
// recomputing. That converts the rust compile-time guarantee into a fail-fast
// runtime one — the strongest fortification Go allows.
type Compute struct {
	ctx  *Context
	node reactiveNode
	gen  uint64
	live bool
}

// newCompute mints a fresh compute view for node, stamped with the context's
// next generation.
func (c *Context) newCompute(node reactiveNode) *Compute {
	c.computeGen++
	return &Compute{ctx: c, node: node, gen: c.computeGen, live: true}
}

// trackNode returns the recomputing node, enforcing the fortification guard: a
// read through a Compute whose recompute has already returned (live=false), or
// one superseded by a newer recompute of the same node, panics instead of
// misattributing the edge.
func (cv *Compute) trackNode() reactiveNode {
	if !cv.live {
		panic(&StaleComputeError{stage: "read after recompute returned"})
	}
	return cv.node
}

// context returns the owning reactive scope.
func (cv *Compute) context() *Context { return cv.ctx }

// Untracked returns the owning Context, the explicit untracked escape. A read
// through it registers no dependency edge.
func (cv *Compute) Untracked() *Context { return cv.ctx }

// Batch runs fn inside a coalescing batch on the owning scope.
func (cv *Compute) Batch(fn func()) { cv.ctx.Batch(fn) }

// close marks the view dead so any later use fails the fortification guard.
func (cv *Compute) close() { cv.live = false }

// StaleComputeError is raised when a Compute view is used outside the recompute
// it belongs to — the runtime half of the non-escapability guarantee (Go cannot
// bind the view by lifetime the way lazily-rs does).
type StaleComputeError struct{ stage string }

func (e *StaleComputeError) Error() string {
	return "lazily: stale Compute view used " + e.stage +
		" (the compute view must not escape its recompute)"
}

// Trackable is any value-bearing node that can be read through a ComputeOps.
// Both *Computed[T] and *Source[T] implement it, so a single generic Get serves
// slots, computed cells, and source cells alike.
type Trackable[T any] interface {
	// getVia reads the value, registering an edge against parent (nil = untracked).
	getVia(parent reactiveNode) T
}

// Get reads a reactive handle through a compute surface (#lzcellkernel). When c
// is a *Compute, the read registers a dependency edge against the recomputing
// node; when c is a *Context (or c.Untracked()), it registers none. This is the
// value-threaded replacement for the ambient zero-argument handle.Get(): the
// node to attribute to is threaded through c, never read from a shared stack.
func Get[T any](c ComputeOps, h Trackable[T]) T {
	return h.getVia(c.trackNode())
}

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

// Computed is a lazy, cached, dependency-tracking computation.
//
// Get returns the cached value if present; otherwise it computes the value
// (tracking every Cell, Signal, or Computed read during computation as a
// dependency), caches it, and returns it. When any dependency changes, the
// cached value is invalidated and the next Get recomputes.
//
// The cached value lives on the Computed itself (value/cached fields), not in a
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
type Computed[T any] struct {
	reactiveBase
	ctx *Context
	// compute receives the per-recompute Compute view carrying this node's id.
	// A tracked read inside it — Get(c, handle) — attributes the dependency edge
	// to this node by value; there is no ambient frame.
	compute func(cv *Compute) T
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
	// eager is the eager bit (design §9.3.3): an eager Computed has a puller
	// Effect keeping it materialized. The bit alone makes Eager idempotent with
	// no lookup; the puller itself lives in Context.eagerBy, off the node, so a
	// lazy computed costs nothing. This replaces the old Signal type — eager
	// is a state a computed is in, not a separate kind.
	eager bool
	Name  string
}

// NewSlot creates a lazy slot bound to ctx. Its closure receives the
// per-recompute Compute view and reads its dependencies via Get(c, handle) — the
// value-threaded tracking surface (#lzcellkernel). No ambient frame is pushed, so
// Compute.Untracked() is genuinely untracked.
func NewSlot[T any](ctx *Context, compute func(c *Compute) T) *Computed[T] {
	s := &Computed[T]{reactiveBase: newReactiveBase(), ctx: ctx, compute: compute}
	s.self = s
	ctx.registerSlot(s)
	return s
}

// NewNamedSlot creates a lazy slot with a debug name.
func NewNamedSlot[T any](ctx *Context, name string, compute func(c *Compute) T) *Computed[T] {
	s := NewSlot(ctx, compute)
	s.Name = name
	return s
}

// Get reads (and caches if needed) the value.
//
// Panics with a *DisposedError if this slot has been disposed. Use TryGet for
// the checked form; see disposal.go for why a read of a torn-down node is a
// panic rather than a returned error.
func (s *Computed[T]) Get() T {
	return s.getVia(nil)
}

// getVia is the value-threaded read core: it attributes the read to parent
// (nil = untracked) rather than to any ambient state. The zero-argument Get() is
// an untracked external read (parent nil); Get(c, slot) threads c's recomputing
// node.
func (s *Computed[T]) getVia(parent reactiveNode) T {
	if s.disposed {
		panic(&DisposedError{Name: s.Name, Kind: "slot"})
	}
	s.trackTo(parent)
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
func (s *Computed[T]) refresh() bool {
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
func (s *Computed[T]) markClean() {
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
func (s *Computed[T]) recomputeNow() bool {
	s.detachUpstream()
	// Value-thread the recomputing node into the compute view: the closure reads
	// via Get(c, handle) and attributes each edge by value. The view is killed the
	// instant compute returns so a stored/escaped view fails the fortification
	// guard.
	cv := s.ctx.newCompute(s.self)
	v := s.compute(cv)
	cv.close()

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
func (s *Computed[T]) Peek() (T, bool) {
	if s.cached {
		return s.value, true
	}
	var zero T
	return zero, false
}

// onInvalidate marks this slot stale. The previous value is deliberately KEPT
// (hasValue stays true) so a later recompute can compare against it; only its
// currency is dropped.
func (s *Computed[T]) onInvalidate() {
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
func (s *Computed[T]) clearCache() {
	s.cached = false
	s.hasValue = false
	s.dirty = true
	s.forceRecompute = true
	var zero T
	s.value = zero
}

// cachedNow reports whether this slot currently holds a cached value.
func (s *Computed[T]) cachedNow() bool { return s.cached }

// Source is a value written from outside the graph; it invalidates its
// dependents when it changes. It is the source kind of the Cell genus (Cell[T]):
// the only kind that carries Set/Merge. A Computed computes from upstream and
// has neither, so `formulaCell.Set(…)` does not compile — the write protection
// the Cell kernel design (§3/§4) puts in the type rather than a runtime gate.
//
// A Source folds writes under a MergePolicy M. The default policy is
// KeepLatest, so a plain Source is exactly the old plain Cell; a Source
// with M ≠ KeepLatest is the old MergeCell. One kind, the policy in a field —
// the Go analogue of the design's Source<T, M>.
//
// Reading Get inside a Computed/Effect computation registers a dependency.
// Set triggers a cascade only when the new value is not equal to the old one —
// the ==-guard. Source uses Go == for equality, so T must be comparable.
type Source[T comparable] struct {
	reactiveBase
	ctx    *Context
	value  T
	policy MergePolicy[T]
}

// NewSource creates a mutable source cell bound to ctx under the default
// KeepLatest policy (the plain cell). This is the design's source(v).
func NewSource[T comparable](ctx *Context, initial T) *Source[T] {
	return NewSourceWithPolicy(ctx, initial, KeepLatest[T]())
}

// NewSourceWithPolicy creates a source cell whose Merge folds under policy —
// the design's source::<M>(v). With KeepLatest it is a plain cell; with Sum/Max
// it is the former MergeCell.
func NewSourceWithPolicy[T comparable](ctx *Context, initial T, policy MergePolicy[T]) *Source[T] {
	c := &Source[T]{reactiveBase: newReactiveBase(), ctx: ctx, value: initial, policy: policy}
	c.self = c
	return c
}

// Get reads the value. Reading inside a computation subscribes the reader.
//
// Panics with a *DisposedError if this cell has been disposed; use TryGet for
// the checked form.
func (c *Source[T]) Get() T {
	return c.getVia(nil)
}

// getVia is the value-threaded read core (see Computed.getVia): it attributes
// the read to parent rather than to any ambient state, so Get(compute, cell)
// tracks against the recomputing node by value and the zero-argument Get() is an
// untracked external read.
func (c *Source[T]) getVia(parent reactiveNode) T {
	if c.disposed {
		panic(&DisposedError{Kind: "cell"})
	}
	c.trackTo(parent)
	return c.value
}

// Peek returns the current value without registering a dependency.
func (c *Source[T]) Peek() T { return c.value }

// Set assigns a new value. If newValue != old, dependents are invalidated.
// Writing a disposed cell is a no-op: it has no dependents left to notify.
func (c *Source[T]) Set(newValue T) {
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
// Set, exists only on Source — the write half of the Cell genus.
func (c *Source[T]) Merge(op T) { c.Set(c.policy.Merge(c.Peek(), op)) }

// Policy returns this cell's merge policy.
func (c *Source[T]) Policy() MergePolicy[T] { return c.policy }

// Invalidate force-invalidates this cell's dependents without changing the
// value. Used by collection layers when an entry is removed.
func (c *Source[T]) Invalidate() {
	c.invalidate()
	c.ctx.flushEffects()
}

func (c *Source[T]) onInvalidate() {} // cells hold their value directly

// NewComputed creates a guarded Computed cell bound to ctx — the design's
// computed(f), guarded by default (§9.3). All computed cells are guarded: a
// recompute yielding a value equal (==) to the previous one suppresses the
// downstream cascade. This is the sole derived constructor now that the former
// Memo is removed — a computed cell IS the guarded form.
//
// The `T comparable` bound is what the guard needs (Go ==). For a value type
// that is not comparable, drop to NewSlot, the bound-free storage-sense
// primitive (T any, no guard) — the escape hatch that mirrors lazily-rs's slot().
// The guard is a pull-time check (see Computed.refresh), so it recomputes
// nothing during invalidation.
func NewComputed[T comparable](ctx *Context, compute func(c *Compute) T) *Computed[T] {
	s := NewSlot(ctx, compute)
	s.equals = func(prev, next T) bool { return prev == next }
	return s
}

// NewComputedRippleWhen creates a guarded Computed cell with an explicit change
// predicate (#lzcellkernel). Like NewComputed, but downstream propagation is
// gated by changed(old, new) instead of the value's natural == : changed returns
// true to PROPAGATE (ripple) the recompute to dependents, and false to SUPPRESS
// it (treat it as "no meaningful change"). So NewComputed(f) is exactly
// NewComputedRippleWhen(f, func(o, n T) bool { return o != n }), and an unguarded
// NewSlot(f) is NewComputedRippleWhen(f, func(_, _ T) bool { return true })
// (always propagate).
//
// Because the predicate is supplied, T carries no comparable bound: this is the
// guarded escape for non-comparable derived values — e.g. a []string / map
// computed guarded via func(o, n []string) bool { return !slices.Equal(o, n) }.
// It also serves a custom significance policy: dedup a large value by a
// version/hash field, epsilon float compare, hysteresis, a monotonic gate, or
// "propagate every N" when the counter lives in the value.
//
// The value is ALWAYS computed (the predicate needs new); changed gates only the
// downstream cascade, not the computation. changed MUST be a pure function of
// (old, new) — reading value-carried state (version/counter/sequence) is fine and
// stays deterministic; capturing external mutable state is not (it keys off
// recompute/read frequency under laziness and breaks determinism).
//
// The engine guards on equality (equal => suppress), so this installs
// equals = !changed(old, new).
func NewComputedRippleWhen[T any](ctx *Context, compute func(c *Compute) T, changed func(old, next T) bool) *Computed[T] {
	s := NewSlot(ctx, compute)
	s.equals = func(prev, next T) bool { return !changed(prev, next) }
	return s
}

// NewNamedComputedRippleWhen is NewComputedRippleWhen with a debug name.
func NewNamedComputedRippleWhen[T any](ctx *Context, name string, compute func(c *Compute) T, changed func(old, next T) bool) *Computed[T] {
	s := NewComputedRippleWhen(ctx, compute, changed)
	s.Name = name
	return s
}

// Eager makes this Computed eager and returns the same handle (design §9.3.1).
//
// Eager is a state a Computed is in, not a separate kind: Eager attaches a puller
// Effect that reads the computed now — materializing its value and its dependency
// edges immediately — and again after every invalidation, from inside the
// invalidating write's effect flush. Because the puller is an ordinary Effect and
// effects are scheduled rather than inline, N invalidations inside a Batch
// coalesce into a single scheduled pull at the flush: the value re-materializes
// once at batch exit, not once per write (#lzsignaleager clause 3). The former
// Signal built the same slot+puller pair as a bespoke type that could, and in
// lazily-go once did, weld a per-write puller into invalidation. Composing it out
// of computed().Eager() makes that bug structurally unwritable.
//
// Eager is idempotent — the eager bit short-circuits a second call, so
// f.Eager().Eager() attaches exactly one puller. It returns f itself (mutated),
// not a driver handle, so the caller holds the thing it reads via ordinary Get.
func (s *Computed[T]) Eager() *Computed[T] {
	if s.disposed || s.eager {
		return s
	}
	s.eager = true
	// The puller reads s on the value-threaded surface: Get(c, s) attributes the
	// puller -> s edge by value, pushing no ambient frame (#lzcellkernel).
	puller := NewEffect(s.ctx, func(c *Compute) func() {
		Get[T](c, s)
		return nil
	})
	s.ctx.eagerBy[s.self] = puller
	return s
}

// Lazy reverts an eager Computed to lazy: it disposes the puller Effect and
// clears the eager bit and side-table entry. The value remains readable and
// recomputes on demand. Idempotent; a no-op on a lazy computed. This is the
// reverse transition that replaces the old dispose_signal.
func (s *Computed[T]) Lazy() {
	if !s.eager {
		return
	}
	if p, ok := s.ctx.eagerBy[s.self]; ok {
		delete(s.ctx.eagerBy, s.self)
		p.Dispose()
	}
	s.eager = false
}

// IsEager reports whether this Computed is eager (has a live puller).
func (s *Computed[T]) IsEager() bool { return s.eager }

// EffectRun is a side-effect function that may return a cleanup callback. It
// receives the per-recompute Compute view and reads its dependencies via
// Get(c, handle) — the value-threaded tracking surface (#lzcellkernel). The
// cleanup (if non-nil) is invoked before the next rerun and on Dispose.
type EffectRun func(c *Compute) (cleanup func())

// Effect is a side-effect observer that reruns whenever a tracked dependency
// changes. It is the eager-push primitive for side effects (logging, I/O). Any
// Cell, Slot, or Signal read inside run becomes a dependency; when any changes,
// the effect is scheduled and reruns after the current cascade (or at Batch
// exit).
type Effect struct {
	reactiveBase
	ctx *Context
	// run receives the per-recompute Compute view; a tracked read inside it
	// (Get(c, handle)) attributes the dependency edge to this effect by value.
	run     func(cv *Compute) (cleanup func())
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

// NewEffect creates and immediately runs a side-effect observer whose body
// receives the per-recompute Compute view and tracks via Get(c, handle) — the
// fortified, value-threaded surface (#lzcellkernel).
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
	// Value-thread the effect node into the compute view; the body reads via
	// Get(c, handle) and attributes each edge by value. Kill the view when the
	// body returns so it cannot escape.
	cv := e.ctx.newCompute(e.self)
	e.cleanup = e.run(cv)
	cv.close()
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
