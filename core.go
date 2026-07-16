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
//   - Slot[T]   — a lazily-computed cached value that automatically tracks its
//     dependencies and recomputes only when read after an upstream change.
//   - Cell[T]   — a mutable source value that invalidates dependent Slots/Signals
//     when it changes.
//   - Signal[T] — an eager derived value that recomputes the instant a
//     dependency changes, with no intermediate unset value.
//
// Values are lazy by default: dependents are marked dirty on invalidation but
// only recompute when accessed. When you need eager push-style semantics, reach
// for Signal.
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
	// onInvalidate is the hook run when this node is invalidated, before the
	// downstream cascade.
	onInvalidate()
	// invalidate runs onInvalidate, snapshots dependents, clears them, and
	// cascades. Memo overrides this to add its equality guard.
	invalidate()
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
}

func newReactiveBase() reactiveBase {
	return reactiveBase{
		dependents:   map[reactiveNode]struct{}{},
		dependencies: map[reactiveNode]struct{}{},
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

// invalidate is the default cascade: run onInvalidate, snapshot dependents,
// clear them, and cascade downstream.
func (b *reactiveBase) invalidate() {
	b.self.onInvalidate()
	if len(b.dependents) == 0 {
		return
	}
	snapshot := make([]reactiveNode, 0, len(b.dependents))
	for d := range b.dependents {
		snapshot = append(snapshot, d)
	}
	clear(b.dependents)
	for _, dependent := range snapshot {
		dependent.invalidate()
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
}

// NewContext creates an empty reactive scope.
func NewContext() *Context {
	return &Context{
		batchedCells:     map[reactiveNode]struct{}{},
		scheduledEffects: map[*Effect]struct{}{},
	}
}

// registerSlot records a value-bearing slot so Clear can reset it. Called once
// per slot at construction; not on any read path.
func (c *Context) registerSlot(s cacheable) { c.slots = append(c.slots, s) }

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
		e.rerun()
	}
	c.pendingEffects = c.pendingEffects[:0]
	c.effectsHead = 0
}

// Slot is a lazy, cached, dependency-tracking computation.
//
// Get returns the cached value if present; otherwise it computes the value
// (tracking every Cell, Signal, or Slot read during computation as a
// dependency), caches it, and returns it. When any dependency changes, the
// cached value is invalidated and the next Get recomputes.
//
// The cached value lives on the Slot itself (value/cached fields), not in a
// shared Context map — so a Get is a direct field read on the node you already
// hold, and read latency does not grow with the total number of nodes.
type Slot[T any] struct {
	reactiveBase
	ctx     *Context
	compute func(ctx *Context) T
	value   T    // last computed value (valid iff cached)
	cached  bool // whether value is current
	Name    string
}

// NewSlot creates a lazy slot bound to ctx.
func NewSlot[T any](ctx *Context, compute func(ctx *Context) T) *Slot[T] {
	s := &Slot[T]{reactiveBase: newReactiveBase(), ctx: ctx, compute: compute}
	s.self = s
	ctx.registerSlot(s)
	return s
}

// NewNamedSlot creates a lazy slot with a debug name.
func NewNamedSlot[T any](ctx *Context, name string, compute func(ctx *Context) T) *Slot[T] {
	s := NewSlot(ctx, compute)
	s.Name = name
	return s
}

// Get reads (and caches if needed) the value.
func (s *Slot[T]) Get() T {
	s.track(s.ctx)
	if s.cached {
		return s.value
	}
	s.detachUpstream()
	s.ctx.push(s.self)
	v := s.compute(s.ctx)
	s.ctx.pop()
	s.value = v
	s.cached = true
	s.ctx.cachedCount++
	return v
}

// Peek returns the cached value without recomputing, and whether it was cached.
func (s *Slot[T]) Peek() (T, bool) {
	if s.cached {
		return s.value, true
	}
	var zero T
	return zero, false
}

func (s *Slot[T]) onInvalidate() {
	if s.cached {
		s.cached = false
		s.ctx.cachedCount--
	}
}

// clearCache drops the memoized value (used by Context.Clear); does not touch
// cachedCount, which Clear resets wholesale.
func (s *Slot[T]) clearCache() { s.cached = false }

// cachedNow reports whether this slot currently holds a cached value.
func (s *Slot[T]) cachedNow() bool { return s.cached }

// Cell is a mutable source value that invalidates dependents when it changes.
//
// Reading Get inside a Slot/Signal computation registers a dependency. Set
// triggers a cascade only when the new value is not equal to the old one — the
// PartialEq guard. Cell uses Go == for equality, so T must be comparable.
type Cell[T comparable] struct {
	reactiveBase
	ctx            *Context
	value          T
	observers      map[uint64]func(T)
	nextObserverID uint64
}

// NewCell creates a mutable source value bound to ctx.
func NewCell[T comparable](ctx *Context, initial T) *Cell[T] {
	c := &Cell[T]{reactiveBase: newReactiveBase(), ctx: ctx, value: initial}
	c.self = c
	return c
}

// Get reads the value. Reading inside a computation subscribes the reader.
func (c *Cell[T]) Get() T {
	c.track(c.ctx)
	return c.value
}

// Peek returns the current value without registering a dependency.
func (c *Cell[T]) Peek() T { return c.value }

// Set assigns a new value. If newValue != old, dependents are invalidated.
func (c *Cell[T]) Set(newValue T) {
	if newValue != c.value {
		c.value = newValue
		c.notifyObservers()
		c.ctx.cellChanged(c.self)
	}
}

// Subscribe registers a persistent observer fired with the new value on each
// change. Returns a disposer; call it to stop observing. Observers are not
// cleared on invalidation.
func (c *Cell[T]) Subscribe(observer func(T)) func() {
	if c.observers == nil {
		c.observers = map[uint64]func(T){}
	}
	id := c.nextObserverID
	c.nextObserverID++
	c.observers[id] = observer
	return func() {
		delete(c.observers, id)
	}
}

// Invalidate force-invalidates this cell's dependents without changing the
// value. Used by collection layers when an entry is removed.
func (c *Cell[T]) Invalidate() {
	c.invalidate()
	c.ctx.flushEffects()
}

func (c *Cell[T]) notifyObservers() {
	if len(c.observers) == 0 {
		return
	}
	snapshot := make([]func(T), 0, len(c.observers))
	for _, o := range c.observers {
		snapshot = append(snapshot, o)
	}
	for _, o := range snapshot {
		o(c.value)
	}
}

func (c *Cell[T]) onInvalidate() {} // cells hold their value directly

// Signal is an eager derived value — recomputes immediately when a dependency
// changes. A recompute that yields an equal value (!= guard) suppresses the
// downstream cascade. T must be comparable for the equality guard.
type Signal[T comparable] struct {
	reactiveBase
	ctx         *Context
	backing     *signalSlot[T]
	value       T
	active      bool
	recomputing bool
}

// NewSignal creates an eager signal bound to ctx. The value is computed now.
func NewSignal[T comparable](ctx *Context, compute func(ctx *Context) T) *Signal[T] {
	s := &Signal[T]{reactiveBase: newReactiveBase(), ctx: ctx, active: true}
	s.self = s
	s.backing = newSignalSlot(ctx, compute)
	s.backing.signal = s
	// Eager activation: compute once now so there is no intermediate unset
	// value and dependency edges are established immediately.
	s.value = s.backing.Get()
	return s
}

// Get reads the current materialized value. Reading inside a computation
// subscribes the reader.
func (s *Signal[T]) Get() T {
	s.track(s.ctx)
	if !s.active {
		return s.backing.Get()
	}
	return s.value
}

func (s *Signal[T]) eagerRecompute() {
	if !s.active || s.recomputing {
		return
	}
	s.recomputing = true
	newValue := s.backing.Get()
	s.recomputing = false
	if newValue != s.value {
		s.value = newValue
		s.invalidate()
	}
}

// Dispose removes the eager puller. The value remains readable but reverts to
// lazy behavior.
func (s *Signal[T]) Dispose() {
	s.active = false
	s.backing.signal = nil
}

// IsActive reports whether the eager puller is still installed.
func (s *Signal[T]) IsActive() bool { return s.active }

func (s *Signal[T]) onInvalidate() {} // signal holds its value directly

// signalSlot backs a Signal. Its invalidation eagerly re-pulls the signal
// instead of leaving it dirty.
type signalSlot[T comparable] struct {
	Slot[T]
	signal *Signal[T]
}

func newSignalSlot[T comparable](ctx *Context, compute func(ctx *Context) T) *signalSlot[T] {
	ss := &signalSlot[T]{Slot: Slot[T]{reactiveBase: newReactiveBase(), ctx: ctx, compute: compute}}
	ss.self = ss
	ctx.registerSlot(ss)
	return ss
}

func (ss *signalSlot[T]) onInvalidate() {
	// Drop the cached slot value so the re-pull recomputes, then eagerly
	// recompute the owning signal.
	if ss.cached {
		ss.cached = false
		ss.ctx.cachedCount--
	}
	if ss.signal != nil {
		ss.signal.eagerRecompute()
	}
}

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

func (e *Effect) onInvalidate() { e.ctx.scheduleEffect(e) }

// Memo is a lazy, cached, dependency-tracking computation with an equality
// guard. It behaves like Slot but suppresses downstream invalidation when a
// recompute yields a value equal (==) to the previous one. On invalidation it
// eagerly recomputes to check equality rather than waiting for a read.
type Memo[T comparable] struct {
	Slot[T]
	guardActive bool
}

// NewMemo creates a memoized slot with an equality guard bound to ctx.
func NewMemo[T comparable](ctx *Context, compute func(ctx *Context) T) *Memo[T] {
	m := &Memo[T]{Slot: Slot[T]{reactiveBase: newReactiveBase(), ctx: ctx, compute: compute}}
	m.self = m
	ctx.registerSlot(m)
	return m
}

// invalidate overrides the default cascade with the memo equality guard.
func (m *Memo[T]) invalidate() {
	if m.guardActive {
		return
	}
	m.guardActive = true
	defer func() { m.guardActive = false }()

	m.detachUpstream()
	m.ctx.push(m.self)
	newValue := m.compute(m.ctx)
	m.ctx.pop()

	if m.cached {
		if newValue == m.value {
			// Value unchanged — suppress the downstream cascade.
			return
		}
	} else {
		m.ctx.cachedCount++
	}
	m.value = newValue
	m.cached = true
	if len(m.dependents) == 0 {
		return
	}
	snapshot := make([]reactiveNode, 0, len(m.dependents))
	for d := range m.dependents {
		snapshot = append(snapshot, d)
	}
	clear(m.dependents)
	for _, dependent := range snapshot {
		dependent.invalidate()
	}
}

func (m *Memo[T]) onInvalidate() {} // Memo manages its own cache in invalidate
