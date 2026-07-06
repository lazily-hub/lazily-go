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
// A Context is the shared scope: it holds an identity-keyed cache and the
// computation stack used for automatic dependency tracking. All reactives that
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
		b.dependents[parent] = struct{}{}
		parent.node().dependencies[b.self] = struct{}{}
	}
}

// detachUpstream detaches this node from all of its current upstream
// dependencies. Called before a recompute so edges reflect only the most recent
// computation.
func (b *reactiveBase) detachUpstream() {
	for dep := range b.dependencies {
		delete(dep.node().dependents, b.self)
	}
	b.dependencies = map[reactiveNode]struct{}{}
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
	b.dependents = map[reactiveNode]struct{}{}
	for _, dependent := range snapshot {
		dependent.invalidate()
	}
}

// Context is a reactive scope: an identity-keyed value cache plus the
// computation stack. All Slots, Cells, and Signals that should react to each
// other must be created with (and thus share) the same Context.
//
// Context is not safe for concurrent use. Wrap it with ThreadSafeContext for
// lock-backed concurrency, or drive it from a single goroutine via AsyncContext.
type Context struct {
	cache map[reactiveNode]any
	stack []reactiveNode

	batchDepth       int
	batchedCells     map[reactiveNode]struct{}
	pendingEffects   []*Effect
	scheduledEffects map[*Effect]struct{}
	flushingEffects  bool
}

// NewContext creates an empty reactive scope.
func NewContext() *Context {
	return &Context{
		cache:            map[reactiveNode]any{},
		batchedCells:     map[reactiveNode]struct{}{},
		scheduledEffects: map[*Effect]struct{}{},
	}
}

// Size reports the number of cached values.
func (c *Context) Size() int { return len(c.cache) }

func (c *Context) contains(n reactiveNode) bool { _, ok := c.cache[n]; return ok }
func (c *Context) read(n reactiveNode) any      { return c.cache[n] }
func (c *Context) write(n reactiveNode, v any)  { c.cache[n] = v }
func (c *Context) evict(n reactiveNode)         { delete(c.cache, n) }

// Clear drops every cached value. Dependency edges are re-established lazily as
// slots are read again.
func (c *Context) Clear() { c.cache = map[reactiveNode]any{} }

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
	c.batchedCells = map[reactiveNode]struct{}{}
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
	for i, pe := range c.pendingEffects {
		if pe == e {
			c.pendingEffects = append(c.pendingEffects[:i], c.pendingEffects[i+1:]...)
			break
		}
	}
}

func (c *Context) flushEffects() {
	if c.flushingEffects {
		return
	}
	c.flushingEffects = true
	defer func() { c.flushingEffects = false }()
	for len(c.pendingEffects) > 0 {
		e := c.pendingEffects[0]
		c.pendingEffects = c.pendingEffects[1:]
		delete(c.scheduledEffects, e)
		e.rerun()
	}
}

// Slot is a lazy, cached, dependency-tracking computation.
//
// Get returns the cached value if present; otherwise it computes the value
// (tracking every Cell, Signal, or Slot read during computation as a
// dependency), caches it, and returns it. When any dependency changes, the
// cached value is invalidated and the next Get recomputes.
type Slot[T any] struct {
	reactiveBase
	ctx     *Context
	compute func(ctx *Context) T
	Name    string
}

// NewSlot creates a lazy slot bound to ctx.
func NewSlot[T any](ctx *Context, compute func(ctx *Context) T) *Slot[T] {
	s := &Slot[T]{reactiveBase: newReactiveBase(), ctx: ctx, compute: compute}
	s.self = s
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
	if s.ctx.contains(s.self) {
		return s.ctx.read(s.self).(T)
	}
	s.detachUpstream()
	s.ctx.push(s.self)
	defer s.ctx.pop()
	v := s.compute(s.ctx)
	s.ctx.write(s.self, v)
	return v
}

// Peek returns the cached value without recomputing, and whether it was cached.
func (s *Slot[T]) Peek() (T, bool) {
	if s.ctx.contains(s.self) {
		return s.ctx.read(s.self).(T), true
	}
	var zero T
	return zero, false
}

func (s *Slot[T]) onInvalidate() { s.ctx.evict(s.self) }

// Cell is a mutable source value that invalidates dependents when it changes.
//
// Reading Get inside a Slot/Signal computation registers a dependency. Set
// triggers a cascade only when the new value is not equal to the old one — the
// PartialEq guard. Cell uses Go == for equality, so T must be comparable.
type Cell[T comparable] struct {
	reactiveBase
	ctx       *Context
	value     T
	observers []*func(T)
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
	obs := &observer
	c.observers = append(c.observers, obs)
	return func() {
		for i, o := range c.observers {
			if o == obs {
				c.observers = append(c.observers[:i], c.observers[i+1:]...)
				break
			}
		}
	}
}

// Invalidate force-invalidates this cell's dependents without changing the
// value. Used by collection layers when an entry is removed.
func (c *Cell[T]) Invalidate() {
	c.invalidate()
	c.ctx.flushEffects()
}

func (c *Cell[T]) notifyObservers() {
	snapshot := make([]*func(T), len(c.observers))
	copy(snapshot, c.observers)
	for _, o := range snapshot {
		(*o)(c.value)
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
	return ss
}

func (ss *signalSlot[T]) onInvalidate() {
	// Evict the cached slot value so the re-pull recomputes, then eagerly
	// recompute the owning signal.
	ss.ctx.evict(ss.self)
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

	if m.ctx.contains(m.self) {
		old := m.ctx.read(m.self).(T)
		if newValue == old {
			// Value unchanged — suppress the downstream cascade.
			return
		}
	}
	m.ctx.write(m.self, newValue)
	if len(m.dependents) == 0 {
		return
	}
	snapshot := make([]reactiveNode, 0, len(m.dependents))
	for d := range m.dependents {
		snapshot = append(snapshot, d)
	}
	m.dependents = map[reactiveNode]struct{}{}
	for _, dependent := range snapshot {
		dependent.invalidate()
	}
}

func (m *Memo[T]) onInvalidate() {} // Memo manages its own cache in invalidate
