// Disposal, teardown scopes, and edge-degree introspection for the *async*
// reactive graph (#lzspecedgeindex).
//
// The synchronous plane's counterpart lives in disposal.go, and the three
// semantics it documents hold here unchanged:
//
//  1. Disposal dirties the surviving dependent cone. The async graph makes this
//     even sharper than the sync one: GetAsync short-circuits on
//     AsyncSlotResolved, so a downstream slot left Resolved serves its cached
//     value forever and no later pull can rescue it. This is the same shape as
//     the cascade defect fixed in bdfdbce, reached by a different route.
//     Teardown reuses AsyncContext.propagate — the very walk that fix
//     installed — rather than adding a second one.
//
//  2. Effects reached by that walk are marked, not rerun. propagate's
//     schedule=false branch is that rule.
//
//  3. Scope teardown is reverse creation order, for effect cleanups.
//
// Everything here runs inside the owner goroutine via AsyncContext.do, so the
// loop-owned graph state stays single-threaded exactly as the rest of the file
// assumes.
package lazily

// AsyncGraphNode is any node in an AsyncContext's graph: *AsyncCellHandle,
// *AsyncSlotHandle, or *AsyncEffectHandle.
//
// Sealed by an unexported method, and — like the synchronous GraphNode — it
// exposes counts only, never the edge sets.
type AsyncGraphNode interface {
	asyncNodeKey() any
	asyncDepSet() map[any]struct{}
	asyncDisposed() bool
}

func (h *AsyncCellHandle[T]) asyncNodeKey() any             { return h.node }
func (h *AsyncCellHandle[T]) asyncDepSet() map[any]struct{} { return nil } // pure source
func (h *AsyncCellHandle[T]) asyncDisposed() bool           { return h.node.disposed }

func (s *AsyncSlotHandle[T]) asyncNodeKey() any             { return s.node }
func (s *AsyncSlotHandle[T]) asyncDepSet() map[any]struct{} { return s.node.deps }
func (s *AsyncSlotHandle[T]) asyncDisposed() bool           { return s.node.disposed }

func (e *AsyncEffectHandle) asyncNodeKey() any { return e }

// asyncDepSet reports the effect's forward edges. Effects are pure sinks, so
// there is no reverse set to report.
func (e *AsyncEffectHandle) asyncDepSet() map[any]struct{} { return e.deps }
func (e *AsyncEffectHandle) asyncDisposed() bool           { return e.disposed }

// isDisposedNode reports whether a graph key names a torn-down node.
func (c *AsyncContext) isDisposedNode(key any) bool {
	switch n := key.(type) {
	case *asyncCell:
		return n.disposed
	case *asyncSlot:
		return n.disposed
	case *AsyncEffectHandle:
		return n.disposed
	}
	return false
}

// DependentCount reports how many nodes currently depend on n — the size of its
// reverse edge set (#lzspecedgeindex). Returns 0 for a disposed node and for an
// effect, which is a pure sink.
//
// As on the synchronous plane, this counts *live* edges: invalidation consumes
// the reverse edge and each dependent re-registers when it recomputes, so a
// degree read between a write and the pull that follows it reports the
// post-cascade state.
func (c *AsyncContext) DependentCount(n AsyncGraphNode) int {
	var count int
	c.do(func() {
		if n.asyncDisposed() {
			return
		}
		count = len(c.dependents[n.asyncNodeKey()])
	})
	return count
}

// DependencyCount reports how many nodes n currently depends on — the size of
// its forward edge set. Returns 0 for a disposed node and for a cell, which is
// a pure source.
func (c *AsyncContext) DependencyCount(n AsyncGraphNode) int {
	var count int
	c.do(func() {
		if n.asyncDisposed() {
			return
		}
		count = len(n.asyncDepSet())
	})
	return count
}

// IsDisposed reports whether n has been torn down.
func (c *AsyncContext) IsDisposed(n AsyncGraphNode) bool {
	var d bool
	c.do(func() { d = n.asyncDisposed() })
	return d
}

// IsActive reports whether this effect is still registered (not disposed).
func (e *AsyncEffectHandle) IsActive() bool {
	var active bool
	e.c.do(func() { active = !e.disposed })
	return active
}

// DisposeAsync tears down this async slot: it cancels any in-flight compute,
// detaches both edge directions, and dirties the surviving dependent cone.
// Idempotent.
//
// Blocked waiters and any later reader receive a *DisposedError — the same
// "errors on next recompute" contract as the synchronous Slot.Dispose.
func (s *AsyncSlotHandle[T]) DisposeAsync() {
	s.c.do(func() { s.c.disposeAsyncSlot(s.node) })
}

// Dispose is an alias for DisposeAsync.
func (s *AsyncSlotHandle[T]) Dispose() { s.DisposeAsync() }

// DisposeAsync tears down this async cell: it detaches its dependents and
// dirties the surviving cone. Cells are pure sources, so only downstream edges
// need detaching. Idempotent.
func (h *AsyncCellHandle[T]) DisposeAsync() {
	h.c.do(func() { h.c.disposeAsyncCell(h.node) })
}

// Dispose is an alias for DisposeAsync.
func (h *AsyncCellHandle[T]) Dispose() { h.DisposeAsync() }

// disposeAsyncSlot runs inside the owner goroutine.
func (c *AsyncContext) disposeAsyncSlot(s *asyncSlot) {
	if s.disposed {
		return
	}
	s.disposed = true
	// Cancel the in-flight run and unblock its waiters with the disposal error
	// rather than a supersession, which would send them round the re-resolve
	// loop only to find the slot gone.
	inf := s.inFlight
	s.inFlight = nil
	delete(c.computing, s)
	if inf != nil {
		inf.cancel()
	}
	ws := s.waiters
	s.waiters = nil
	for _, w := range ws {
		w <- asyncResult{err: &DisposedError{Kind: "slot"}}
	}
	s.state = AsyncSlotEmpty
	s.value = nil
	s.hasValue = false

	// Upstream: drop this slot from each dependency's dependent set.
	for dep := range s.deps {
		c.removeDependent(dep, s)
	}
	s.deps = map[any]struct{}{}
	c.detachAsyncNode(s)
}

// disposeAsyncCell runs inside the owner goroutine.
func (c *AsyncContext) disposeAsyncCell(n *asyncCell) {
	if n.disposed {
		return
	}
	n.disposed = true
	c.detachAsyncNode(n)
}

// detachAsyncNode drops key's reverse edges and dirties its dependent cone
// without running anything (semantics 1 and 2). Upstream detachment is the
// caller's job, since only slots and effects have forward edges.
func (c *AsyncContext) detachAsyncNode(key any) {
	// Downstream: drop the forward half-edge on each direct dependent, so
	// neither direction is left dangling.
	for d := range c.dependents[key] {
		if o, ok := d.(depOwner); ok {
			delete(o.depSet(), key)
		}
	}
	// propagate consumes c.dependents[key] as it walks — that is the detach of
	// the reverse direction — and marks the whole cone dirty. schedule=false is
	// semantic 2.
	c.propagate(key, false)
	delete(c.dependents, key)
}

// ---------------------------------------------------------------------------
// Teardown scopes
// ---------------------------------------------------------------------------

// AsyncTeardownScope groups async nodes so they can be torn down together.
// The Close/Disarm/Own shape and its rationale are identical to the synchronous
// TeardownScope; see disposal.go.
type AsyncTeardownScope struct {
	c      *AsyncContext
	owned  []func()
	closed bool
}

// Scope opens a teardown scope on this async context.
func (c *AsyncContext) Scope() *AsyncTeardownScope { return &AsyncTeardownScope{c: c} }

// WithScope runs fn with a fresh async teardown scope and closes it on return,
// including on panic.
func (c *AsyncContext) WithScope(fn func(s *AsyncTeardownScope)) {
	s := c.Scope()
	defer s.Close()
	fn(s)
}

// OwnAsync places n under s's ownership and returns it.
//
// A free function for the same reason as the synchronous Own: Go methods cannot
// take type parameters, and being generic in the node type lets one function
// cover cells, slots, and effects.
func OwnAsync[N AsyncGraphNode](s *AsyncTeardownScope, n N) N {
	if s == nil || s.closed {
		return n
	}
	// The disposer is captured as a closure because the three handle kinds have
	// no common teardown method — an effect's disposal runs its cleanup on the
	// caller's goroutine, a slot's and a cell's run on the loop.
	s.owned = append(s.owned, func() { disposeAsyncNode(n) })
	return n
}

func disposeAsyncNode(n AsyncGraphNode) {
	switch h := any(n).(type) {
	case *AsyncEffectHandle:
		h.DisposeAsync()
	default:
		type disposer interface{ DisposeAsync() }
		if d, ok := h.(disposer); ok {
			d.DisposeAsync()
		}
	}
}

// Len reports how many nodes this scope currently owns.
func (s *AsyncTeardownScope) Len() int { return len(s.owned) }

// Disarm cancels this scope's teardown: Close then disposes nothing and the
// nodes revert to plain context ownership, untouched and individually
// disposable.
func (s *AsyncTeardownScope) Disarm() { s.owned = nil }

// Close tears down every node this scope owns, in reverse creation order.
// Idempotent.
func (s *AsyncTeardownScope) Close() {
	owned := s.owned
	s.owned = nil
	s.closed = true
	for i := len(owned) - 1; i >= 0; i-- {
		owned[i]()
	}
}
