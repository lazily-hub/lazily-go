// Disposal, teardown scopes, and edge-degree introspection for the synchronous
// reactive graph (#lzspecedgeindex).
//
// Why this exists: a Go handle is a pointer, and dropping the last pointer to a
// *Slot reclaims nothing reactive. The node's edge on each of its dependencies
// is a *strong* reference held by the context's graph, so a long-lived source
// retains every node that ever read it. Under subscribe/unsubscribe churn the
// dependent set grows without bound even though the live subscriber count is
// constant, and the cost is paid twice — memory, and propagation, since every
// write walks the whole list. Explicit disposal is the fix, exactly as in
// lazily-rs (`Context::dispose_slot` / `dispose_cell`) and lazily-js
// (`disposeSlot`).
//
// # Three semantics this file must preserve
//
//  1. Disposal dirties the surviving dependent cone. Detaching edges without
//     marking dependents leaves a live reader frozen on the value it cached
//     before the disposal — the defect fixed in lazily-rs 5db90d2 and
//     lazily-js 4d20670. The cone walk here reuses reactiveBase.invalidate
//     (core.go) rather than adding a second walk that could drift from it.
//
//  2. Effects (and other eager nodes) reached by that walk are marked, not run.
//     Disposal is not a publish: running an effect mid-teardown re-enters a
//     compute that reads the node being disposed, which breaks idempotence.
//     Context.disposing gates this; see Effect.onInvalidate,
//     signalSlot.onInvalidate, and Memo.invalidate in core.go.
//
//  3. Scope teardown is reverse creation order. Graph state is order
//     independent, but effect *cleanups* are side effects with an observable
//     order, and ending a scope is proved observationally equal to disposing
//     each member individually (lazily-formal `disposeScope_eq_disposeAll`).
//
// # Why reads of a disposed node panic
//
// A compute closure has signature `func(*Context) T`. There is no error channel
// through it, so a nested read of a disposed dependency cannot be reported by a
// return value without changing every user compute's signature. Panic/recover
// is the only mechanism that crosses an arbitrary closure, and it matches the
// reference binding: lazily-rs panics on a read of a torn-down node too.
// Disposal is a caller contract ("nothing may still read it"), and Go already
// uses panic for contract violations. TryGet is the checked form at the
// boundary; Get is unchanged on the hot path.
package lazily

import "errors"

// ErrDisposed is the sentinel behind every *DisposedError, for errors.Is.
var ErrDisposed = errors.New("lazily: read of a disposed reactive node")

// DisposedError is the panic value raised by a read of a disposed node, and the
// error returned by TryGet.
type DisposedError struct {
	// Name is the node's debug name when it has one (NewNamedSlot).
	Name string
	// Kind is "slot", "cell", or "signal".
	Kind string
}

func (e *DisposedError) Error() string {
	if e.Name != "" {
		return "lazily: read of disposed " + e.Kind + " " + e.Name
	}
	return "lazily: read of disposed " + e.Kind
}

// Unwrap makes errors.Is(err, ErrDisposed) work.
func (e *DisposedError) Unwrap() error { return ErrDisposed }

// GraphNode is any node in a Context's reactive graph: *Slot, *Cell, *Signal,
// *Memo, or *Effect.
//
// Sealed — its only method is unexported, so it cannot be implemented outside
// this package. It exists so the degree accessors and TeardownScope.Own take
// any node kind without exposing the edge sets themselves. The accessors return
// *counts*, never the sets: there is no path from here to a node's internals
// and no way to mutate the graph through it.
type GraphNode interface {
	node() *reactiveBase
}

// DependentCount reports how many nodes currently depend on n — the size of its
// reverse edge set (#lzspecedgeindex).
//
// This is the observable the disposal contract is written against: a
// subscribe/unsubscribe cycle that disposes what it creates must leave this at
// its starting value no matter how many cycles run. A binding that leaks shows
// total-ever-created here instead of live-subscriber count.
//
// Returns 0 for a disposed node, and for *Effect, which is a pure sink.
//
// Note that this counts *live* edges. Because invalidation in this binding
// consumes the reverse edge (a dependent re-registers when it recomputes),
// reading a degree immediately after a write and before the dependents are
// pulled reports the post-cascade state, not the pre-cascade one.
func (c *Context) DependentCount(n GraphNode) int {
	b := n.node()
	if b.disposed {
		return 0
	}
	return len(b.dependents)
}

// DependencyCount reports how many nodes n currently depends on — the size of
// its forward edge set (#lzspecedgeindex).
//
// Counterpart to DependentCount: disposal must detach both directions, and a
// binding that detaches only one leaves a dangling half-edge visible here.
// Returns 0 for a disposed node and for *Cell, which is a pure source.
func (c *Context) DependencyCount(n GraphNode) int {
	b := n.node()
	if b.disposed {
		return 0
	}
	return len(b.dependencies)
}

// IsDisposed reports whether n has been torn down.
func (c *Context) IsDisposed(n GraphNode) bool { return n.node().disposed }

// Dispose tears down this slot: detaches both edge directions, drops the cached
// value, and dirties the surviving dependent cone. Idempotent.
//
// Callers must ensure nothing still reads the slot in a live compute. A reader
// that still names it errors on its next recompute — the same contract as
// Effect.Dispose and lazily-rs's dispose_slot.
func (s *Slot[T]) Dispose() { s.ctx.disposeNode(s.self) }

// Dispose tears down this source cell: detaches its dependents and dirties the
// surviving cone. Cells have no dependencies, so only downstream edges need
// detaching. Same contract as Slot.Dispose. Idempotent.
func (c *Cell[T]) Dispose() { c.ctx.disposeNode(c.self) }

// DisposeNode tears down this memoized slot. Same contract as Slot.Dispose.
func (m *Memo[T]) DisposeNode() { m.ctx.disposeNode(m.self) }

// DisposeNode tears down this signal's graph node: it detaches both edge
// directions and dirties the surviving cone.
//
// This is distinct from Signal.Dispose, which predates the teardown plane and
// means something narrower — "remove the eager puller, stay readable as a lazy
// value". Renaming that would be a breaking change to an already-shipped
// method, so the graph-teardown operation takes the new name and Signal.Dispose
// keeps its meaning.
func (s *Signal[T]) DisposeNode() {
	s.Dispose()
	s.ctx.disposeNode(s.self)
	s.ctx.disposeNode(s.backing.self)
}

// disposeNode is the single teardown path for every node kind.
func (c *Context) disposeNode(n reactiveNode) {
	if n == nil || n.node().disposed {
		return // idempotent: disposing twice is a no-op, not an error
	}
	if e, ok := n.(*Effect); ok {
		// Effects own a cleanup callback and a pending-queue entry, so their
		// teardown is Effect.Dispose. It detaches upstream and marks the base
		// disposed; effects are sinks, so there is no cone to dirty.
		e.Dispose()
		return
	}
	c.teardown(n)
}

// teardown detaches a node in both directions and dirties what survives.
func (c *Context) teardown(n reactiveNode) {
	b := n.node()
	b.disposed = true

	// Drop the memoized value and its registry slot, so a churn workload does
	// not grow Context.slots without bound.
	if cn, ok := n.(cacheable); ok {
		if cn.cachedNow() {
			c.cachedCount--
		}
		cn.clearCache()
		c.unregisterSlot(b)
	}

	// Upstream: remove this node from each dependency's dependent set.
	b.detachUpstream()

	// Downstream: snapshot the dependents, then drop the edge in both
	// directions before cascading, so nothing observes a half-detached graph.
	if len(b.dependents) == 0 {
		return
	}
	survivors := make([]reactiveNode, 0, len(b.dependents))
	for d := range b.dependents {
		survivors = append(survivors, d)
		delete(d.node().dependencies, n)
	}
	clear(b.dependents)

	// Semantic 1: the surviving cone must be dirtied, or a live reader stays
	// frozen on its cached value forever. Semantic 2: `disposing` makes that
	// cascade mark-only — no effect runs, no eager recompute.
	c.disposing++
	for _, d := range survivors {
		d.invalidate()
	}
	c.disposing--
}

// TryGet reads the slot, returning a *DisposedError instead of panicking when
// this slot — or any node it reads while recomputing — has been disposed.
//
// This is the boundary form: use it where a read may race a teardown. It
// recovers the panic and restores the context's tracking stack to the depth it
// had on entry, so a read that unwinds out of a half-finished compute cannot
// strand a frame and corrupt every later read.
func (s *Slot[T]) TryGet() (v T, err error) {
	depth := len(s.ctx.stack)
	defer func() {
		if r := recover(); r != nil {
			de, ok := r.(*DisposedError)
			if !ok {
				panic(r)
			}
			s.ctx.stack = s.ctx.stack[:depth]
			var zero T
			v, err = zero, de
		}
	}()
	return s.Get(), nil
}

// TryGet reads the cell, returning a *DisposedError if it has been disposed.
func (c *Cell[T]) TryGet() (T, error) {
	if c.disposed {
		var zero T
		return zero, &DisposedError{Kind: "cell"}
	}
	return c.Get(), nil
}

// TryGet reads the signal, returning a *DisposedError if it — or a node it
// re-pulls through — has been disposed.
func (s *Signal[T]) TryGet() (v T, err error) {
	depth := len(s.ctx.stack)
	defer func() {
		if r := recover(); r != nil {
			de, ok := r.(*DisposedError)
			if !ok {
				panic(r)
			}
			s.ctx.stack = s.ctx.stack[:depth]
			var zero T
			v, err = zero, de
		}
	}()
	return s.Get(), nil
}

// ---------------------------------------------------------------------------
// Teardown scopes
// ---------------------------------------------------------------------------

// TeardownScope groups nodes so they can be torn down together.
//
// # Why Close, and not a destructor
//
// lazily-rs ends a scope when it drops. Go has no destructors, so the end of a
// scope has to be a call. Close is that call, named for `defer scope.Close()` —
// the Go idiom that already means "this ends when the enclosing function does",
// which is precisely Rust's Drop timing expressed in the language Go actually
// has. It returns nothing: teardown cannot fail, and an error return would make
// every `defer scope.Close()` an unchecked-error lint. Context.WithScope wraps
// the same thing as a callback for the common lexical case.
//
// Grouping bounds *teardown*, not visibility: a scoped node reads unscoped or
// sibling-scope nodes freely, and an unscoped node may read a scoped one. Same
// caveat as Slot.Dispose — closing a scope tears down its members even if
// something outside still reads them, and that reader errors on its next
// recompute.
type TeardownScope struct {
	ctx    *Context
	owned  []reactiveNode
	closed bool
}

// Scope opens a teardown scope. Nodes added with Own are disposed by Close.
func (c *Context) Scope() *TeardownScope { return &TeardownScope{ctx: c} }

// WithScope runs fn with a fresh teardown scope and closes it on return,
// including on panic — the callback form of Scope/Close.
func (c *Context) WithScope(fn func(s *TeardownScope)) {
	s := c.Scope()
	defer s.Close()
	fn(s)
}

// Own places n under s's ownership and returns it, so a node can be created and
// scoped in one expression:
//
//	total := lazily.Own(scope, lazily.NewSlot(ctx, compute))
//
// It is a free function rather than a method because Go methods cannot take
// type parameters — and being generic in the *node* type rather than in the
// value type means this one function covers Slot, Cell, Signal, Memo, and
// Effect, instead of mirroring every constructor onto the scope the way
// lazily-rs must.
func Own[N GraphNode](s *TeardownScope, n N) N {
	if s != nil && !s.closed {
		s.owned = append(s.owned, n.node().self)
	}
	return n
}

// Len reports how many nodes this scope currently owns.
func (s *TeardownScope) Len() int { return len(s.owned) }

// Disarm cancels this scope's teardown: it releases every node it owns back to
// plain context ownership, so Close disposes nothing. The nodes themselves are
// untouched — no disposal, no detachment — and each stays individually
// disposable. Same sense as defusing a scope guard.
func (s *TeardownScope) Disarm() { s.owned = nil }

// Close tears down every node this scope owns, in reverse creation order, then
// marks the scope closed. Idempotent.
//
// Reverse order matters for effect cleanups, which are observable side effects;
// graph state alone is order independent. It also keeps a scope from
// transiently dangling inside itself, since dependents go before what they
// read.
func (s *TeardownScope) Close() {
	owned := s.owned
	s.owned = nil
	s.closed = true
	for i := len(owned) - 1; i >= 0; i-- {
		s.ctx.disposeNode(owned[i])
	}
}
