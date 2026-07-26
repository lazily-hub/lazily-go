package lazily

import "sync"

// ThreadSafeReactiveMap is the thread-safe keyed reactive map (#reactivemap),
// generic over the entry handle kind H, with all present-set mutation serialized
// by an internal mutex and all graph work serialized by the owning
// ThreadSafeContext's lock.
//
// It is graph-backed. Before the Core-surface work this map stored plain values
// in a map[K]V with no reactive nodes at all, no context, and no ordering
// surface: it had PresentKeys and PresentCount and nothing else. "Thread-safe
// map" was a mutex-guarded cache wearing the reactive family's name. Entries are
// now real nodes on the underlying graph, and membership and order are real
// signals minted on that same graph — the ordering plane binds this flavor
// exactly as it binds the single-threaded one, because a move touches no entry
// handle and awaits nothing.
//
// Reads take a ComputeOps read surface (#lzcellkernel). A *Compute registers a
// dependency edge; a *Context registers none. A read spellable only as a
// zero-argument call could never subscribe from inside a derived node — which is
// precisely how the single-threaded map's Keys/Len/ContainsKey silently
// registered no edge at all.
//
// V is constrained comparable to mirror the single-threaded map (an input entry
// needs an equality guard). Once built its address is stable, so concurrent
// readers may share a *ThreadSafeReactiveMap.
type ThreadSafeReactiveMap[K comparable, V comparable, H any] struct {
	tsctx *ThreadSafeContext
	kind  EntryKind

	// mint allocates one entry node with compute as its canonical value
	// producer; observe reads it through the caller's read surface; clear
	// detaches a removed entry's node from the graph.
	mint    func(ctx *Context, compute func() V) H
	observe func(c ComputeOps, h H) V
	clear   func(h H)

	// mu guards the present set. It is never held across graph work: a set can
	// drive a dependent recompute that re-enters this map.
	mu    sync.RWMutex
	keyed keyedOrder[K, H]

	// Membership and order signals, minted on THIS flavor's graph. A shared
	// graph-agnostic core cannot supply reactivity; each flavor owns its cells.
	membership  *Source[int]
	orderSignal *Source[int]

	// Version mirrors, guarded by mu.
	membershipVersion int
	orderVersion      int
}

func newThreadSafeReactiveMap[K comparable, V comparable, H any](
	ts *ThreadSafeContext,
	kind EntryKind,
	mint func(ctx *Context, compute func() V) H,
	observe func(c ComputeOps, h H) V,
	clear func(h H),
) *ThreadSafeReactiveMap[K, V, H] {
	m := &ThreadSafeReactiveMap[K, V, H]{
		tsctx:   ts,
		kind:    kind,
		mint:    mint,
		observe: observe,
		clear:   clear,
		keyed:   newKeyedOrder[K, H](),
	}
	ts.WithLock(func(ctx *Context) {
		m.membership = NewSource[int](ctx, 0)
		m.orderSignal = NewSource[int](ctx, 0)
	})
	return m
}

// bumpOrder bumps only the order signal (invalidates Keys readers). A pure move
// bumps only this. Runs under the context lock with mu released.
func (m *ThreadSafeReactiveMap[K, V, H]) bumpOrder() {
	m.mu.Lock()
	m.orderVersion++
	next := m.orderVersion
	m.mu.Unlock()
	m.tsctx.WithLock(func(*Context) { m.orderSignal.Set(next) })
}

// bumpMembership bumps set-membership (invalidates Len / ContainsKey readers),
// then the order signal too — the key set changed, so the ordered list did too.
func (m *ThreadSafeReactiveMap[K, V, H]) bumpMembership() {
	m.mu.Lock()
	m.membershipVersion++
	next := m.membershipVersion
	m.mu.Unlock()
	m.tsctx.WithLock(func(*Context) { m.membership.Set(next) })
	m.bumpOrder()
}

// applyMove bumps the order signal only when the order actually changed. A
// no-op move still reports success but must invalidate no reader.
func (m *ThreadSafeReactiveMap[K, V, H]) applyMove(outcome mapMove) bool {
	if !outcome.applied() {
		return false
	}
	if outcome.changed() {
		m.bumpOrder()
	}
	return true
}

// mintWith allocates key's entry node on first access, caches the handle,
// records order, and bumps reactive membership. A warm key returns its cached
// handle unchanged (cell-identity).
func (m *ThreadSafeReactiveMap[K, V, H]) mintWith(key K, compute func() V) H {
	m.mu.RLock()
	warm, ok := m.keyed.get(key)
	m.mu.RUnlock()
	if ok {
		return warm
	}

	// Allocate off mu, under the context lock.
	var fresh H
	m.tsctx.WithLock(func(ctx *Context) { fresh = m.mint(ctx, compute) })

	m.mu.Lock()
	stored, mutation := m.keyed.insert(key, fresh)
	m.mu.Unlock()

	// Lost a materialization race: first writer wins so the key keeps a stable
	// handle. Our freshly-allocated node is orphaned (never observed).
	if mutation.changed() {
		m.bumpMembership()
	}
	return stored
}

// read runs a graph read under the context lock.
//
// EVERY read that touches the graph has to go through here, not just the writes.
// A read is not passive in this kernel: a `Get` of a stale derived entry runs
// `refresh` → `recomputeNow` → `Context.newCompute`, which bumps `computeGen`
// and `cachedCount` on the shared single-threaded Context. Two goroutines
// reading two *different* keys therefore write the same context fields, and
// `-race` reported exactly that. Even a `Source` read mutates the dependency
// edge set when `c` is a `*Compute`.
//
// The lock is reentrant, so this is safe when the caller already holds it —
// which it does whenever an entry's compute reads back into the map. Lock order
// stays context-then-`mu`: `mu` is never held across graph work (see mintWith),
// so this cannot invert against a concurrent mint.
func (m *ThreadSafeReactiveMap[K, V, H]) read(fn func() V) V {
	return Read(m.tsctx, func(*Context) V { return fn() })
}

// track runs a subscription-only graph read (a membership or order signal) under
// the context lock. Same argument as read: registering a dependency edge is a
// graph mutation.
func (m *ThreadSafeReactiveMap[K, V, H]) track(fn func()) {
	m.tsctx.WithLock(func(*Context) { fn() })
}

// GetOrInsertWith returns key's value, minting the entry via factory(key) on
// first access (the lazy pull). An existing key returns its current value
// without re-running the factory.
func (m *ThreadSafeReactiveMap[K, V, H]) GetOrInsertWith(c ComputeOps, key K, factory func(K) V) V {
	h := m.mintWith(key, func() V { return factory(key) })
	return m.read(func() V { return m.observe(c, h) })
}

// Handle returns key's existing entry handle, or (zero, false). Non-minting.
func (m *ThreadSafeReactiveMap[K, V, H]) Handle(key K) (H, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keyed.get(key)
}

// Observe reads key's value if the entry is present, subscribing the caller to
// that entry's node. Returns (zero, false) if absent. Non-minting.
func (m *ThreadSafeReactiveMap[K, V, H]) Observe(c ComputeOps, key K) (V, bool) {
	h, ok := m.Handle(key)
	if !ok {
		var zero V
		return zero, false
	}
	return m.read(func() V { return m.observe(c, h) }), true
}

// IsPresent reports whether key is currently materialized. Non-reactive.
func (m *ThreadSafeReactiveMap[K, V, H]) IsPresent(key K) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keyed.contains(key)
}

// PresentKeys returns a snapshot of the currently-materialized keys, in current
// order. Non-reactive — see Keys for the tracked read.
func (m *ThreadSafeReactiveMap[K, V, H]) PresentKeys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keyed.keys()
}

// PresentCount returns the number of currently-materialized entries.
func (m *ThreadSafeReactiveMap[K, V, H]) PresentCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keyed.length()
}

// -- Core surface: ordering, atomic move, and reactive membership --

// Keys returns a reactive snapshot of the keys in their current order.
// Subscribes the caller to order changes (add/remove and move/reorder), not to
// per-entry value changes.
func (m *ThreadSafeReactiveMap[K, V, H]) Keys(c ComputeOps) []K {
	m.track(func() { Get[int](c, m.orderSignal) })
	return m.PresentKeys()
}

// Len reports the reactive entry count. Subscribes the caller to membership
// changes only.
func (m *ThreadSafeReactiveMap[K, V, H]) Len(c ComputeOps) int {
	m.track(func() { Get[int](c, m.membership) })
	return m.PresentCount()
}

// IsEmpty reports the reactive emptiness check.
func (m *ThreadSafeReactiveMap[K, V, H]) IsEmpty(c ComputeOps) bool { return m.Len(c) == 0 }

// ContainsKey reports the reactive membership test for key. Subscribes the
// caller to membership changes (add/remove of any key), not to value changes.
func (m *ThreadSafeReactiveMap[K, V, H]) ContainsKey(c ComputeOps, key K) bool {
	m.track(func() { Get[int](c, m.membership) })
	return m.IsPresent(key)
}

// LenUntracked reports the non-reactive count.
func (m *ThreadSafeReactiveMap[K, V, H]) LenUntracked() int { return m.PresentCount() }

// Position reports key's current 0-based position in the order. Non-reactive.
func (m *ThreadSafeReactiveMap[K, V, H]) Position(key K) (int, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keyed.position(key)
}

// MoveTo atomically moves key to index in the order (#lzcellmove). The entry
// keeps the same node, the same dependents, and its CRDT lineage — unlike a
// Remove + re-mint, which re-allocates and bumps membership twice. Only the
// order signal is bumped, so Keys readers recompute while Len / ContainsKey
// readers stay cached. index is clamped to [0, len).
func (m *ThreadSafeReactiveMap[K, V, H]) MoveTo(key K, index int) bool {
	m.mu.Lock()
	outcome := m.keyed.moveTo(key, index)
	m.mu.Unlock()
	return m.applyMove(outcome)
}

// MoveBefore atomically moves key to just before anchor (#lzcellmove).
func (m *ThreadSafeReactiveMap[K, V, H]) MoveBefore(key, anchor K) bool {
	m.mu.Lock()
	outcome := m.keyed.moveBefore(key, anchor)
	m.mu.Unlock()
	return m.applyMove(outcome)
}

// MoveAfter atomically moves key to just after anchor (#lzcellmove).
func (m *ThreadSafeReactiveMap[K, V, H]) MoveAfter(key, anchor K) bool {
	m.mu.Lock()
	outcome := m.keyed.moveAfter(key, anchor)
	m.mu.Unlock()
	return m.applyMove(outcome)
}

// Remove removes key's entry, detaching the removed node so no reader is left
// on a stale value, and bumps reactive membership. Returns whether the key was
// present.
func (m *ThreadSafeReactiveMap[K, V, H]) Remove(key K) bool {
	m.mu.Lock()
	h, mutation := m.keyed.remove(key)
	m.mu.Unlock()
	if !mutation.changed() {
		return false
	}
	// Off mu: teardown and the membership bump can both drive a dependent
	// recompute that re-enters this map.
	m.tsctx.WithLock(func(*Context) { m.clear(h) })
	m.bumpMembership()
	return true
}

// EntryKind returns this map's entry kind.
func (m *ThreadSafeReactiveMap[K, V, H]) EntryKind() EntryKind { return m.kind }

// ThreadSafeSourceMap is the input-cell specialization of ThreadSafeReactiveMap:
// every entry is a settable input cell. Adds the cell-only Set.
type ThreadSafeSourceMap[K comparable, V comparable] struct {
	*ThreadSafeReactiveMap[K, V, *Source[V]]
}

// NewThreadSafeSourceMap creates an empty thread-safe input-cell map bound to ts.
func NewThreadSafeSourceMap[K comparable, V comparable](ts *ThreadSafeContext) *ThreadSafeSourceMap[K, V] {
	return &ThreadSafeSourceMap[K, V]{newThreadSafeReactiveMap[K, V, *Source[V]](
		ts,
		EntryKindSource,
		func(ctx *Context, compute func() V) *Source[V] { return NewSource(ctx, compute()) },
		func(c ComputeOps, h *Source[V]) V { return Get[V](c, h) },
		func(h *Source[V]) { h.Invalidate() },
	)}
}

// ThreadSafeCellMap is the pre-v2-kernel name for ThreadSafeSourceMap.
//
// Deprecated: renamed to ThreadSafeSourceMap.
type ThreadSafeCellMap[K comparable, V comparable] = ThreadSafeSourceMap[K, V]

// NewThreadSafeCellMap creates an empty thread-safe input-cell map.
//
// Deprecated: renamed to NewThreadSafeSourceMap.
func NewThreadSafeCellMap[K comparable, V comparable](ts *ThreadSafeContext) *ThreadSafeSourceMap[K, V] {
	return NewThreadSafeSourceMap[K, V](ts)
}

// Cell returns key's existing input cell, or nil. Non-reactive.
func (m *ThreadSafeSourceMap[K, V]) Cell(key K) *Source[V] {
	h, _ := m.Handle(key)
	return h
}

// Set overwrites key's value, materializing the entry if absent. Cell-only: a
// derived ComputedMap slot is not settable. Updating an existing entry leaves
// membership and order untouched and invalidates only that entry's dependents.
func (m *ThreadSafeSourceMap[K, V]) Set(key K, value V) {
	if h, ok := m.Handle(key); ok {
		m.tsctx.WithLock(func(*Context) { h.Set(value) })
		return
	}
	m.mintWith(key, func() V { return value })
}

// ThreadSafeComputedMap is the derived-slot specialization: GetOrInsertWith
// mints a slot on first access (lazy); MaterializeAll pre-mints the keyset
// (eager). No Set.
type ThreadSafeComputedMap[K comparable, V comparable] struct {
	*ThreadSafeReactiveMap[K, V, *Computed[V]]
}

// NewThreadSafeComputedMap creates an empty thread-safe derived-slot map.
func NewThreadSafeComputedMap[K comparable, V comparable](ts *ThreadSafeContext) *ThreadSafeComputedMap[K, V] {
	return &ThreadSafeComputedMap[K, V]{newThreadSafeReactiveMap[K, V, *Computed[V]](
		ts,
		EntryKindComputed,
		func(ctx *Context, compute func() V) *Computed[V] {
			return NewSlot(ctx, func(c *Compute) V { return compute() })
		},
		func(c ComputeOps, h *Computed[V]) V { return Get[V](c, h) },
		func(h *Computed[V]) { h.invalidate() },
	)}
}

// ThreadSafeSlotMap is the pre-v2-kernel name for ThreadSafeComputedMap.
//
// Deprecated: renamed to ThreadSafeComputedMap.
type ThreadSafeSlotMap[K comparable, V comparable] = ThreadSafeComputedMap[K, V]

// NewThreadSafeSlotMap creates an empty thread-safe derived-slot map.
//
// Deprecated: renamed to NewThreadSafeComputedMap.
func NewThreadSafeSlotMap[K comparable, V comparable](ts *ThreadSafeContext) *ThreadSafeComputedMap[K, V] {
	return NewThreadSafeComputedMap[K, V](ts)
}

// Slot returns key's derived slot handle, or nil. Non-reactive.
func (m *ThreadSafeComputedMap[K, V]) Slot(key K) *Computed[V] {
	h, _ := m.Handle(key)
	return h
}

// MaterializeAll eagerly pre-mints every key via factory. Observationally
// identical to minting each lazily on first read.
func (m *ThreadSafeComputedMap[K, V]) MaterializeAll(keys []K, factory func(K) V) {
	for _, key := range keys {
		m.mintWith(key, func() V { return factory(key) })
	}
}
