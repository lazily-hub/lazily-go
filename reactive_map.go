package lazily

// Unified keyed reactive collection — the generic ReactiveMap primitive and its
// ComputedMap specialization (#reactivemap). SourceMap (collections.go) is the other
// specialization.
//
// Spec:   lazily-spec/cell-model.md § Keyed cell collections
// Formal: lazily-formal/LazilyFormal/Materialization.lean
// Rust reference: lazily-rs/src/cell_family.rs
//
// There is a single keyed primitive, ReactiveMap[K, V, H], generic over the
// entry's handle kind H (*Cell[V] input cells or *Slot[V] derived slots — the two
// node kinds of the cell model). It adds a keyed, reactive layer on top of the
// Context's opaque node graph: a hash collection whose *membership* and *order*
// are themselves reactive, with one independently-tracked reactive node per
// entry.
//
//   - SourceMap[K, V]   = ReactiveMap over *Cell[V] — input-cell entries. Adds
//     the cell-only Set and eager value-minting (Entry / EntryWith).
//   - ComputedMap[K, V] = ReactiveMap over *Slot[V] — derived-slot entries.
//     GetOrInsertWith mints a slot on first access (lazy materialization);
//     MaterializeAll pre-mints the keyset (eager). A slot's value is derived, so
//     ComputedMap has no Set. There is no eager/lazy mode flag — eager is a
//     pre-mint loop, lazy is mint-on-access.
//
// The shared surface — GetOrInsertWith / Remove / Move* / membership / order /
// Keys / Len / ContainsKey — lives on the generic ReactiveMap. Go generics cannot
// add methods to a type alias, so SourceMap and ComputedMap are thin distinct
// structs embedding *ReactiveMap with the handle kind fixed; the kind-specific
// methods (Set on SourceMap, MaterializeAll on ComputedMap) live on those wrappers.

// EntryKind is which kind of reactive node a ReactiveMap entry is — the
// handle-kind axis the map abstracts over. Mirrors EntryKind in lazily-formal's
// Materialization module and the Rust MapHandle::KIND.
type EntryKind int

const (
	// EntryKindCell is an input cell (*Cell[V]) — always materialized on read.
	EntryKindCell EntryKind = iota
	// EntryKindSlot is a derived slot (*Slot[V]) — materialized eagerly (pre-mint)
	// or lazily on first read.
	EntryKindSlot
)

// String renders the entry kind for diagnostics.
func (k EntryKind) String() string {
	switch k {
	case EntryKindCell:
		return "cell"
	case EntryKindSlot:
		return "slot"
	default:
		return "unknown"
	}
}

// ReactiveMap is a keyed reactive collection generic over the entry handle kind
// H (*Cell[V] input cells or *Slot[V] derived slots): a hash map of K -> H with
// reactive membership and independently-tracked per-entry nodes.
//
// Operations run against the owning Context, single-goroutine like the rest of
// lazily (the reactive graph is not itself synchronized). The three reactivity
// planes stay independent: writing one entry's value invalidates only that
// entry's readers; add/remove invalidates membership readers (Len / ContainsKey)
// and order readers (Keys); a pure reorder (atomic move) invalidates order
// readers only.
//
// The handle-kind operations (mint / observe / clear) are supplied by the
// SourceMap / ComputedMap constructor — the Go analog of the Rust MapHandle trait.
type ReactiveMap[K comparable, V comparable, H any] struct {
	ctx  *Context
	kind EntryKind

	// mint allocates one entry node with compute as its canonical value producer
	// (a cell sets it directly; a derived slot wraps it as its recomputation).
	mint func(ctx *Context, compute func() V) H
	// observe reads the entry through its owning context (subscribes the caller).
	observe func(h H) V
	// clear detaches a removed entry's node from the graph (clears dependents).
	clear func(h H)

	entries map[K]H
	// order is the authoritative key list in first-materialization order.
	order []K

	// membership is bumped only when the set of keys changes (add/remove).
	membership *Source[int]
	// orderSignal is bumped on add/remove and on move/reorder.
	orderSignal *Source[int]

	membershipVersion int
	orderVersion      int
}

// newReactiveMap builds the shared reactive-map core bound to ctx, with the
// given handle-kind operations. Used by NewSourceMap / NewComputedMap.
func newReactiveMap[K comparable, V comparable, H any](
	ctx *Context,
	kind EntryKind,
	mint func(ctx *Context, compute func() V) H,
	observe func(h H) V,
	clear func(h H),
) *ReactiveMap[K, V, H] {
	return &ReactiveMap[K, V, H]{
		ctx:         ctx,
		kind:        kind,
		mint:        mint,
		observe:     observe,
		clear:       clear,
		entries:     map[K]H{},
		order:       nil,
		membership:  NewSource[int](ctx, 0),
		orderSignal: NewSource[int](ctx, 0),
	}
}

// bumpOrder bumps only the order signal (invalidates Keys readers). A pure move
// bumps only this.
func (m *ReactiveMap[K, V, H]) bumpOrder() {
	m.orderVersion++
	m.orderSignal.Set(m.orderVersion)
}

// bumpMembership bumps set-membership (invalidates Len / ContainsKey readers),
// then the order signal too (the key set changed, so the ordered list did too).
func (m *ReactiveMap[K, V, H]) bumpMembership() {
	m.membershipVersion++
	m.membership.Set(m.membershipVersion)
	m.bumpOrder()
}

// mintWith mints the entry node for key on first access (via mint with compute
// as its canonical value producer), caches the handle, records order, and bumps
// reactive membership. Re-minting an existing key returns the cached handle.
func (m *ReactiveMap[K, V, H]) mintWith(key K, compute func() V) H {
	if h, ok := m.entries[key]; ok {
		return h // warm: already allocated.
	}
	h := m.mint(m.ctx, compute)
	m.entries[key] = h
	m.order = append(m.order, key)
	m.bumpMembership()
	return h
}

// GetOrInsertWith returns the value at key, minting the entry via factory(key)
// first if absent — the mint-on-access recipe. For a ComputedMap this is the lazy
// materialization pull; for a SourceMap it seeds an input cell. Bumps reactive
// membership only on insert; an existing key returns its current value without
// re-running the factory.
func (m *ReactiveMap[K, V, H]) GetOrInsertWith(key K, factory func(K) V) V {
	if h, ok := m.entries[key]; ok {
		return m.observe(h)
	}
	h := m.mintWith(key, func() V { return factory(key) })
	return m.observe(h)
}

// Handle returns the existing entry handle for key, or (zero, false).
// Non-reactive: does not subscribe the caller to membership.
func (m *ReactiveMap[K, V, H]) Handle(key K) (H, bool) {
	h, ok := m.entries[key]
	return h, ok
}

// Observe reads the value at key if present, subscribing the caller to that
// entry's node (reactive on that entry only). Returns (zero, false) if absent.
func (m *ReactiveMap[K, V, H]) Observe(key K) (V, bool) {
	if h, ok := m.entries[key]; ok {
		return m.observe(h), true
	}
	var zero V
	return zero, false
}

// Remove removes key's entry. Bumps reactive membership and clears the removed
// entry's dependents. Returns whether the key was present.
//
// The orphaned node stops driving any dependents; the runtime exposes no
// node-recycle yet (mirrors lazily-rs).
func (m *ReactiveMap[K, V, H]) Remove(key K) bool {
	h, ok := m.entries[key]
	if !ok {
		return false
	}
	delete(m.entries, key)
	m.removeFromOrder(key)
	m.clear(h)
	m.bumpMembership()
	return true
}

func (m *ReactiveMap[K, V, H]) removeFromOrder(key K) {
	for i, k := range m.order {
		if k == key {
			m.order = append(m.order[:i], m.order[i+1:]...)
			return
		}
	}
}

// Keys returns a reactive snapshot of the keys in their current order.
// Subscribes the caller to order changes (add/remove and move/reorder), not to
// per-entry value changes.
func (m *ReactiveMap[K, V, H]) Keys() []K {
	m.orderSignal.Get() // subscribe to the order signal.
	out := make([]K, len(m.order))
	copy(out, m.order)
	return out
}

// PresentKeys returns the currently-materialized keys in first-materialization
// order. Non-reactive; the present set only grows (deferral, not de-allocation).
func (m *ReactiveMap[K, V, H]) PresentKeys() []K {
	out := make([]K, len(m.order))
	copy(out, m.order)
	return out
}

// PresentCount returns the number of currently-materialized entries.
// Non-reactive.
func (m *ReactiveMap[K, V, H]) PresentCount() int { return len(m.order) }

// IsPresent reports whether key is currently materialized (present in the
// allocated set). Non-reactive.
func (m *ReactiveMap[K, V, H]) IsPresent(key K) bool {
	_, ok := m.entries[key]
	return ok
}

// Position reports the current 0-based position of key in the order, or false
// if absent. Non-reactive.
func (m *ReactiveMap[K, V, H]) Position(key K) (int, bool) {
	for i, k := range m.order {
		if k == key {
			return i, true
		}
	}
	return 0, false
}

// MoveTo atomically moves key to index in the order (#lzcellmove).
//
// This is the atomic, optimized reorder: the entry keeps the same node, the same
// dependents, and its CRDT lineage — unlike the naive Remove + re-mint which
// re-allocates the node and bumps membership twice. Only the order signal is
// bumped (once), so Keys readers recompute but Len / ContainsKey readers stay
// cached.
//
// index is clamped to [0, len). A no-op move (already at position) bumps
// nothing. Returns whether key was present.
func (m *ReactiveMap[K, V, H]) MoveTo(key K, index int) bool {
	from, ok := m.Position(key)
	if !ok {
		return false
	}
	to := index
	if to < 0 {
		to = 0
	}
	if hi := len(m.order) - 1; to > hi {
		to = hi
	}
	if from == to {
		return true
	}
	m.order = append(m.order[:from], m.order[from+1:]...)
	// Re-insert at `to`.
	m.order = append(m.order, key) // grow
	copy(m.order[to+1:], m.order[to:])
	m.order[to] = key
	m.bumpOrder()
	return true
}

// MoveBefore atomically moves key to just before anchor (#lzcellmove). Returns
// whether the move could be expressed.
func (m *ReactiveMap[K, V, H]) MoveBefore(key, anchor K) bool {
	anchorIdx, ok := m.Position(anchor)
	if !ok {
		return false
	}
	from, ok := m.Position(key)
	if !ok {
		return false
	}
	// Removing key first shifts anchor left by one when key precedes it.
	target := anchorIdx
	if from < anchorIdx {
		target = anchorIdx - 1
	}
	return m.MoveTo(key, target)
}

// MoveAfter atomically moves key to just after anchor (#lzcellmove).
func (m *ReactiveMap[K, V, H]) MoveAfter(key, anchor K) bool {
	anchorIdx, ok := m.Position(anchor)
	if !ok {
		return false
	}
	from, ok := m.Position(key)
	if !ok {
		return false
	}
	target := anchorIdx + 1
	if from <= anchorIdx {
		target = anchorIdx
	}
	return m.MoveTo(key, target)
}

// Len reports the reactive entry count. Subscribes the caller to membership
// changes only.
func (m *ReactiveMap[K, V, H]) Len() int {
	m.membership.Get()
	return len(m.order)
}

// IsEmpty reports the reactive emptiness check. Subscribes the caller to
// membership changes.
func (m *ReactiveMap[K, V, H]) IsEmpty() bool { return m.Len() == 0 }

// ContainsKey reports the reactive membership test for key. Subscribes the
// caller to membership changes (add/remove of any key), not to value changes.
func (m *ReactiveMap[K, V, H]) ContainsKey(key K) bool {
	m.membership.Get()
	_, ok := m.entries[key]
	return ok
}

// LenUntracked reports the non-reactive count. Does not subscribe the caller to
// anything.
func (m *ReactiveMap[K, V, H]) LenUntracked() int { return len(m.order) }

// EntryKind returns this map's entry kind (EntryKindCell for a SourceMap,
// EntryKindSlot for a ComputedMap).
func (m *ReactiveMap[K, V, H]) EntryKind() EntryKind { return m.kind }

// ComputedMap is the derived-slot specialization of ReactiveMap: every entry is
// a derived *Slot[V]. GetOrInsertWith mints a slot on first access (lazy
// materialization); MaterializeAll pre-mints the keyset (eager). A slot's value
// is derived, so ComputedMap has no Set.
type ComputedMap[K comparable, V comparable] struct {
	*ReactiveMap[K, V, *Computed[V]]
}

// NewComputedMap creates an empty derived-slot map bound to ctx.
func NewComputedMap[K comparable, V comparable](ctx *Context) *ComputedMap[K, V] {
	rm := newReactiveMap[K, V, *Computed[V]](
		ctx,
		EntryKindSlot,
		func(ctx *Context, compute func() V) *Computed[V] {
			return NewSlot(ctx, func(c *Compute) V { return compute() })
		},
		func(h *Computed[V]) V { return h.Get() },
		func(h *Computed[V]) { h.invalidate() },
	)
	return &ComputedMap[K, V]{ReactiveMap: rm}
}

// SlotMap is the pre-v2-kernel name for ComputedMap, kept as an alias so
// existing callers keep compiling.
//
// Deprecated: renamed to ComputedMap.
type SlotMap[K comparable, V comparable] = ComputedMap[K, V]

// NewSlotMap creates an empty derived-slot map bound to ctx.
//
// Deprecated: renamed to NewComputedMap.
func NewSlotMap[K comparable, V comparable](ctx *Context) *ComputedMap[K, V] {
	return NewComputedMap[K, V](ctx)
}

// Slot returns the derived slot handle for key (nil if not materialized).
// Non-reactive.
func (m *ComputedMap[K, V]) Slot(key K) *Computed[V] {
	h, _ := m.Handle(key)
	return h
}

// MaterializeAll eagerly pre-mints a derived slot for every key via factory, up
// front. Observationally identical to minting each key lazily on first read
// (GetOrInsertWith) — it only changes when the nodes are allocated.
func (m *ComputedMap[K, V]) MaterializeAll(keys []K, factory func(K) V) {
	for _, key := range keys {
		m.GetOrInsertWith(key, factory)
	}
}
