package lazily

// Async keyed reactive map (`AsyncReactiveMap`) — the async flavor of
// ReactiveMap (#reactivemap, async).
//
// Spec:   lazily-spec/cell-model.md § Keyed cell collections (async)
// Formal: lazily-formal/LazilyFormal/AsyncMaterialization.lean
//   (eventual_transparency, async_resolved_matches_sync, observe_pending_is_none,
//    cell_resolved_at_build, resolve_monotone, resolve_preserves_observe)
// Rust reference: lazily-rs/src/async_reactive_family.rs
//
// Keys K map to per-entry async reactive values guarded by a sync.Mutex, so the
// map can live in a cross-goroutine owner. Async adds a resolution axis
// orthogonal to the present-set (allocation) axis: a derived (slot) entry is
// pending until driven to resolution (Drive, the analog of
// AsyncContext.GetAsync), then resolved. Input (cell) entries are resolved at
// mint. A non-blocking read returns (value, ok): (_, false) while pending,
// (value, true) once resolved.
//
// The single-threaded transparency law weakens to eventual transparency: once a
// node resolves, its observed value is the canonical value — identical to what
// the synchronous map observes.
//
// Its two specializations are AsyncSourceMap (input cells — adds Set) and
// AsyncComputedMap (derived slots — adds MaterializeAll, no Set). Go generics
// cannot add methods to a type alias, so both are thin distinct structs embedding
// *AsyncReactiveMap with the handle kind fixed.

import "sync"

// AsyncMapHandle is the entry-handle kind an AsyncReactiveMap abstracts over —
// the async analog of the Rust AsyncMapHandle trait. Sealed to the two node kinds
// of the cell model. resolvedOnMint reports whether a freshly-minted entry is
// resolved (a cell — always resolved) or pending (a derived slot).
type AsyncMapHandle interface {
	mapEntryKind() EntryKind
	resolvedOnMint() bool
}

type asyncSourceNodeHandle struct{}

func (asyncSourceNodeHandle) mapEntryKind() EntryKind { return EntryKindSource }
func (asyncSourceNodeHandle) resolvedOnMint() bool    { return true }

type asyncComputedNodeHandle struct{}

func (asyncComputedNodeHandle) mapEntryKind() EntryKind { return EntryKindComputed }
func (asyncComputedNodeHandle) resolvedOnMint() bool    { return false }

// asyncMapEntry is one allocated (present) entry: resolved tracks the async
// resolution axis, value caches the canonical value once resolved. A pending
// entry's value is the zero value and unspecified.
type asyncMapEntry[V comparable] struct {
	// id is the entry's stable birth identity. It survives a reorder and changes
	// only on a re-mint, which is exactly what the atomic-move law needs to
	// distinguish a move from a remove + insert.
	//
	// It is an identity, not a graph node: async entries are still cached values
	// rather than nodes on the async graph, so they carry no dependents and no
	// lineage. That is the same partial status lazily-zig's sync map carries, and
	// it is tracked separately — the Core surface below (ordering, atomic move,
	// reactive membership) is fully graph-backed regardless, because none of it
	// touches an entry handle.
	id       uint64
	resolved bool
	// cell is the entry's node on the async graph. Entries used to be plain
	// cached values, which meant a per-entry read registered no edge and a write
	// invalidated nobody — the map was a value cache wearing the reactive
	// family's name. The resolution axis is orthogonal: `resolved` still tracks
	// pending-vs-resolved, while the cell carries the value and its dependents.
	cell *AsyncSource[V]
}

// value reads the entry's canonical value without registering an edge.
func (e asyncMapEntry[V]) value() V {
	var zero V
	if e.cell == nil {
		return zero
	}
	return e.cell.Peek()
}

// AsyncReactiveMap is the async keyed reactive map (#reactivemap) generic over
// the entry handle kind H, each entry carrying a resolution flag. V is comparable
// to mirror the single-threaded map.
//
// Once built its address is stable, so concurrent readers may share a
// *AsyncReactiveMap. See the package doc for the eager/lazy contract, present-set
// monotonicity, and the eventual-transparency law.
type AsyncReactiveMap[K comparable, V comparable, H AsyncMapHandle] struct {
	actx *AsyncContext

	mu sync.Mutex
	// keyed is the present set + key order + the move algebra, shared with the
	// other two flavors. Guarded by mu.
	keyed keyedOrder[K, asyncMapEntry[V]]

	// Membership and order signals minted on THIS flavor's graph — the async
	// graph, not the synchronous one. Ordering is not async-coloured (a move
	// awaits nothing), so the async map carries the same Core surface as the
	// other two flavors.
	membership  *AsyncSource[int]
	orderSignal *AsyncSource[int]

	membershipVersion int
	orderVersion      int
	nextEntryID       uint64
}

func newAsyncReactiveMap[K comparable, V comparable, H AsyncMapHandle](c *AsyncContext) *AsyncReactiveMap[K, V, H] {
	return &AsyncReactiveMap[K, V, H]{
		actx:        c,
		keyed:       newKeyedOrder[K, asyncMapEntry[V]](),
		membership:  NewAsyncSource(c, 0),
		orderSignal: NewAsyncSource(c, 0),
	}
}

// trackOrder / trackMembership register the read edge when the caller is inside
// an async compute, and read untracked otherwise. A read spellable only as a
// zero-argument call could never subscribe from inside a derived node.
func (m *AsyncReactiveMap[K, V, H]) trackOrder(cc *AsyncComputeContext) {
	if cc != nil {
		TrackSource(cc, m.orderSignal)
		return
	}
	m.orderSignal.Peek()
}

func (m *AsyncReactiveMap[K, V, H]) trackMembership(cc *AsyncComputeContext) {
	if cc != nil {
		TrackSource(cc, m.membership)
		return
	}
	m.membership.Peek()
}

// bumpOrder bumps only the order signal (invalidates Keys readers). Runs with mu
// released: a set can drive a dependent recompute that re-enters this map.
func (m *AsyncReactiveMap[K, V, H]) bumpOrder() {
	m.mu.Lock()
	m.orderVersion++
	next := m.orderVersion
	m.mu.Unlock()
	m.orderSignal.Set(next)
}

// bumpMembership bumps set-membership, then the order signal too — the key set
// changed, so the ordered list did too.
func (m *AsyncReactiveMap[K, V, H]) bumpMembership() {
	m.mu.Lock()
	m.membershipVersion++
	next := m.membershipVersion
	m.mu.Unlock()
	m.membership.Set(next)
	m.bumpOrder()
}

// applyMove bumps the order signal only when the order actually changed.
func (m *AsyncReactiveMap[K, V, H]) applyMove(outcome mapMove) bool {
	if !outcome.applied() {
		return false
	}
	if outcome.changed() {
		m.bumpOrder()
	}
	return true
}

// materializeLocked allocates key if absent (present-set grows), recording order.
// A cell entry is resolved immediately with factory(key); a slot entry starts
// pending. Caller MUST hold mu. A warm key is a no-op.
// Returns the entry plus whether this call allocated it, so the caller can bump
// membership after releasing mu.
func (m *AsyncReactiveMap[K, V, H]) materialize(key K, factory func(K) V) (asyncMapEntry[V], bool) {
	m.mu.Lock()
	if e, ok := m.keyed.get(key); ok {
		m.mu.Unlock()
		return e, false // warm.
	}
	m.mu.Unlock()

	// Allocate the node off mu: minting posts to the async loop, and a set can
	// drive a dependent recompute that re-enters this map.
	var h H
	var zero V
	e := asyncMapEntry[V]{cell: NewAsyncSource(m.actx, zero)}
	if h.resolvedOnMint() {
		e.resolved = true
		e.cell.Set(factory(key))
	}

	m.mu.Lock()
	m.nextEntryID++
	e.id = m.nextEntryID
	// First writer wins on a race so the key keeps a stable identity.
	stored, mutation := m.keyed.insert(key, e)
	m.mu.Unlock()
	return stored, mutation.changed()
}

// Observe is a non-blocking read: (value, true) once resolved, (_, false) while
// pending. Allocates the entry via factory if absent — a freshly allocated slot
// is pending, so a first Observe of a slot returns (_, false) until Driven; a
// cell is resolved at allocation, so it returns (value, true) immediately.
func (m *AsyncReactiveMap[K, V, H]) Observe(key K, factory func(K) V) (V, bool) {
	e, allocated := m.materialize(key, factory)
	if allocated {
		m.bumpMembership()
	}
	if e.resolved {
		return e.value(), true
	}
	var zero V
	return zero, false
}

// GetOrInsertWith mints key on first access and returns the current observation:
// (value, true) for a cell or a warm-resolved slot, (_, false) for a freshly
// pending slot. Mint-on-access; drive a pending slot with Drive.
func (m *AsyncReactiveMap[K, V, H]) GetOrInsertWith(key K, factory func(K) V) (V, bool) {
	return m.Observe(key, factory)
}

// Drive drives key to resolution — the analog of AsyncContext.GetAsync: allocate
// if absent, resolve if pending (produce + cache the canonical value via
// factory), and return the resolved value. A warm-resolved key returns its cached
// value unchanged. The eventual-transparency completion.
func (m *AsyncReactiveMap[K, V, H]) Drive(key K, factory func(K) V) V {
	e, allocated := m.materialize(key, factory)
	if !e.resolved {
		// Resolution keeps the entry's identity AND its node: driving a pending
		// slot is not a re-mint, so its dependents survive.
		e.cell.Set(factory(key))
		m.mu.Lock()
		e.resolved = true
		m.keyed.entries[key] = e
		m.mu.Unlock()
	}
	if allocated {
		m.bumpMembership()
	}
	return e.value()
}

// IsPresent reports whether key is currently allocated (present). Non-reactive.
func (m *AsyncReactiveMap[K, V, H]) IsPresent(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keyed.contains(key)
}

// IsResolved reports whether key is allocated AND resolved (a non-blocking
// Observe would return a value).
func (m *AsyncReactiveMap[K, V, H]) IsResolved(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.keyed.get(key)
	return ok && e.resolved
}

// PresentKeys returns a stable snapshot of the currently-allocated keys, in
// first-materialization order (a copy — the internal order must not escape the
// lock).
func (m *AsyncReactiveMap[K, V, H]) PresentKeys() []K {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keyed.keys()
}

// PresentCount returns the number of currently-allocated entries.
func (m *AsyncReactiveMap[K, V, H]) PresentCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keyed.length()
}

// -- Core surface: ordering, atomic move, and reactive membership --

// Entry returns key's node on the async graph, or nil. Reading it through
// TrackSource inside an async compute registers a per-entry dependency edge.
func (m *AsyncReactiveMap[K, V, H]) Entry(key K) *AsyncSource[V] {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.keyed.get(key)
	if !ok {
		return nil
	}
	return e.cell
}

// ObserveTracked is the reactive per-entry read: inside an async compute it
// registers an edge against that entry's node, so a later write to this key
// invalidates the reader and a write to any OTHER key does not.
func (m *AsyncReactiveMap[K, V, H]) ObserveTracked(cc *AsyncComputeContext, key K) (V, bool) {
	m.mu.Lock()
	e, ok := m.keyed.get(key)
	m.mu.Unlock()
	if !ok || !e.resolved {
		var zero V
		return zero, false
	}
	if cc != nil {
		return TrackSource(cc, e.cell), true
	}
	return e.value(), true
}

// EntryID returns key's stable birth identity — the async analog of a node
// handle. It survives a reorder and changes only on a re-mint.
func (m *AsyncReactiveMap[K, V, H]) EntryID(key K) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.keyed.get(key)
	return e.id, ok
}

// Keys returns a reactive snapshot of the keys in their current order.
// Subscribes the caller to order changes when read inside an async compute.
func (m *AsyncReactiveMap[K, V, H]) Keys(cc *AsyncComputeContext) []K {
	m.trackOrder(cc)
	return m.PresentKeys()
}

// Len reports the reactive entry count. Subscribes to membership changes only.
func (m *AsyncReactiveMap[K, V, H]) Len(cc *AsyncComputeContext) int {
	m.trackMembership(cc)
	return m.PresentCount()
}

// IsEmpty reports the reactive emptiness check.
func (m *AsyncReactiveMap[K, V, H]) IsEmpty(cc *AsyncComputeContext) bool { return m.Len(cc) == 0 }

// ContainsKey reports the reactive membership test for key.
func (m *AsyncReactiveMap[K, V, H]) ContainsKey(cc *AsyncComputeContext, key K) bool {
	m.trackMembership(cc)
	return m.IsPresent(key)
}

// LenUntracked reports the non-reactive count.
func (m *AsyncReactiveMap[K, V, H]) LenUntracked() int { return m.PresentCount() }

// Position reports key's current 0-based position in the order. Non-reactive.
func (m *AsyncReactiveMap[K, V, H]) Position(key K) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keyed.position(key)
}

// MoveTo atomically moves key to index in the order (#lzcellmove). The entry
// keeps its identity and its resolution state; only the order signal is bumped,
// so Keys readers recompute while Len / ContainsKey readers stay cached. index
// is clamped to [0, len).
func (m *AsyncReactiveMap[K, V, H]) MoveTo(key K, index int) bool {
	m.mu.Lock()
	outcome := m.keyed.moveTo(key, index)
	m.mu.Unlock()
	return m.applyMove(outcome)
}

// MoveBefore atomically moves key to just before anchor (#lzcellmove).
func (m *AsyncReactiveMap[K, V, H]) MoveBefore(key, anchor K) bool {
	m.mu.Lock()
	outcome := m.keyed.moveBefore(key, anchor)
	m.mu.Unlock()
	return m.applyMove(outcome)
}

// MoveAfter atomically moves key to just after anchor (#lzcellmove).
func (m *AsyncReactiveMap[K, V, H]) MoveAfter(key, anchor K) bool {
	m.mu.Lock()
	outcome := m.keyed.moveAfter(key, anchor)
	m.mu.Unlock()
	return m.applyMove(outcome)
}

// Remove removes key's entry and bumps reactive membership. Returns whether the
// key was present. The removed entry's cached value goes with it, so a later
// read cannot serve a stale resolution.
func (m *AsyncReactiveMap[K, V, H]) Remove(key K) bool {
	m.mu.Lock()
	_, mutation := m.keyed.remove(key)
	m.mu.Unlock()
	if !mutation.changed() {
		return false
	}
	m.bumpMembership()
	return true
}

// EntryKind returns this map's entry kind.
func (m *AsyncReactiveMap[K, V, H]) EntryKind() EntryKind {
	var h H
	return h.mapEntryKind()
}

// AsyncSourceMap is the input-cell specialization of AsyncReactiveMap: every entry
// is an always-resolved input cell. Adds the cell-only Set.
type AsyncSourceMap[K comparable, V comparable] struct {
	*AsyncReactiveMap[K, V, asyncSourceNodeHandle]
}

// NewAsyncSourceMap creates an empty async input-cell map.
func NewAsyncSourceMap[K comparable, V comparable](c *AsyncContext) *AsyncSourceMap[K, V] {
	return &AsyncSourceMap[K, V]{newAsyncReactiveMap[K, V, asyncSourceNodeHandle](c)}
}

// AsyncCellMap is the pre-v2-kernel name for AsyncSourceMap, kept as an alias so
// existing callers keep compiling.
//
// Deprecated: renamed to AsyncSourceMap.
type AsyncCellMap[K comparable, V comparable] = AsyncSourceMap[K, V]

// NewAsyncCellMap creates an empty async input-cell map through the v1 name.
//
// Deprecated: renamed to NewAsyncSourceMap.
func NewAsyncCellMap[K comparable, V comparable](c *AsyncContext) *AsyncSourceMap[K, V] {
	return NewAsyncSourceMap[K, V](c)
}

// Set overwrites key's value (cells are writable, always resolved), materializing
// the entry if absent. Cell-only: a derived AsyncComputedMap slot is not settable.
func (m *AsyncSourceMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	existing, warm := m.keyed.get(key)
	m.mu.Unlock()
	if warm {
		// Overwrite in place: the entry keeps its identity and node, membership
		// and order are untouched, and only this entry's readers see a change.
		existing.cell.Set(value)
		if !existing.resolved {
			m.mu.Lock()
			existing.resolved = true
			m.keyed.entries[key] = existing
			m.mu.Unlock()
		}
		return
	}
	_, allocated := m.materialize(key, func(K) V { return value })
	if allocated {
		m.bumpMembership()
	}
}

// AsyncDependencyMap is the async-graph exact-key availability family.
type AsyncDependencyMap[K comparable, V comparable] struct {
	*AsyncSourceMap[K, DependencyAvailability[V]]
}

// NewAsyncDependencyMap creates an empty async dependency family.
func NewAsyncDependencyMap[K comparable, V comparable](
	c *AsyncContext,
) *AsyncDependencyMap[K, V] {
	return &AsyncDependencyMap[K, V]{
		AsyncSourceMap: NewAsyncSourceMap[K, DependencyAvailability[V]](c),
	}
}

// ObserveDependency materializes one availability source and registers an
// exact-key async dependency when cc is non-nil.
func (m *AsyncDependencyMap[K, V]) ObserveDependency(
	cc *AsyncComputeContext,
	key K,
) DependencyAvailability[V] {
	m.Observe(key, func(K) DependencyAvailability[V] {
		return UnavailableDependency[V]()
	})
	value, ok := m.ObserveTracked(cc, key)
	if !ok {
		panic("dependency availability source must be resolved")
	}
	return value
}

// Publish transitions key to Available(value).
func (m *AsyncDependencyMap[K, V]) Publish(key K, value V) {
	m.Set(key, AvailableDependency(value))
}

// Unpublish transitions key back to Unavailable.
func (m *AsyncDependencyMap[K, V]) Unpublish(key K) {
	m.Set(key, UnavailableDependency[V]())
}

// AsyncComputedMap is the derived-slot specialization of AsyncReactiveMap: entries
// are minted pending and driven to resolution via Drive; MaterializeAll pre-mints
// the keyset (still pending until driven). No Set.
type AsyncComputedMap[K comparable, V comparable] struct {
	*AsyncReactiveMap[K, V, asyncComputedNodeHandle]
}

// NewAsyncComputedMap creates an empty async derived-slot map.
func NewAsyncComputedMap[K comparable, V comparable](c *AsyncContext) *AsyncComputedMap[K, V] {
	return &AsyncComputedMap[K, V]{newAsyncReactiveMap[K, V, asyncComputedNodeHandle](c)}
}

// AsyncSlotMap is the pre-v2-kernel name for AsyncComputedMap, kept as an alias
// so existing callers keep compiling.
//
// Deprecated: renamed to AsyncComputedMap.
type AsyncSlotMap[K comparable, V comparable] = AsyncComputedMap[K, V]

// NewAsyncSlotMap creates an empty async derived-slot map through the v1 name.
//
// Deprecated: renamed to NewAsyncComputedMap.
func NewAsyncSlotMap[K comparable, V comparable](c *AsyncContext) *AsyncComputedMap[K, V] {
	return NewAsyncComputedMap[K, V](c)
}

// MaterializeAll eagerly pre-mints (allocates, still pending) a derived slot for
// every key. Drive each to resolution. Observationally identical (once driven) to
// minting lazily on first access.
func (m *AsyncComputedMap[K, V]) MaterializeAll(keys []K, factory func(K) V) {
	for _, key := range keys {
		m.Observe(key, factory)
	}
}
