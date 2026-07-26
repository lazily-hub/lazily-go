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

type asyncCellHandle struct{}

func (asyncCellHandle) mapEntryKind() EntryKind { return EntryKindCell }
func (asyncCellHandle) resolvedOnMint() bool    { return true }

type asyncSlotHandle struct{}

func (asyncSlotHandle) mapEntryKind() EntryKind { return EntryKindSlot }
func (asyncSlotHandle) resolvedOnMint() bool    { return false }

// asyncMapEntry is one allocated (present) entry: resolved tracks the async
// resolution axis, value caches the canonical value once resolved. A pending
// entry's value is the zero value and unspecified.
type asyncMapEntry[V comparable] struct {
	resolved bool
	value    V
}

// AsyncReactiveMap is the async keyed reactive map (#reactivemap) generic over
// the entry handle kind H, each entry carrying a resolution flag. V is comparable
// to mirror the single-threaded map.
//
// Once built its address is stable, so concurrent readers may share a
// *AsyncReactiveMap. See the package doc for the eager/lazy contract, present-set
// monotonicity, and the eventual-transparency law.
type AsyncReactiveMap[K comparable, V comparable, H AsyncMapHandle] struct {
	mu sync.Mutex
	// materialized is the currently-allocated ("present") set. Guarded by mu;
	// grows on materialize, never shrinks. Resolution only ever flips false→true.
	materialized map[K]asyncMapEntry[V]
	// order is the present set in first-materialization order (grows only).
	order []K
}

func newAsyncReactiveMap[K comparable, V comparable, H AsyncMapHandle]() *AsyncReactiveMap[K, V, H] {
	return &AsyncReactiveMap[K, V, H]{materialized: map[K]asyncMapEntry[V]{}}
}

// materializeLocked allocates key if absent (present-set grows), recording order.
// A cell entry is resolved immediately with factory(key); a slot entry starts
// pending. Caller MUST hold mu. A warm key is a no-op.
func (m *AsyncReactiveMap[K, V, H]) materializeLocked(key K, factory func(K) V) asyncMapEntry[V] {
	if e, ok := m.materialized[key]; ok {
		return e // warm.
	}
	var h H
	var e asyncMapEntry[V]
	if h.resolvedOnMint() {
		e = asyncMapEntry[V]{resolved: true, value: factory(key)}
	} else {
		e = asyncMapEntry[V]{resolved: false}
	}
	m.materialized[key] = e
	m.order = append(m.order, key)
	return e
}

// Observe is a non-blocking read: (value, true) once resolved, (_, false) while
// pending. Allocates the entry via factory if absent — a freshly allocated slot
// is pending, so a first Observe of a slot returns (_, false) until Driven; a
// cell is resolved at allocation, so it returns (value, true) immediately.
func (m *AsyncReactiveMap[K, V, H]) Observe(key K, factory func(K) V) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.materializeLocked(key, factory)
	if e.resolved {
		return e.value, true
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
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.materializeLocked(key, factory)
	if !e.resolved {
		e = asyncMapEntry[V]{resolved: true, value: factory(key)}
		m.materialized[key] = e
	}
	return e.value
}

// IsPresent reports whether key is currently allocated (present). Non-reactive.
func (m *AsyncReactiveMap[K, V, H]) IsPresent(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.materialized[key]
	return ok
}

// IsResolved reports whether key is allocated AND resolved (a non-blocking
// Observe would return a value).
func (m *AsyncReactiveMap[K, V, H]) IsResolved(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.materialized[key]
	return ok && e.resolved
}

// PresentKeys returns a stable snapshot of the currently-allocated keys, in
// first-materialization order (a copy — the internal order must not escape the
// lock).
func (m *AsyncReactiveMap[K, V, H]) PresentKeys() []K {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]K, len(m.order))
	copy(out, m.order)
	return out
}

// PresentCount returns the number of currently-allocated entries.
func (m *AsyncReactiveMap[K, V, H]) PresentCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.order)
}

// EntryKind returns this map's entry kind.
func (m *AsyncReactiveMap[K, V, H]) EntryKind() EntryKind {
	var h H
	return h.mapEntryKind()
}

// AsyncSourceMap is the input-cell specialization of AsyncReactiveMap: every entry
// is an always-resolved input cell. Adds the cell-only Set.
type AsyncSourceMap[K comparable, V comparable] struct {
	*AsyncReactiveMap[K, V, asyncCellHandle]
}

// NewAsyncSourceMap creates an empty async input-cell map.
func NewAsyncSourceMap[K comparable, V comparable]() *AsyncSourceMap[K, V] {
	return &AsyncSourceMap[K, V]{newAsyncReactiveMap[K, V, asyncCellHandle]()}
}

// AsyncCellMap is the pre-v2-kernel name for AsyncSourceMap, kept as an alias so
// existing callers keep compiling.
//
// Deprecated: renamed to AsyncSourceMap.
type AsyncCellMap[K comparable, V comparable] = AsyncSourceMap[K, V]

// NewAsyncCellMap creates an empty async input-cell map.
//
// Deprecated: renamed to NewAsyncSourceMap.
func NewAsyncCellMap[K comparable, V comparable]() *AsyncSourceMap[K, V] {
	return NewAsyncSourceMap[K, V]()
}

// Set overwrites key's value (cells are writable, always resolved), materializing
// the entry if absent. Cell-only: a derived AsyncComputedMap slot is not settable.
func (m *AsyncSourceMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.materialized[key]; !ok {
		m.order = append(m.order, key)
	}
	m.materialized[key] = asyncMapEntry[V]{resolved: true, value: value}
}

// AsyncComputedMap is the derived-slot specialization of AsyncReactiveMap: entries
// are minted pending and driven to resolution via Drive; MaterializeAll pre-mints
// the keyset (still pending until driven). No Set.
type AsyncComputedMap[K comparable, V comparable] struct {
	*AsyncReactiveMap[K, V, asyncSlotHandle]
}

// NewAsyncComputedMap creates an empty async derived-slot map.
func NewAsyncComputedMap[K comparable, V comparable]() *AsyncComputedMap[K, V] {
	return &AsyncComputedMap[K, V]{newAsyncReactiveMap[K, V, asyncSlotHandle]()}
}

// AsyncSlotMap is the pre-v2-kernel name for AsyncComputedMap, kept as an alias
// so existing callers keep compiling.
//
// Deprecated: renamed to AsyncComputedMap.
type AsyncSlotMap[K comparable, V comparable] = AsyncComputedMap[K, V]

// NewAsyncSlotMap creates an empty async derived-slot map.
//
// Deprecated: renamed to NewAsyncComputedMap.
func NewAsyncSlotMap[K comparable, V comparable]() *AsyncComputedMap[K, V] {
	return NewAsyncComputedMap[K, V]()
}

// MaterializeAll eagerly pre-mints (allocates, still pending) a derived slot for
// every key. Drive each to resolution. Observationally identical (once driven) to
// minting lazily on first access.
func (m *AsyncComputedMap[K, V]) MaterializeAll(keys []K, factory func(K) V) {
	for _, key := range keys {
		m.Observe(key, factory)
	}
}
