package lazily

// Thread-safe keyed reactive map (`ThreadSafeReactiveMap`) — the Send + Sync
// flavor of ReactiveMap (#reactivemap, thread-safe).
//
// Spec:   lazily-spec/cell-model.md § Keyed cell collections
// Formal: lazily-formal/LazilyFormal/Materialization.lean
//   (materialize_present_comm / materialize_observe_comm — the confluence pair)
// Rust reference: lazily-rs/src/thread_safe_reactive_family.rs
//
// Where ReactiveMap (reactive_map.go) is a single-goroutine keyed map over the
// (non-synchronized) reactive graph, this map guards its present-set state — the
// materialized value cache + first-materialization order — behind a sync.Mutex,
// so a keyed map can live in an owner shared across goroutines and serve
// Observe / GetOrInsertWith from any goroutine. Like the Zig/Rust flavors it
// carries its own lock and caches canonical values directly (a pure factory
// produces each), rather than routing through a per-thread reactive Context.
//
// Its two specializations are ThreadSafeSourceMap (input cells — adds Set) and
// ThreadSafeComputedMap (derived slots — adds MaterializeAll, no Set). Go
// generics cannot add methods to a type alias, so both are thin distinct structs
// embedding *ThreadSafeReactiveMap with the handle kind fixed.
//
// It obeys the same laws as the single-threaded map plus materialization
// confluence: the present set and every observed value are independent of the
// order in which keys are materialized (materialize_present_comm /
// materialize_observe_comm).

import "sync"

// ThreadSafeMapHandle is the entry-handle kind a ThreadSafeReactiveMap abstracts
// over — the Send + Sync analog of the Rust ThreadSafeMapHandle trait. Sealed to
// the two node kinds of the cell model (input cells / derived slots); bindings do
// not add new kinds.
type ThreadSafeMapHandle interface{ mapEntryKind() EntryKind }

type threadSafeCellHandle struct{}

func (threadSafeCellHandle) mapEntryKind() EntryKind { return EntryKindCell }

type threadSafeSlotHandle struct{}

func (threadSafeSlotHandle) mapEntryKind() EntryKind { return EntryKindSlot }

// ThreadSafeReactiveMap is the thread-safe keyed reactive map (#reactivemap)
// generic over the entry handle kind H, with all present-set mutation serialized
// by an internal mutex. V is constrained comparable to mirror the single-threaded
// map (a cell entry needs a PartialEq guard).
//
// Once built its address is stable, so concurrent readers may share a
// *ThreadSafeReactiveMap. See the package doc for the eager/lazy behavior,
// observational transparency, present-set monotonicity, and confluence.
type ThreadSafeReactiveMap[K comparable, V comparable, H ThreadSafeMapHandle] struct {
	mu sync.RWMutex
	// materialized is the currently-allocated ("present") set and each entry's
	// cached canonical value. Guarded by mu; grows on materialize, never shrinks.
	materialized map[K]V
	// order is the present set in first-materialization order (grows only).
	order []K
}

func newThreadSafeReactiveMap[K comparable, V comparable, H ThreadSafeMapHandle]() *ThreadSafeReactiveMap[K, V, H] {
	return &ThreadSafeReactiveMap[K, V, H]{materialized: map[K]V{}}
}

// materializeLocked caches key's value on first access (the lazy pull), recording
// first-materialization order. Caller MUST hold mu. A warm key is a no-op — the
// present set only grows.
func (m *ThreadSafeReactiveMap[K, V, H]) materializeLocked(key K, factory func(K) V) V {
	if v, ok := m.materialized[key]; ok {
		return v // warm: already allocated.
	}
	v := factory(key)
	m.materialized[key] = v
	m.order = append(m.order, key)
	return v
}

// GetOrInsertWith returns key's value, minting it via factory(key) on first
// access (the lazy pull) under the lock. An existing key returns its cached value
// without re-running the factory. For a ComputedMap this is the lazy
// materialization pull; for a SourceMap it seeds an input cell.
func (m *ThreadSafeReactiveMap[K, V, H]) GetOrInsertWith(key K, factory func(K) V) V {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.materializeLocked(key, factory)
}

// Observe reads key's cached value if present, without minting. Returns
// (zero, false) if the key is not materialized.
func (m *ThreadSafeReactiveMap[K, V, H]) Observe(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.materialized[key]
	return v, ok
}

// IsPresent reports whether key is currently materialized. Non-reactive.
func (m *ThreadSafeReactiveMap[K, V, H]) IsPresent(key K) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.materialized[key]
	return ok
}

// PresentKeys returns a stable snapshot of the currently-materialized keys, in
// first-materialization order (a copy — the internal order must not escape the
// lock).
func (m *ThreadSafeReactiveMap[K, V, H]) PresentKeys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]K, len(m.order))
	copy(out, m.order)
	return out
}

// PresentCount returns the number of currently-materialized entries.
func (m *ThreadSafeReactiveMap[K, V, H]) PresentCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.order)
}

// EntryKind returns this map's entry kind.
func (m *ThreadSafeReactiveMap[K, V, H]) EntryKind() EntryKind {
	var h H
	return h.mapEntryKind()
}

// ThreadSafeSourceMap is the input-cell specialization of ThreadSafeReactiveMap:
// every entry is a settable input cell. Adds the cell-only Set.
type ThreadSafeSourceMap[K comparable, V comparable] struct {
	*ThreadSafeReactiveMap[K, V, threadSafeCellHandle]
}

// NewThreadSafeSourceMap creates an empty thread-safe input-cell map.
func NewThreadSafeSourceMap[K comparable, V comparable]() *ThreadSafeSourceMap[K, V] {
	return &ThreadSafeSourceMap[K, V]{newThreadSafeReactiveMap[K, V, threadSafeCellHandle]()}
}

// ThreadSafeCellMap is the pre-v2-kernel name for ThreadSafeSourceMap, kept as
// an alias so existing callers keep compiling.
//
// Deprecated: renamed to ThreadSafeSourceMap.
type ThreadSafeCellMap[K comparable, V comparable] = ThreadSafeSourceMap[K, V]

// NewThreadSafeCellMap creates an empty thread-safe input-cell map.
//
// Deprecated: renamed to NewThreadSafeSourceMap.
func NewThreadSafeCellMap[K comparable, V comparable]() *ThreadSafeSourceMap[K, V] {
	return NewThreadSafeSourceMap[K, V]()
}

// Set overwrites key's value (cells are writable inputs), materializing the
// entry if absent. Cell-only: a derived ComputedMap slot is not settable.
func (m *ThreadSafeSourceMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.materialized[key]; !ok {
		m.order = append(m.order, key)
	}
	m.materialized[key] = value // overwrite in place; no re-order.
}

// ThreadSafeComputedMap is the derived-slot specialization of
// ThreadSafeReactiveMap: GetOrInsertWith mints a slot on first access (lazy);
// MaterializeAll pre-mints the keyset (eager). No Set.
type ThreadSafeComputedMap[K comparable, V comparable] struct {
	*ThreadSafeReactiveMap[K, V, threadSafeSlotHandle]
}

// NewThreadSafeComputedMap creates an empty thread-safe derived-slot map.
func NewThreadSafeComputedMap[K comparable, V comparable]() *ThreadSafeComputedMap[K, V] {
	return &ThreadSafeComputedMap[K, V]{newThreadSafeReactiveMap[K, V, threadSafeSlotHandle]()}
}

// ThreadSafeSlotMap is the pre-v2-kernel name for ThreadSafeComputedMap, kept as
// an alias so existing callers keep compiling.
//
// Deprecated: renamed to ThreadSafeComputedMap.
type ThreadSafeSlotMap[K comparable, V comparable] = ThreadSafeComputedMap[K, V]

// NewThreadSafeSlotMap creates an empty thread-safe derived-slot map.
//
// Deprecated: renamed to NewThreadSafeComputedMap.
func NewThreadSafeSlotMap[K comparable, V comparable]() *ThreadSafeComputedMap[K, V] {
	return NewThreadSafeComputedMap[K, V]()
}

// MaterializeAll eagerly pre-mints every key via factory. Observationally
// identical to minting each key lazily on first read (GetOrInsertWith).
func (m *ThreadSafeComputedMap[K, V]) MaterializeAll(keys []K, factory func(K) V) {
	for _, key := range keys {
		m.GetOrInsertWith(key, factory)
	}
}
