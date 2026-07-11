package lazily

// Thread-safe keyed reactive family (`ThreadSafeReactiveFamily`) — the
// `Send + Sync` flavor of ReactiveFamily (#lzmatmode, thread-safe).
//
// Spec:   lazily-spec/cell-model.md § Materialization mode
// Formal: lazily-formal/LazilyFormal/Materialization.lean
//   (materialize_present_comm / materialize_observe_comm — the confluence pair)
// Rust reference: lazily-rs/src/thread_safe_reactive_family.rs
// Zig reference:  lazily-zig/src/lazily/thread_safe_reactive_family.zig
//
// Where ReactiveFamily (reactive_family.go) is a single-goroutine keyed reactive
// family over the (non-synchronized) reactive graph, this family guards its
// present-set state — the materialized value cache + first-materialization order
// — behind a sync.Mutex, so a keyed family can live in an owner shared across
// goroutines and serve Observe/Get from any goroutine with no per-key locking of
// the value axis. Like the Zig/Rust flavors it carries its own lock and caches
// canonical values directly (a pure factory produces each), rather than routing
// through a per-thread reactive Context — so it needs no separate thread-safe
// context type.
//
// It obeys the same three laws as the single-threaded family
// (see reactive_family.go):
//   - Eager/lazy contract: eager materializes every declared node at build;
//     lazy defers derived (slot) nodes to first read. Cell entries are always
//     materialized regardless of mode.
//   - Observational transparency: Observe(key) returns an identical value under
//     either mode.
//   - Present-set monotonicity: the materialized set only grows (deferral,
//     never de-allocation).
//
// plus materialization confluence: the present set and every observed value are
// independent of the order in which keys are materialized. A mutex admits a
// concurrent workload as *some* sequential order of the per-key
// materializations; confluence is what makes any such order observationally
// identical (materialize_present_comm / materialize_observe_comm).

import "sync"

// ThreadSafeReactiveFamily is the thread-safe unified keyed reactive family
// (#lzmatmode): keys K map to per-entry reactive values of one EntryKind,
// allocated per its MaterializationMode, with all present-set mutation
// serialized by an internal mutex. V is constrained comparable to mirror the
// single-threaded family (a cell entry needs a PartialEq guard).
//
// Once built its address is stable, so concurrent readers may share a
// *ThreadSafeReactiveFamily. See the package doc for the eager/lazy contract,
// observational transparency, present-set monotonicity, and confluence.
type ThreadSafeReactiveFamily[K comparable, V comparable] struct {
	// ctx is the owning reactive context. The value axis is served from the
	// mutex-guarded cache below (a pure factory), so ctx is retained only for
	// structural parity with ReactiveFamily and future graph integration.
	ctx     *Context
	mode    MaterializationMode
	kind    EntryKind
	factory func(K) V

	mu sync.Mutex
	// materialized is the currently-allocated ("present") set and each entry's
	// cached canonical value. Guarded by mu; grows on materialize, never shrinks.
	materialized map[K]V
	// order is the present set in first-materialization order (grows only).
	order []K
}

// NewThreadSafeReactiveFamily builds a thread-safe family of the given entry
// kind and materialization mode over keys, with factory producing each key's
// canonical value. Under eager, every declared key is materialized now; under
// lazy, only cell entries are materialized at build and slot entries defer to
// first read.
func NewThreadSafeReactiveFamily[K comparable, V comparable](
	ctx *Context,
	kind EntryKind,
	mode MaterializationMode,
	keys []K,
	factory func(K) V,
) *ThreadSafeReactiveFamily[K, V] {
	f := &ThreadSafeReactiveFamily[K, V]{
		ctx:          ctx,
		mode:         mode,
		kind:         kind,
		factory:      factory,
		materialized: make(map[K]V),
	}
	// No lock needed at build: no other goroutine can observe the family before
	// it is returned. A cell entry is always materialized regardless of mode; a
	// slot entry only under eager.
	for _, key := range keys {
		if kind == EntryKindCell || mode == Eager {
			f.materializeKeyLocked(key)
		}
	}
	return f
}

// ThreadSafeEagerSlotFamily builds an eager thread-safe family of derived slots.
func ThreadSafeEagerSlotFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *ThreadSafeReactiveFamily[K, V] {
	return NewThreadSafeReactiveFamily(ctx, EntryKindSlot, Eager, keys, factory)
}

// ThreadSafeLazySlotFamily builds a lazy thread-safe family of derived slots;
// each slot is deferred to its first read.
func ThreadSafeLazySlotFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *ThreadSafeReactiveFamily[K, V] {
	return NewThreadSafeReactiveFamily(ctx, EntryKindSlot, Lazy, keys, factory)
}

// ThreadSafeEagerCellFamily builds an eager thread-safe family of input cells
// (cells are always materialized at build, any mode).
func ThreadSafeEagerCellFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *ThreadSafeReactiveFamily[K, V] {
	return NewThreadSafeReactiveFamily(ctx, EntryKindCell, Eager, keys, factory)
}

// ThreadSafeLazyCellFamily builds a thread-safe family of input cells declared
// under lazy mode; cell entries still materialize at build.
func ThreadSafeLazyCellFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *ThreadSafeReactiveFamily[K, V] {
	return NewThreadSafeReactiveFamily(ctx, EntryKindCell, Lazy, keys, factory)
}

// materializeKeyLocked allocates key's value on first access and caches it (the
// lazy pull), recording first-materialization order. Caller MUST hold mu (or be
// building). A warm key is a no-op — the present set only grows.
func (f *ThreadSafeReactiveFamily[K, V]) materializeKeyLocked(key K) {
	if _, ok := f.materialized[key]; ok {
		return // warm: already allocated.
	}
	f.materialized[key] = f.factory(key)
	f.order = append(f.order, key)
}

// Get returns key's value, materializing it on first access (the lazy pull)
// under the lock. Under eager an entry is already present.
func (f *ThreadSafeReactiveFamily[K, V]) Get(key K) V {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.materializeKeyLocked(key)
	return f.materialized[key]
}

// Observe returns key's value — the transparency law: identical under either
// mode. Materializes the entry if absent.
func (f *ThreadSafeReactiveFamily[K, V]) Observe(key K) V {
	return f.Get(key)
}

// Set overwrites an input cell entry's value (cells are writable inputs),
// materializing it if absent, and reports success; it fails (false) on a slot
// family, whose entries are derived.
func (f *ThreadSafeReactiveFamily[K, V]) Set(key K, value V) bool {
	if f.kind != EntryKindCell {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.materializeKeyLocked(key)
	f.materialized[key] = value // overwrite in place; no re-order.
	return true
}

// IsPresent reports whether key is currently materialized. Non-reactive.
func (f *ThreadSafeReactiveFamily[K, V]) IsPresent(key K) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.materialized[key]
	return ok
}

// PresentKeys returns a stable snapshot of the currently-materialized keys, in
// first-materialization order. The internal order slice must not escape the
// lock (a concurrent materialize could reallocate it), so a copy is returned.
func (f *ThreadSafeReactiveFamily[K, V]) PresentKeys() []K {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]K, len(f.order))
	copy(out, f.order)
	return out
}

// PresentCount returns the number of currently-materialized entries.
func (f *ThreadSafeReactiveFamily[K, V]) PresentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.order)
}

// Mode returns this family's materialization mode.
func (f *ThreadSafeReactiveFamily[K, V]) Mode() MaterializationMode { return f.mode }

// EntryKind returns this family's entry kind.
func (f *ThreadSafeReactiveFamily[K, V]) EntryKind() EntryKind { return f.kind }
