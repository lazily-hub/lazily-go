package lazily

// Async keyed reactive family (`AsyncReactiveFamily`) — the async flavor of
// ReactiveFamily (#lzmatmode, async).
//
// Spec:   lazily-spec/cell-model.md § Materialization mode (async)
// Formal: lazily-formal/LazilyFormal/AsyncMaterialization.lean
//   (eventual_transparency, async_resolved_matches_sync, observe_pending_is_none,
//    cell_resolved_at_build, resolve_monotone, resolve_preserves_observe)
// Rust reference: lazily-rs/src/async_reactive_family.rs
// Zig reference:  lazily-zig/src/lazily/async_reactive_family.zig
//
// Keys K map to per-entry async reactive nodes allocated per the family's
// MaterializationMode. Like the thread-safe flavor it guards its state behind a
// sync.Mutex, so it can live in a cross-goroutine owner.
//
// Async adds a resolution axis orthogonal to the present-set (allocation) axis
// of the single-threaded family: a derived (slot) entry is pending until it is
// driven to resolution (Drive, the analog of AsyncContext.GetAsync), then
// resolved. Input (cell) entries are resolved at build. A non-blocking read
// therefore returns (value, ok): (_, false) while pending, (value, true) once
// resolved.
//
// The single-threaded transparency law weakens to eventual transparency: once a
// node resolves, its observed value is the canonical value — identical to what
// the synchronous family observes.

import "sync"

// asyncFamilyEntry is one allocated (present) family entry: resolved tracks the
// async resolution axis, value caches its canonical value once resolved. A
// pending entry's value is the zero value and unspecified.
type asyncFamilyEntry[V comparable] struct {
	resolved bool
	value    V
}

// AsyncReactiveFamily is the async unified keyed reactive family (#lzmatmode):
// keys K map to per-entry async reactive nodes of one EntryKind, allocated per
// its MaterializationMode, each carrying a resolution flag. V is comparable to
// mirror the single-threaded family.
//
// Once built its address is stable, so concurrent readers may share a
// *AsyncReactiveFamily. See the package doc for the eager/lazy contract,
// present-set monotonicity, and the eventual-transparency law.
type AsyncReactiveFamily[K comparable, V comparable] struct {
	// ctx is the owning async reactive context, retained for structural parity
	// with ReactiveFamily; the value axis is served from the mutex-guarded cache.
	ctx     *Context
	mode    MaterializationMode
	kind    EntryKind
	factory func(K) V

	mu sync.Mutex
	// materialized is the currently-allocated ("present") set. Guarded by mu;
	// grows on materialize, never shrinks (deferral-not-dealloc). Resolution only
	// ever flips false→true (resolve_monotone).
	materialized map[K]asyncFamilyEntry[V]
	// order is the present set in first-materialization order (grows only).
	order []K
}

// NewAsyncReactiveFamily builds an async family of the given entry kind and
// materialization mode over keys, with factory producing each key's canonical
// value. Cell entries are allocated and resolved at build (any mode); slot
// entries are allocated under eager (but start pending — the async value is only
// produced when driven), deferred under lazy.
func NewAsyncReactiveFamily[K comparable, V comparable](
	ctx *Context,
	kind EntryKind,
	mode MaterializationMode,
	keys []K,
	factory func(K) V,
) *AsyncReactiveFamily[K, V] {
	f := &AsyncReactiveFamily[K, V]{
		ctx:          ctx,
		mode:         mode,
		kind:         kind,
		factory:      factory,
		materialized: make(map[K]asyncFamilyEntry[V]),
	}
	for _, key := range keys {
		if kind == EntryKindCell || mode == Eager {
			f.materializeKeyLocked(key)
		}
	}
	return f
}

// AsyncEagerSlotFamily builds an eager async family of derived slots (allocated
// now but pending until driven).
func AsyncEagerSlotFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *AsyncReactiveFamily[K, V] {
	return NewAsyncReactiveFamily(ctx, EntryKindSlot, Eager, keys, factory)
}

// AsyncLazySlotFamily builds a lazy async family of derived slots (deferred to
// first touch, then driven to resolve).
func AsyncLazySlotFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *AsyncReactiveFamily[K, V] {
	return NewAsyncReactiveFamily(ctx, EntryKindSlot, Lazy, keys, factory)
}

// AsyncEagerCellFamily builds an eager async family of input cells (resolved at
// build).
func AsyncEagerCellFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *AsyncReactiveFamily[K, V] {
	return NewAsyncReactiveFamily(ctx, EntryKindCell, Eager, keys, factory)
}

// AsyncLazyCellFamily builds an async family of input cells declared under lazy
// mode; cell entries still materialize (and resolve) at build.
func AsyncLazyCellFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *AsyncReactiveFamily[K, V] {
	return NewAsyncReactiveFamily(ctx, EntryKindCell, Lazy, keys, factory)
}

// materializeKeyLocked allocates key if absent (present-set grows), recording
// order. A cell entry is resolved immediately with its value; a slot entry
// starts pending (resolved = false). Caller MUST hold mu (or be building). A
// warm key is a no-op — the present set only grows.
func (f *AsyncReactiveFamily[K, V]) materializeKeyLocked(key K) {
	if _, ok := f.materialized[key]; ok {
		return // warm.
	}
	if f.kind == EntryKindCell {
		f.materialized[key] = asyncFamilyEntry[V]{resolved: true, value: f.factory(key)}
	} else {
		var zero V
		f.materialized[key] = asyncFamilyEntry[V]{resolved: false, value: zero}
	}
	f.order = append(f.order, key)
}

// Drive drives key to resolution — the analog of AsyncContext.GetAsync:
// allocate if absent, resolve if pending (produce + cache the canonical value),
// and return the resolved value. A warm-resolved key returns its cached value
// unchanged. The eventual-transparency completion.
func (f *AsyncReactiveFamily[K, V]) Drive(key K) V {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.materializeKeyLocked(key)
	entry := f.materialized[key]
	if !entry.resolved {
		entry = asyncFamilyEntry[V]{resolved: true, value: f.factory(key)}
		f.materialized[key] = entry
	}
	return entry.value
}

// Observe is a non-blocking read: (value, true) once resolved, (_, false) while
// pending (observe_pending_is_none). Allocates the entry if absent — a freshly
// allocated slot is pending, so a first Observe of a slot returns (_, false)
// until it is Driven; a cell is resolved at allocation, so it returns
// (value, true) immediately.
func (f *AsyncReactiveFamily[K, V]) Observe(key K) (V, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.materializeKeyLocked(key)
	entry := f.materialized[key]
	if entry.resolved {
		return entry.value, true
	}
	var zero V
	return zero, false
}

// Set overwrites an input cell entry's value (cells are writable, always
// resolved), materializing it if absent, and reports success; it fails (false)
// on a slot family, whose entries are derived.
func (f *AsyncReactiveFamily[K, V]) Set(key K, value V) bool {
	if f.kind != EntryKindCell {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.materializeKeyLocked(key)
	f.materialized[key] = asyncFamilyEntry[V]{resolved: true, value: value}
	return true
}

// IsPresent reports whether key is currently allocated (present). Non-reactive.
func (f *AsyncReactiveFamily[K, V]) IsPresent(key K) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.materialized[key]
	return ok
}

// IsResolved reports whether key is allocated AND resolved (a non-blocking
// Observe would return a value).
func (f *AsyncReactiveFamily[K, V]) IsResolved(key K) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.materialized[key]
	return ok && entry.resolved
}

// PresentKeys returns a stable snapshot of the currently-allocated keys, in
// first-materialization order (a copy — the internal order must not escape the
// lock).
func (f *AsyncReactiveFamily[K, V]) PresentKeys() []K {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]K, len(f.order))
	copy(out, f.order)
	return out
}

// PresentCount returns the number of currently-allocated entries.
func (f *AsyncReactiveFamily[K, V]) PresentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.order)
}

// Mode returns this family's materialization mode.
func (f *AsyncReactiveFamily[K, V]) Mode() MaterializationMode { return f.mode }

// EntryKind returns this family's entry kind.
func (f *AsyncReactiveFamily[K, V]) EntryKind() EntryKind { return f.kind }
