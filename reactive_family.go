package lazily

// ReactiveFamily — the unified keyed reactive family + materialization mode
// (#lzmatmode).
//
// Spec:   lazily-spec/cell-model.md § Materialization mode
// Formal: lazily-formal/LazilyFormal/Materialization.lean
// Rust reference: lazily-rs/src/reactive_family.rs
//
// A ReactiveFamily maps keys K to per-entry reactive nodes and abstracts over
// the entry's handle kind (the axis the Rust binding expresses as the type
// parameter H in `ReactiveFamily<K, V, H>`):
//
//   - Cell entries (EntryKindCell) are INPUT nodes (*Cell[V]) — always
//     materialized regardless of mode; an input has no derivation to defer.
//   - Slot entries (EntryKindSlot) are DERIVED nodes (*Slot[V]) — these are what
//     materialization mode governs: eager allocates them up front, lazy defers
//     each to first read.
//
// Go has no sealed handle-kind trait, so a single ReactiveFamily fixes one entry
// kind (like the Rust `H` type parameter); a mixed-kind key space is modelled by
// a cell family over the cell entries and a slot family over the slot entries
// sharing one logical key space (see reactive_family_test.go). The existing
// CellFamily (collections.go) is the input-cell collection specialization that
// mints cells lazily on first Get — a collection concern, not the
// materialization axis.

// EntryKind is which kind of reactive node a ReactiveFamily entry is — the
// handle-kind axis the family abstracts over, kept orthogonal to
// MaterializationMode. Mirrors EntryKind in lazily-formal's Materialization
// module.
type EntryKind int

const (
	// EntryKindCell is an input cell (*Cell[V]) — always materialized, any mode.
	EntryKindCell EntryKind = iota
	// EntryKindSlot is a derived slot (*Slot[V]) — materialized eagerly, or
	// lazily on first read.
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

// MaterializationMode fixes WHEN a ReactiveFamily's derived (slot) entries are
// allocated. Orthogonal to EntryKind; never observable on the value axis. The
// zero value is Eager — the required default (Mode.default = Mode.eager).
// Mirrors Mode in lazily-formal's Materialization module.
type MaterializationMode int

const (
	// Eager allocates every derived node up front at build time. The shared
	// high-performance core and the required default (the zero value).
	Eager MaterializationMode = iota
	// Lazy allocates a derived node on its first read ("materialize on pull"),
	// addressed by key rather than a held handle. An opt-in overlay on the
	// eager core — never the default, never a per-read toggle.
	Lazy
)

// String renders the mode for diagnostics.
func (m MaterializationMode) String() string {
	switch m {
	case Eager:
		return "eager"
	case Lazy:
		return "lazy"
	default:
		return "unknown"
	}
}

// DefaultMaterializationMode is the required default (Eager).
func DefaultMaterializationMode() MaterializationMode { return Eager }

// ReactiveFamily is the unified keyed reactive family (#lzmatmode): keys K map
// to per-entry reactive nodes of one EntryKind (input *Cell[V] or derived
// *Slot[V]), allocated per its MaterializationMode. V is constrained comparable
// so a cell entry (which needs a PartialEq guard) and a slot entry share one
// type — mirroring the Rust `V: PartialEq` bound.
//
// Operations run against the owning Context, single-goroutine like the rest of
// lazily (the reactive graph is not itself synchronized).
type ReactiveFamily[K comparable, V comparable] struct {
	ctx     *Context
	mode    MaterializationMode
	kind    EntryKind
	factory func(K) V
	cells   map[K]*Cell[V]
	slots   map[K]*Slot[V]
	// order is the present set in first-materialization order; it only grows
	// (deferral, not de-allocation).
	order   []K
	present map[K]struct{}
}

// NewReactiveFamily builds a family of the given entry kind and materialization
// mode over keys, with factory producing each key's canonical value (a derived
// slot's recomputation; an input cell's initial value). Under eager, every
// declared key is materialized now; under lazy, only cell entries are
// materialized at build (cells are always materialized) and slot entries defer
// to first read.
func NewReactiveFamily[K comparable, V comparable](
	ctx *Context,
	kind EntryKind,
	mode MaterializationMode,
	keys []K,
	factory func(K) V,
) *ReactiveFamily[K, V] {
	f := &ReactiveFamily[K, V]{
		ctx:     ctx,
		mode:    mode,
		kind:    kind,
		factory: factory,
		cells:   make(map[K]*Cell[V]),
		slots:   make(map[K]*Slot[V]),
		present: make(map[K]struct{}),
	}
	for _, key := range keys {
		// A cell entry is always materialized regardless of mode; a slot entry
		// only under eager (present := isInput ∨ eager).
		if kind == EntryKindCell || mode == Eager {
			f.materializeKey(key)
		}
	}
	return f
}

// EagerSlotFamily builds an eager family of derived slots: every declared key's
// node is allocated now. Eager is the default mode.
func EagerSlotFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *ReactiveFamily[K, V] {
	return NewReactiveFamily(ctx, EntryKindSlot, Eager, keys, factory)
}

// LazySlotFamily builds a lazy family of derived slots: each slot is deferred to
// its first read. Pass empty keys for a purely on-demand slot family.
func LazySlotFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *ReactiveFamily[K, V] {
	return NewReactiveFamily(ctx, EntryKindSlot, Lazy, keys, factory)
}

// EagerCellFamily builds an eager family of input cells. Cells are always
// materialized at build (any mode); this is the explicit-eager constructor.
func EagerCellFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *ReactiveFamily[K, V] {
	return NewReactiveFamily(ctx, EntryKindCell, Eager, keys, factory)
}

// LazyCellFamily builds a family of input cells declared under lazy mode. Cell
// entries are still materialized at build (cells are always materialized); the
// mode is recorded but only governs derived slots, of which a cell family has
// none.
func LazyCellFamily[K comparable, V comparable](ctx *Context, keys []K, factory func(K) V) *ReactiveFamily[K, V] {
	return NewReactiveFamily(ctx, EntryKindCell, Lazy, keys, factory)
}

// materializeKey allocates key's node on first access and caches it (the lazy
// pull), then no-ops on re-access. The first read of a key constructs the same
// node the eager build would have.
func (f *ReactiveFamily[K, V]) materializeKey(key K) {
	if _, ok := f.present[key]; ok {
		return // warm: already allocated.
	}
	switch f.kind {
	case EntryKindCell:
		f.cells[key] = NewCell(f.ctx, f.factory(key))
	default:
		k := key
		f.slots[key] = NewSlot(f.ctx, func(*Context) V { return f.factory(k) })
	}
	f.present[key] = struct{}{}
	f.order = append(f.order, key)
}

// Observe returns key's value — the headline transparency law: the returned
// value is identical under either mode. Materializes the entry if absent.
func (f *ReactiveFamily[K, V]) Observe(key K) V {
	f.materializeKey(key)
	if f.kind == EntryKindCell {
		return f.cells[key].Get()
	}
	return f.slots[key].Get()
}

// GetCell returns the input cell handle for key (materializing on first access),
// or (nil, false) if this is not a cell family.
func (f *ReactiveFamily[K, V]) GetCell(key K) (*Cell[V], bool) {
	if f.kind != EntryKindCell {
		return nil, false
	}
	f.materializeKey(key)
	return f.cells[key], true
}

// GetSlot returns the derived slot handle for key (materializing on first
// access — the lazy pull), or (nil, false) if this is not a slot family.
func (f *ReactiveFamily[K, V]) GetSlot(key K) (*Slot[V], bool) {
	if f.kind != EntryKindSlot {
		return nil, false
	}
	f.materializeKey(key)
	return f.slots[key], true
}

// Set writes an input cell entry's value (materializing it if absent) and
// reports success; it fails (false) on a slot family, whose entries are derived.
func (f *ReactiveFamily[K, V]) Set(key K, value V) bool {
	if f.kind != EntryKindCell {
		return false
	}
	f.materializeKey(key)
	f.cells[key].Set(value)
	return true
}

// IsPresent reports whether key is currently materialized (present in the
// allocated set). Non-reactive.
func (f *ReactiveFamily[K, V]) IsPresent(key K) bool {
	_, ok := f.present[key]
	return ok
}

// PresentKeys returns the currently-materialized keys in first-materialization
// order. The present set only grows (deferral, not de-allocation).
func (f *ReactiveFamily[K, V]) PresentKeys() []K {
	out := make([]K, len(f.order))
	copy(out, f.order)
	return out
}

// PresentCount returns the number of currently-materialized entries.
func (f *ReactiveFamily[K, V]) PresentCount() int { return len(f.order) }

// Mode returns this family's materialization mode.
func (f *ReactiveFamily[K, V]) Mode() MaterializationMode { return f.mode }

// EntryKind returns this family's entry kind.
func (f *ReactiveFamily[K, V]) EntryKind() EntryKind { return f.kind }
