// Phase 1 of the RelayCell backpressure plan (#relaycell) — the merge algebra.
//
// See lazily-spec/docs/reactive-graph.md § "MergeCell and the merge algebra" and
// relaycell-backpressure-analysis.md §4.0/§4.3. A merge policy is an associative
// fold ⊕: T×T→T; the properties it satisfies (associativity always; commutativity
// = reordering tax; idempotency = durability tax) select which overflow behaviour
// is sound. MergeCell generalizes a plain Cell — Cell ≡ MergeCell(KeepLatest) —
// a source whose write is a merge. Backed by an ordinary cell, so it inherits the
// Phase-0 ==-guard + store-without-cascade.
//
// Go note: Cell[T] requires `comparable`, so a MergeCell can wrap the
// comparable-valued policies (KeepLatest / Sum / Max). The SetUnion / RawFifo
// policies (map / slice valued) are defined for the algebra + law-tests but
// cannot back a Cell; they compose at the RelayCell layer over their own storage.

package lazily

// Number constrains the additive/ordered policies (Sum, Max).
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// MergePolicy is an associative merge ⊕ with its transport-selected property
// flags. Associativity ((a⊕b)⊕c == a⊕(b⊕c)) is a law, verified by the
// law-tests, not a flag. Commutative is the reordering tax; Idempotent the
// durability tax; Conflates gates the Conflate overflow (Phase 2 — only RawFifo
// cannot bound).
type MergePolicy[T any] struct {
	Name        string
	Merge       func(old, op T) T
	Commutative bool
	Idempotent  bool
	Conflates   bool
}

// KeepLatest is the keep-latest band (old ⊕ op = op) — the policy behind a plain
// Cell. Associative and idempotent, not commutative.
func KeepLatest[T any]() MergePolicy[T] {
	return MergePolicy[T]{
		Name:        "KeepLatest",
		Merge:       func(_ T, op T) T { return op },
		Commutative: false,
		Idempotent:  true,
		Conflates:   true,
	}
}

// Sum is the additive commutative monoid (old + op). Not idempotent.
func Sum[T Number]() MergePolicy[T] {
	return MergePolicy[T]{
		Name:        "Sum",
		Merge:       func(a, b T) T { return a + b },
		Commutative: true,
		Idempotent:  false,
		Conflates:   true,
	}
}

// Max is the max semilattice (max(old, op)). Associative, commutative, idempotent.
func Max[T Number]() MergePolicy[T] {
	return MergePolicy[T]{
		Name: "Max",
		Merge: func(a, b T) T {
			if b > a {
				return b
			}
			return a
		},
		Commutative: true,
		Idempotent:  true,
		Conflates:   true,
	}
}

// SetUnion is the grow-only set-union semilattice over map[E]struct{}.
func SetUnion[E comparable]() MergePolicy[map[E]struct{}] {
	return MergePolicy[map[E]struct{}]{
		Name: "SetUnion",
		Merge: func(old, op map[E]struct{}) map[E]struct{} {
			out := make(map[E]struct{}, len(old)+len(op))
			for k := range old {
				out[k] = struct{}{}
			}
			for k := range op {
				out[k] = struct{}{}
			}
			return out
		},
		Commutative: true,
		Idempotent:  true,
		Conflates:   true,
	}
}

// RawFifo is raw FIFO append over []E (old ++ op). Order + multiplicity are
// meaning — associative only; cannot conflate.
func RawFifo[E any]() MergePolicy[[]E] {
	return MergePolicy[[]E]{
		Name: "RawFifo",
		Merge: func(old, op []E) []E {
			out := make([]E, 0, len(old)+len(op))
			out = append(out, old...)
			out = append(out, op...)
			return out
		},
		Commutative: false,
		Idempotent:  false,
		Conflates:   false,
	}
}

// MergeCell is a cell whose write is a merge under a policy rather than a
// replace. Cell ≡ MergeCell(KeepLatest). Reads track like any cell; Merge routes
// through the cell's ==-guarded Set, so an idempotent policy's no-op merge fires
// no cascade (free dedup) and store-without-cascade still applies.
type MergeCell[T comparable] struct {
	cell   *Cell[T]
	policy MergePolicy[T]
}

// NewMergeCell creates a MergeCell over ctx with the given initial value and policy.
func NewMergeCell[T comparable](ctx *Context, initial T, policy MergePolicy[T]) *MergeCell[T] {
	return &MergeCell[T]{cell: NewCell(ctx, initial), policy: policy}
}

// Cell returns the underlying reactive cell (for wiring derived readers).
func (m *MergeCell[T]) Cell() *Cell[T] { return m.cell }

// Policy returns the merge policy.
func (m *MergeCell[T]) Policy() MergePolicy[T] { return m.policy }

// Get reads the current converged value (tracks a dependency in a computation).
func (m *MergeCell[T]) Get() T { return m.cell.Get() }

// Set replaces the value outright (the keep-latest write), bypassing the policy.
func (m *MergeCell[T]) Set(value T) { m.cell.Set(value) }

// Merge folds op into the current value under the policy. Reads the current
// value untracked (Peek) and writes the merged result.
func (m *MergeCell[T]) Merge(op T) { m.cell.Set(m.policy.Merge(m.cell.Peek(), op)) }

// Reactive is the read supertype: get + subscribe (analysis §4.0). Every reader
// (Slot/Signal/Cell/MergeCell) satisfies it.
type Reactive[T any] interface {
	Get() T
}

// Source is a writable Reactive — adds Set (replace) and Merge (fold under the
// source's policy). Cell/MergeCell are Source; a derived Slot is not.
type Source[T any] interface {
	Reactive[T]
	Set(value T)
	Merge(op T)
}
