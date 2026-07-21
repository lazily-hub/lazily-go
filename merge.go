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
// Go note: Source[T] requires `comparable` (the ==-guard), so a merge-policy
// Source can wrap the comparable-valued policies (KeepLatest / Sum / Max). The
// SetUnion / RawFifo policies (map / slice valued) are defined for the algebra +
// law-tests but cannot back a Source; they compose at the RelayCell layer over
// their own storage.

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

// v2 (#lzcellkernel): the compatibility shims `MergeCell` (type alias for
// Source) and `NewMergeCell` (alias for NewSourceWithPolicy) are removed. Under
// the Cell kernel a "merge cell" is just a Source whose policy is not KeepLatest
// — "one kind, the policy in a field" — so the two collapse into Source with no
// separate handle. Use Source / NewSourceWithPolicy directly.
//
// v2 (#lzcellkernel): the `Cell[T]` read-genus interface is dropped. `Cell` is a
// conceptual word for a value-bearing reactive node (Source / Computed), not a
// type. v2 no longer needs a genus for write-protection — the two concrete
// handle structs Source (Set/Merge) and Computed (no Set/Merge) carry it in the
// type. No Go generic code used the interface as a bound, so nothing kept it.
