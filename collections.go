// Keyed cell collections — SourceMap, SourceTree, and keyed reconciliation
// (cell-model.md § Keyed cell collections).
//
// A keyed cell collection is a *composition of cells*, not a new cell kind. It
// maps keys K to per-entry Cells and adds dedicated membership and order
// reactive signals so the three reactivity planes stay independent:
//
//   - writing one entry's value invalidates only that entry's value readers;
//   - adding/removing a key invalidates membership readers (Len / ContainsKey)
//     and order readers (Keys), but not unrelated entry value readers;
//   - a pure reorder (atomic move) invalidates order readers only.
//
// Ported from lazily-dart lib/src/collections.dart, mirroring
// lazily-rs/src/cell_family.rs (reactive) and lazily-rs/src/reconcile.rs (LIS).
// Validated against lazily-spec/conformance/collections/{cellmap_independence,
// cellmap_atomic_move,keyed_reconciliation_lis}.json.
//
// Semantic note on the `comparable` constraint: core.go's Cell[T] requires a
// comparable T for the PartialEq guard, and keyed reconciliation compares entry
// values for the Update op. Both mean the value type V must support ==, so this
// module constrains V to `comparable` and stores per-key values in a plain
// Cell[V] (no boxing needed). This matches the Dart port's use of Dart `==` /
// PartialEq exactly, and gives real handle stability: an atomic move never
// re-mints the entry's *Cell, so the same pointer (and its dependents) survive.
package lazily

// SourceMap is the input-cell specialization of ReactiveMap: a keyed collection
// of reactive cells with independent value / membership / order reactivity
// (cell-model.md § Keyed cell collections).
//
// Each entry is an ordinary Cell[V]; the collection adds no new merge unit. The
// shared reactive-membership / order / move / remove surface is inherited from
// the embedded ReactiveMap; SourceMap adds the cell-only Set and eager
// value-minting (Entry / EntryWith), plus the Go-specific Insert / Reconcile
// keyed-reconciliation helpers.
//
//   - Keys subscribes only to the order signal;
//   - Len / ContainsKey subscribe only to the membership signal;
//   - a value read subscribes only to that entry's cell.
//
// An atomic move (MoveTo / MoveBefore / MoveAfter, inherited) bumps only the
// order signal once and keeps the moved entry's same Cell handle, dependents,
// and lineage — it is not a remove + re-mint.
type SourceMap[K comparable, V comparable] struct {
	*ReactiveMap[K, V, *Source[V]]
}

// NewSourceMap creates an empty keyed cell collection bound to ctx.
func NewSourceMap[K comparable, V comparable](ctx *Context) *SourceMap[K, V] {
	rm := newReactiveMap[K, V, *Source[V]](
		ctx,
		EntryKindSource,
		func(ctx *Context, compute func() V) *Source[V] { return NewSource(ctx, compute()) },
		func(c ComputeOps, h *Source[V]) V { return Get[V](c, h) },
		// Invalidate the orphaned cell's dependents on remove (mirrors lazily-rs
		// CellHandle::clear_dependents): any reader that read this entry is
		// notified that its source is gone.
		func(h *Source[V]) { h.Invalidate() },
	)
	return &SourceMap[K, V]{ReactiveMap: rm}
}

// CellMap is the pre-v2-kernel name for SourceMap, kept as an alias so existing
// callers keep compiling. The v2 kernel renamed the node kinds to Source and
// Computed; the keyed collections follow.
//
// Deprecated: renamed to SourceMap.
type CellMap[K comparable, V comparable] = SourceMap[K, V]

// NewCellMap creates an empty keyed cell collection bound to ctx.
//
// Deprecated: renamed to NewSourceMap.
func NewCellMap[K comparable, V comparable](ctx *Context) *SourceMap[K, V] {
	return NewSourceMap[K, V](ctx)
}

// EntryWith returns the value cell for key, minting it with defaultValue() on
// first access. Adding a new key bumps reactive membership; re-fetching an
// existing key does not.
func (m *SourceMap[K, V]) EntryWith(key K, defaultValue func() V) *Source[V] {
	if existing, ok := m.keyed.get(key); ok {
		return existing
	}
	return m.mintWith(key, defaultValue)
}

// Entry returns the value cell for key, minting it with defaultValue on first
// access. Convenience wrapper over EntryWith.
func (m *SourceMap[K, V]) Entry(key K, defaultValue V) *Source[V] {
	return m.EntryWith(key, func() V { return defaultValue })
}

// Cell returns the existing value cell for key, or nil. Non-reactive: does not
// subscribe the caller to membership.
func (m *SourceMap[K, V]) Cell(key K) *Source[V] {
	handle, _ := m.keyed.get(key)
	return handle
}

// Get reads the value at key if present (peek). Non-reactive.
func (m *SourceMap[K, V]) Get(key K) (V, bool) {
	if handle, ok := m.keyed.get(key); ok {
		return handle.Peek(), true
	}
	var zero V
	return zero, false
}

// Read reads the value at key if present, subscribing the caller to that
// entry's cell (reactive inside a Slot / Signal computation).
func (m *SourceMap[K, V]) Read(key K) (V, bool) {
	if handle, ok := m.keyed.get(key); ok {
		return handle.Get(), true
	}
	var zero V
	return zero, false
}

// Set assigns the value at key, inserting a new entry (and bumping membership)
// if it does not exist yet. Updating an existing entry leaves membership
// untouched and invalidates only that entry's dependents.
//
// Cell-only: an input is settable; a derived ComputedMap slot is not.
func (m *SourceMap[K, V]) Set(key K, value V) {
	if handle, ok := m.keyed.get(key); ok {
		handle.Set(value)
		return
	}
	m.mintWith(key, func() V { return value })
}

// Insert inserts key with value at the position specified by at (relative to
// anchor for InsertAtBefore / InsertAtAfter; anchor is ignored otherwise).
// Bumps membership + order. Returns whether the key was newly inserted (false
// if it already existed; in that case the value is updated in place and only
// the entry's value readers invalidate).
//
// Go lacks Dart's optional named args: pass InsertAtEnd with a zero anchor for
// the common append case.
func (m *SourceMap[K, V]) Insert(key K, value V, at InsertAt, anchor K) bool {
	if ok := m.keyed.contains(key); ok {
		m.Set(key, value)
		return false
	}
	m.EntryWith(key, func() V { return value })
	switch at {
	case InsertAtEnd, InsertAtIndex:
		// Already at end (InsertAtIndex positions via a follow-up MoveTo).
	case InsertAtBefore:
		m.MoveBefore(key, anchor)
	case InsertAtAfter:
		m.MoveAfter(key, anchor)
	}
	return true
}

// Len, IsEmpty, ContainsKey, LenUntracked, Keys, Remove, Position, and the
// atomic Move* operations are inherited from the embedded ReactiveMap.

// Reconcile reconciles to targetOrder + targetValues: compute the minimal diff
// and apply it per-cell. Stable entries (unchanged value, in the LIS) keep
// their cell handles and stay cached.
func (m *SourceMap[K, V]) Reconcile(targetOrder []K, targetValues map[K]V) {
	prior := make([]KeyValue[K, V], 0, m.keyed.length())
	for _, k := range m.keyed.keys() {
		v, _ := m.Get(k)
		prior = append(prior, KeyValue[K, V]{Key: k, Value: v})
	}
	target := make([]KeyValue[K, V], 0, len(targetOrder))
	for _, k := range targetOrder {
		target = append(target, KeyValue[K, V]{Key: k, Value: targetValues[k]})
	}
	for _, op := range ReconcileDiff(prior, target) {
		switch op := op.(type) {
		case DiffOpInsert[K, V]:
			m.Insert(op.Key, op.Value, InsertAtEnd, *new(K))
		case DiffOpRemove[K, V]:
			m.Remove(op.Key)
		case DiffOpMove[K, V]:
			m.MoveTo(op.Key, op.To)
		case DiffOpUpdate[K, V]:
			m.Set(op.Key, op.Value)
		}
	}
}

// InsertAt is the position specifier for SourceMap.Insert (mirrors
// lazily-kt::InsertAt). The string values are the normative wire tokens.
type InsertAt string

const (
	// InsertAtEnd appends at the end (default).
	InsertAtEnd InsertAt = "end"
	// InsertAtIndex inserts at an absolute index (use SourceMap.MoveTo after
	// insert to position).
	InsertAtIndex InsertAt = "at"
	// InsertAtBefore inserts just before the anchor.
	InsertAtBefore InsertAt = "before"
	// InsertAtAfter inserts just after the anchor.
	InsertAtAfter InsertAt = "after"
)

// KeyValue is an ordered key/value pair, the input unit for ReconcileDiff
// (mirrors Dart's MapEntry<K, V>).
type KeyValue[K comparable, V comparable] struct {
	Key   K
	Value V
}

// DiffOp is a keyed reconciliation op (cell-model.md § Keyed reconciliation):
// one of DiffOpInsert, DiffOpRemove, DiffOpMove, or DiffOpUpdate. It is a sealed
// union — only the four concrete types in this package implement it.
type DiffOp[K comparable, V comparable] interface {
	isDiffOp()
}

// DiffOpInsert inserts a brand-new key (not present in prior) at Index (its
// final position in the target sequence).
type DiffOpInsert[K comparable, V comparable] struct {
	Key   K
	Value V
	Index int
}

func (DiffOpInsert[K, V]) isDiffOp() {}

// DiffOpRemove removes a key present in prior but absent in target.
type DiffOpRemove[K comparable, V comparable] struct {
	Key K
}

func (DiffOpRemove[K, V]) isDiffOp() {}

// DiffOpMove atomic-moves a common key from its prior position to To (the target
// index). Keeps the entry's same cell handle, dependents, and lineage.
type DiffOpMove[K comparable, V comparable] struct {
	Key K
	To  int
}

func (DiffOpMove[K, V]) isDiffOp() {}

// DiffOpUpdate updates an existing key's value (PartialEq-guarded at the cell).
type DiffOpUpdate[K comparable, V comparable] struct {
	Key   K
	Value V
}

func (DiffOpUpdate[K, V]) isDiffOp() {}

// ReconcileDiff computes the move-minimized keyed reconciliation (cell-model.md
// § Keyed reconciliation).
//
// Diffs two keyed sequences by stable key, not position, emitting the minimal
// {insert, remove, move, update} op set: removes, then inserts + moves (in
// target order), then updates. Moves are move-minimized: the
// longest-increasing-subsequence (LIS) over prior indices of the common keys is
// held fixed, and only the remainder move. O(n log n) via patience sorting
// (strictly increasing), mirroring
// lazily-rs/src/reconcile.rs::longest_increasing_subsequence.
func ReconcileDiff[K comparable, V comparable](
	prior []KeyValue[K, V],
	target []KeyValue[K, V],
) []DiffOp[K, V] {
	priorIndex := make(map[K]int, len(prior))
	priorValue := make(map[K]V, len(prior))
	for i, e := range prior {
		priorIndex[e.Key] = i
		priorValue[e.Key] = e.Value
	}
	targetValue := make(map[K]V, len(target))
	for _, e := range target {
		targetValue[e.Key] = e.Value
	}

	ops := []DiffOp[K, V]{}

	// Removes: keys in prior not in target.
	for _, e := range prior {
		if _, ok := targetValue[e.Key]; !ok {
			ops = append(ops, DiffOpRemove[K, V]{Key: e.Key})
		}
	}

	// Common keys in target order, with their prior indices (for the LIS).
	commonKeys := []K{}
	commonIdxOf := map[K]int{}
	priorIdxSeq := []int{}
	for _, e := range target {
		if pi, ok := priorIndex[e.Key]; ok {
			commonIdxOf[e.Key] = len(commonKeys)
			commonKeys = append(commonKeys, e.Key)
			priorIdxSeq = append(priorIdxSeq, pi)
		}
	}
	stableSet := map[int]struct{}{} // indices into commonKeys held fixed by the LIS
	for _, i := range longestIncreasingSubsequence(priorIdxSeq) {
		stableSet[i] = struct{}{}
	}

	// Inserts + moves: walk target left-to-right. Inserts mint new keys at their
	// final index; common keys either stay (LIS) or move to their final index.
	for ti, e := range target {
		k := e.Key
		if _, ok := priorIndex[k]; !ok {
			ops = append(ops, DiffOpInsert[K, V]{Key: k, Value: e.Value, Index: ti})
			continue
		}
		commonIdx := commonIdxOf[k]
		if _, stable := stableSet[commonIdx]; !stable {
			ops = append(ops, DiffOpMove[K, V]{Key: k, To: ti})
		}
	}

	// Updates: common keys whose value changed.
	for _, e := range target {
		if pv, ok := priorValue[e.Key]; ok && pv != e.Value {
			ops = append(ops, DiffOpUpdate[K, V]{Key: e.Key, Value: e.Value})
		}
	}

	return ops
}

// longestIncreasingSubsequence returns the indices (into seq) of a longest
// strictly-increasing subsequence, in ascending order. Patience-sort LIS,
// O(n log n). Mirrors lazily-rs/src/reconcile.rs::longest_increasing_subsequence.
func longestIncreasingSubsequence(seq []int) []int {
	n := len(seq)
	if n == 0 {
		return nil
	}
	tails := []int{} // tails[k] = index into seq of smallest tail of an IS of len k+1
	prev := make([]int, n)
	for i := range prev {
		prev[i] = -1
	}
	for i := 0; i < n; i++ {
		lo, hi := 0, len(tails)
		for lo < hi {
			mid := (lo + hi) / 2
			if seq[tails[mid]] < seq[i] {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo > 0 {
			prev[i] = tails[lo-1]
		}
		if lo == len(tails) {
			tails = append(tails, i)
		} else {
			tails[lo] = i
		}
	}
	if len(tails) == 0 {
		return nil
	}
	res := []int{}
	for k := tails[len(tails)-1]; k != -1; k = prev[k] {
		res = append(res, k)
	}
	// Reverse into ascending order.
	for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}
	return res
}

// SourceTree is an ordered keyed tree (cell-model.md § Ordered keyed tree).
//
// Each node is (stable id, value cell, ordered keyed child collection). A
// node's children are a SourceMap keyed by child id, so per-level
// membership/order reactivity and the atomic-move guarantee are inherited. The
// tree is still a composition of cells — not a new cell kind — so per-cell merge
// applies node-by-node. Recursive, mirroring lazily-rs/src/cell_tree.rs.
type SourceTree[K comparable, V comparable] struct {
	ctx      *Context
	ID       K
	Value    *Source[V]
	Children *SourceMap[K, *SourceTree[K, V]]
}

// NewSourceTree creates a tree node with id and initialValue and an empty child
// collection.
func NewSourceTree[K comparable, V comparable](ctx *Context, id K, initialValue V) *SourceTree[K, V] {
	return &SourceTree[K, V]{
		ctx:      ctx,
		ID:       id,
		Value:    NewSource[V](ctx, initialValue),
		Children: NewSourceMap[K, *SourceTree[K, V]](ctx),
	}
}

// CellTree is the pre-v2-kernel name for SourceTree, kept as an alias so
// existing callers keep compiling. The v2 kernel renamed the node kinds to
// Source and Computed; the keyed collections follow.
//
// Deprecated: renamed to SourceTree.
type CellTree[K comparable, V comparable] = SourceTree[K, V]

// NewCellTree creates a tree node with id and initialValue and an empty child
// collection.
//
// Deprecated: renamed to NewSourceTree.
func NewCellTree[K comparable, V comparable](ctx *Context, id K, initialValue V) *SourceTree[K, V] {
	return NewSourceTree[K, V](ctx, id, initialValue)
}

// Get reads this node's value (reactive).
func (t *SourceTree[K, V]) Get() V { return t.Value.Get() }

// Set sets this node's value (PartialEq-guarded).
func (t *SourceTree[K, V]) Set(next V) { t.Value.Set(next) }

// NodeID returns the id of this node (stable handle).
func (t *SourceTree[K, V]) NodeID() K { return t.ID }

// InsertChild inserts a fresh child id with value, returning the child node. If
// the child already exists, its value is updated and the existing node returned.
func (t *SourceTree[K, V]) InsertChild(id K, value V) *SourceTree[K, V] {
	if existing := t.Children.Cell(id); existing != nil {
		existing.Peek().Set(value)
		return existing.Peek()
	}
	child := NewSourceTree[K, V](t.ctx, id, value)
	t.Children.Set(id, child)
	return child
}

// Child returns the child node for id, or nil. Non-reactive.
func (t *SourceTree[K, V]) Child(id K) *SourceTree[K, V] {
	child, ok := t.Children.Get(id)
	if !ok {
		return nil
	}
	return child
}

// RemoveChild removes the child id. Returns whether it was present.
func (t *SourceTree[K, V]) RemoveChild(id K) bool { return t.Children.Remove(id) }

// MoveChildTo atomically moves child id to index within this node's children.
func (t *SourceTree[K, V]) MoveChildTo(id K, index int) bool { return t.Children.MoveTo(id, index) }

// MoveChildBefore atomically moves child id to just before anchor.
func (t *SourceTree[K, V]) MoveChildBefore(id, anchor K) bool {
	return t.Children.MoveBefore(id, anchor)
}

// MoveChildAfter atomically moves child id to just after anchor.
func (t *SourceTree[K, V]) MoveChildAfter(id, anchor K) bool { return t.Children.MoveAfter(id, anchor) }

// ChildIDs returns a reactive snapshot of this node's child ids in order.
func (t *SourceTree[K, V]) ChildIDs(c ComputeOps) []K { return t.Children.Keys(c) }

// ChildCount returns the reactive child count for this node.
func (t *SourceTree[K, V]) ChildCount(c ComputeOps) int { return t.Children.Len(c) }

// HasChild reports the reactive membership test for a child of this node.
func (t *SourceTree[K, V]) HasChild(c ComputeOps, id K) bool {
	return t.Children.ContainsKey(c, id)
}
