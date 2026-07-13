package lazily

// CrdtTree is the lossless mergeable document contract (#lzcrdttree).
//
// Snapshot and incremental replication use the same identity-preserving delta:
// DeltaSince(empty frontier) is the whole-state snapshot. MergeFrom and
// ApplyDelta must therefore be commutative, associative, and idempotent.
type CrdtTree[V any, D any, T any] interface {
	VersionVector() V
	DeltaSince(V) D
	ApplyDelta(D) bool
	Text() string
	Value() T
	MergeFrom(CrdtTree[V, D, T]) bool
}

// Value returns the visible lossless-tree value.
func (t *TextCrdt) Value() string { return t.Text() }

// MergeFrom joins another CrdtTree through the identity-preserving delta path.
func (t *TextCrdt) MergeFrom(other CrdtTree[map[PeerId]int64, []TextOp, string]) bool {
	return t.ApplyDelta(other.DeltaSince(t.VersionVector()))
}

var _ CrdtTree[map[PeerId]int64, []TextOp, string] = (*TextCrdt)(nil)
