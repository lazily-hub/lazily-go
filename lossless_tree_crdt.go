package lazily

// Lossless full-document tree CRDT — M1 syntax-agnostic core (#lzlosstree).
//
// A single rooted concrete-syntax tree whose *leaves own every rendered byte*.
// The defining invariant is losslessness — render(tree) == source_text for
// valid, invalid, and unknown source alike — so the tree itself can be the wire
// authority instead of a semantic AST over a separate text floor. Internal
// element nodes own *structure only*; all text lives in leaf nodes tagged
// Token / Trivia / Raw / Error, so unknown/invalid spans round-trip exactly as
// Raw/Error leaves rather than being discarded.
//
// M1 scope: create / tombstone / intra-parent reorder / leaf-edit / split-leaf /
// merge-adjacent-leaves, plus op-based delta sync over a dotted, non-contiguous
// version frontier. Positions and seed text travel inside ops so both replicas
// store byte-identical keys and converge. Leaf text embeds TextCrdt wholesale;
// child order is a minimal fractional index (keyBetween, mirroring SeqCrdt);
// the clock is a Lamport op id (the shared OpId type). Leaf-local wire offsets
// are UTF-8 bytes; Go strings are natively UTF-8, so byteToCodePoint is a
// rune-boundary check rather than a UTF-16 conversion.
//
// Go port of lazily-js `src/lossless-tree-crdt.js` and lazily-kt
// `LosslessTreeCrdt.kt`, mirroring the lazily-rs reference. Conforms to
// lazily-spec `schemas/lossless-tree.json` + `schemas/lossless-tree-delta.json`
// and replays the shared `conformance/lossless-tree/` compute fixtures.
//
// Wire conventions (NORMATIVE, from lossless-tree.json):
//   - OpId / TreeNodeId is the transparent {counter, peer} form (reuses OpId);
//     the document root is {counter: 0, peer: 0}.
//   - SortKey.frac is a JSON array of u8 (0..255), NOT base64.
//   - LeafKind is PascalCase on the wire (Token/Trivia/Raw/Error).
//   - NodeSeed and TreeOpKind are externally tagged (single-key object).
//   - SplitLeaf carries at_char (a Unicode scalar count); MergeLeaves carries
//     prev_left / prev_right (snake_case on the wire).

import (
	"encoding/json"
	"fmt"
	"sort"
	"unicode/utf8"
)

// ---------------------------------------------------------------------------
// LeafKind (lossless-tree.json#/$defs/LeafKind)
// ---------------------------------------------------------------------------

// LeafKind classifies a leaf's exact source span. Every rendered byte belongs
// to a leaf; unknown/invalid spans are Raw/Error so nothing is discarded.
// Serialized as the PascalCase wire string.
type LeafKind string

const (
	// LeafKindToken is a syntax delimiter or marker.
	LeafKindToken LeafKind = "Token"
	// LeafKindTrivia is whitespace, blank lines, indentation, comments.
	LeafKindTrivia LeafKind = "Trivia"
	// LeafKindRaw is valid text the adapter deliberately keeps opaque.
	LeafKindRaw LeafKind = "Raw"
	// LeafKindError is invalid/ambiguous text that must still round-trip.
	LeafKindError LeafKind = "Error"
)

// leafKindFromWireLower parses a lowercase fixture leaf kind (the seed-tree
// form) into the PascalCase wire LeafKind.
func leafKindFromWireLower(v string) (LeafKind, error) {
	switch v {
	case "token":
		return LeafKindToken, nil
	case "trivia":
		return LeafKindTrivia, nil
	case "raw":
		return LeafKindRaw, nil
	case "error":
		return LeafKindError, nil
	default:
		return "", fmt.Errorf("unknown leaf kind: %q", v)
	}
}

// ---------------------------------------------------------------------------
// SortKey (lossless-tree.json#/$defs/SortKey)
// ---------------------------------------------------------------------------

// TreeSortKey is a fractional-index child position: orderable bytes (0..255)
// tiebroken by the minting peer. Frac is a []int (not []byte) so it marshals
// as a JSON number array, never base64.
type TreeSortKey struct {
	Frac []int  `json:"frac"`
	Peer PeerId `json:"peer"`
}

// compare returns -1, 0, or +1 for the lexicographic (frac, peer) order.
func (a TreeSortKey) compare(b TreeSortKey) int {
	n := len(a.Frac)
	if len(b.Frac) < n {
		n = len(b.Frac)
	}
	for i := 0; i < n; i++ {
		if a.Frac[i] != b.Frac[i] {
			if a.Frac[i] < b.Frac[i] {
				return -1
			}
			return 1
		}
	}
	if len(a.Frac) != len(b.Frac) {
		if len(a.Frac) < len(b.Frac) {
			return -1
		}
		return 1
	}
	if a.Peer != b.Peer {
		if a.Peer < b.Peer {
			return -1
		}
		return 1
	}
	return 0
}

// copy returns a deep copy with a fresh Frac slice.
func (k TreeSortKey) copy() TreeSortKey {
	out := TreeSortKey{Frac: make([]int, len(k.Frac)), Peer: k.Peer}
	copy(out.Frac, k.Frac)
	return out
}

// validateSortKeyFrac returns an error if any frac byte is outside 0..255.
func validateSortKeyFrac(frac []int) error {
	for i, v := range frac {
		if v < 0 || v > 255 {
			return fmt.Errorf("sort frac[%d] must be in 0..255 (was %d)", i, v)
		}
	}
	return nil
}

// treeKeyBetween returns a fractional key strictly between lo and hi (each nil =
// open end), compared lexicographically. Mirrors SeqCrdt's keyBetween; bytes
// are ints in 0..255 (kept as []int so the wire form is a JSON number array,
// never base64).
func treeKeyBetween(lo, hi []int) []int {
	result := []int{}
	i := 0
	loLen := 0
	if lo != nil {
		loLen = len(lo)
	}
	hiLen := 0
	if hi != nil {
		hiLen = len(hi)
	}
	cap := loLen + hiLen + 2
	for i <= cap {
		var a int
		if lo != nil && i < len(lo) {
			a = lo[i]
		} else {
			a = 0
		}
		var b int
		if hi == nil {
			b = 256
		} else if i < len(hi) {
			b = hi[i]
		} else {
			b = 0
		}
		if a+1 < b {
			result = append(result, (a+b)/2)
			return result
		}
		result = append(result, a)
		i++
		if a < b {
			var loTail []int
			if lo != nil && i <= len(lo) {
				loTail = lo[i:]
			}
			result = append(result, treeKeyBetween(loTail, nil)...)
			return result
		}
	}
	result = append(result, 128)
	return result
}

// ---------------------------------------------------------------------------
// NodeSeed (lossless-tree.json#/$defs/NodeSeed)
// ---------------------------------------------------------------------------

// TreeNodeSeed is what a CreateNode materializes: an element shell or a text
// leaf seeded from exact text. Externally tagged on the wire
// ({"Element": {"kind": ...}} or {"Leaf": {"kind": ..., "text": ...}}).
type TreeNodeSeed interface {
	isTreeNodeSeed()
}

// TreeNodeSeedElement is an internal semantic node with a kind and ordered
// children. Owns structure only, never text.
type TreeNodeSeedElement struct {
	Kind string
}

func (TreeNodeSeedElement) isTreeNodeSeed() {}

// TreeNodeSeedLeaf is a leaf seeded from exact source text.
type TreeNodeSeedLeaf struct {
	Kind LeafKind
	Text string
}

func (TreeNodeSeedLeaf) isTreeNodeSeed() {}

// MarshalJSON renders the externally-tagged wire form.
func (s TreeNodeSeedElement) MarshalJSON() ([]byte, error) {
	return taggedJSON("Element", struct {
		Kind string `json:"kind"`
	}{s.Kind})
}

// MarshalJSON renders the externally-tagged wire form.
func (s TreeNodeSeedLeaf) MarshalJSON() ([]byte, error) {
	return taggedJSON("Leaf", struct {
		Kind LeafKind `json:"kind"`
		Text string   `json:"text"`
	}{s.Kind, s.Text})
}

func unmarshalTreeNodeSeed(raw json.RawMessage) (TreeNodeSeed, error) {
	tag, body, err := splitTagged(raw, "NodeSeed")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "Element":
		var w struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(body, &w); err != nil {
			return nil, err
		}
		return TreeNodeSeedElement{Kind: w.Kind}, nil
	case "Leaf":
		var w struct {
			Kind string `json:"kind"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(body, &w); err != nil {
			return nil, err
		}
		kind, err := leafKindFromWireLower(w.Kind)
		if err != nil {
			// Accept PascalCase wire form too (a seed may already be canonical).
			k := LeafKind(w.Kind)
			switch k {
			case LeafKindToken, LeafKindTrivia, LeafKindRaw, LeafKindError:
				kind = k
			default:
				return nil, err
			}
		}
		return TreeNodeSeedLeaf{Kind: kind, Text: w.Text}, nil
	default:
		return nil, fmt.Errorf("unknown NodeSeed variant: %s", tag)
	}
}

// ---------------------------------------------------------------------------
// TreeOpKind (lossless-tree.json#/$defs/TreeOpKind)
// ---------------------------------------------------------------------------

// TreeOpKind is the M1 op vocabulary: CreateNode / Tombstone / Reorder /
// LeafEdit / SplitLeaf / MergeLeaves. Externally tagged on the wire. Positions
// and seed text travel inside the op so both replicas store byte-identical keys
// and converge without consulting local clocks.
type TreeOpKind interface {
	isTreeOpKind()
}

// TreeOpCreateNode materializes an element shell or a text leaf seeded from
// exact text.
type TreeOpCreateNode struct {
	Id     OpId
	Parent OpId
	Sort   TreeSortKey
	Seed   TreeNodeSeed
}

func (TreeOpCreateNode) isTreeOpKind() {}

// TreeOpTombstone tombstones a node (sticky; smaller op id wins concurrently).
type TreeOpTombstone struct {
	Node OpId
}

func (TreeOpTombstone) isTreeOpKind() {}

// TreeOpReorder is a LWW position reassignment within the parent (identity +
// payload preserved).
type TreeOpReorder struct {
	Node OpId
	Sort TreeSortKey
}

func (TreeOpReorder) isTreeOpKind() {}

// TreeOpLeafEdit applies an embedded text-CRDT delta to one leaf. Prev is the
// prior text-op id (the leaf's textHead), forming a per-leaf causal chain.
type TreeOpLeafEdit struct {
	Node OpId
	Prev OpId
	Ops  []TextOp
}

func (TreeOpLeafEdit) isTreeOpKind() {}

// TreeOpSplitLeaf splits a leaf at a char boundary into two adjacent leaves of
// the same kind. AtChar is a Unicode scalar count (binding-stable).
type TreeOpSplitLeaf struct {
	Node   OpId
	NewId  OpId
	Sort   TreeSortKey
	AtChar int
	Prev   OpId
}

func (TreeOpSplitLeaf) isTreeOpKind() {}

// TreeOpMergeLeaves merges two adjacent leaf siblings; total text unchanged.
type TreeOpMergeLeaves struct {
	Left      OpId
	Right     OpId
	PrevLeft  OpId
	PrevRight OpId
}

func (TreeOpMergeLeaves) isTreeOpKind() {}

// MarshalJSON renders the externally-tagged wire form for an op kind.
func (k TreeOpCreateNode) MarshalJSON() ([]byte, error) {
	return taggedJSON("CreateNode", struct {
		Id     OpId         `json:"id"`
		Parent OpId         `json:"parent"`
		Sort   TreeSortKey  `json:"sort"`
		Seed   TreeNodeSeed `json:"seed"`
	}{k.Id, k.Parent, k.Sort, k.Seed})
}

// MarshalJSON renders the externally-tagged wire form for an op kind.
func (k TreeOpTombstone) MarshalJSON() ([]byte, error) {
	return taggedJSON("Tombstone", struct {
		Node OpId `json:"node"`
	}{k.Node})
}

// MarshalJSON renders the externally-tagged wire form for an op kind.
func (k TreeOpReorder) MarshalJSON() ([]byte, error) {
	return taggedJSON("Reorder", struct {
		Node OpId        `json:"node"`
		Sort TreeSortKey `json:"sort"`
	}{k.Node, k.Sort})
}

// MarshalJSON renders the externally-tagged wire form for an op kind.
func (k TreeOpLeafEdit) MarshalJSON() ([]byte, error) {
	return taggedJSON("LeafEdit", struct {
		Node OpId     `json:"node"`
		Prev OpId     `json:"prev"`
		Ops  []TextOp `json:"ops"`
	}{k.Node, k.Prev, nonNilSlice(k.Ops)})
}

// MarshalJSON renders the externally-tagged wire form for an op kind.
func (k TreeOpSplitLeaf) MarshalJSON() ([]byte, error) {
	return taggedJSON("SplitLeaf", struct {
		Node   OpId        `json:"node"`
		NewId  OpId        `json:"new"`
		Sort   TreeSortKey `json:"sort"`
		AtChar int         `json:"at_char"`
		Prev   OpId        `json:"prev"`
	}{k.Node, k.NewId, k.Sort, k.AtChar, k.Prev})
}

// MarshalJSON renders the externally-tagged wire form for an op kind.
func (k TreeOpMergeLeaves) MarshalJSON() ([]byte, error) {
	return taggedJSON("MergeLeaves", struct {
		Left      OpId `json:"left"`
		Right     OpId `json:"right"`
		PrevLeft  OpId `json:"prev_left"`
		PrevRight OpId `json:"prev_right"`
	}{k.Left, k.Right, k.PrevLeft, k.PrevRight})
}

func unmarshalTreeOpKind(raw json.RawMessage) (TreeOpKind, error) {
	tag, body, err := splitTagged(raw, "TreeOpKind")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "CreateNode":
		var w struct {
			Id     OpId            `json:"id"`
			Parent OpId            `json:"parent"`
			Sort   TreeSortKey     `json:"sort"`
			Seed   json.RawMessage `json:"seed"`
		}
		if err := json.Unmarshal(body, &w); err != nil {
			return nil, err
		}
		if err := validateSortKeyFrac(w.Sort.Frac); err != nil {
			return nil, err
		}
		seed, err := unmarshalTreeNodeSeed(w.Seed)
		if err != nil {
			return nil, err
		}
		return TreeOpCreateNode{Id: w.Id, Parent: w.Parent, Sort: w.Sort, Seed: seed}, nil
	case "Tombstone":
		var w struct {
			Node OpId `json:"node"`
		}
		if err := json.Unmarshal(body, &w); err != nil {
			return nil, err
		}
		return TreeOpTombstone{Node: w.Node}, nil
	case "Reorder":
		var w struct {
			Node OpId        `json:"node"`
			Sort TreeSortKey `json:"sort"`
		}
		if err := json.Unmarshal(body, &w); err != nil {
			return nil, err
		}
		if err := validateSortKeyFrac(w.Sort.Frac); err != nil {
			return nil, err
		}
		return TreeOpReorder{Node: w.Node, Sort: w.Sort}, nil
	case "LeafEdit":
		var w struct {
			Node OpId     `json:"node"`
			Prev OpId     `json:"prev"`
			Ops  []TextOp `json:"ops"`
		}
		if err := json.Unmarshal(body, &w); err != nil {
			return nil, err
		}
		return TreeOpLeafEdit{Node: w.Node, Prev: w.Prev, Ops: nonNilSlice(w.Ops)}, nil
	case "SplitLeaf":
		var w struct {
			Node   OpId        `json:"node"`
			NewId  OpId        `json:"new"`
			Sort   TreeSortKey `json:"sort"`
			AtChar int         `json:"at_char"`
			Prev   OpId        `json:"prev"`
		}
		if err := json.Unmarshal(body, &w); err != nil {
			return nil, err
		}
		if err := validateSortKeyFrac(w.Sort.Frac); err != nil {
			return nil, err
		}
		return TreeOpSplitLeaf{Node: w.Node, NewId: w.NewId, Sort: w.Sort, AtChar: w.AtChar, Prev: w.Prev}, nil
	case "MergeLeaves":
		var w struct {
			Left      OpId `json:"left"`
			Right     OpId `json:"right"`
			PrevLeft  OpId `json:"prev_left"`
			PrevRight OpId `json:"prev_right"`
		}
		if err := json.Unmarshal(body, &w); err != nil {
			return nil, err
		}
		return TreeOpMergeLeaves{Left: w.Left, Right: w.Right, PrevLeft: w.PrevLeft, PrevRight: w.PrevRight}, nil
	default:
		return nil, fmt.Errorf("unknown TreeOpKind variant: %s", tag)
	}
}

// ---------------------------------------------------------------------------
// TreeOp / TreeUpdate (lossless-tree.json#/$defs/TreeOp, lossless-tree-delta.json)
// ---------------------------------------------------------------------------

// TreeOp is a transport-ready tree operation: its dotted id plus the change it
// encodes.
type TreeOp struct {
	Id   OpId
	Kind TreeOpKind
}

// MarshalJSON renders {"id": ..., "kind": <externally-tagged kind>}.
func (op TreeOp) MarshalJSON() ([]byte, error) {
	kindBytes, err := json.Marshal(op.Kind)
	if err != nil {
		return nil, err
	}
	idBytes, err := json.Marshal(op.Id)
	if err != nil {
		return nil, err
	}
	var buf []byte
	buf = append(buf, '{')
	buf = append(buf, `"id":`...)
	buf = append(buf, idBytes...)
	buf = append(buf, `,"kind":`...)
	buf = append(buf, kindBytes...)
	buf = append(buf, '}')
	return buf, nil
}

// UnmarshalJSON decodes a TreeOp from its externally-tagged kind form.
func (op *TreeOp) UnmarshalJSON(b []byte) error {
	var w struct {
		Id   OpId            `json:"id"`
		Kind json.RawMessage `json:"kind"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	k, err := unmarshalTreeOpKind(w.Kind)
	if err != nil {
		return err
	}
	op.Id = w.Id
	op.Kind = k
	return nil
}

// TreeUpdate is the op-delta wire message: the output of Diff and the input to
// ApplyUpdate. Ops are ordered by dotted id; dependencies are buffered on apply
// until they arrive, so delivery need not be contiguous.
type TreeUpdate struct {
	Ops []TreeOp
}

// MarshalJSON renders {"ops": [...]} with ops always an array (never null).
func (u TreeUpdate) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Ops []TreeOp `json:"ops"`
	}{nonNilSlice(u.Ops)})
}

// UnmarshalJSON decodes a TreeUpdate.
func (u *TreeUpdate) UnmarshalJSON(b []byte) error {
	var w struct {
		Ops []TreeOp `json:"ops"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	u.Ops = nonNilSlice(w.Ops)
	return nil
}

// TreeUpdateFromWire decodes a TreeUpdate from JSON bytes.
func TreeUpdateFromWire(data []byte) (TreeUpdate, error) {
	var u TreeUpdate
	if err := json.Unmarshal(data, &u); err != nil {
		return TreeUpdate{}, err
	}
	return u, nil
}

// ---------------------------------------------------------------------------
// Dotted version frontier (lossless-tree.json#/$defs/DotRange, TreeVersionFrontier)
// ---------------------------------------------------------------------------

// TreeDotRange is the observed dots for one peer: a contiguous prefix plus
// out-of-order holes. Never a per-peer max — a hole above Contiguous stays
// representable in Sparse so it is re-requested rather than skipped.
type TreeDotRange struct {
	Contiguous int64
	Sparse     map[int64]struct{}
}

// NewTreeDotRange returns an empty dot range.
func NewTreeDotRange() *TreeDotRange {
	return &TreeDotRange{Sparse: map[int64]struct{}{}}
}

// Contains reports whether counter is held.
func (r *TreeDotRange) Contains(counter int64) bool {
	if counter <= r.Contiguous {
		return true
	}
	_, ok := r.Sparse[counter]
	return ok
}

// Observe records a counter, collapsing the contiguous prefix forward.
func (r *TreeDotRange) Observe(counter int64) {
	if r.Sparse == nil {
		r.Sparse = map[int64]struct{}{}
	}
	if counter <= r.Contiguous {
		return
	}
	r.Sparse[counter] = struct{}{}
	for {
		next := r.Contiguous + 1
		if _, ok := r.Sparse[next]; !ok {
			break
		}
		delete(r.Sparse, next)
		r.Contiguous = next
	}
}

// Copy returns a deep copy.
func (r *TreeDotRange) Copy() *TreeDotRange {
	out := &TreeDotRange{Contiguous: r.Contiguous, Sparse: map[int64]struct{}{}}
	for k := range r.Sparse {
		out.Sparse[k] = struct{}{}
	}
	return out
}

// TreeVersionFrontier is a dotted version frontier: per peer, exactly which op
// dots are held. Unlike a version vector (per-peer max), this represents
// non-contiguous delivery so Diff never omits a missing interior op.
type TreeVersionFrontier struct {
	dots map[PeerId]*TreeDotRange
}

// NewTreeVersionFrontier returns an empty frontier.
func NewTreeVersionFrontier() *TreeVersionFrontier {
	return &TreeVersionFrontier{dots: map[PeerId]*TreeDotRange{}}
}

// Contains reports whether id is held by this frontier.
func (f *TreeVersionFrontier) Contains(id OpId) bool {
	r, ok := f.dots[id.Peer]
	if !ok {
		return false
	}
	return r.Contains(id.Counter)
}

// Observe records id as held.
func (f *TreeVersionFrontier) Observe(id OpId) {
	if f.dots == nil {
		f.dots = map[PeerId]*TreeDotRange{}
	}
	r, ok := f.dots[id.Peer]
	if !ok {
		r = NewTreeDotRange()
		f.dots[id.Peer] = r
	}
	r.Observe(id.Counter)
}

// Copy returns a deep copy.
func (f *TreeVersionFrontier) Copy() *TreeVersionFrontier {
	out := NewTreeVersionFrontier()
	for k, r := range f.dots {
		out.dots[k] = r.Copy()
	}
	return out
}

// ---------------------------------------------------------------------------
// UTF-8 byte offset → code-point index (leaf-local, #lzlosstree Offset policy)
// ---------------------------------------------------------------------------

// byteToCodePoint returns the number of Unicode scalars (code points) before
// UTF-8 byte offset `b` in s, and false if b is out of range or does not land
// on a rune boundary. Go strings are natively UTF-8, so this is a boundary
// check rather than a UTF-16 conversion. Mirrors the JS byteToCodePoint.
func byteToCodePoint(s string, b int) (int, bool) {
	if b < 0 {
		return 0, false
	}
	pos := 0
	cp := 0
	for _, r := range s {
		if pos == b {
			return cp, true
		}
		size := utf8.RuneLen(r)
		pos += size
		if pos > b {
			return 0, false // offset falls inside this character
		}
		cp++
	}
	if pos == b {
		return cp, true
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// LosslessTreeCrdt
// ---------------------------------------------------------------------------

// TreeRoot is the sentinel id of the document root: {counter: 0, peer: 0}.
// Reuses OpId; the zero value is the root.
var TreeRoot = OpId{}

// treeBody is the materialized content of a node: either an element shell or a
// text leaf (embedding a full TextCrdt).
type treeBody struct {
	kind        string // "element" | "leaf"
	elementKind string
	leafKind    LeafKind
	text        *TextCrdt
}

// treeNode is a node's stored state: identity (the creating op id), parent,
// fractional position, the stamp of the op that last moved it, the body, a
// sticky tombstone, and the textHead (the id of the last text-mutating op on
// this leaf).
type treeNode struct {
	id        OpId
	parent    *OpId
	sort      TreeSortKey
	sortStamp OpId
	body      treeBody
	tomb      *OpId
	textHead  OpId
}

// LosslessTreeCrdt is a lossless concrete-syntax tree CRDT (M1 core).
//
// Not safe for concurrent use; share across goroutines via a single owner
// goroutine or wrap in a lock.
type LosslessTreeCrdt struct {
	peer     PeerId
	counter  int64
	nodes    map[OpId]*treeNode
	frontier *TreeVersionFrontier
	log      []TreeOp
	buffered []TreeOp
}

// NewLosslessTreeCrdt creates an empty replica for the given peer id, seeded
// with just the document root element.
func NewLosslessTreeCrdt(peer PeerId) *LosslessTreeCrdt {
	t := &LosslessTreeCrdt{
		peer:     peer,
		nodes:    map[OpId]*treeNode{},
		frontier: NewTreeVersionFrontier(),
	}
	rootParent := (*OpId)(nil)
	t.nodes[TreeRoot] = &treeNode{
		id:        TreeRoot,
		parent:    rootParent,
		sort:      TreeSortKey{Frac: []int{}, Peer: 0},
		sortStamp: OpId{},
		body:      treeBody{kind: "element", elementKind: "root"},
		tomb:      nil,
		textHead:  OpId{},
	}
	return t
}

func (t *LosslessTreeCrdt) nextOpId() OpId {
	t.counter++
	return OpId{Counter: t.counter, Peer: t.peer}
}

func (t *LosslessTreeCrdt) get(id OpId) *treeNode {
	return t.nodes[id]
}

// liveChildren returns the live children of parent, in rendered (SortKey)
// order. A node's identity is the id of the op that created it; the JS impl
// returns that id, which we store on the record as `id`.
func (t *LosslessTreeCrdt) liveChildren(parent OpId) []OpId {
	var kids []*treeNode
	for _, r := range t.nodes {
		if r.parent != nil && *r.parent == parent && r.tomb == nil {
			kids = append(kids, r)
		}
	}
	sort.Slice(kids, func(i, j int) bool {
		return kids[i].sort.compare(kids[j].sort) < 0
	})
	out := make([]OpId, len(kids))
	for i, r := range kids {
		out[i] = r.id
	}
	return out
}

// Render returns the whole document by concatenating live-leaf text in tree
// order (depth-first over live children).
func (t *LosslessTreeCrdt) Render() string {
	var out []byte
	var walk func(id OpId)
	walk = func(id OpId) {
		r := t.get(id)
		if r == nil {
			return
		}
		if r.body.kind == "leaf" {
			out = append(out, r.body.text.Text()...)
			return
		}
		for _, child := range t.liveChildren(id) {
			walk(child)
		}
	}
	walk(TreeRoot)
	return string(out)
}

// LiveNodeCount returns the live nodes excluding the root — grows by one on
// split, restored on merge.
func (t *LosslessTreeCrdt) LiveNodeCount() int {
	n := 0
	for id, r := range t.nodes {
		if id == TreeRoot || r.tomb != nil {
			continue
		}
		n++
	}
	return n
}

// Frontier returns this replica's dotted version frontier (what to advertise to
// a partner).
func (t *LosslessTreeCrdt) Frontier() *TreeVersionFrontier {
	return t.frontier.Copy()
}

// ElementKind returns the kind of an element node, or "" if absent or a leaf.
func (t *LosslessTreeCrdt) ElementKind(node OpId) string {
	r := t.get(node)
	if r == nil || r.body.kind != "element" {
		return ""
	}
	return r.body.elementKind
}

// LeafKind returns the kind of a leaf node, or "" if absent or an element.
func (t *LosslessTreeCrdt) LeafKind(node OpId) LeafKind {
	r := t.get(node)
	if r == nil || r.body.kind != "leaf" {
		return ""
	}
	return r.body.leafKind
}

// Children returns the live children of parent in rendered order.
func (t *LosslessTreeCrdt) Children(parent OpId) []OpId {
	return t.liveChildren(parent)
}

// LeafText returns a leaf's current text. Panics if node is absent or an
// element.
func (t *LosslessTreeCrdt) LeafText(node OpId) string {
	r := t.get(node)
	if r == nil {
		panic("lossless-tree: node not found")
	}
	if r.body.kind != "leaf" {
		panic("lossless-tree: node is not a leaf")
	}
	return r.body.text.Text()
}

// keyAfter computes a SortKey for a new/reordered child positioned just after
// `after` (front when nil). Mirrors the JS #keyAfter.
func (t *LosslessTreeCrdt) keyAfter(parent OpId, after *OpId) TreeSortKey {
	order := t.liveChildren(parent)
	var loFrac, hiFrac []int
	if after == nil {
		if len(order) > 0 {
			hiFrac = t.get(order[0]).sort.Frac
		}
	} else {
		idx := -1
		for i, x := range order {
			if x == *after {
				idx = i
				break
			}
		}
		if idx >= 0 {
			loFrac = t.get(*after).sort.Frac
			if idx+1 < len(order) {
				hiFrac = t.get(order[idx+1]).sort.Frac
			}
		} else if len(order) > 0 {
			// Anchor gone: append at end.
			loFrac = t.get(order[len(order)-1]).sort.Frac
		}
	}
	return TreeSortKey{Frac: treeKeyBetween(loFrac, hiFrac), Peer: t.peer}
}

// CreateNode creates a node under parent, positioned after after (front when
// nil), and returns the new node's id.
func (t *LosslessTreeCrdt) CreateNode(parent OpId, after *OpId, seed TreeNodeSeed) OpId {
	if t.get(parent) == nil {
		panic("lossless-tree: parent node not found")
	}
	sort := t.keyAfter(parent, after)
	opId := t.nextOpId()
	node := opId
	t.commitLocal(TreeOp{Id: opId, Kind: TreeOpCreateNode{Id: node, Parent: parent, Sort: sort, Seed: seed}})
	return node
}

// TombstoneNode tombstones a node (its subtree renders away once the ancestor
// is gone).
func (t *LosslessTreeCrdt) TombstoneNode(node OpId) {
	if t.get(node) == nil || node == TreeRoot {
		panic("lossless-tree: node not found")
	}
	opId := t.nextOpId()
	t.commitLocal(TreeOp{Id: opId, Kind: TreeOpTombstone{Node: node}})
}

// ReorderChild reorders node within its parent to just after after (front when
// nil).
func (t *LosslessTreeCrdt) ReorderChild(node OpId, after *OpId) {
	rec := t.get(node)
	if rec == nil || rec.parent == nil {
		panic("lossless-tree: node not found")
	}
	sort := t.keyAfter(*rec.parent, after)
	opId := t.nextOpId()
	t.commitLocal(TreeOp{Id: opId, Kind: TreeOpReorder{Node: node, Sort: sort}})
}

// EditLeaf edits a leaf's text: delete deleteBytes and insert insert at UTF-8
// byte offset atByte (leaf-local). Offsets must land on rune boundaries.
func (t *LosslessTreeCrdt) EditLeaf(node OpId, atByte, deleteBytes int, insert string) {
	s := t.LeafText(node)
	start, ok := byteToCodePoint(s, atByte)
	if !ok {
		panic("lossless-tree: offset not on a char boundary")
	}
	end, ok := byteToCodePoint(s, atByte+deleteBytes)
	if !ok {
		panic("lossless-tree: offset not on a char boundary")
	}
	deleteCount := end - start

	// Re-own the leaf's text under this replica so concurrent edits from
	// different peers mint distinct char ids (no collision on merge).
	rec := t.get(node)
	rec.body.text = rec.body.text.Fork(t.peer)
	vv := rec.body.text.VersionVector()
	for i := 0; i < deleteCount; i++ {
		rec.body.text.Delete(start)
	}
	rec.body.text.InsertStr(start, insert)
	ops := rec.body.text.DeltaSince(vv)

	prev := rec.textHead
	opId := t.nextOpId()
	t.commitLocal(TreeOp{Id: opId, Kind: TreeOpLeafEdit{Node: node, Prev: prev, Ops: ops}})
}

// SplitLeaf splits a leaf at UTF-8 byte offset atByte into two adjacent leaves
// of the same kind (head keeps node, tail is a fresh node returned here).
func (t *LosslessTreeCrdt) SplitLeaf(node OpId, atByte int) OpId {
	s := t.LeafText(node)
	atChar, ok := byteToCodePoint(s, atByte)
	if !ok {
		panic("lossless-tree: offset not on a char boundary")
	}
	rec := t.get(node)
	if rec.parent == nil {
		panic("lossless-tree: node not found")
	}
	sort := t.keyAfter(*rec.parent, &node)
	prev := rec.textHead
	opId := t.nextOpId()
	newNode := opId
	t.commitLocal(TreeOp{Id: opId, Kind: TreeOpSplitLeaf{Node: node, NewId: newNode, Sort: sort, AtChar: atChar, Prev: prev}})
	return newNode
}

// MergeAdjacentLeaves merges right into left when they are adjacent live leaf
// siblings.
func (t *LosslessTreeCrdt) MergeAdjacentLeaves(left, right OpId) {
	t.LeafText(left) // validate leaf-ness
	t.LeafText(right)
	rec := t.get(left)
	if rec.parent == nil {
		panic("lossless-tree: node not found")
	}
	order := t.liveChildren(*rec.parent)
	idx := -1
	for i, x := range order {
		if x == left {
			idx = i
			break
		}
	}
	adjacent := idx >= 0 && idx+1 < len(order) && order[idx+1] == right
	if !adjacent {
		panic("lossless-tree: leaves are not adjacent live siblings")
	}
	prevLeft := t.get(left).textHead
	prevRight := t.get(right).textHead
	opId := t.nextOpId()
	t.commitLocal(TreeOp{Id: opId, Kind: TreeOpMergeLeaves{Left: left, Right: right, PrevLeft: prevLeft, PrevRight: prevRight}})
}

// Fork deep-copies this replica's full state under a new owning peer (new
// identity).
func (t *LosslessTreeCrdt) Fork(peer PeerId) *LosslessTreeCrdt {
	out := NewLosslessTreeCrdt(peer)
	out.counter = t.counter
	out.nodes = map[OpId]*treeNode{}
	for id, r := range t.nodes {
		var body treeBody
		if r.body.kind == "leaf" {
			body = treeBody{kind: "leaf", leafKind: r.body.leafKind, text: r.body.text.Clone()}
		} else {
			body = treeBody{kind: "element", elementKind: r.body.elementKind}
		}
		out.nodes[id] = &treeNode{
			id:        r.id,
			parent:    cloneOpIdPtr(r.parent),
			sort:      r.sort.copy(),
			sortStamp: r.sortStamp,
			body:      body,
			tomb:      cloneOpIdPtr(r.tomb),
			textHead:  r.textHead,
		}
	}
	out.frontier = t.frontier.Copy()
	out.log = append([]TreeOp(nil), t.log...)
	out.buffered = append([]TreeOp(nil), t.buffered...)
	return out
}

// Diff returns the ops this replica holds that their frontier lacks, ordered by
// dotted id.
func (t *LosslessTreeCrdt) Diff(their *TreeVersionFrontier) TreeUpdate {
	var ops []TreeOp
	for _, op := range t.log {
		if !their.Contains(op.Id) {
			ops = append(ops, op)
		}
	}
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].Id.Compare(ops[j].Id) < 0
	})
	return TreeUpdate{Ops: ops}
}

// ApplyUpdate applies a batch of remote ops. Idempotent (already-held ops
// skipped) and order-tolerant (an op whose target/parent has not arrived is
// buffered and retried). Advances the Lamport counter past every observed op.
func (t *LosslessTreeCrdt) ApplyUpdate(update TreeUpdate) {
	for _, op := range update.Ops {
		if op.Id.Counter > t.counter {
			t.counter = op.Id.Counter
		}
		if t.frontier.Contains(op.Id) {
			continue
		}
		t.buffered = append(t.buffered, op)
	}
	t.drainBuffered()
}

func (t *LosslessTreeCrdt) drainBuffered() {
	for {
		progressed := false
		pending := t.buffered
		t.buffered = nil
		for _, op := range pending {
			if t.frontier.Contains(op.Id) {
				continue
			}
			if t.dependenciesReady(op) {
				t.applyOp(op)
				t.record(op)
				progressed = true
			} else {
				t.buffered = append(t.buffered, op)
			}
		}
		if !progressed {
			break
		}
	}
}

func (t *LosslessTreeCrdt) dependenciesReady(op TreeOp) bool {
	switch k := op.Kind.(type) {
	case TreeOpCreateNode:
		return t.get(k.Parent) != nil
	case TreeOpTombstone:
		return t.get(k.Node) != nil
	case TreeOpReorder:
		return t.get(k.Node) != nil
	case TreeOpLeafEdit:
		return t.get(k.Node) != nil && t.frontier.Contains(k.Prev)
	case TreeOpSplitLeaf:
		return t.get(k.Node) != nil && t.frontier.Contains(k.Prev)
	case TreeOpMergeLeaves:
		return t.get(k.Left) != nil && t.get(k.Right) != nil &&
			t.frontier.Contains(k.PrevLeft) && t.frontier.Contains(k.PrevRight)
	}
	return false
}

func (t *LosslessTreeCrdt) commitLocal(op TreeOp) {
	t.applyOp(op)
	t.record(op)
}

func (t *LosslessTreeCrdt) record(op TreeOp) {
	t.frontier.Observe(op.Id)
	t.log = append(t.log, op)
}

func minOpId(a, b OpId) OpId {
	if a.Compare(b) <= 0 {
		return a
	}
	return b
}

func (t *LosslessTreeCrdt) applyOp(op TreeOp) {
	switch k := op.Kind.(type) {
	case TreeOpCreateNode:
		if t.get(k.Id) != nil {
			return
		}
		var body treeBody
		switch s := k.Seed.(type) {
		case TreeNodeSeedLeaf:
			body = treeBody{kind: "leaf", leafKind: s.Kind, text: TextCrdtFromStr(k.Id.Peer, s.Text)}
		case TreeNodeSeedElement:
			body = treeBody{kind: "element", elementKind: s.Kind}
		}
		parent := k.Parent
		t.nodes[k.Id] = &treeNode{
			id:        k.Id,
			parent:    &parent,
			sort:      k.Sort,
			sortStamp: op.Id,
			body:      body,
			tomb:      nil,
			textHead:  op.Id,
		}
	case TreeOpTombstone:
		rec := t.get(k.Node)
		if rec != nil {
			if rec.tomb == nil {
				tb := op.Id
				rec.tomb = &tb
			} else {
				v := minOpId(*rec.tomb, op.Id)
				rec.tomb = &v
			}
		}
	case TreeOpReorder:
		rec := t.get(k.Node)
		if rec != nil && op.Id.Compare(rec.sortStamp) > 0 {
			rec.sort = k.Sort
			rec.sortStamp = op.Id
		}
	case TreeOpLeafEdit:
		rec := t.get(k.Node)
		if rec != nil && rec.body.kind == "leaf" {
			rec.body.text.ApplyDelta(k.Ops)
			rec.textHead = op.Id
		}
	case TreeOpSplitLeaf:
		t.applySplit(k.Node, k.NewId, k.Sort, k.AtChar, op.Id)
	case TreeOpMergeLeaves:
		t.applyMerge(k.Left, k.Right, op.Id)
	}
}

func (t *LosslessTreeCrdt) applySplit(node, newNode OpId, sort TreeSortKey, atChar int, opId OpId) {
	rec := t.get(node)
	if rec == nil || rec.body.kind != "leaf" {
		return
	}
	leafKind := rec.body.leafKind
	parent := rec.parent
	text := rec.body.text.Text()
	// Code-point slice.
	cps := []rune(text)
	clamp := atChar
	if clamp > len(cps) {
		clamp = len(cps)
	}
	head := string(cps[:clamp])
	tail := string(cps[clamp:])
	// Reseed head under the original node's create peer so both replicas rebuild
	// byte-identical leaf state.
	rec.body = treeBody{kind: "leaf", leafKind: leafKind, text: TextCrdtFromStr(node.Peer, head)}
	rec.textHead = opId
	if t.get(newNode) == nil {
		newParent := parent
		t.nodes[newNode] = &treeNode{
			id:        newNode,
			parent:    newParent,
			sort:      sort,
			sortStamp: opId,
			body:      treeBody{kind: "leaf", leafKind: leafKind, text: TextCrdtFromStr(newNode.Peer, tail)},
			tomb:      nil,
			textHead:  opId,
		}
	}
}

func (t *LosslessTreeCrdt) applyMerge(left, right, opId OpId) {
	l := t.get(left)
	r := t.get(right)
	if l == nil || r == nil || l.body.kind != "leaf" || r.body.kind != "leaf" {
		return
	}
	combined := l.body.text.Text() + r.body.text.Text()
	l.body = treeBody{kind: "leaf", leafKind: l.body.leafKind, text: TextCrdtFromStr(left.Peer, combined)}
	l.textHead = opId
	if r.tomb == nil {
		tb := opId
		r.tomb = &tb
	} else {
		v := minOpId(*r.tomb, opId)
		r.tomb = &v
	}
}
