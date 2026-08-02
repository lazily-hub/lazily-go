package lazily

// Fugue/RGA-style free-text character CRDT.
//
// Each character is an element with a unique OpId (counter, peer) and a
// left-origin. Deletes are sticky tombstones carrying the delete op's own id.
// Visible order is a pure deterministic function of the element set: a
// pre-order DFS of the origin tree with siblings sorted DESCENDING by OpId
// (most-recent first). Merge is commutative, associative, and idempotent;
// concurrent same-point inserts keep both, ordered by the peer tiebreak.
//
// Port of lazily-dart lib/src/text_crdt.dart; mirrors lazily-rs `TextCrdt`,
// lazily-js `text-crdt.js`. Conforms to lazily-spec
// conformance/collections/textcrdt_convergence.json and
// textcrdt_delta_sync.json (#lztextsync). Delta-sync wire fields are
// snake_case (counter, peer, id, ch, origin, deleted, version_vector).
//
// Delta sync (version_vector / delta_since / apply_delta) preserves each
// character's OpId across a whole-state snapshot, so a replica rebuilt via
// ApplyDelta still merges conflict-free with a concurrent edit — unlike
// re-parsing, which would mint fresh ids and duplicate content on merge.

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// OpId is a globally-unique operation identifier for a character CRDT op.
//
// Ordered ascending by (Counter, Peer) so later ops sort after earlier ones
// from the same peer, and ties between peers break deterministically. OpId is
// a comparable value type, usable directly as a map key.
type OpId struct {
	Counter int64  `json:"counter"`
	Peer    PeerId `json:"peer"`
}

// Compare returns -1, 0, or +1 for the ascending (counter, peer) order.
func (id OpId) Compare(other OpId) int {
	switch {
	case id.Counter != other.Counter:
		if id.Counter < other.Counter {
			return -1
		}
		return 1
	case id.Peer != other.Peer:
		if id.Peer < other.Peer {
			return -1
		}
		return 1
	default:
		return 0
	}
}

// String renders the Dart-style debug form OpId(counter,peer).
func (id OpId) String() string {
	return "OpId(" + strconv.FormatInt(id.Counter, 10) + "," + strconv.FormatInt(id.Peer, 10) + ")"
}

// ToWire renders the snake_case wire map {counter, peer}.
func (id OpId) ToWire() map[string]any {
	return map[string]any{"counter": id.Counter, "peer": id.Peer}
}

// OpIdFromWire parses an OpId from its decoded-JSON map form, rejecting a
// malformed one rather than coercing it.
//
// A silent zero here is not a harmless default: counters minted by nextId are
// always >= 1, so OpId{0,0} is exactly textRootKey, the byOrigin bucket for
// elements whose origin is the document start. Coercing an absent, null, or
// non-numeric `counter`/`peer` to 0 would reparent a foreign op onto the
// document root and merge it into the visible text as if it were well-formed.
// The sibling bindings reject the same frames: lazily-rs derives serde on
// `OpId`, so a string or missing `counter` is a deserialization error there.
func OpIdFromWire(v any) (OpId, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return OpId{}, fmt.Errorf("OpId must be a JSON object, got %T", v)
	}
	counter, err := wireInt64(m["counter"])
	if err != nil {
		return OpId{}, fmt.Errorf("OpId.counter: %w", err)
	}
	peer, err := wireInt64(m["peer"])
	if err != nil {
		return OpId{}, fmt.Errorf("OpId.peer: %w", err)
	}
	return OpId{Counter: counter, Peer: peer}, nil
}

// textElem is a single character element: a code point, its left-origin (nil
// = document start), and its delete op id (nil = live).
type textElem struct {
	ch      string
	origin  *OpId
	deleted *OpId
}

// TextOp is a single text-CRDT operation in delta-sync wire form. Origin and
// Deleted serialize as JSON null when absent (matching the Dart toWire, which
// always emits all four keys).
type TextOp struct {
	Id      OpId   `json:"id"`
	Ch      string `json:"ch"`
	Origin  *OpId  `json:"origin"`
	Deleted *OpId  `json:"deleted"`
}

// ToWire renders the wire map {id, ch, origin, deleted}. Absent origin/deleted
// map to nil (JSON null), matching Dart.
func (op TextOp) ToWire() map[string]any {
	var origin, deleted any
	if op.Origin != nil {
		origin = op.Origin.ToWire()
	}
	if op.Deleted != nil {
		deleted = op.Deleted.ToWire()
	}
	return map[string]any{
		"id":      op.Id.ToWire(),
		"ch":      op.Ch,
		"origin":  origin,
		"deleted": deleted,
	}
}

// TextOpFromWire parses a TextOp from its decoded-JSON map form, rejecting a
// malformed op rather than coercing it. `origin` and `deleted` are the only
// legitimately-absent keys (JSON null = document start / live), so an explicit
// null stays lenient while a present-but-malformed value is refused.
func TextOpFromWire(v any) (TextOp, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return TextOp{}, fmt.Errorf("TextOp must be a JSON object, got %T", v)
	}
	id, err := OpIdFromWire(m["id"])
	if err != nil {
		return TextOp{}, fmt.Errorf("TextOp.id: %w", err)
	}
	ch, ok := m["ch"].(string)
	if !ok {
		return TextOp{}, fmt.Errorf("TextOp.ch must be a string, got %T", m["ch"])
	}
	op := TextOp{Id: id, Ch: ch}
	if o, ok := m["origin"]; ok && o != nil {
		origin, err := OpIdFromWire(o)
		if err != nil {
			return TextOp{}, fmt.Errorf("TextOp.origin: %w", err)
		}
		op.Origin = &origin
	}
	if d, ok := m["deleted"]; ok && d != nil {
		deleted, err := OpIdFromWire(d)
		if err != nil {
			return TextOp{}, fmt.Errorf("TextOp.deleted: %w", err)
		}
		op.Deleted = &deleted
	}
	return op, nil
}

// textRootKey is the byOrigin bucket for elements whose origin is the document
// start. Counters minted by nextId are always >= 1, so {0,0} never collides
// with a real OpId.
var textRootKey = OpId{Counter: 0, Peer: 0}

// TextCrdt is a Fugue/RGA-style free-text character CRDT.
type TextCrdt struct {
	peer    PeerId
	counter int64
	elems   map[OpId]*textElem
	// Cached visible orderings (#lztextordcache). Both nil after every
	// mutation; populated lazily on the next read. Repeated Text()/Len()
	// between mutations drops O(N log N) -> O(1) (after the first rebuild)
	// instead of rebuilding per call.
	orderedLiveCache []OpId
	orderedAllCache  []OpId
}

// NewTextCrdt creates an empty replica for the given peer id.
func NewTextCrdt(peer PeerId) *TextCrdt {
	return &TextCrdt{peer: peer, elems: map[OpId]*textElem{}}
}

// TextCrdtFromStr seeds a new CRDT from s as if a single peer typed it
// sequentially.
func TextCrdtFromStr(peer PeerId, s string) *TextCrdt {
	c := NewTextCrdt(peer)
	c.InsertStr(0, s)
	return c
}

// Peer returns the peer id of this replica.
func (t *TextCrdt) Peer() PeerId { return t.peer }

func (t *TextCrdt) nextId() OpId {
	t.counter++
	return OpId{Counter: t.counter, Peer: t.peer}
}

// invalidateOrdered clears both orderings; callers must invoke after any
// mutation that changes the element set or origin tree.
func (t *TextCrdt) invalidateOrdered() {
	t.orderedLiveCache = nil
	t.orderedAllCache = nil
}

// orderedIds returns the visible ordering: pre-order DFS of the origin tree,
// siblings sorted descending by OpId. When includeDeleted is false, tombstoned
// elements are omitted from the output but still traversed (their descendants
// remain reachable).
func (t *TextCrdt) orderedIds(includeDeleted bool) []OpId {
	// Cache hit (#lztextordcache).
	if includeDeleted {
		if t.orderedAllCache != nil {
			return t.orderedAllCache
		}
	} else {
		if t.orderedLiveCache != nil {
			return t.orderedLiveCache
		}
	}

	byOrigin := map[OpId][]OpId{}
	for id, e := range t.elems {
		ok := textRootKey
		if e.origin != nil {
			ok = *e.origin
		}
		byOrigin[ok] = append(byOrigin[ok], id)
	}
	for k := range byOrigin {
		list := byOrigin[k]
		sort.Slice(list, func(i, j int) bool {
			return list[i].Compare(list[j]) > 0 // descending, most-recent first
		})
	}

	var out []OpId
	var dfs func(originKey OpId)
	dfs = func(originKey OpId) {
		for _, id := range byOrigin[originKey] {
			elem := t.elems[id]
			if includeDeleted || elem.deleted == nil {
				out = append(out, id)
			}
			dfs(id)
		}
	}
	dfs(textRootKey)
	if includeDeleted {
		t.orderedAllCache = out
	} else {
		t.orderedLiveCache = out
	}
	return out
}

// Insert inserts a single character ch at the visible index.
func (t *TextCrdt) Insert(index int, ch string) {
	visible := t.orderedIds(false)
	var origin *OpId
	if index != 0 && index-1 < len(visible) {
		v := visible[index-1]
		origin = &v
	}
	id := t.nextId()
	t.invalidateOrdered()
	t.elems[id] = &textElem{ch: ch, origin: origin, deleted: nil}
}

// InsertStr inserts a multi-character string at the visible index with origin
// chaining (#lztextinsertchain): one `orderedIds()` pass + N chain appends
// instead of N full-tree rebuilds. Sequential chars chain naturally — char i+1's
// left-origin is char i's just-minted OpId — so DFS visits them in chain order
// (counter strictly increases under one peer). Concurrent inserts at the same
// point still sort by peer tiebreak (standard CRDT convergence).
func (t *TextCrdt) InsertStr(index int, s string) {
	visible := t.orderedIds(false)
	var origin *OpId
	if index != 0 && index-1 < len(visible) {
		v := visible[index-1]
		origin = &v
	}
	t.invalidateOrdered()
	for _, r := range s {
		id := t.nextId()
		// Each iteration needs its own origin pointer (don't alias &id).
		var originForThis *OpId
		if origin != nil {
			v := *origin
			originForThis = &v
		}
		t.elems[id] = &textElem{ch: string(r), origin: originForThis, deleted: nil}
		next := id
		origin = &next
	}
}

// Delete tombstones the visible character at index. No-op if out of range.
func (t *TextCrdt) Delete(index int) {
	visible := t.orderedIds(false)
	if index < 0 || index >= len(visible) {
		return
	}
	id := visible[index]
	elem := t.elems[id]
	del := t.nextId()
	if elem.deleted == nil {
		elem.deleted = &del
		// Tombstone flip changes the live (filtered) ordering but not the
		// full DFS — only invalidate the live cache.
		t.orderedLiveCache = nil
	}
}

// Text returns the visible text.
func (t *TextCrdt) Text() string {
	var sb strings.Builder
	for _, id := range t.orderedIds(false) {
		sb.WriteString(t.elems[id].ch)
	}
	return sb.String()
}

// Len returns the count of live (non-deleted) elements.
func (t *TextCrdt) Len() int { return len(t.orderedIds(false)) }

// IsEmpty reports whether there are no live elements.
func (t *TextCrdt) IsEmpty() bool { return t.Len() == 0 }

// TombstoneCount returns the count of tombstoned (deleted) elements.
func (t *TextCrdt) TombstoneCount() int { return len(t.elems) - t.Len() }

// Clock returns the current clock position.
func (t *TextCrdt) Clock() OpId { return OpId{Counter: t.counter, Peer: t.peer} }

// Fork deep-copies this replica under a new peer, adopting the same element set
// and copying the counter so future ops don't collide with prior ones.
func (t *TextCrdt) Fork(peer PeerId) *TextCrdt {
	c := NewTextCrdt(peer)
	c.counter = t.counter
	for id, e := range t.elems {
		c.elems[id] = &textElem{ch: e.ch, origin: cloneOpIdPtr(e.origin), deleted: cloneOpIdPtr(e.deleted)}
	}
	return c
}

// Clone deep-copies this replica with the same peer.
func (t *TextCrdt) Clone() *TextCrdt { return t.Fork(t.peer) }

// Merge folds another replica's state into this one. Returns whether the
// visible text changed.
func (t *TextCrdt) Merge(other *TextCrdt) bool {
	before := t.Text()
	anyChange := false
	for id, e := range other.elems {
		if t.mergeElem(id, e) {
			anyChange = true
		}
	}
	if anyChange {
		t.invalidateOrdered()
	}
	return t.Text() != before
}

// mergeElem is the commutative/associative/idempotent element merge shared by
// Merge and ApplyDelta. Returns true if any state changed (new element adopted
// or tombstone id updated).
func (t *TextCrdt) mergeElem(id OpId, oe *textElem) bool {
	m := t.counter
	if id.Counter > m {
		m = id.Counter
	}
	if oe.deleted != nil && oe.deleted.Counter > m {
		m = oe.deleted.Counter
	}
	t.counter = m

	existing, ok := t.elems[id]
	if ok {
		switch {
		case existing.deleted != nil && oe.deleted != nil:
			// Concurrent deletes -> smaller delete id wins (sticky on both).
			if oe.deleted.Compare(*existing.deleted) < 0 {
				existing.deleted = cloneOpIdPtr(oe.deleted)
				return true
			}
		case oe.deleted != nil && existing.deleted == nil:
			existing.deleted = cloneOpIdPtr(oe.deleted)
			return true
		}
		return false
	}
	t.elems[id] = &textElem{ch: oe.ch, origin: cloneOpIdPtr(oe.origin), deleted: cloneOpIdPtr(oe.deleted)}
	return true
}

// GcWith collects stable tombstones. An element is collectable when it is
// deleted, isStable confirms its delete op is stable, AND nothing references it
// as a left-origin. Runs to a fixpoint (collecting a leaf may expose its
// parent). Returns the number removed.
func (t *TextCrdt) GcWith(isStable func(deleteOpId OpId) bool) int {
	removed := 0
	for {
		referenced := map[OpId]struct{}{}
		for _, e := range t.elems {
			if e.origin != nil {
				referenced[*e.origin] = struct{}{}
			}
		}
		var collectable []OpId
		for id, e := range t.elems {
			if e.deleted != nil && isStable(*e.deleted) {
				if _, ref := referenced[id]; !ref {
					collectable = append(collectable, id)
				}
			}
		}
		if len(collectable) == 0 {
			break
		}
		for _, k := range collectable {
			delete(t.elems, k)
			removed++
		}
	}
	if removed > 0 {
		t.invalidateOrdered()
	}
	return removed
}

// --- Delta sync (#lztextsync) ---

// VersionVector returns {peer -> max counter} taken over BOTH insert ids and
// tombstone delete ids. An absent peer implies 0. Serializes to JSON with
// string peer keys (e.g. {"1":4}), matching the spec fixtures.
func (t *TextCrdt) VersionVector() map[PeerId]int64 {
	vv := map[PeerId]int64{}
	bump := func(id OpId) {
		if cur, ok := vv[id.Peer]; !ok || id.Counter > cur {
			vv[id.Peer] = id.Counter
		}
	}
	for id, e := range t.elems {
		bump(id)
		if e.deleted != nil {
			bump(*e.deleted)
		}
	}
	return vv
}

// DeltaSince returns the elements whose insert id or tombstone delete id is
// newer than theirVv. A whole-state snapshot is DeltaSince(nil).
func (t *TextCrdt) DeltaSince(theirVv map[PeerId]int64) []TextOp {
	seen := func(id OpId) bool { return id.Counter <= theirVv[id.Peer] }
	var out []TextOp
	for id, e := range t.elems {
		insertNew := !seen(id)
		deleteNew := e.deleted != nil && !seen(*e.deleted)
		if insertNew || deleteNew {
			out = append(out, TextOp{
				Id:      id,
				Ch:      e.ch,
				Origin:  cloneOpIdPtr(e.origin),
				Deleted: cloneOpIdPtr(e.deleted),
			})
		}
	}
	return out
}

// ApplyDelta applies a delta with the same commutative/associative/idempotent
// algebra as Merge. Returns whether the visible text changed.
func (t *TextCrdt) ApplyDelta(ops []TextOp) bool {
	before := t.Text()
	anyChange := false
	for _, op := range ops {
		if t.mergeElem(op.Id, &textElem{ch: op.Ch, origin: op.Origin, deleted: op.Deleted}) {
			anyChange = true
		}
	}
	if anyChange {
		t.invalidateOrdered()
	}
	return t.Text() != before
}

// cloneOpIdPtr returns a fresh pointer to a copy of p (nil-safe), so merged
// elements never alias another replica's OpId storage.
func cloneOpIdPtr(p *OpId) *OpId {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// wireInt64 converts a decoded-JSON numeric value to int64 (JSON decodes
// numbers as float64 by default, or json.Number under UseNumber).
//
// It rejects rather than coerces. Returning 0 for a nil, string, bool, or
// out-of-range value would type-check and read like a decision, but it hands
// the caller a well-formed-looking integer it never received — the accidental
// fail-open this language makes easiest. Every rejection names the offending
// value so the caller can attribute it to a field.
func wireInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case float64:
		if n != math.Trunc(n) || n < math.MinInt64 || n >= math.MaxInt64 {
			return 0, fmt.Errorf("expected an integer, got %v", n)
		}
		return int64(n), nil
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, fmt.Errorf("expected an integer, got %q: %w", n.String(), err)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("expected an integer, got %T (%v)", v, v)
	}
}
