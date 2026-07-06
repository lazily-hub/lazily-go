package lazily

// Move-aware sequence CRDT and LWW register.
//
// Each element is three independent LWW registers — value, position
// (fractional-index byte key + peer), deleted — each stamped by an HlcStamp
// (see hlc.go). A move is a single LWW reassignment of the position register
// (not delete + reinsert), so concurrent moves of the same element converge to
// the later stamp without duplication; a concurrent move + value-edit both
// apply because the registers are independent. Removal is an LWW tombstone.
// Order is the lexicographic total order on (frac, peer).
//
// Ported from lazily-dart lib/src/seq_crdt.dart, which mirrors
// lazily-js/src/seq-crdt.js and lazily-rs. Conforms to lazily-spec
// conformance/collections/seqcrdt_convergence.json.
//
// The HLC (Hlc/HlcStamp) is shared with the distributed CRDT plane and lives in
// hlc.go; it is NOT redefined here. Use MaxStamp(a, b) for the LWW winner and
// stamp.Compare for ordering.

import "sort"

// LwwRegister is a last-writer-wins register. Ties (equal stamps) are broken in
// favor of the incumbent (Set requires a strictly greater stamp).
type LwwRegister[V any] struct {
	Value V
	Stamp HlcStamp
}

// NewLwwRegister creates a register holding value stamped at stamp.
func NewLwwRegister[V any](value V, stamp HlcStamp) *LwwRegister[V] {
	return &LwwRegister[V]{Value: value, Stamp: stamp}
}

// Set assigns newValue if newStamp is strictly greater than the current stamp.
// Returns whether the value was updated.
func (r *LwwRegister[V]) Set(newValue V, newStamp HlcStamp) bool {
	if newStamp.Compare(r.Stamp) > 0 {
		r.Value = newValue
		r.Stamp = newStamp
		return true
	}
	return false
}

// MergeFrom merges another register into this one. Returns whether the value
// changed.
func (r *LwwRegister[V]) MergeFrom(other *LwwRegister[V]) bool {
	return r.Set(other.Value, other.Stamp)
}

// Copy returns a shallow copy of the register.
func (r *LwwRegister[V]) Copy() *LwwRegister[V] {
	return &LwwRegister[V]{Value: r.Value, Stamp: r.Stamp}
}

// Position is a fractional-index position: (frac bytes, peer) ordered
// lexicographically. Frac bytes are 0..255.
type Position struct {
	Frac []byte
	Peer PeerId
}

// Compare returns -1, 0, or +1 for the lexicographic order over (frac, peer):
// byte-wise on the shared prefix, then shorter frac first, then peer.
func (p Position) Compare(other Position) int {
	n := len(p.Frac)
	if len(other.Frac) < n {
		n = len(other.Frac)
	}
	for i := 0; i < n; i++ {
		if p.Frac[i] != other.Frac[i] {
			if p.Frac[i] < other.Frac[i] {
				return -1
			}
			return 1
		}
	}
	if len(p.Frac) != len(other.Frac) {
		if len(p.Frac) < len(other.Frac) {
			return -1
		}
		return 1
	}
	if p.Peer != other.Peer {
		if p.Peer < other.Peer {
			return -1
		}
		return 1
	}
	return 0
}

// keyBetween computes a fractional-index byte key strictly between lo
// (exclusive) and hi (exclusive). A nil lo means -∞; a nil hi means +∞.
//
// A non-nil empty lo behaves identically to nil lo (both yield a low bound of
// 0). An empty non-nil hi is a real bound of 0; +∞ is signalled only by nil hi.
func keyBetween(lo, hi []byte) []byte {
	loLen := len(lo)
	hiLen := len(hi)
	capLen := loLen + hiLen + 2
	result := []byte{}
	i := 0
	for i <= capLen {
		a := 0
		if i < loLen {
			a = int(lo[i])
		}
		b := 256
		if hi != nil {
			if i < hiLen {
				b = int(hi[i])
			} else {
				b = 0
			}
		}
		if a+1 < b {
			result = append(result, byte((a+b)/2))
			return result
		}
		result = append(result, byte(a))
		i++
		if a < b {
			// Dropped below hi; append the midpoint to +∞.
			var tail []byte
			if i < loLen {
				tail = keyBetween(lo[i:], nil)
			} else {
				tail = keyBetween(nil, nil)
			}
			result = append(result, tail...)
			return result
		}
		// a == b: shared prefix, continue.
	}
	result = append(result, 128)
	return result
}

// seqEntry is a single element: three independent LWW registers. The per-field
// *Stamp fields mirror the Dart source and are retained through copy for
// fidelity; ordering and merge use the register stamps directly.
type seqEntry[V any] struct {
	value    *LwwRegister[V]
	position *LwwRegister[Position]
	deleted  *LwwRegister[bool]

	valueStamp HlcStamp
	posStamp   HlcStamp
	delStamp   HlcStamp
}

func newSeqEntry[V any](value *LwwRegister[V], position *LwwRegister[Position], deleted *LwwRegister[bool], stamp HlcStamp) *seqEntry[V] {
	return &seqEntry[V]{
		value:      value,
		position:   position,
		deleted:    deleted,
		valueStamp: stamp,
		posStamp:   stamp,
		delStamp:   stamp,
	}
}

func (e *seqEntry[V]) maxStamp() HlcStamp {
	return MaxStamp(e.value.Stamp, MaxStamp(e.position.Stamp, e.deleted.Stamp))
}

func (e *seqEntry[V]) copy() *seqEntry[V] {
	c := newSeqEntry(e.value.Copy(), e.position.Copy(), e.deleted.Copy(), e.value.Stamp)
	c.valueStamp = e.valueStamp
	c.posStamp = e.posStamp
	c.delStamp = e.delStamp
	return c
}

func (e *seqEntry[V]) mergeFrom(other *seqEntry[V]) bool {
	changed := false
	if e.value.MergeFrom(other.value) {
		changed = true
	}
	if e.position.MergeFrom(other.position) {
		changed = true
	}
	if e.deleted.MergeFrom(other.deleted) {
		changed = true
	}
	return changed
}

// SeqValue is a live (id, value) pair in position order, returned by
// SeqCrdt.Values.
type SeqValue[Id comparable, V any] struct {
	Id    Id
	Value V
}

// SeqCrdt is a move-aware sequence CRDT. IDs are caller-supplied. Id must be
// comparable (it keys the entry map); V is unconstrained.
type SeqCrdt[Id comparable, V any] struct {
	peer    PeerId
	hlc     *Hlc
	entries map[Id]*seqEntry[V]
}

// NewSeqCrdt creates an empty sequence CRDT for the given peer.
func NewSeqCrdt[Id comparable, V any](peer PeerId) *SeqCrdt[Id, V] {
	return &SeqCrdt[Id, V]{
		peer:    peer,
		hlc:     NewHlc(peer),
		entries: map[Id]*seqEntry[V]{},
	}
}

// Peer returns this replica's peer id.
func (s *SeqCrdt[Id, V]) Peer() PeerId { return s.peer }

func (s *SeqCrdt[Id, V]) stamp(nowMicros int64) HlcStamp { return s.hlc.Tick(nowMicros) }

// InsertBetween inserts a new element between left and right (nil means -∞/+∞).
// No-op if id already exists.
func (s *SeqCrdt[Id, V]) InsertBetween(id Id, value V, left, right *Id, nowMicros int64) {
	if _, ok := s.entries[id]; ok {
		return
	}
	lo := s.fracOf(left)
	hi := s.fracOf(right)
	frac := keyBetween(lo, hi)
	pos := Position{Frac: frac, Peer: s.peer}
	stamp := s.stamp(nowMicros)
	s.entries[id] = newSeqEntry(
		NewLwwRegister(value, stamp),
		NewLwwRegister(pos, stamp),
		NewLwwRegister(false, stamp),
		stamp,
	)
}

// fracOf returns the frac bytes of the referenced entry, or nil if ref is nil or
// the entry is absent (mirrors Dart `_entries[x]?.position.value.frac`).
func (s *SeqCrdt[Id, V]) fracOf(ref *Id) []byte {
	if ref == nil {
		return nil
	}
	if e, ok := s.entries[*ref]; ok {
		return e.position.Value.Frac
	}
	return nil
}

// InsertBack inserts at the back (+∞).
func (s *SeqCrdt[Id, V]) InsertBack(id Id, value V, nowMicros int64) {
	s.InsertBetween(id, value, s.lastId(), nil, nowMicros)
}

// InsertFront inserts at the front (-∞).
func (s *SeqCrdt[Id, V]) InsertFront(id Id, value V, nowMicros int64) {
	s.InsertBetween(id, value, nil, s.firstId(), nowMicros)
}

func (s *SeqCrdt[Id, V]) firstId() *Id {
	var best *Id
	var bestPos Position
	for k, e := range s.entries {
		if e.deleted.Value {
			continue
		}
		p := e.position.Value
		if best == nil || p.Compare(bestPos) < 0 {
			bestPos = p
			kk := k
			best = &kk
		}
	}
	return best
}

func (s *SeqCrdt[Id, V]) lastId() *Id {
	var best *Id
	var bestPos Position
	for k, e := range s.entries {
		if e.deleted.Value {
			continue
		}
		p := e.position.Value
		if best == nil || p.Compare(bestPos) > 0 {
			bestPos = p
			kk := k
			best = &kk
		}
	}
	return best
}

// SetValue updates the value of id. Returns whether it changed.
func (s *SeqCrdt[Id, V]) SetValue(id Id, value V, nowMicros int64) bool {
	e, ok := s.entries[id]
	if !ok {
		return false
	}
	return e.value.Set(value, s.stamp(nowMicros))
}

// MoveBetween moves id between left and right (nil means -∞/+∞) via a single LWW
// reassignment of the position register. Returns whether the move applied.
func (s *SeqCrdt[Id, V]) MoveBetween(id Id, left, right *Id, nowMicros int64) bool {
	e, ok := s.entries[id]
	if !ok {
		return false
	}
	lo := s.fracOf(left)
	hi := s.fracOf(right)
	frac := keyBetween(lo, hi)
	pos := Position{Frac: frac, Peer: s.peer}
	return e.position.Set(pos, s.stamp(nowMicros))
}

// MoveAfter moves id to just after anchor.
func (s *SeqCrdt[Id, V]) MoveAfter(id, anchor Id, nowMicros int64) bool {
	order := s.orderedIds()
	ai := seqIndexOf(order, anchor)
	if ai < 0 {
		return false
	}
	var right *Id
	if ai+1 < len(order) {
		r := order[ai+1]
		right = &r
	}
	a := anchor
	return s.MoveBetween(id, &a, right, nowMicros)
}

// MoveBefore moves id to just before anchor.
func (s *SeqCrdt[Id, V]) MoveBefore(id, anchor Id, nowMicros int64) bool {
	order := s.orderedIds()
	ai := seqIndexOf(order, anchor)
	if ai < 0 {
		return false
	}
	var left *Id
	if ai > 0 {
		l := order[ai-1]
		left = &l
	}
	a := anchor
	return s.MoveBetween(id, left, &a, nowMicros)
}

// Remove tombstones id. Returns whether the removal applied.
func (s *SeqCrdt[Id, V]) Remove(id Id, nowMicros int64) bool {
	e, ok := s.entries[id]
	if !ok {
		return false
	}
	return e.deleted.Set(true, s.stamp(nowMicros))
}

// Contains reports whether id exists and is not tombstoned.
func (s *SeqCrdt[Id, V]) Contains(id Id) bool {
	e, ok := s.entries[id]
	return ok && !e.deleted.Value
}

// Get returns the value for id and true, or the zero value and false if the
// element is absent or tombstoned.
func (s *SeqCrdt[Id, V]) Get(id Id) (V, bool) {
	e, ok := s.entries[id]
	if !ok || e.deleted.Value {
		var zero V
		return zero, false
	}
	return e.value.Value, true
}

func (s *SeqCrdt[Id, V]) orderedIds() []Id {
	type live struct {
		id  Id
		pos Position
	}
	items := make([]live, 0, len(s.entries))
	for k, e := range s.entries {
		if !e.deleted.Value {
			items = append(items, live{id: k, pos: e.position.Value})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].pos.Compare(items[j].pos) < 0
	})
	ids := make([]Id, len(items))
	for i, it := range items {
		ids[i] = it.id
	}
	return ids
}

// Order returns the live element ids in position order.
func (s *SeqCrdt[Id, V]) Order() []Id { return s.orderedIds() }

// Values returns the live (id, value) pairs in position order.
func (s *SeqCrdt[Id, V]) Values() []SeqValue[Id, V] {
	order := s.orderedIds()
	out := make([]SeqValue[Id, V], 0, len(order))
	for _, id := range order {
		v, _ := s.Get(id)
		out = append(out, SeqValue[Id, V]{Id: id, Value: v})
	}
	return out
}

// TombstoneCount returns the count of tombstoned elements.
func (s *SeqCrdt[Id, V]) TombstoneCount() int {
	n := 0
	for _, e := range s.entries {
		if e.deleted.Value {
			n++
		}
	}
	return n
}

// EntryCount returns the total entry count including tombstones.
func (s *SeqCrdt[Id, V]) EntryCount() int { return len(s.entries) }

// Len returns the count of live elements.
func (s *SeqCrdt[Id, V]) Len() int { return len(s.orderedIds()) }

// Fork deep-copies the CRDT with a new peer id, preserving the clock state.
func (s *SeqCrdt[Id, V]) Fork(peer PeerId) *SeqCrdt[Id, V] {
	c := NewSeqCrdt[Id, V](peer)
	c.hlc.lastWall = s.hlc.lastWall
	c.hlc.lastLogical = s.hlc.lastLogical
	for k, e := range s.entries {
		c.entries[k] = e.copy()
	}
	return c
}

// Clone deep-copies the CRDT with the same peer id.
func (s *SeqCrdt[Id, V]) Clone() *SeqCrdt[Id, V] { return s.Fork(s.peer) }

// Merge folds another replica's state into this one. Returns whether anything
// changed. The clock is advanced past every remote stamp first.
func (s *SeqCrdt[Id, V]) Merge(other *SeqCrdt[Id, V], nowMicros int64) bool {
	for _, e := range other.entries {
		s.hlc.Observe(e.maxStamp(), nowMicros)
	}
	changed := false
	for k, oe := range other.entries {
		if existing, ok := s.entries[k]; ok {
			if existing.mergeFrom(oe) {
				changed = true
			}
		} else {
			s.entries[k] = oe.copy()
			changed = true
		}
	}
	return changed
}

// GcWith garbage-collects entries whose tombstone is stable per isStable.
// Returns the number of removed entries.
func (s *SeqCrdt[Id, V]) GcWith(isStable func(stamp HlcStamp) bool) int {
	var toRemove []Id
	for k, e := range s.entries {
		if e.deleted.Value && isStable(e.deleted.Stamp) {
			toRemove = append(toRemove, k)
		}
	}
	for _, id := range toRemove {
		delete(s.entries, id)
	}
	return len(toRemove)
}

// Gc garbage-collects entries tombstoned at or before watermark.
func (s *SeqCrdt[Id, V]) Gc(watermark HlcStamp) int {
	return s.GcWith(func(st HlcStamp) bool { return st.Compare(watermark) <= 0 })
}

// seqIndexOf returns the index of target in s, or -1 if absent.
func seqIndexOf[Id comparable](s []Id, target Id) int {
	for i, v := range s {
		if v == target {
			return i
		}
	}
	return -1
}
