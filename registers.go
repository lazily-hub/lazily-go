package lazily

// CRDT register primitives: multi-value register, PN counter, and the
// reactive-cell-backed CRDT bridge.
//
// The LWW register lives in seq_crdt.go (LwwRegister). This module adds the
// multi-value register (MvRegister), the positive-negative counter (PnCounter),
// and the reactive-cell CRDT bridge (CellCrdt).
//
// Ported from lazily-dart lib/src/registers.dart, which mirrors
// lazily-rs src/registers.rs. Conforms to lazily-spec protocol.md § Distributed
// and cell-model.md. Merge is commutative, associative, and idempotent.

import "strconv"

// MvRegister is a multi-value register. Concurrent writes surface as a set of
// values; a write that observes all prior values collapses back to a singleton.
//
// stamps and values are kept index-parallel: entry i is the value written under
// stamp i. HLC stamps are unique per event, so no de-duplication of stamps is
// required on write.
type MvRegister[V any] struct {
	stamps []HlcStamp
	values []V
}

// NewMvRegister creates an empty multi-value register.
func NewMvRegister[V any]() *MvRegister[V] {
	return &MvRegister[V]{}
}

// Values returns the current visible values (concurrent writes = multiple,
// causal write = one). The returned slice is a copy.
func (r *MvRegister[V]) Values() []V {
	out := make([]V, len(r.values))
	copy(out, r.values)
	return out
}

// Write adds value under stamp. If observedStamps covers every current stamp,
// the register collapses to the singleton being written. A nil observedStamps
// means the write observed nothing prior.
func (r *MvRegister[V]) Write(value V, stamp HlcStamp, observedStamps map[HlcStamp]struct{}) {
	if observedStamps != nil && containsAllStamps(observedStamps, r.stamps) {
		r.stamps = nil
		r.values = nil
	} else if len(r.stamps) > 0 {
		// Collect the stamps this write did NOT observe (concurrent). If it
		// observed everything, collapse.
		concurrent := 0
		for _, s := range r.stamps {
			if observedStamps == nil {
				concurrent++
				continue
			}
			if _, ok := observedStamps[s]; !ok {
				concurrent++
			}
		}
		if concurrent == 0 {
			r.stamps = nil
			r.values = nil
		}
	}
	r.stamps = append(r.stamps, stamp)
	r.values = append(r.values, value)
}

// Merge folds another MV register into this one (state-based, idempotent).
func (r *MvRegister[V]) Merge(other *MvRegister[V]) {
	for i, stamp := range other.stamps {
		if !r.containsStamp(stamp) {
			r.stamps = append(r.stamps, stamp)
			r.values = append(r.values, other.values[i])
		}
	}
}

// ObservedStamps returns a copy of the stamps observed by this register.
func (r *MvRegister[V]) ObservedStamps() map[HlcStamp]struct{} {
	out := make(map[HlcStamp]struct{}, len(r.stamps))
	for _, s := range r.stamps {
		out[s] = struct{}{}
	}
	return out
}

// Copy returns a deep copy of the register.
func (r *MvRegister[V]) Copy() *MvRegister[V] {
	c := &MvRegister[V]{
		stamps: make([]HlcStamp, len(r.stamps)),
		values: make([]V, len(r.values)),
	}
	copy(c.stamps, r.stamps)
	copy(c.values, r.values)
	return c
}

func (r *MvRegister[V]) containsStamp(stamp HlcStamp) bool {
	for _, s := range r.stamps {
		if s == stamp {
			return true
		}
	}
	return false
}

// containsAllStamps reports whether observed contains every stamp in want.
func containsAllStamps(observed map[HlcStamp]struct{}, want []HlcStamp) bool {
	for _, s := range want {
		if _, ok := observed[s]; !ok {
			return false
		}
	}
	return true
}

// PnCounter is a positive-negative counter (state-based CvRDT). Each peer owns
// its own positive and negative components; the value is the sum of all
// positives minus the sum of all negatives. Merge is component-wise max.
type PnCounter struct {
	peer     PeerId
	positive map[PeerId]int64
	negative map[PeerId]int64
}

// NewPnCounter creates a counter owned by peer.
func NewPnCounter(peer PeerId) *PnCounter {
	return &PnCounter{
		peer:     peer,
		positive: map[PeerId]int64{},
		negative: map[PeerId]int64{},
	}
}

// Peer returns the peer that owns this counter's local components.
func (c *PnCounter) Peer() PeerId { return c.peer }

// Increment adds 1 to this peer's positive component.
func (c *PnCounter) Increment() { c.IncrementBy(1) }

// IncrementBy adds amount to this peer's positive component.
func (c *PnCounter) IncrementBy(amount int64) {
	c.positive[c.peer] += amount
}

// Decrement adds 1 to this peer's negative component.
func (c *PnCounter) Decrement() { c.DecrementBy(1) }

// DecrementBy adds amount to this peer's negative component.
func (c *PnCounter) DecrementBy(amount int64) {
	c.negative[c.peer] += amount
}

// Value returns the current counter value: sum(positive) - sum(negative).
func (c *PnCounter) Value() int64 {
	var sum int64
	for _, v := range c.positive {
		sum += v
	}
	for _, v := range c.negative {
		sum -= v
	}
	return sum
}

// Merge folds another PN counter in via component-wise max (idempotent).
func (c *PnCounter) Merge(other *PnCounter) {
	for peer, v := range other.positive {
		if v > c.positive[peer] {
			c.positive[peer] = v
		}
	}
	for peer, v := range other.negative {
		if v > c.negative[peer] {
			c.negative[peer] = v
		}
	}
}

// ToWire renders the counter to its spec wire form: positive/negative maps keyed
// by stringified peer id.
func (c *PnCounter) ToWire() map[string]any {
	positive := make(map[string]int64, len(c.positive))
	for peer, v := range c.positive {
		positive[strconv.FormatInt(peer, 10)] = v
	}
	negative := make(map[string]int64, len(c.negative))
	for peer, v := range c.negative {
		negative[strconv.FormatInt(peer, 10)] = v
	}
	return map[string]any{
		"positive": positive,
		"negative": negative,
	}
}

// Copy returns a deep copy of the counter.
func (c *PnCounter) Copy() *PnCounter {
	cp := NewPnCounter(c.peer)
	for peer, v := range c.positive {
		cp.positive[peer] = v
	}
	for peer, v := range c.negative {
		cp.negative[peer] = v
	}
	return cp
}

// CellCrdt is a reactive cell whose value is resolved by merging concurrent
// writes. Backed by a Cell[T] and a pluggable merge function (LWW, MV, or
// custom). Reads are reactive; writes fold the incoming value into the current
// one via the merge function.
//
// T must be comparable because the backing Cell uses the == PartialEq guard.
type CellCrdt[T comparable] struct {
	ctx   *Context
	cell  *Cell[T]
	merge func(current, incoming T) T
}

// NewCellCrdt creates a CRDT-backed cell with initial value and merge function.
func NewCellCrdt[T comparable](ctx *Context, initial T, merge func(current, incoming T) T) *CellCrdt[T] {
	return &CellCrdt[T]{
		ctx:   ctx,
		cell:  NewCell(ctx, initial),
		merge: merge,
	}
}

// Value returns the current merged value (reactive read).
func (c *CellCrdt[T]) Value() T { return c.cell.Get() }

// Write merges incoming into the current value.
func (c *CellCrdt[T]) Write(incoming T) {
	c.cell.Set(c.merge(c.cell.Peek(), incoming))
}

// Cell returns the underlying reactive cell.
func (c *CellCrdt[T]) Cell() *Cell[T] { return c.cell }
