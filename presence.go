package lazily

// Presence + ephemeral plane (#lzpresence).
//
// The CRDT plane is durable; collaborative apps also need an ephemeral plane
// that does not persist (live cursors, typing indicators, presence). Each
// primitive is a pure compute core (a keyed map / single value + TTL over the
// logical clock) split from a reactive cell projecting the live view onto a
// reactive reader that invalidates only on a live-view change.
//
// Port of lazily-rs/src/presence.rs.

import "reflect"

// Plane marks which plane a value lives on. Ephemeral values MUST NOT be
// persisted; Durable values may be written to the durable outbox. In lazily-rs
// these are the `Ephemeral`/`Durable` marker traits (a durable sink statically
// rejects an ephemeral value — a compile-fail doctest). Go has no equivalent
// static rejection, so the markers are exposed as simple plane constants.
type Plane int

const (
	// Ephemeral tags values that MUST NOT be persisted.
	Ephemeral Plane = iota
	// Durable tags values that may be written to the durable outbox.
	Durable
)

// ===========================================================================
// Ephemeral single value
// ===========================================================================

// EphemeralCore is the single-value auto-expiry compute core — "the last value
// seen in window N".
type EphemeralCore[V comparable] struct {
	value   V
	present bool
	expiry  uint64
}

// NewEphemeralCore returns an empty core.
func NewEphemeralCore[V comparable]() *EphemeralCore[V] {
	return &EphemeralCore[V]{}
}

// Set the value, expiring at now + ttl.
func (c *EphemeralCore[V]) Set(value V, now, ttl uint64) {
	c.value = value
	c.present = true
	c.expiry = now + ttl
}

// Tick clears the value once now >= expiry.
func (c *EphemeralCore[V]) Tick(now uint64) {
	if c.present && now >= c.expiry {
		var zero V
		c.value = zero
		c.present = false
	}
}

// Value returns the live value (respecting the last tick).
func (c *EphemeralCore[V]) Value() (V, bool) {
	return c.value, c.present
}

// EphemeralCell is a reactive single-value ephemeral cell. Value() invalidates
// only when the live value changes (the Cell == guard).
type EphemeralCell[V comparable] struct {
	core  *EphemeralCore[V]
	value *Cell[Opt[V]]
}

// NewEphemeralCell builds an empty ephemeral cell in ctx.
func NewEphemeralCell[V comparable](ctx *Context) *EphemeralCell[V] {
	return &EphemeralCell[V]{
		core:  NewEphemeralCore[V](),
		value: NewCell(ctx, Opt[V]{}),
	}
}

func (c *EphemeralCell[V]) refresh() {
	v, present := c.core.Value()
	c.value.Set(Opt[V]{Present: present, Value: v})
}

// Set stamps the value with expiry = now + ttl.
func (c *EphemeralCell[V]) Set(value V, now, ttl uint64) {
	c.core.Set(value, now, ttl)
	c.refresh()
}

// Tick clears the value at now >= expiry.
func (c *EphemeralCell[V]) Tick(now uint64) {
	c.core.Tick(now)
	c.refresh()
}

// Value returns the live value and whether one is present.
func (c *EphemeralCell[V]) Value() (V, bool) {
	o := c.value.Get()
	return o.Value, o.Present
}

// ValueCell exposes the underlying reactive reader (Option scalar).
func (c *EphemeralCell[V]) ValueCell() *Cell[Opt[V]] {
	return c.value
}

// ===========================================================================
// Keyed per-peer ephemeral map (shared by presence + awareness)
// ===========================================================================

type ephemeralEntry[V any] struct {
	value  V
	expiry uint64
}

// EphemeralMapCore is a per-key ephemeral map with TTL eviction — the shared
// core behind presence and awareness. Each entry carries an expiry; Tick evicts
// lapsed entries.
type EphemeralMapCore[K comparable, V any] struct {
	entries map[K]ephemeralEntry[V]
}

// NewEphemeralMapCore returns an empty core.
func NewEphemeralMapCore[K comparable, V any]() *EphemeralMapCore[K, V] {
	return &EphemeralMapCore[K, V]{entries: map[K]ephemeralEntry[V]{}}
}

// Set/refresh key's value (last-writer wins), expiring at now + ttl.
func (c *EphemeralMapCore[K, V]) Set(key K, value V, now, ttl uint64) {
	c.entries[key] = ephemeralEntry[V]{value: value, expiry: now + ttl}
}

// Evict drops key immediately (membership Dead/Left).
func (c *EphemeralMapCore[K, V]) Evict(key K) {
	delete(c.entries, key)
}

// Tick evicts entries whose TTL has lapsed (now >= expiry).
func (c *EphemeralMapCore[K, V]) Tick(now uint64) {
	for k, e := range c.entries {
		if now >= e.expiry {
			delete(c.entries, k)
		}
	}
}

// Get returns the live value for key (respecting now).
func (c *EphemeralMapCore[K, V]) Get(key K, now uint64) (V, bool) {
	e, ok := c.entries[key]
	if !ok || now >= e.expiry {
		var zero V
		return zero, false
	}
	return e.value, true
}

// Present returns the live key -> value map at now.
func (c *EphemeralMapCore[K, V]) Present(now uint64) map[K]V {
	out := map[K]V{}
	for k, e := range c.entries {
		if now < e.expiry {
			out[k] = e.value
		}
	}
	return out
}

// presentReader is the shared version-cell projection behind PresenceCell and
// AwarenessCell: a collection reader (a map, not comparable) is projected via
// an internal version cell bumped only when the live map structurally changes.
type presentReader[K comparable, V comparable] struct {
	core    *EphemeralMapCore[K, V]
	version *Cell[uint64]
	last    map[K]V
}

func newPresentReader[K comparable, V comparable](ctx *Context, core *EphemeralMapCore[K, V]) *presentReader[K, V] {
	return &presentReader[K, V]{
		core:    core,
		version: NewCell[uint64](ctx, 0),
		last:    map[K]V{},
	}
}

// refresh recomputes the live map; if it structurally differs from the last
// projection, it bumps the version (invalidate only on a live-view change).
func (r *presentReader[K, V]) refresh(now uint64) {
	next := r.core.Present(now)
	if !reflect.DeepEqual(next, r.last) {
		r.last = next
		r.version.Set(r.version.Peek() + 1)
	}
}

// present subscribes to the version cell then returns a fresh snapshot.
func (r *presentReader[K, V]) present() map[K]V {
	_ = r.version.Get()
	out := make(map[K]V, len(r.last))
	for k, v := range r.last {
		out[k] = v
	}
	return out
}

// PresenceCell is reactive per-peer presence: heartbeat-kept, membership- and
// TTL-evicted. Present() is the live peer -> value map, invalidating only when
// the live view changes.
type PresenceCell[K comparable, V comparable] struct {
	core   *EphemeralMapCore[K, V]
	reader *presentReader[K, V]
	ttl    uint64
}

// NewPresenceCell builds a presence cell with a heartbeat TTL.
func NewPresenceCell[K comparable, V comparable](ctx *Context, ttl uint64) *PresenceCell[K, V] {
	core := NewEphemeralMapCore[K, V]()
	return &PresenceCell[K, V]{
		core:   core,
		reader: newPresentReader(ctx, core),
		ttl:    ttl,
	}
}

// Heartbeat a peer's presence (expiring at now + ttl).
func (c *PresenceCell[K, V]) Heartbeat(peer K, value V, now uint64) {
	c.core.Set(peer, value, now, c.ttl)
	c.reader.refresh(now)
}

// Evict a peer on membership loss.
func (c *PresenceCell[K, V]) Evict(peer K, now uint64) {
	c.core.Evict(peer)
	c.reader.refresh(now)
}

// Tick evicts peers whose TTL has lapsed.
func (c *PresenceCell[K, V]) Tick(now uint64) {
	c.core.Tick(now)
	c.reader.refresh(now)
}

// Present returns the live peer -> value snapshot.
func (c *PresenceCell[K, V]) Present() map[K]V {
	return c.reader.present()
}

// PresentCell exposes the internal version cell backing the present projection.
func (c *PresenceCell[K, V]) PresentCell() *Cell[uint64] {
	return c.reader.version
}

// AwarenessCell is a reactive typed ephemeral broadcast (cursors / selections):
// last-writer-per-peer with a TTL. Values do NOT merge.
type AwarenessCell[K comparable, V comparable] struct {
	core   *EphemeralMapCore[K, V]
	reader *presentReader[K, V]
	ttl    uint64
}

// NewAwarenessCell builds an awareness cell with a TTL.
func NewAwarenessCell[K comparable, V comparable](ctx *Context, ttl uint64) *AwarenessCell[K, V] {
	core := NewEphemeralMapCore[K, V]()
	return &AwarenessCell[K, V]{
		core:   core,
		reader: newPresentReader(ctx, core),
		ttl:    ttl,
	}
}

// Set a peer's awareness value (last-writer wins, no merge).
func (c *AwarenessCell[K, V]) Set(peer K, value V, now uint64) {
	c.core.Set(peer, value, now, c.ttl)
	c.reader.refresh(now)
}

// Tick evicts expired entries.
func (c *AwarenessCell[K, V]) Tick(now uint64) {
	c.core.Tick(now)
	c.reader.refresh(now)
}

// Get returns a peer's live value (respecting now).
func (c *AwarenessCell[K, V]) Get(peer K, now uint64) (V, bool) {
	return c.core.Get(peer, now)
}

// Present returns the live peer -> value snapshot.
func (c *AwarenessCell[K, V]) Present() map[K]V {
	return c.reader.present()
}

// PresentCell exposes the internal version cell backing the present projection.
func (c *AwarenessCell[K, V]) PresentCell() *Cell[uint64] {
	return c.reader.version
}
