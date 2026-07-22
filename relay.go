// RelayCell backpressure plan (#relaycell), Phases 2–6 — the Go port.
//
// See lazily-spec/docs/relaycell.md and relaycell-backpressure-analysis.md. A
// RelayCell is an *algebra-typed conflating relay*: it accumulates a fast ingress
// into a hot head (a MergePolicy fold), bounds it with a reactive
// BackpressurePolicy, and lets a slow egress drain the coalesced window. The
// converged egress state is independent of the drain schedule whenever the merge
// ⊕ is associative (the relay_converges invariant, pinned in LazilyFormal.Relay).
//
// Phase 2 RelayCell + BackpressurePolicy · Phase 3 SpillStore · Phase 4 Transport
// · Phase 5 Outbox/Inbox roles · Phase 6 Rate/Window/Expiry/Priority/Keyed
// policies. Time is a logical clock (a monotone tick) so behaviour is
// deterministic and portable.
//
// Go note: the hot head is a reactive Cell (core.go's Cell[T comparable]), so a
// RelayCell — like MergeCell — constrains T to comparable. The KeepLatest / Sum /
// Max policies used by the relay tests are all comparable-valued. RawFifo's
// slice-valued algebra cannot back a comparable cell (Go slices are not
// comparable), so the "Conflate rejected for a non-conflating policy" guard is
// exercised with a comparable non-conflating policy in the tests.

package lazily

// -- Phase 2: RelayCell + BackpressurePolicy ---------------------------------

// BoundDim is what a bound measures (analysis §4.4). The core meters Count.
type BoundDim string

const (
	BoundCount BoundDim = "Count"
	BoundBytes BoundDim = "Bytes"
	BoundKeys  BoundDim = "Keys"
	BoundAge   BoundDim = "Age"
)

// Overflow is the action taken when the hot head crosses high_water (§4.4).
type Overflow string

const (
	// OverflowBlock refuses ingress; the producer backpressures (observes
	// IsFull). Lossless.
	OverflowBlock Overflow = "Block"
	// OverflowDropNewest discards the incoming op. Lossy.
	OverflowDropNewest Overflow = "DropNewest"
	// OverflowDropOldest resets the window to the incoming op, discarding what
	// accumulated. Lossy.
	OverflowDropOldest Overflow = "DropOldest"
	// OverflowConflate keeps merging — the coalescence *is* the bound. Requires
	// the policy's Conflates flag.
	OverflowConflate Overflow = "Conflate"
	// OverflowSpill pages the accumulated window to a durable tail (Phase 3).
	OverflowSpill Overflow = "Spill"
)

// RelayConfigError is why a construction/merge-swap was rejected (§4.3).
type RelayConfigError struct{ msg string }

func (e *RelayConfigError) Error() string { return e.msg }

// ErrConflateNotBounding is returned when Conflate is chosen for a non-conflating
// policy (e.g. RawFifo).
var ErrConflateNotBounding = &RelayConfigError{msg: "ConflateNotBounding"}

// IngressOutcome is the outcome of a single ingress op.
type IngressOutcome string

const (
	// IngressAccepted — merged into an empty window (window depth was 0).
	IngressAccepted IngressOutcome = "Accepted"
	// IngressConflated — merged into a non-empty window (coalesced with prior).
	IngressConflated IngressOutcome = "Conflated"
	// IngressDropped — dropped by DropNewest/DropOldest overflow.
	IngressDropped IngressOutcome = "Dropped"
	// IngressBlocked — refused by Block overflow; retry after a drain.
	IngressBlocked IngressOutcome = "Blocked"
)

// BackpressurePolicy holds reactive backpressure limits (analysis §4.4). Every
// field is a cell, so an operator or adaptive controller retunes it live and
// dependent relays react. Hysteresis (HighWater ≠ LowWater) prevents flapping.
type BackpressurePolicy struct {
	Dimension *Source[BoundDim]
	HighWater *Source[uint64]
	LowWater  *Source[uint64]
	Overflow  *Source[Overflow]
}

// NewBackpressurePolicy builds a reactive backpressure policy over ctx.
func NewBackpressurePolicy(ctx *Context, dimension BoundDim, highWater, lowWater uint64, overflow Overflow) BackpressurePolicy {
	return BackpressurePolicy{
		Dimension: NewSource(ctx, dimension),
		HighWater: NewSource(ctx, highWater),
		LowWater:  NewSource(ctx, lowWater),
		Overflow:  NewSource(ctx, overflow),
	}
}

// headOpt models Option<T> as a comparable struct so the hot head can live in a
// reactive Cell. present=false is the empty window.
type headOpt[T comparable] struct {
	val     T
	present bool
}

// RelayCell is the algebra-typed conflating relay (Phase 2, in-proc core). The
// hot head is a cell; Depth/IsFull/IsEmpty are demand-driven slots, so an
// unobserved relay costs N·⊕ and no more (the merge cost law).
type RelayCell[T comparable] struct {
	ctx    *Context
	policy BackpressurePolicy
	merge  MergePolicy[T]

	// Hot head: current window's coalesced value (present=false = empty window).
	head *Source[headOpt[T]]
	// Ops merged into the current window since the last drain (the Count bound).
	pending *Source[uint64]

	depth   *Computed[uint64]
	isFull  *Computed[bool]
	isEmpty *Computed[bool]
}

// NewRelayCell builds a relay over policy, validating the initial overflow
// against the merge policy's algebra flags (§4.3): Conflate requires
// merge.Conflates. Returns ErrConflateNotBounding otherwise.
func NewRelayCell[T comparable](ctx *Context, policy BackpressurePolicy, merge MergePolicy[T]) (*RelayCell[T], error) {
	if policy.Overflow.Peek() == OverflowConflate && !merge.Conflates {
		return nil, ErrConflateNotBounding
	}
	r := &RelayCell[T]{
		ctx:     ctx,
		policy:  policy,
		merge:   merge,
		head:    NewSource(ctx, headOpt[T]{}),
		pending: NewSource(ctx, uint64(0)),
	}
	r.depth = NewSlot(ctx, func(c *Compute) uint64 { return Get(c, r.pending) })
	r.isFull = NewSlot(ctx, func(c *Compute) bool {
		return Get(c, r.depth) >= Get(c, policy.HighWater)
	})
	r.isEmpty = NewSlot(ctx, func(c *Compute) bool { return !Get(c, r.head).present })
	return r, nil
}

// OverflowIsLegal reports whether the current overflow choice is legal for the
// merge policy — a runtime guard mirroring NewRelayCell's construction check
// (the overflow cell is reactive).
func (r *RelayCell[T]) OverflowIsLegal() bool {
	return r.policy.Overflow.Peek() != OverflowConflate || r.merge.Conflates
}

// Depth is the demand-driven reader: current window depth (Count).
func (r *RelayCell[T]) Depth() uint64 { return r.depth.Get() }

// IsFull is the demand-driven reader: window is at/over HighWater.
func (r *RelayCell[T]) IsFull() bool { return r.isFull.Get() }

// IsEmpty is the demand-driven reader: window is empty (nothing to drain).
func (r *RelayCell[T]) IsEmpty() bool { return r.isEmpty.Get() }

// DepthSlot / IsFullSlot / IsEmptySlot expose the reader slots for wiring into
// effects and computations.
func (r *RelayCell[T]) DepthSlot() *Computed[uint64] { return r.depth }
func (r *RelayCell[T]) IsFullSlot() *Computed[bool]  { return r.isFull }
func (r *RelayCell[T]) IsEmptySlot() *Computed[bool] { return r.isEmpty }

func (r *RelayCell[T]) readFull() bool {
	return r.pending.Peek() >= r.policy.HighWater.Peek()
}

func (r *RelayCell[T]) mergeIntoHead(op T) {
	cur := r.head.Peek()
	var next T
	if !cur.present {
		next = op
	} else {
		next = r.merge.Merge(cur.val, op)
	}
	r.head.Set(headOpt[T]{val: next, present: true})
}

// Ingress ingests one op. Applies the reactive overflow policy when the window is
// at HighWater; otherwise merges the op into the hot head under the merge policy.
func (r *RelayCell[T]) Ingress(op T) IngressOutcome {
	wasEmpty := r.pending.Peek() == 0

	if r.readFull() {
		switch r.policy.Overflow.Peek() {
		case OverflowBlock:
			return IngressBlocked
		case OverflowDropNewest:
			return IngressDropped
		case OverflowDropOldest:
			// Discard the accumulated window, restart from this op.
			r.head.Set(headOpt[T]{val: op, present: true})
			r.pending.Set(1)
			return IngressDropped
		case OverflowConflate, OverflowSpill:
			// Conflate keeps merging (the coalescence is the bound); Spill is
			// Phase 3 and, until wired, degrades to Conflate for a bounding
			// policy. Both fall through to the merge below.
		}
	}

	r.mergeIntoHead(op)
	r.pending.Set(r.pending.Peek() + 1)
	if wasEmpty {
		return IngressAccepted
	}
	return IngressConflated
}

// Drain takes the hot head's value and resets the window. The second return is
// false for an empty window. relay_converges guarantees the egress fold equals
// the flat fold of every ingested op, for any drain schedule.
func (r *RelayCell[T]) Drain() (T, bool) {
	cur := r.head.Peek()
	if cur.present {
		r.head.Set(headOpt[T]{})
		r.pending.Set(0)
	}
	return cur.val, cur.present
}

// Peek returns the current coalesced window without draining.
func (r *RelayCell[T]) Peek() (T, bool) {
	cur := r.head.Peek()
	return cur.val, cur.present
}

// -- Phase 3: SpillStore -----------------------------------------------------

// SpillMode is how spilled windows are laid out on the durable tail (§6).
type SpillMode string

const (
	// SpillCompactOnWrite merges each spilled window into the open page until it
	// fills — minimizes disk (keep-latest / semilattice). One page holds a
	// coalesced run.
	SpillCompactOnWrite SpillMode = "CompactOnWrite"
	// SpillAppendCompact appends each spilled window as its own page — preserves
	// increments for an accumulating (non-idempotent) policy that must not
	// double-count.
	SpillAppendCompact SpillMode = "AppendCompact"
)

// SpillPage is one immutable cold page: a coalesced window summary plus its
// manifest entry.
type SpillPage[T any] struct {
	ID      uint64
	Summary T
	Bytes   uint64
}

// ManifestEntry is a bounded per-page metadata record (page id, bytes).
type ManifestEntry struct {
	ID    uint64
	Bytes uint64
}

// SpillStore is a paged durable tail for a RelayCell (Phase 3, in-memory
// reference backend). Holds immutable cold pages, a bounded manifest, an egress
// cursor, and ack-before-reclaim. Memory is O(hot) + O(manifest).
type SpillStore[T any] struct {
	merge    MergePolicy[T]
	mode     SpillMode
	pageSize uint64
	pages    []SpillPage[T]
	openFill uint64
	nextID   uint64
	acked    int // pages acked from the front (reclaimable)
}

// NewSpillStore creates a spill store in the given mode with the given page size.
func NewSpillStore[T any](mode SpillMode, pageSize uint64, merge MergePolicy[T]) *SpillStore[T] {
	if pageSize < 1 {
		pageSize = 1
	}
	return &SpillStore[T]{merge: merge, mode: mode, pageSize: pageSize}
}

// Spill writes one coalesced window summary to the durable tail. AppendCompact
// always opens a new page; CompactOnWrite merges into the open page until it
// reaches pageSize, then seals it.
func (s *SpillStore[T]) Spill(window T, bytes uint64) {
	switch s.mode {
	case SpillAppendCompact:
		s.pushPage(window, bytes)
	default: // CompactOnWrite
		if s.openFill >= s.pageSize || len(s.pages) == 0 {
			s.pushPage(window, bytes)
			s.openFill = 1
		} else {
			last := &s.pages[len(s.pages)-1]
			last.Summary = s.merge.Merge(last.Summary, window)
			last.Bytes += bytes
			s.openFill++
		}
	}
}

func (s *SpillStore[T]) pushPage(summary T, bytes uint64) {
	s.pages = append(s.pages, SpillPage[T]{ID: s.nextID, Summary: summary, Bytes: bytes})
	s.nextID++
}

// Manifest returns (id, bytes) for every live page (bounded metadata).
func (s *SpillStore[T]) Manifest() []ManifestEntry {
	out := make([]ManifestEntry, len(s.pages))
	for i, p := range s.pages {
		out[i] = ManifestEntry{ID: p.ID, Bytes: p.Bytes}
	}
	return out
}

// PendingPages returns the pages the egress has not yet acked (at/after cursor).
func (s *SpillStore[T]) PendingPages() []SpillPage[T] {
	return s.pages[s.acked:]
}

// PageCount is the number of live pages.
func (s *SpillStore[T]) PageCount() int { return len(s.pages) }

// AckThrough acks every page through id (inclusive), advancing the reclaim cursor.
func (s *SpillStore[T]) AckThrough(id uint64) {
	for s.acked < len(s.pages) && s.pages[s.acked].ID <= id {
		s.acked++
	}
}

// Reclaim drops acked pages (durable reclaim). Manifest/cursor stay consistent.
func (s *SpillStore[T]) Reclaim() {
	if s.acked > 0 {
		s.pages = append(s.pages[:0], s.pages[s.acked:]...)
		s.acked = 0
	}
}

// FoldPages folds every live cold page (oldest first) into s0 — the durable
// tail's contribution to the converged state.
func (s *SpillStore[T]) FoldPages(s0 T) T {
	acc := s0
	for _, p := range s.pages {
		acc = s.merge.Merge(acc, p.Summary)
	}
	return acc
}

// Reconstruct (spill_lossless) folds the cold tail then the hot head, reproducing
// the flat fold of every op the relay ever ingested. hasHot=false means an empty
// hot window.
func (s *SpillStore[T]) Reconstruct(s0 T, hot T, hasHot bool) T {
	cold := s.FoldPages(s0)
	if hasHot {
		return s.merge.Merge(cold, hot)
	}
	return cold
}

// ReplayUnacked (crash replay) re-delivers every unacked page from the ack cursor
// into downstream. For an idempotent policy re-applying an already-delivered page
// is a no-op (spill_replay_idempotent), so at-least-once replay converges.
func (s *SpillStore[T]) ReplayUnacked(downstream T) T {
	acc := downstream
	for _, p := range s.PendingPages() {
		acc = s.merge.Merge(acc, p.Summary)
	}
	return acc
}

// -- Phase 4: Transport ------------------------------------------------------
//
// Transport abstracts ingress/egress delivery so the mechanism is pluggable. A
// RelayCell is written once against Transport; the merge algebra — not the
// transport — guarantees converged state (transport_independent), so transports
// may differ across bindings and still converge.

// Transport is a pluggable delivery mechanism for relay ops. Deliver enqueues;
// Poll pulls the next transport-defined frame (a batch of ready ops). Framing is
// the transport's business; the relay merges whatever each frame delivers.
type Transport[T any] interface {
	Deliver(op T)
	Poll() []T
	HasPending() bool
}

// InProcTransport is direct delivery: every buffered op is handed over in one
// frame.
type InProcTransport[T any] struct {
	buf []T
}

// NewInProcTransport creates a direct in-process transport.
func NewInProcTransport[T any]() *InProcTransport[T] { return &InProcTransport[T]{} }

func (t *InProcTransport[T]) Deliver(op T) { t.buf = append(t.buf, op) }
func (t *InProcTransport[T]) Poll() []T {
	out := t.buf
	t.buf = nil
	return out
}
func (t *InProcTransport[T]) HasPending() bool { return len(t.buf) > 0 }

// FramedTransport models CrossThread/Ipc/Ws: ops are delivered in bounded frames
// of at most frameSize (an MTU / batch boundary). Different frameSizes are
// different framings of the same op stream.
type FramedTransport[T any] struct {
	buf       []T
	frameSize int
}

// NewFramedTransport creates a framed transport with the given frame size.
func NewFramedTransport[T any](frameSize int) *FramedTransport[T] {
	if frameSize < 1 {
		frameSize = 1
	}
	return &FramedTransport[T]{frameSize: frameSize}
}

func (t *FramedTransport[T]) Deliver(op T) { t.buf = append(t.buf, op) }
func (t *FramedTransport[T]) Poll() []T {
	n := t.frameSize
	if n > len(t.buf) {
		n = len(t.buf)
	}
	out := make([]T, n)
	copy(out, t.buf[:n])
	t.buf = t.buf[n:]
	return out
}
func (t *FramedTransport[T]) HasPending() bool { return len(t.buf) > 0 }

// -- Phase 5: Outbox / Inbox roles -------------------------------------------
//
// RelayCell is direction-neutral; Outbox and Inbox are role facades (typed
// constructors with direction-appropriate defaults), not reimplementations. They
// differ in the backpressure-propagation contract. A network link is
// Outbox → Transport → Inbox.

// Outbox is the app → transport send side (§4.7). Backpressures the local
// producer directly via IsFull. Default overflow Conflate (state broadcast).
type Outbox[T comparable] struct {
	relay *RelayCell[T]
}

// NewOutbox builds an outbox bounded by highWater with the role default overflow
// (Conflate — the state-broadcast case). Validates the policy flags.
func NewOutbox[T comparable](ctx *Context, highWater uint64, merge MergePolicy[T]) (*Outbox[T], error) {
	return NewOutboxWithOverflow(ctx, BoundCount, highWater, OverflowConflate, merge)
}

// NewOutboxWithOverflow builds an outbox with an explicit dimension/overflow
// (e.g. Spill for a lossless event channel).
func NewOutboxWithOverflow[T comparable](ctx *Context, dimension BoundDim, highWater uint64, overflow Overflow, merge MergePolicy[T]) (*Outbox[T], error) {
	policy := NewBackpressurePolicy(ctx, dimension, highWater, highWater/2, overflow)
	relay, err := NewRelayCell(ctx, policy, merge)
	if err != nil {
		return nil, err
	}
	return &Outbox[T]{relay: relay}, nil
}

// Send has the local producer send an op. A Blocked outcome is the producer's
// backpressure signal — it should await a drain before retrying.
func (o *Outbox[T]) Send(op T) IngressOutcome { return o.relay.Ingress(op) }

// Drain has the transport drain the coalesced window for egress.
func (o *Outbox[T]) Drain() (T, bool) { return o.relay.Drain() }

// IsFull is the producer-facing backpressure signal (window at/over watermark).
func (o *Outbox[T]) IsFull() bool { return o.relay.IsFull() }

// IsFullSlot exposes the backpressure reader slot.
func (o *Outbox[T]) IsFullSlot() *Computed[bool] { return o.relay.IsFullSlot() }

// Relay accesses the underlying relay (for wiring extra egress stages).
func (o *Outbox[T]) Relay() *RelayCell[T] { return o.relay }

// Inbox is the transport → app receive side (§4.7). Cannot block the remote
// directly; backpressure is a credit meter the app replenishes.
type Inbox[T comparable] struct {
	relay      *RelayCell[T]
	credits    uint64
	maxCredits uint64
}

// NewInbox builds an inbox bounded by highWater with the role default overflow
// (Conflate for inbound state) and a credit budget of maxCredits.
func NewInbox[T comparable](ctx *Context, highWater, maxCredits uint64, merge MergePolicy[T]) (*Inbox[T], error) {
	return NewInboxWithOverflow(ctx, highWater, OverflowConflate, maxCredits, merge)
}

// NewInboxWithOverflow builds an inbox with an explicit overflow policy.
func NewInboxWithOverflow[T comparable](ctx *Context, highWater uint64, overflow Overflow, maxCredits uint64, merge MergePolicy[T]) (*Inbox[T], error) {
	policy := NewBackpressurePolicy(ctx, BoundCount, highWater, highWater/2, overflow)
	relay, err := NewRelayCell(ctx, policy, merge)
	if err != nil {
		return nil, err
	}
	return &Inbox[T]{relay: relay, credits: maxCredits, maxCredits: maxCredits}, nil
}

// Ready reports whether the transport may deliver another message (a credit is
// available). When false, the transport must stop reading → the remote throttles.
func (i *Inbox[T]) Ready() bool { return i.credits > 0 }

// Credits are the credits currently available to the remote.
func (i *Inbox[T]) Credits() uint64 { return i.credits }

// Receive has the transport deliver a received op. Consumes a credit; the caller
// MUST have checked Ready (a delivery without credit still applies but drives
// credits to zero, signalling the remote to stop).
func (i *Inbox[T]) Receive(op T) IngressOutcome {
	if i.credits > 0 {
		i.credits--
	}
	return i.relay.Ingress(op)
}

// Consume has the app consume the coalesced window and replenish n credits (up to
// the budget), re-opening the remote's flow.
func (i *Inbox[T]) Consume(replenish uint64) (T, bool) {
	out, ok := i.relay.Drain()
	i.credits += replenish
	if i.credits > i.maxCredits {
		i.credits = i.maxCredits
	}
	return out, ok
}

// -- Phase 6: extra reactive policies ----------------------------------------
//
// Each policy is an optional reactive stage composed onto a relay egress; they
// only change where/when a relay flushes or which ops survive. Time is a logical
// clock (a monotone tick) — a binding drives Tick/Advance from its own runtime
// timer.

// RatePolicy — Case 9, rate-limited egress (token bucket). A drain is permitted
// only when a token is available. Refilled refillPerTick tokens per logical tick,
// capped at capacity.
type RatePolicy struct {
	capacity      uint64
	tokens        uint64
	refillPerTick uint64
}

// NewRatePolicy creates a token bucket that starts full.
func NewRatePolicy(capacity, refillPerTick uint64) *RatePolicy {
	return &RatePolicy{capacity: capacity, tokens: capacity, refillPerTick: refillPerTick}
}

// Tokens are the tokens currently available.
func (r *RatePolicy) Tokens() uint64 { return r.tokens }

// TryEgress consumes one token for an egress; returns true if paced through.
func (r *RatePolicy) TryEgress() bool {
	if r.tokens > 0 {
		r.tokens--
		return true
	}
	return false
}

// Tick advances the logical clock, refilling the bucket (saturating at capacity).
func (r *RatePolicy) Tick() {
	r.tokens += r.refillPerTick
	if r.tokens > r.capacity {
		r.tokens = r.capacity
	}
}

// WindowPolicy — Case 8, time-windowed coalescence (debounce/throttle). Flushes
// when it reaches windowOps ops or on an explicit Tick. Because a window is just a
// flush group, associativity keeps the converged state unchanged.
type WindowPolicy struct {
	windowOps uint64
	pending   uint64
}

// NewWindowPolicy creates a window that flushes every windowOps ops (min 1).
func NewWindowPolicy(windowOps uint64) *WindowPolicy {
	if windowOps < 1 {
		windowOps = 1
	}
	return &WindowPolicy{windowOps: windowOps}
}

// OnIngress records one ingress; returns true when the window is full and should
// flush.
func (w *WindowPolicy) OnIngress() bool {
	w.pending++
	if w.pending >= w.windowOps {
		w.pending = 0
		return true
	}
	return false
}

// Tick signals the debounce/throttle interval elapsed: flush whatever is pending.
func (w *WindowPolicy) Tick() bool {
	if w.pending > 0 {
		w.pending = 0
		return true
	}
	return false
}

// ExpiryPolicy — Case 10, TTL / deadline expiry. Drops elements whose age exceeds
// ttl against a logical clock. Lossy-by-age (explicit); used to shed cold data.
type ExpiryPolicy struct {
	ttl uint64
	now uint64
}

// NewExpiryPolicy creates a TTL policy with the given time-to-live.
func NewExpiryPolicy(ttl uint64) *ExpiryPolicy { return &ExpiryPolicy{ttl: ttl} }

// Advance advances the logical clock.
func (e *ExpiryPolicy) Advance(by uint64) { e.now += by }

// Now is the current logical time.
func (e *ExpiryPolicy) Now() uint64 { return e.now }

// IsLive reports whether an element stamped at stampedAt is still live.
func (e *ExpiryPolicy) IsLive(stampedAt uint64) bool {
	age := uint64(0)
	if e.now > stampedAt {
		age = e.now - stampedAt
	}
	return age <= e.ttl
}

// TimedValue is a (timestamp, value) pair for RetainLive.
type TimedValue[T any] struct {
	At    uint64
	Value T
}

// RetainLive returns only the live elements of a timestamped batch (drops the
// aged tail). A free function because Go methods cannot carry their own type
// parameters.
func RetainLive[T any](e *ExpiryPolicy, batch []TimedValue[T]) []T {
	out := make([]T, 0, len(batch))
	for _, tv := range batch {
		if e.IsLive(tv.At) {
			out = append(out, tv.Value)
		}
	}
	return out
}

// PriorityStorage — Case 11, priority egress. Ingress carries a priority; egress
// pops the highest priority first (not FIFO), FIFO within equal priority.
// Reordering, so sound for a commutative merge downstream (reorder_adjacent).
type PriorityStorage[T any] struct {
	items []priorityItem[T]
	seq   uint64
}

type priorityItem[T any] struct {
	priority uint64
	seq      uint64
	value    T
}

// NewPriorityStorage creates an empty priority storage.
func NewPriorityStorage[T any]() *PriorityStorage[T] { return &PriorityStorage[T]{} }

// Push adds a value at the given priority.
func (p *PriorityStorage[T]) Push(priority uint64, value T) {
	p.items = append(p.items, priorityItem[T]{priority: priority, seq: p.seq, value: value})
	p.seq++
}

// Pop removes and returns the highest-priority element (FIFO within equal
// priority). The second return is false when empty.
func (p *PriorityStorage[T]) Pop() (T, bool) {
	if len(p.items) == 0 {
		var zero T
		return zero, false
	}
	best := 0
	for i := 1; i < len(p.items); i++ {
		a, b := p.items[i], p.items[best]
		if a.priority > b.priority || (a.priority == b.priority && a.seq < b.seq) {
			best = i
		}
	}
	out := p.items[best].value
	p.items = append(p.items[:best], p.items[best+1:]...)
	return out, true
}

// Len is the number of stored elements.
func (p *PriorityStorage[T]) Len() int { return len(p.items) }

// IsEmpty reports whether the storage is empty.
func (p *PriorityStorage[T]) IsEmpty() bool { return len(p.items) == 0 }

// KeyedRelay — Case 18, keyed sharding. N independent relays keyed by K; an op
// routes to its key's shard. Merging across shards requires a commutative merge.
// The converged per-key state equals a single relay per key.
type KeyedRelay[K comparable, T comparable] struct {
	ctx       *Context
	highWater uint64
	overflow  Overflow
	merge     MergePolicy[T]
	shards    map[K]*RelayCell[T]
	order     []K
}

// NewKeyedRelay creates a keyed relay. Returns ErrConflateNotBounding if overflow
// is Conflate on a non-conflating policy (same guard as RelayCell).
func NewKeyedRelay[K comparable, T comparable](ctx *Context, highWater uint64, overflow Overflow, merge MergePolicy[T]) (*KeyedRelay[K, T], error) {
	if overflow == OverflowConflate && !merge.Conflates {
		return nil, ErrConflateNotBounding
	}
	return &KeyedRelay[K, T]{
		ctx:       ctx,
		highWater: highWater,
		overflow:  overflow,
		merge:     merge,
		shards:    map[K]*RelayCell[T]{},
	}, nil
}

// Ingress routes op to key's shard, creating the shard on first use.
func (k *KeyedRelay[K, T]) Ingress(key K, op T) IngressOutcome {
	relay, ok := k.shards[key]
	if !ok {
		policy := NewBackpressurePolicy(k.ctx, BoundCount, k.highWater, k.highWater/2, k.overflow)
		// Config was validated at construction, so this cannot error.
		relay, _ = NewRelayCell(k.ctx, policy, k.merge)
		k.shards[key] = relay
		k.order = append(k.order, key)
	}
	return relay.Ingress(op)
}

// Drain drains a key's coalesced window (false when the key has no shard/window).
func (k *KeyedRelay[K, T]) Drain(key K) (T, bool) {
	relay, ok := k.shards[key]
	if !ok {
		var zero T
		return zero, false
	}
	return relay.Drain()
}

// Keys returns the shard keys in first-use order.
func (k *KeyedRelay[K, T]) Keys() []K {
	out := make([]K, len(k.order))
	copy(out, k.order)
	return out
}
