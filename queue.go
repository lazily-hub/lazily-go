// Reactive queue — QueueCell (SPSC primitive with MPSC usage rule) plus the
// pluggable QueueStorage backend (cell-model.md § Reactive queues).
//
// A reactive queue is a FIFO collection *composed of cells* — not a new cell
// kind — that adds queue semantics (push to tail, pop from head) to the
// reactive graph. It adds no new merge unit; each element is an ordinary value
// subject to the same single-writer / multi-write classification.
//
// The distinguishing property of a reactive queue is that invalidation is
// scoped to **reader kind**, not to individual positions:
//
//   - a push invalidates length/is_empty (and head when transitioning from
//     empty), plus is_full when it fills a bounded queue;
//   - a pop invalidates head/length/is_empty (plus is_full when it un-fills);
//   - neither push nor pop touches the closed reader; only Close does;
//   - a no-op (push at capacity → Full, pop on empty → Empty/Closed, close of
//     an already-closed queue) invalidates nothing.
//
// QueueCell is specified as a single-producer, single-consumer (SPSC)
// primitive: one writer owns the tail, one reader owns the head, so the
// producer is the natural FIFO sequencer (push order = delivery order). MPSC
// (multi-producer, single-consumer) is a *usage rule on the same primitive*,
// not a separate type: multiple producers push to the same tail inside a
// Context.Batch boundary; the batch serializes the pushes into a deterministic
// order and coalesces the cascade into one observable transition. There is no
// MPSCQueueCell type — introducing one would imply SPMC/MPMC siblings that in
// fact differ in semantics, not cardinality (see TopicCell / WorkQueueCell).
//
// The shell / storage split: the reactive shell owns the demand-driven
// reader-kinds and the invalidation logic (storage-agnostic — this is what the
// formal model LazilyFormal.QueueCell pins); the storage backend owns the FIFO
// data structure and is pluggable via the QueueStorage interface. The default
// VecDequeStorage is an unbounded slice-backed queue; a bounded one exposes a
// capacity and reactive backpressure via IsFull. Distribution is a
// storage-backend property, not a shell property.
//
// Ported from lazily-rs/src/queue.rs, mirroring lazily-kt Queue.kt,
// lazily-cpp queue.hpp, lazily-js queue.js, and lazily-zig queue.zig.
// Validated against lazily-spec/conformance/collections/queuecell_*.json.
package lazily

// QueuePushError is the outcome of a push attempt. The zero value
// (QueuePushOk, the empty string) means success; the sentinels distinguish the
// two failure modes the observable contract separates.
//
//   - Full   — the bounded queue is at capacity (overflow policy = reject, the
//     default VecDequeStorage behavior; other backends may block / drop-oldest
//     / drop-newest, but the shell only distinguishes Full from Empty/Closed).
//   - Closed — the queue is closed; push after close is an error.
type QueuePushError string

const (
	// QueuePushOk is the zero-value success sentinel (push accepted).
	QueuePushOk QueuePushError = ""
	// QueuePushFull means a bounded queue rejected the push at capacity.
	QueuePushFull QueuePushError = "Full"
	// QueuePushClosed means the push was rejected because the queue is closed.
	QueuePushClosed QueuePushError = "Closed"
)

// Ok reports whether the push succeeded (the error is the zero value).
func (e QueuePushError) Ok() bool { return e == QueuePushOk }

// String renders the fixture/wire label ("Full" / "Closed"; "" for success).
func (e QueuePushError) String() string { return string(e) }

// QueuePopError is the failure mode of a pop attempt. The zero value
// (QueuePopOk, the empty string) means a value was returned.
//
//   - Empty  — the queue is open but holds no elements.
//   - Closed — the queue is closed and empty. This is distinct from Empty so a
//     consumer can tell "no work right now" from "no work will ever arrive"
//     (the drain-completion signal).
type QueuePopError string

const (
	// QueuePopOk is the zero-value success sentinel (a value was popped).
	QueuePopOk QueuePopError = ""
	// QueuePopEmpty means the open queue had no element to pop.
	QueuePopEmpty QueuePopError = "Empty"
	// QueuePopClosed means a closed, empty queue was popped (drain complete).
	QueuePopClosed QueuePopError = "Closed"
)

// Ok reports whether the pop succeeded (the error is the zero value).
func (e QueuePopError) Ok() bool { return e == QueuePopOk }

// String renders the fixture/wire label ("Empty" / "Closed"; "" for success).
func (e QueuePopError) String() string { return string(e) }

// QueueStorage is the backend contract a QueueCell reactive shell sits over
// (cell-model.md § Storage backend contract). A conforming backend:
//
//  1. preserves FIFO order — TryPop returns elements in the order they were
//     TryPush-ed (no reordering, no silent drops);
//  2. exposes a native producer/consumer shape that is a superset of the
//     shell's required SPSC shape (MPSC usage needs a multi-writer backend);
//  3. optionally exposes a capacity: TryPush returns QueuePushFull when at
//     capacity. The overflow policy is a backend property;
//  4. phrases state over reader kind (head/len/empty/full), never exposing
//     storage indices that could cause spurious invalidations when (say) a
//     ring-buffer slot index wraps.
//
// Invalidation is the shell's job, not the backend's: the backend reports raw
// state (Len / IsClosed) and the shell layers its own demand-driven reader-kinds
// above it.
//
// Minimal required contract (Phase 0, #relaycell): TryPush / TryPop / Len /
// IsClosed / Close. Peek and Capacity are OPTIONAL capabilities — a backend that
// implements PeekableStorage[T] gains a Head reader; one that implements
// BoundedStorage gains an IsFull reader. A raw-channel-style backend that
// satisfies only QueueStorage is fully conforming (no Head, never full).
//
// Implement QueueStorage with a pointer receiver so mutation is visible to the
// owning shell; pass that pointer as the QueueCell's S.
type QueueStorage[T any] interface {
	// TryPush appends value to the tail. Returns QueuePushOk on success,
	// QueuePushFull if a bounded queue is at capacity, or QueuePushClosed if
	// the queue is closed (push after close is rejected regardless of
	// capacity).
	TryPush(value T) QueuePushError
	// TryPop removes and returns the head element. On success it returns the
	// value and QueuePopOk. On an open empty queue it returns the zero T and
	// QueuePopEmpty; on a closed empty queue it returns the zero T and
	// QueuePopClosed (drain-complete). Pop on a closed non-empty queue keeps
	// draining — it returns the next element, not Closed.
	TryPop() (T, QueuePopError)
	// Len reports the number of elements currently held.
	Len() int
	// IsClosed reports whether the queue has been closed. Closure is monotonic
	// (once closed, stays closed).
	IsClosed() bool
	// Close marks the queue closed. Idempotent: closing an already-closed
	// queue is a no-op. Close is terminal: a closed queue cannot reopen.
	Close()
}

// PeekableStorage is the OPTIONAL peek capability. A backend implementing it
// gains a reactive Head reader; a backend without it has no Head (Head returns
// the zero value and false), exactly as an unbounded backend has no IsFull.
type PeekableStorage[T any] interface {
	// Peek returns the current head element and true, or the zero T and false
	// when empty. Non-mutating.
	Peek() (T, bool)
}

// BoundedStorage is the OPTIONAL bound capability. A backend implementing it and
// reporting a bound gains a reactive IsFull backpressure reader; a backend
// without it is treated as unbounded (IsFull is always false).
type BoundedStorage interface {
	// Capacity reports the bound and true for a bounded backend, or 0 and
	// false for the unbounded default.
	Capacity() (int, bool)
}

// VecDequeStorage is the reference QueueStorage backend: an unbounded (or
// optionally bounded) slice-backed FIFO. This is the default and the storage
// form the conformance fixtures serialize — element order is FIFO order.
//
// The overflow policy is reject: TryPush on a bounded, full queue returns
// QueuePushFull and leaves the queue unchanged.
type VecDequeStorage[T any] struct {
	elements []T
	capacity int // 0 = unbounded; >0 = bounded bound (len >= capacity == full)
	closed   bool
}

// NewVecDequeStorage returns an unbounded VecDequeStorage.
func NewVecDequeStorage[T any]() *VecDequeStorage[T] {
	return &VecDequeStorage[T]{}
}

// NewBoundedVecDequeStorage returns a bounded VecDequeStorage with capacity n.
// Panics if n <= 0 (a zero-capacity queue can never accept an element and has
// no meaningful backpressure signal).
func NewBoundedVecDequeStorage[T any](n int) *VecDequeStorage[T] {
	if n <= 0 {
		panic("VecDequeStorage: capacity must be > 0")
	}
	return &VecDequeStorage[T]{capacity: n}
}

// TryPush appends value, or returns Full/Closed without mutating on reject.
func (s *VecDequeStorage[T]) TryPush(value T) QueuePushError {
	if s.closed {
		return QueuePushClosed
	}
	if s.capacity > 0 && len(s.elements) >= s.capacity {
		return QueuePushFull
	}
	s.elements = append(s.elements, value)
	return QueuePushOk
}

// TryPop removes and returns the head element. A closed non-empty queue keeps
// draining; only a closed empty queue returns Closed.
func (s *VecDequeStorage[T]) TryPop() (T, QueuePopError) {
	if len(s.elements) == 0 {
		var zero T
		if s.closed {
			return zero, QueuePopClosed
		}
		return zero, QueuePopEmpty
	}
	v := s.elements[0]
	s.elements = append(s.elements[:0:0], s.elements[1:]...)
	return v, QueuePopOk
}

// Peek returns the head element and true, or the zero T and false when empty.
func (s *VecDequeStorage[T]) Peek() (T, bool) {
	if len(s.elements) == 0 {
		var zero T
		return zero, false
	}
	return s.elements[0], true
}

// Len reports the number of elements held.
func (s *VecDequeStorage[T]) Len() int { return len(s.elements) }

// Capacity reports the bound and true for a bounded storage, or 0 and false.
func (s *VecDequeStorage[T]) Capacity() (int, bool) {
	if s.capacity > 0 {
		return s.capacity, true
	}
	return 0, false
}

// IsClosed reports whether the queue has been closed.
func (s *VecDequeStorage[T]) IsClosed() bool { return s.closed }

// Close marks the queue closed. Idempotent and terminal.
func (s *VecDequeStorage[T]) Close() { s.closed = true }

// Elements returns a copy of the FIFO contents in delivery order (head first).
// Used by the conformance harness and for snapshot serialization; production
// code may choose a more efficient binary encoding.
func (s *VecDequeStorage[T]) Elements() []T {
	out := make([]T, len(s.elements))
	copy(out, s.elements)
	return out
}

// queueHead is a comparable optional wrapper around the head value — the
// reactive head reader-kind cell stores this so a present head, an absent head
// (empty queue), and a changed head are all distinguishable to Cell's PartialEq
// (!=) guard. nil-head is not representable as a plain T because T may have a
// legitimate zero value; the ok bit is the empty/non-empty flag.
type queueHead[T comparable] struct {
	value T
	ok    bool
}

// QueueCell is a reactive FIFO queue — a shell of reader-kind version cells
// layered over a pluggable QueueStorage backend (cell-model.md § Reactive
// queues).
//
// The shell owns five reader-kind cells whose values are re-derived from
// storage after each successful op:
//
//   - Head      — the current head value, or none when empty. Invalidated on
//     every pop (the head value always changes) and on a push that transitions
//     the queue from empty to non-empty (the head appears for the first time);
//     NOT invalidated by a push to a non-empty queue (the head is unchanged).
//   - Len       — the element count. Invalidated on every push and every pop
//     that changes the count.
//   - IsEmpty   — the emptiness flag. Invalidated when the queue transitions
//     between empty and non-empty.
//   - IsFull    — the fullness flag (bounded queues only). Invalidated when the
//     queue transitions across the capacity boundary in either direction, so a
//     consumer's pop that makes room wakes a producer's IsFull subscription
//     (reactive backpressure).
//   - IsClosed  — the closed flag. Invalidated only by the first Close (a
//     terminal false→true transition); neither push nor pop touches it.
//
// Reader-kind independence comes "for free" from the host Cell's PartialEq
// guard: after each op the shell re-derives all four content cells and writes
// them back inside one Context.Batch; a cell whose value did not change
// suppresses its cascade, so a push to a non-empty queue (head unchanged)
// invalidates Len/IsEmpty but not Head.
//
// T must be comparable so the head cell's PartialEq guard can detect a head
// change; storage itself needs no equality. SPSC by construction; for MPSC,
// push inside Context.Batch.
type QueueCell[T comparable, S QueueStorage[T]] struct {
	ctx     *Context
	storage S

	// Demand-driven reader-kinds: memoized Slots deriving from storage (were
	// eagerly-Set Cells). Each re-derives on first Get after invalidation; the
	// shell invalidates only the ones that provably changed on an op. closed
	// stays a Cell (a direct input, set by Close).
	head     *Computed[queueHead[T]]
	lenCell  *Computed[int]
	empty    *Computed[bool]
	full     *Computed[bool]
	closed   *Source[bool]
	bounded  bool             // mirrored from BoundedStorage.Capacity() at construction
	capacity int              // mirrored from BoundedStorage.Capacity() at construction
	peek     func() (T, bool) // the backend's Peek, or nil when it has no peek capability
}

// NewQueueCell builds an unbounded QueueCell with the default VecDequeStorage
// backend. The queue can grow without bound and has no IsFull reader to
// invalidate.
func NewQueueCell[T comparable](ctx *Context) *QueueCell[T, *VecDequeStorage[T]] {
	return NewQueueCellWithStorage[T](ctx, NewVecDequeStorage[T]())
}

// NewBoundedQueueCell builds a bounded QueueCell with a VecDequeStorage of the
// given capacity. The queue exposes IsFull as a reactive reader (the
// backpressure signal): a pop that makes room invalidates IsFull readers.
// Panics if capacity <= 0.
func NewBoundedQueueCell[T comparable](ctx *Context, capacity int) *QueueCell[T, *VecDequeStorage[T]] {
	return NewQueueCellWithStorage[T](ctx, NewBoundedVecDequeStorage[T](capacity))
}

// NewQueueCellWithStorage builds a QueueCell over an arbitrary QueueStorage
// backend (custom ring buffer, broker client, consensus log, ...). The shell is
// storage-agnostic: it reads Len / IsClosed (the required contract) and, when
// the backend offers them, the optional PeekableStorage / BoundedStorage
// capabilities. Pass a pointer to your storage so the shell observes mutations.
func NewQueueCellWithStorage[T comparable, S QueueStorage[T]](ctx *Context, storage S) *QueueCell[T, S] {
	q := &QueueCell[T, S]{ctx: ctx, storage: storage}
	// Capacity and Peek are optional capabilities (Phase 0 #relaycell). Cache
	// the bound once (it is contractually fixed) and resolve Peek to a func, or
	// nil when the backend cannot peek.
	if b, ok := any(storage).(BoundedStorage); ok {
		if cap, ok := b.Capacity(); ok {
			q.bounded = true
			q.capacity = cap
		}
	}
	if p, ok := any(storage).(PeekableStorage[T]); ok {
		q.peek = p.Peek
	}
	// Reader-kinds derive lazily from storage; nothing is materialized until a
	// reader is observed. Head is trivially empty when the backend has no peek.
	q.head = NewSlot[queueHead[T]](ctx, func(c *Compute) queueHead[T] {
		if q.peek == nil {
			return queueHead[T]{}
		}
		v, ok := q.peek()
		return queueHead[T]{value: v, ok: ok}
	})
	q.lenCell = NewSlot[int](ctx, func(c *Compute) int { return q.storage.Len() })
	q.empty = NewSlot[bool](ctx, func(c *Compute) bool { return q.storage.Len() == 0 })
	q.full = NewSlot[bool](ctx, func(c *Compute) bool {
		return q.bounded && q.storage.Len() >= q.capacity
	})
	q.closed = NewSource[bool](ctx, storage.IsClosed())
	return q
}

// TryPush appends value to the tail. On success it syncs the reader-kind cells
// and returns QueuePushOk. On reject (Full / Closed) it leaves all readers
// untouched — a failed push invalidates nothing.
//
// For MPSC, call TryPush inside a Context.Batch so the per-producer pushes
// appear as one atomic, coalesced transition to concurrent observers.
func (q *QueueCell[T, S]) TryPush(value T) QueuePushError {
	lenBefore := q.storage.Len()
	err := q.storage.TryPush(value)
	if err.Ok() {
		// Head changes on a push only when the queue was empty.
		q.invalidateReaders(lenBefore, lenBefore+1, lenBefore == 0)
	}
	return err
}

// TryPop removes and returns the head element. A closed non-empty queue keeps
// draining (returns the next element); only a closed empty queue returns
// QueuePopClosed, and only an open empty queue returns QueuePopEmpty. On
// success the reader-kind cells are synced; a failed pop invalidates nothing.
func (q *QueueCell[T, S]) TryPop() (T, QueuePopError) {
	lenBefore := q.storage.Len()
	v, err := q.storage.TryPop()
	if err.Ok() {
		// A successful pop always advances head and decrements len.
		q.invalidateReaders(lenBefore, lenBefore-1, true)
	}
	return v, err
}

// Close marks the queue closed. Idempotent: closing an already-closed queue is
// a no-op that invalidates nothing. Terminal: a closed queue cannot reopen
// (the formal Closed_then_stays_Closed invariant). Neither push nor pop can
// change the closed flag.
func (q *QueueCell[T, S]) Close() {
	if q.storage.IsClosed() {
		return
	}
	q.storage.Close()
	// Only the closed reader-kind cell transitions, and only false→true. The
	// content cells (head/len/empty/full) are untouched: close preserves
	// elements by the formal close_preserves_{elements,head,length}.
	q.closed.Set(true)
}

// invalidateReaders invalidates exactly the reader-kind Slots whose derived
// value changed on a successful op that took the queue from lenBefore to
// lenAfter. No reader value is derived here — invalidating a Slot only drops its
// cache and cascades to its dependents (each re-derives lazily on its next Get),
// so an unobserved reader pays effectively nothing. headChanged is passed by the
// caller because head depends on op direction, not just len (a pop always
// changes head; a push changes it only from empty) — so no Peek is needed to
// decide. The changed Slots invalidate together and effects flush once, so a
// subscriber never sees a partial state (Len bumped before IsFull flips). Inside
// a Context.Batch the flush is deferred to the batch boundary. The closed cell
// is intentionally not touched here.
func (q *QueueCell[T, S]) invalidateReaders(lenBefore, lenAfter int, headChanged bool) {
	q.lenCell.invalidate() // len always changes on a successful op
	if (lenBefore == 0) != (lenAfter == 0) {
		q.empty.invalidate()
	}
	if q.bounded && (lenBefore >= q.capacity) != (lenAfter >= q.capacity) {
		q.full.invalidate()
	}
	if headChanged {
		q.head.invalidate()
	}
	if !q.ctx.IsBatching() {
		q.ctx.flushEffects()
	}
}

// Head is the reactive head read. Subscribes the caller to the head reader-kind
// cell: it recomputes (is invalidated) on every pop and on a push that
// transitions empty→non-empty, but not on a push to a non-empty queue. Returns
// the head value and true, or the zero T and false when empty.
func (q *QueueCell[T, S]) Head() (T, bool) {
	h := q.head.Get()
	return h.value, h.ok
}

// Len is the reactive element count. Subscribes the caller to the length
// reader-kind cell.
func (q *QueueCell[T, S]) Len() int { return q.lenCell.Get() }

// IsEmpty is the reactive emptiness flag. Subscribes the caller to the
// emptiness reader-kind cell.
func (q *QueueCell[T, S]) IsEmpty() bool { return q.empty.Get() }

// IsFull is the reactive fullness flag (bounded queues only). Subscribes the
// caller to the fullness reader-kind cell — the backpressure signal: a
// consumer's pop that makes room invalidates this reader so a push-side effect
// can resume without polling. An unbounded queue's IsFull is always false and
// never invalidates.
func (q *QueueCell[T, S]) IsFull() bool { return q.full.Get() }

// IsClosed is the reactive closed flag. Subscribes the caller to the closed
// reader-kind cell, which transitions only once (the first Close).
func (q *QueueCell[T, S]) IsClosed() bool { return q.closed.Get() }

// Capacity reports the bound and true for a bounded backend, or 0 and false
// for the unbounded default. Non-reactive (a queue's capacity never changes
// after construction).
func (q *QueueCell[T, S]) Capacity() (int, bool) { return q.capacity, q.bounded }

// LenUntracked reports the current element count without subscribing the caller
// to the length reader-kind cell. Non-reactive.
func (q *QueueCell[T, S]) LenUntracked() int { return q.storage.Len() }

// IsClosedUntracked reports the closed flag without subscribing the caller.
// Non-reactive.
func (q *QueueCell[T, S]) IsClosedUntracked() bool { return q.storage.IsClosed() }

// Storage returns the backing QueueStorage. Exposed so callers can reach a
// backend-specific surface (e.g. (*VecDequeStorage[T]).Elements() for a
// snapshot, or a consensus backend's anti-entropy handle).
func (q *QueueCell[T, S]) Storage() S { return q.storage }

// QueueReaderHandles exposes the underlying reader-kinds directly, for advanced
// wiring (custom slots, effect dependency tracking, graph bridges). The four
// derived reader-kinds are demand-driven Slots; IsClosed is the Cell backing the
// closed flag (a direct input).
type QueueReaderHandles[T comparable] struct {
	Head     *Computed[queueHead[T]]
	Len      *Computed[int]
	IsEmpty  *Computed[bool]
	IsFull   *Computed[bool]
	IsClosed *Source[bool]
}

// ReaderHandles returns the five reader-kinds backing the reactive reads.
func (q *QueueCell[T, S]) ReaderHandles() QueueReaderHandles[T] {
	return QueueReaderHandles[T]{
		Head:     q.head,
		Len:      q.lenCell,
		IsEmpty:  q.empty,
		IsFull:   q.full,
		IsClosed: q.closed,
	}
}

// TopicDurability controls whether a subscription survives disconnect and
// participates in the retained-log GC frontier.
type TopicDurability string

const (
	TopicDurable   TopicDurability = "durable"
	TopicEphemeral TopicDurability = "ephemeral"
)

// TopicSubscribeOutcome describes whether Subscribe minted or resumed a cursor.
type TopicSubscribeOutcome string

const (
	TopicSubscribed        TopicSubscribeOutcome = "subscribed"
	TopicReconnected       TopicSubscribeOutcome = "reconnected"
	TopicAlreadySubscribed TopicSubscribeOutcome = "already_subscribed"
)

// TopicSubscriptionSnapshot is the persistent state for one stable subscriber.
type TopicSubscriptionSnapshot struct {
	ID         string
	Cursor     int
	Durability TopicDurability
	Connected  bool
}

// TopicSnapshot is a portable TopicCell retained-log snapshot.
type TopicSnapshot[T any] struct {
	BaseOffset    int
	Elements      []T
	Subscriptions []TopicSubscriptionSnapshot
}

type topicSubscription struct {
	cursor     int
	durability TopicDurability
	connected  bool
}

// TopicRead is the memoized per-subscriber suffix returned by a reader Slot.
type TopicRead[T any] struct {
	Elements []T
	Exists   bool
}

// TopicCell is a broadcast log with one absolute reactive cursor per subscriber.
// Durable offline subscribers retain their cursor; ephemeral ones disappear on
// disconnect. GC drops only the prefix below the slowest durable cursor.
type TopicCell[T any] struct {
	ctx           *Context
	baseOffset    int
	elements      []T
	subscriptions map[string]*topicSubscription
	readers       map[string]*Computed[TopicRead[T]]
}

// NewTopicCell creates an empty broadcast topic.
func NewTopicCell[T any](ctx *Context) *TopicCell[T] {
	return &TopicCell[T]{
		ctx:           ctx,
		subscriptions: make(map[string]*topicSubscription),
		readers:       make(map[string]*Computed[TopicRead[T]]),
	}
}

// NewTopicCellFromSnapshot restores retained elements and absolute cursors.
func NewTopicCellFromSnapshot[T any](ctx *Context, snapshot TopicSnapshot[T]) *TopicCell[T] {
	t := NewTopicCell[T](ctx)
	if snapshot.BaseOffset < 0 {
		panic("TopicCell: base offset must be non-negative")
	}
	t.baseOffset = snapshot.BaseOffset
	t.elements = append([]T(nil), snapshot.Elements...)
	tail := t.TailOffset()
	for _, saved := range snapshot.Subscriptions {
		if saved.Cursor < t.baseOffset || saved.Cursor > tail {
			panic("TopicCell: subscription cursor outside retained log")
		}
		if saved.Durability != TopicDurable && saved.Durability != TopicEphemeral {
			panic("TopicCell: invalid subscription durability")
		}
		if saved.Durability == TopicEphemeral && !saved.Connected {
			panic("TopicCell: disconnected ephemeral subscription must be removed")
		}
		t.subscriptions[saved.ID] = &topicSubscription{
			cursor: saved.Cursor, durability: saved.Durability, connected: saved.Connected,
		}
		t.ensureReader(saved.ID)
	}
	return t
}

func (t *TopicCell[T]) ensureReader(id string) *Computed[TopicRead[T]] {
	if reader, ok := t.readers[id]; ok {
		return reader
	}
	reader := NewSlot[TopicRead[T]](t.ctx, func(c *Compute) TopicRead[T] {
		return t.readUntracked(id)
	})
	t.readers[id] = reader
	return reader
}

func (t *TopicCell[T]) invalidate(ids []string) {
	for _, id := range ids {
		if reader, ok := t.readers[id]; ok {
			reader.invalidate()
		}
	}
	if len(ids) > 0 && !t.ctx.IsBatching() {
		t.ctx.flushEffects()
	}
}

// Subscribe starts a new cursor at the current tail, or resumes an offline
// durable cursor with the same stable id.
func (t *TopicCell[T]) Subscribe(id string, durability TopicDurability) TopicSubscribeOutcome {
	if durability != TopicDurable && durability != TopicEphemeral {
		panic("TopicCell: invalid subscription durability")
	}
	if sub, ok := t.subscriptions[id]; ok {
		if sub.connected {
			return TopicAlreadySubscribed
		}
		if sub.durability != TopicDurable {
			panic("TopicCell: only durable subscriptions can reconnect")
		}
		sub.connected = true
		t.invalidate([]string{id})
		return TopicReconnected
	}
	t.subscriptions[id] = &topicSubscription{
		cursor: t.TailOffset(), durability: durability, connected: true,
	}
	t.ensureReader(id)
	return TopicSubscribed
}

// Reconnect resumes an offline durable subscription at its saved cursor.
func (t *TopicCell[T]) Reconnect(id string) {
	sub, ok := t.subscriptions[id]
	if !ok || sub.durability != TopicDurable {
		panic("TopicCell: durable subscription not found")
	}
	if !sub.connected {
		sub.connected = true
		t.invalidate([]string{id})
	}
}

// Disconnect retains durable cursors and removes ephemeral subscriptions.
func (t *TopicCell[T]) Disconnect(id string) {
	sub, ok := t.subscriptions[id]
	if !ok || !sub.connected {
		return
	}
	sub.connected = false
	if sub.durability == TopicEphemeral {
		delete(t.subscriptions, id)
	}
	t.invalidate([]string{id})
}

// Publish appends a value and invalidates each connected reader independently.
func (t *TopicCell[T]) Publish(value T) int {
	offset := t.TailOffset()
	t.elements = append(t.elements, value)
	ids := make([]string, 0, len(t.subscriptions))
	for id, sub := range t.subscriptions {
		if sub.connected {
			ids = append(ids, id)
		}
	}
	t.invalidate(ids)
	return offset
}

func (t *TopicCell[T]) readUntracked(id string) TopicRead[T] {
	sub, ok := t.subscriptions[id]
	if !ok || !sub.connected {
		return TopicRead[T]{}
	}
	start := sub.cursor - t.baseOffset
	return TopicRead[T]{Elements: append([]T(nil), t.elements[start:]...), Exists: true}
}

// ReadStream reactively reads the complete retained suffix at this cursor.
func (t *TopicCell[T]) ReadStream(id string) ([]T, bool) {
	read := t.ensureReader(id).Get()
	return read.Elements, read.Exists
}

// Read reactively reads the next value without advancing the cursor.
func (t *TopicCell[T]) Read(id string) (T, bool) {
	values, exists := t.ReadStream(id)
	if exists && len(values) > 0 {
		return values[0], true
	}
	var zero T
	return zero, false
}

// Advance moves only the named subscriber's absolute cursor.
func (t *TopicCell[T]) Advance(id string, count int) int {
	sub, ok := t.subscriptions[id]
	if !ok || count < 0 {
		panic("TopicCell: invalid cursor advance")
	}
	if !sub.connected || sub.cursor == t.TailOffset() {
		return sub.cursor
	}
	if sub.cursor+count > t.TailOffset() {
		panic("TopicCell: invalid cursor advance")
	}
	if count > 0 {
		sub.cursor += count
		t.invalidate([]string{id})
	}
	return sub.cursor
}

// GC drops only the prefix below every durable cursor and invalidates nothing.
func (t *TopicCell[T]) GC() int {
	frontier := t.TailOffset()
	for _, sub := range t.subscriptions {
		if sub.durability == TopicDurable && sub.cursor < frontier {
			frontier = sub.cursor
		}
	}
	removed := frontier - t.baseOffset
	t.elements = append([]T(nil), t.elements[removed:]...)
	t.baseOffset = frontier
	return removed
}

func (t *TopicCell[T]) BaseOffset() int { return t.baseOffset }
func (t *TopicCell[T]) TailOffset() int { return t.baseOffset + len(t.elements) }
func (t *TopicCell[T]) Elements() []T   { return append([]T(nil), t.elements...) }

// Subscription reports a copy of a subscriber's current state.
func (t *TopicCell[T]) Subscription(id string) (TopicSubscriptionSnapshot, bool) {
	sub, ok := t.subscriptions[id]
	if !ok {
		return TopicSubscriptionSnapshot{}, false
	}
	return TopicSubscriptionSnapshot{id, sub.cursor, sub.durability, sub.connected}, true
}

func (t *TopicCell[T]) ReaderHandle(id string) *Computed[TopicRead[T]] { return t.ensureReader(id) }

// Snapshot copies the retained log and stable subscription table.
func (t *TopicCell[T]) Snapshot() TopicSnapshot[T] {
	snapshot := TopicSnapshot[T]{BaseOffset: t.baseOffset, Elements: t.Elements()}
	for id, sub := range t.subscriptions {
		snapshot.Subscriptions = append(snapshot.Subscriptions, TopicSubscriptionSnapshot{
			ID: id, Cursor: sub.cursor, Durability: sub.durability, Connected: sub.connected,
		})
	}
	return snapshot
}

// Restart models a process restart; persisted state and reader values are stable.
func (t *TopicCell[T]) Restart() {}
