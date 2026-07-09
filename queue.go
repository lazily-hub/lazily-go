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
// The shell / storage split: the reactive shell owns the reader-kind version
// cells and the invalidation logic (storage-agnostic — this is what the formal
// model LazilyFormal.QueueCell pins); the storage backend owns the actual FIFO
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
// state (Len / Peek / IsClosed / capacity) and the shell layers its own
// reader-kind version cells above it.
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
	// Peek returns the current head element and true, or the zero T and false
	// when empty. Non-mutating; the shell reads this to materialize the head
	// reader-kind cell.
	Peek() (T, bool)
	// Len reports the number of elements currently held.
	Len() int
	// Capacity reports the bound and true for a bounded backend, or 0 and
	// false for the unbounded default. The shell uses this to decide whether
	// the is_full reader-kind cell can ever be invalidated.
	Capacity() (int, bool)
	// IsClosed reports whether the queue has been closed. Closure is monotonic
	// (once closed, stays closed).
	IsClosed() bool
	// Close marks the queue closed. Idempotent: closing an already-closed
	// queue is a no-op. Close is terminal: a closed queue cannot reopen.
	Close()
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

	head     *Cell[queueHead[T]]
	lenCell  *Cell[int]
	empty    *Cell[bool]
	full     *Cell[bool]
	closed   *Cell[bool]
	bounded  bool // mirrored from storage.Capacity() at construction
	capacity int  // mirrored from storage.Capacity() at construction
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
// backend (custom ring buffer, broker client, consensus log, ...). The shell
// is storage-agnostic: it reads Len / Peek / IsClosed / Capacity and layers
// its own reader-kind version cells above. Pass a pointer to your storage so
// the shell observes its mutations.
func NewQueueCellWithStorage[T comparable, S QueueStorage[T]](ctx *Context, storage S) *QueueCell[T, S] {
	q := &QueueCell[T, S]{ctx: ctx, storage: storage}
	if cap, ok := storage.Capacity(); ok {
		q.bounded = true
		q.capacity = cap
	}
	l := storage.Len()
	head, headOk := storage.Peek()
	empty := l == 0
	full := q.bounded && l >= q.capacity
	q.head = NewCell[queueHead[T]](ctx, queueHead[T]{value: head, ok: headOk})
	q.lenCell = NewCell[int](ctx, l)
	q.empty = NewCell[bool](ctx, empty)
	q.full = NewCell[bool](ctx, full)
	q.closed = NewCell[bool](ctx, storage.IsClosed())
	return q
}

// TryPush appends value to the tail. On success it syncs the reader-kind cells
// and returns QueuePushOk. On reject (Full / Closed) it leaves all readers
// untouched — a failed push invalidates nothing.
//
// For MPSC, call TryPush inside a Context.Batch so the per-producer pushes
// appear as one atomic, coalesced transition to concurrent observers.
func (q *QueueCell[T, S]) TryPush(value T) QueuePushError {
	err := q.storage.TryPush(value)
	if err.Ok() {
		q.syncContent()
	}
	return err
}

// TryPop removes and returns the head element. A closed non-empty queue keeps
// draining (returns the next element); only a closed empty queue returns
// QueuePopClosed, and only an open empty queue returns QueuePopEmpty. On
// success the reader-kind cells are synced; a failed pop invalidates nothing.
func (q *QueueCell[T, S]) TryPop() (T, QueuePopError) {
	v, err := q.storage.TryPop()
	if err.Ok() {
		q.syncContent()
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

// syncContent re-derives the four content reader-kind cells from storage and
// writes them inside one Context.Batch. The host Cell's PartialEq (!=) guard
// suppresses any cell whose value is unchanged, so reader-kind independence is
// automatic: a push to a non-empty queue leaves Head cached, a pop that does
// not cross the capacity boundary leaves IsFull cached, and so on. Batching
// guarantees a concurrent observer never sees a glitch (Len bumped before
// IsFull flips). The closed cell is intentionally not touched here.
func (q *QueueCell[T, S]) syncContent() {
	l := q.storage.Len()
	head, _ := q.storage.Peek()
	empty := l == 0
	full := q.bounded && l >= q.capacity
	q.ctx.Batch(func() {
		q.head.Set(queueHead[T]{value: head, ok: !empty})
		q.lenCell.Set(l)
		q.empty.Set(empty)
		q.full.Set(full)
	})
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

// QueueReaderHandles exposes the underlying reader-kind cells directly, for
// advanced wiring (custom slots, effect dependency tracking, graph bridges).
// Each Handle is the Cell backing the corresponding reactive read.
type QueueReaderHandles[T comparable] struct {
	Head     *Cell[queueHead[T]]
	Len      *Cell[int]
	IsEmpty  *Cell[bool]
	IsFull   *Cell[bool]
	IsClosed *Cell[bool]
}

// ReaderHandles returns the five reader-kind cells backing the reactive reads.
func (q *QueueCell[T, S]) ReaderHandles() QueueReaderHandles[T] {
	return QueueReaderHandles[T]{
		Head:     q.head,
		Len:      q.lenCell,
		IsEmpty:  q.empty,
		IsFull:   q.full,
		IsClosed: q.closed,
	}
}
