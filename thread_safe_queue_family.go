package lazily

// Thread-safe queue-family shells.
//
// ThreadSafeContext owns the graph lock, so these shells bind the existing
// flavor-neutral queue algorithms to that context and serialize every graph and
// storage access through the same re-entrant boundary. Reader handles are minted
// on the wrapped context's graph; they are not counters or polling facades.

// ThreadSafeQueueCell is the lock-serialized QueueCell flavor.
type ThreadSafeQueueCell[T comparable, S QueueStorage[T]] struct {
	ts    *ThreadSafeContext
	inner *QueueCell[T, S]
}

// NewThreadSafeQueueCell creates an unbounded thread-safe queue.
func NewThreadSafeQueueCell[T comparable](ts *ThreadSafeContext) *ThreadSafeQueueCell[T, *VecDequeStorage[T]] {
	return NewThreadSafeQueueCellWithStorage[T](ts, NewVecDequeStorage[T]())
}

// NewBoundedThreadSafeQueueCell creates a bounded thread-safe queue.
func NewBoundedThreadSafeQueueCell[T comparable](ts *ThreadSafeContext, capacity int) *ThreadSafeQueueCell[T, *VecDequeStorage[T]] {
	return NewThreadSafeQueueCellWithStorage[T](ts, NewBoundedVecDequeStorage[T](capacity))
}

// NewThreadSafeQueueCellWithStorage creates a thread-safe queue over storage.
func NewThreadSafeQueueCellWithStorage[T comparable, S QueueStorage[T]](
	ts *ThreadSafeContext,
	storage S,
) *ThreadSafeQueueCell[T, S] {
	var inner *QueueCell[T, S]
	ts.WithLock(func(ctx *Context) {
		inner = NewQueueCellWithStorage[T](ctx, storage)
	})
	return &ThreadSafeQueueCell[T, S]{ts: ts, inner: inner}
}

func (q *ThreadSafeQueueCell[T, S]) TryPush(value T) QueuePushError {
	var result QueuePushError
	q.ts.WithLock(func(*Context) { result = q.inner.TryPush(value) })
	return result
}

func (q *ThreadSafeQueueCell[T, S]) TryPop() (T, QueuePopError) {
	var value T
	var result QueuePopError
	q.ts.WithLock(func(*Context) { value, result = q.inner.TryPop() })
	return value, result
}

func (q *ThreadSafeQueueCell[T, S]) Close() {
	q.ts.WithLock(func(*Context) { q.inner.Close() })
}

func (q *ThreadSafeQueueCell[T, S]) Head() (T, bool) {
	type result struct {
		value T
		ok    bool
	}
	got := Read(q.ts, func(*Context) result {
		value, ok := q.inner.Head()
		return result{value: value, ok: ok}
	})
	return got.value, got.ok
}

func (q *ThreadSafeQueueCell[T, S]) Len() int {
	return Read(q.ts, func(*Context) int { return q.inner.Len() })
}

func (q *ThreadSafeQueueCell[T, S]) IsEmpty() bool {
	return Read(q.ts, func(*Context) bool { return q.inner.IsEmpty() })
}

func (q *ThreadSafeQueueCell[T, S]) IsFull() bool {
	return Read(q.ts, func(*Context) bool { return q.inner.IsFull() })
}

func (q *ThreadSafeQueueCell[T, S]) IsClosed() bool {
	return Read(q.ts, func(*Context) bool { return q.inner.IsClosed() })
}

func (q *ThreadSafeQueueCell[T, S]) Capacity() (int, bool) {
	return q.inner.Capacity()
}

func (q *ThreadSafeQueueCell[T, S]) Elements() []T {
	return Read(q.ts, func(*Context) []T {
		if storage, ok := any(q.inner.Storage()).(interface{ Elements() []T }); ok {
			return storage.Elements()
		}
		return nil
	})
}

func (q *ThreadSafeQueueCell[T, S]) ReaderHandles() QueueReaderHandles[T] {
	return q.inner.ReaderHandles()
}

// ThreadSafeTopicCell is the lock-serialized TopicCell flavor.
type ThreadSafeTopicCell[T any] struct {
	ts    *ThreadSafeContext
	inner *TopicCell[T]
}

func NewThreadSafeTopicCell[T any](ts *ThreadSafeContext) *ThreadSafeTopicCell[T] {
	var inner *TopicCell[T]
	ts.WithLock(func(ctx *Context) { inner = NewTopicCell[T](ctx) })
	return &ThreadSafeTopicCell[T]{ts: ts, inner: inner}
}

func NewThreadSafeTopicCellFromSnapshot[T any](
	ts *ThreadSafeContext,
	snapshot TopicSnapshot[T],
) *ThreadSafeTopicCell[T] {
	var inner *TopicCell[T]
	ts.WithLock(func(ctx *Context) { inner = NewTopicCellFromSnapshot(ctx, snapshot) })
	return &ThreadSafeTopicCell[T]{ts: ts, inner: inner}
}

func (t *ThreadSafeTopicCell[T]) Subscribe(id string, durability TopicDurability) TopicSubscribeOutcome {
	var result TopicSubscribeOutcome
	t.ts.WithLock(func(*Context) { result = t.inner.Subscribe(id, durability) })
	return result
}

func (t *ThreadSafeTopicCell[T]) Reconnect(id string) {
	t.ts.WithLock(func(*Context) { t.inner.Reconnect(id) })
}

func (t *ThreadSafeTopicCell[T]) Disconnect(id string) {
	t.ts.WithLock(func(*Context) { t.inner.Disconnect(id) })
}

func (t *ThreadSafeTopicCell[T]) Publish(value T) int {
	return Read(t.ts, func(*Context) int { return t.inner.Publish(value) })
}

func (t *ThreadSafeTopicCell[T]) ReadStream(id string) ([]T, bool) {
	type result struct {
		values []T
		ok     bool
	}
	got := Read(t.ts, func(*Context) result {
		values, ok := t.inner.ReadStream(id)
		return result{values: values, ok: ok}
	})
	return got.values, got.ok
}

func (t *ThreadSafeTopicCell[T]) Read(id string) (T, bool) {
	type result struct {
		value T
		ok    bool
	}
	got := Read(t.ts, func(*Context) result {
		value, ok := t.inner.Read(id)
		return result{value: value, ok: ok}
	})
	return got.value, got.ok
}

func (t *ThreadSafeTopicCell[T]) Advance(id string, count int) int {
	return Read(t.ts, func(*Context) int { return t.inner.Advance(id, count) })
}

func (t *ThreadSafeTopicCell[T]) GC() int {
	return Read(t.ts, func(*Context) int { return t.inner.GC() })
}

func (t *ThreadSafeTopicCell[T]) BaseOffset() int {
	return Read(t.ts, func(*Context) int { return t.inner.BaseOffset() })
}

func (t *ThreadSafeTopicCell[T]) TailOffset() int {
	return Read(t.ts, func(*Context) int { return t.inner.TailOffset() })
}

func (t *ThreadSafeTopicCell[T]) Elements() []T {
	return Read(t.ts, func(*Context) []T { return t.inner.Elements() })
}

func (t *ThreadSafeTopicCell[T]) Subscription(id string) (TopicSubscriptionSnapshot, bool) {
	type result struct {
		snapshot TopicSubscriptionSnapshot
		ok       bool
	}
	got := Read(t.ts, func(*Context) result {
		snapshot, ok := t.inner.Subscription(id)
		return result{snapshot: snapshot, ok: ok}
	})
	return got.snapshot, got.ok
}

func (t *ThreadSafeTopicCell[T]) ReaderHandle(id string) *Computed[TopicRead[T]] {
	return Read(t.ts, func(*Context) *Computed[TopicRead[T]] {
		return t.inner.ReaderHandle(id)
	})
}

func (t *ThreadSafeTopicCell[T]) Snapshot() TopicSnapshot[T] {
	return Read(t.ts, func(*Context) TopicSnapshot[T] { return t.inner.Snapshot() })
}

func (t *ThreadSafeTopicCell[T]) Restart() {
	t.ts.WithLock(func(*Context) { t.inner.Restart() })
}

// ThreadSafeWorkQueueCell is the lock-serialized WorkQueueCell flavor.
type ThreadSafeWorkQueueCell[T any] struct {
	ts    *ThreadSafeContext
	inner *WorkQueueCell[T]
}

func NewThreadSafeWorkQueueCell[T any](
	ts *ThreadSafeContext,
	visibilityTimeout int64,
	maxDeliveries uint64,
) *ThreadSafeWorkQueueCell[T] {
	var inner *WorkQueueCell[T]
	ts.WithLock(func(ctx *Context) {
		inner = NewWorkQueueCell[T](ctx, visibilityTimeout, maxDeliveries)
	})
	return &ThreadSafeWorkQueueCell[T]{ts: ts, inner: inner}
}

func (q *ThreadSafeWorkQueueCell[T]) Push(value T) uint64 {
	return Read(q.ts, func(*Context) uint64 { return q.inner.Push(value) })
}

func (q *ThreadSafeWorkQueueCell[T]) Claim(worker string, now int64) (WorkQueueDelivery[T], bool) {
	type result struct {
		delivery WorkQueueDelivery[T]
		ok       bool
	}
	got := Read(q.ts, func(*Context) result {
		delivery, ok := q.inner.Claim(worker, now)
		return result{delivery: delivery, ok: ok}
	})
	return got.delivery, got.ok
}

func (q *ThreadSafeWorkQueueCell[T]) Ack(worker string, deliveryID uint64) bool {
	return Read(q.ts, func(*Context) bool { return q.inner.Ack(worker, deliveryID) })
}

func (q *ThreadSafeWorkQueueCell[T]) Nack(worker string, deliveryID uint64) bool {
	return Read(q.ts, func(*Context) bool { return q.inner.Nack(worker, deliveryID) })
}

func (q *ThreadSafeWorkQueueCell[T]) ReapExpired(now int64) int {
	return Read(q.ts, func(*Context) int { return q.inner.ReapExpired(now) })
}

func (q *ThreadSafeWorkQueueCell[T]) PendingLen() int {
	return Read(q.ts, func(*Context) int { return q.inner.PendingLen() })
}

func (q *ThreadSafeWorkQueueCell[T]) IsEmpty() bool {
	return Read(q.ts, func(*Context) bool { return q.inner.IsEmpty() })
}

func (q *ThreadSafeWorkQueueCell[T]) InFlightLen() int {
	return Read(q.ts, func(*Context) int { return q.inner.InFlightLen() })
}

func (q *ThreadSafeWorkQueueCell[T]) DeadLetterLen() int {
	return Read(q.ts, func(*Context) int { return q.inner.DeadLetterLen() })
}

func (q *ThreadSafeWorkQueueCell[T]) ReaderHandles() WorkQueueReaderHandles {
	return q.inner.ReaderHandles()
}

func (q *ThreadSafeWorkQueueCell[T]) PendingItems() []WorkQueueItem[T] {
	return Read(q.ts, func(*Context) []WorkQueueItem[T] { return q.inner.PendingItems() })
}

func (q *ThreadSafeWorkQueueCell[T]) InFlightDeliveries() []WorkQueueDelivery[T] {
	return Read(q.ts, func(*Context) []WorkQueueDelivery[T] {
		return q.inner.InFlightDeliveries()
	})
}

func (q *ThreadSafeWorkQueueCell[T]) DeadLetterItems() []WorkQueueDeadLetter[T] {
	return Read(q.ts, func(*Context) []WorkQueueDeadLetter[T] {
		return q.inner.DeadLetterItems()
	})
}
