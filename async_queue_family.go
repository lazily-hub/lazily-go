package lazily

import (
	"context"
	"sync"
)

// resolveAsyncReader drives a synchronous-value reader kind living on the
// async graph. A nil compute surface is an untracked top-level read; a non-nil
// surface registers the caller on the derived reader before returning.
func resolveAsyncReader[T any](cc *AsyncComputeContext, reader *AsyncSlotHandle[T]) T {
	var (
		value T
		err   error
	)
	if cc == nil {
		value, err = reader.GetAsync(context.Background())
	} else {
		value, err = TrackAsync(cc, reader)
	}
	if err != nil {
		panic(err)
	}
	return value
}

// AsyncQueueReaderHandles exposes the five queue reader kinds on the async
// graph. The content readers are memoized derives; Closed is a direct input.
type AsyncQueueReaderHandles[T comparable] struct {
	Head     *AsyncSlotHandle[queueHead[T]]
	Len      *AsyncSlotHandle[int]
	IsEmpty  *AsyncSlotHandle[bool]
	IsFull   *AsyncSlotHandle[bool]
	IsClosed *AsyncCellHandle[bool]
}

// AsyncQueueCell is the AsyncContext FIFO flavor. Storage is serialized by mu;
// changed reader kinds are invalidated together through AsyncContext.Batch.
type AsyncQueueCell[T comparable, S QueueStorage[T]] struct {
	ctx     *AsyncContext
	mu      sync.Mutex
	storage S

	bounded  bool
	capacity int
	peek     func() (T, bool)

	headVersion  *AsyncCellHandle[uint64]
	lenVersion   *AsyncCellHandle[uint64]
	emptyVersion *AsyncCellHandle[uint64]
	fullVersion  *AsyncCellHandle[uint64]
	versions     [4]uint64

	readers AsyncQueueReaderHandles[T]
}

func NewAsyncQueueCell[T comparable](ctx *AsyncContext) *AsyncQueueCell[T, *VecDequeStorage[T]] {
	return NewAsyncQueueCellWithStorage[T](ctx, NewVecDequeStorage[T]())
}

func NewBoundedAsyncQueueCell[T comparable](
	ctx *AsyncContext,
	capacity int,
) *AsyncQueueCell[T, *VecDequeStorage[T]] {
	return NewAsyncQueueCellWithStorage[T](ctx, NewBoundedVecDequeStorage[T](capacity))
}

func NewAsyncQueueCellWithStorage[T comparable, S QueueStorage[T]](
	ctx *AsyncContext,
	storage S,
) *AsyncQueueCell[T, S] {
	q := &AsyncQueueCell[T, S]{ctx: ctx, storage: storage}
	if bounded, ok := any(storage).(BoundedStorage); ok {
		if capacity, ok := bounded.Capacity(); ok {
			q.bounded = true
			q.capacity = capacity
		}
	}
	if peekable, ok := any(storage).(PeekableStorage[T]); ok {
		q.peek = peekable.Peek
	}
	q.headVersion = NewAsyncCell(ctx, uint64(0))
	q.lenVersion = NewAsyncCell(ctx, uint64(0))
	q.emptyVersion = NewAsyncCell(ctx, uint64(0))
	q.fullVersion = NewAsyncCell(ctx, uint64(0))
	q.readers.Head = NewAsyncSlot(ctx, func(cc *AsyncComputeContext) (queueHead[T], error) {
		TrackCell(cc, q.headVersion)
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.peek == nil {
			return queueHead[T]{}, nil
		}
		value, ok := q.peek()
		return queueHead[T]{value: value, ok: ok}, nil
	})
	q.readers.Len = NewAsyncSlot(ctx, func(cc *AsyncComputeContext) (int, error) {
		TrackCell(cc, q.lenVersion)
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.storage.Len(), nil
	})
	q.readers.IsEmpty = NewAsyncSlot(ctx, func(cc *AsyncComputeContext) (bool, error) {
		TrackCell(cc, q.emptyVersion)
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.storage.Len() == 0, nil
	})
	q.readers.IsFull = NewAsyncSlot(ctx, func(cc *AsyncComputeContext) (bool, error) {
		TrackCell(cc, q.fullVersion)
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.bounded && q.storage.Len() >= q.capacity, nil
	})
	q.readers.IsClosed = NewAsyncCell(ctx, storage.IsClosed())
	return q
}

// invalidateLocked publishes every changed reader root in one async batch.
// Caller holds mu so storage and version publication are one linearized op.
func (q *AsyncQueueCell[T, S]) invalidateLocked(
	lenBefore, lenAfter int,
	headChanged bool,
) {
	emptyChanged := (lenBefore == 0) != (lenAfter == 0)
	fullChanged := q.bounded &&
		(lenBefore >= q.capacity) != (lenAfter >= q.capacity)
	q.versions[0]++
	if emptyChanged {
		q.versions[1]++
	}
	if fullChanged {
		q.versions[2]++
	}
	if headChanged {
		q.versions[3]++
	}
	q.ctx.Batch(func() {
		q.lenVersion.Set(q.versions[0])
		if emptyChanged {
			q.emptyVersion.Set(q.versions[1])
		}
		if fullChanged {
			q.fullVersion.Set(q.versions[2])
		}
		if headChanged {
			q.headVersion.Set(q.versions[3])
		}
	})
}

func (q *AsyncQueueCell[T, S]) TryPush(value T) QueuePushError {
	q.mu.Lock()
	defer q.mu.Unlock()
	lenBefore := q.storage.Len()
	result := q.storage.TryPush(value)
	if result.Ok() {
		q.invalidateLocked(lenBefore, lenBefore+1, lenBefore == 0)
	}
	return result
}

func (q *AsyncQueueCell[T, S]) TryPop() (T, QueuePopError) {
	q.mu.Lock()
	defer q.mu.Unlock()
	lenBefore := q.storage.Len()
	value, result := q.storage.TryPop()
	if result.Ok() {
		q.invalidateLocked(lenBefore, lenBefore-1, true)
	}
	return value, result
}

func (q *AsyncQueueCell[T, S]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.storage.IsClosed() {
		return
	}
	q.storage.Close()
	q.readers.IsClosed.Set(true)
}

func (q *AsyncQueueCell[T, S]) Head(cc *AsyncComputeContext) (T, bool) {
	head := resolveAsyncReader(cc, q.readers.Head)
	return head.value, head.ok
}

func (q *AsyncQueueCell[T, S]) Len(cc *AsyncComputeContext) int {
	return resolveAsyncReader(cc, q.readers.Len)
}

func (q *AsyncQueueCell[T, S]) IsEmpty(cc *AsyncComputeContext) bool {
	return resolveAsyncReader(cc, q.readers.IsEmpty)
}

func (q *AsyncQueueCell[T, S]) IsFull(cc *AsyncComputeContext) bool {
	return resolveAsyncReader(cc, q.readers.IsFull)
}

func (q *AsyncQueueCell[T, S]) IsClosed(cc *AsyncComputeContext) bool {
	if cc != nil {
		return TrackCell(cc, q.readers.IsClosed)
	}
	return q.readers.IsClosed.Peek()
}

func (q *AsyncQueueCell[T, S]) Capacity() (int, bool) {
	return q.capacity, q.bounded
}

func (q *AsyncQueueCell[T, S]) Elements() []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	if storage, ok := any(q.storage).(interface{ Elements() []T }); ok {
		return storage.Elements()
	}
	return nil
}

func (q *AsyncQueueCell[T, S]) ReaderHandles() AsyncQueueReaderHandles[T] {
	return q.readers
}

type asyncTopicReader[T any] struct {
	versionValue uint64
	version      *AsyncCellHandle[uint64]
	slot         *AsyncSlotHandle[TopicRead[T]]
}

// AsyncTopicCell is the AsyncContext broadcast-log flavor.
type AsyncTopicCell[T any] struct {
	ctx     *AsyncContext
	mu      sync.Mutex
	inner   *TopicCell[T]
	readers map[string]*asyncTopicReader[T]
}

func NewAsyncTopicCell[T any](ctx *AsyncContext) *AsyncTopicCell[T] {
	return newAsyncTopicCell(ctx, NewTopicCell[T](NewContext()))
}

func NewAsyncTopicCellFromSnapshot[T any](
	ctx *AsyncContext,
	snapshot TopicSnapshot[T],
) *AsyncTopicCell[T] {
	t := newAsyncTopicCell(ctx, NewTopicCellFromSnapshot(NewContext(), snapshot))
	t.mu.Lock()
	for _, saved := range snapshot.Subscriptions {
		t.ensureReaderLocked(saved.ID)
	}
	t.mu.Unlock()
	return t
}

func newAsyncTopicCell[T any](ctx *AsyncContext, inner *TopicCell[T]) *AsyncTopicCell[T] {
	return &AsyncTopicCell[T]{
		ctx: ctx, inner: inner, readers: make(map[string]*asyncTopicReader[T]),
	}
}

func (t *AsyncTopicCell[T]) ensureReaderLocked(id string) *asyncTopicReader[T] {
	if reader := t.readers[id]; reader != nil {
		return reader
	}
	reader := &asyncTopicReader[T]{version: NewAsyncCell(t.ctx, uint64(0))}
	reader.slot = NewAsyncSlot(t.ctx, func(cc *AsyncComputeContext) (TopicRead[T], error) {
		TrackCell(cc, reader.version)
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.inner.readUntracked(id), nil
	})
	t.readers[id] = reader
	return reader
}

func (t *AsyncTopicCell[T]) invalidateReadersLocked(ids []string) {
	if len(ids) == 0 {
		return
	}
	t.ctx.Batch(func() {
		for _, id := range ids {
			reader := t.ensureReaderLocked(id)
			reader.versionValue++
			reader.version.Set(reader.versionValue)
		}
	})
}

func (t *AsyncTopicCell[T]) Subscribe(
	id string,
	durability TopicDurability,
) TopicSubscribeOutcome {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ensureReaderLocked(id)
	result := t.inner.Subscribe(id, durability)
	if result == TopicSubscribed || result == TopicReconnected {
		t.invalidateReadersLocked([]string{id})
	}
	return result
}

func (t *AsyncTopicCell[T]) Reconnect(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	before, ok := t.inner.Subscription(id)
	t.inner.Reconnect(id)
	if ok && !before.Connected {
		t.invalidateReadersLocked([]string{id})
	}
}

func (t *AsyncTopicCell[T]) Disconnect(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	before, ok := t.inner.Subscription(id)
	t.inner.Disconnect(id)
	if ok && before.Connected {
		t.invalidateReadersLocked([]string{id})
	}
}

func (t *AsyncTopicCell[T]) Publish(value T) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	ids := make([]string, 0, len(t.inner.subscriptions))
	for id, sub := range t.inner.subscriptions {
		if sub.connected {
			ids = append(ids, id)
		}
	}
	offset := t.inner.Publish(value)
	t.invalidateReadersLocked(ids)
	return offset
}

func (t *AsyncTopicCell[T]) ReadStream(cc *AsyncComputeContext, id string) ([]T, bool) {
	t.mu.Lock()
	reader := t.ensureReaderLocked(id)
	t.mu.Unlock()
	read := resolveAsyncReader(cc, reader.slot)
	return read.Elements, read.Exists
}

func (t *AsyncTopicCell[T]) Read(cc *AsyncComputeContext, id string) (T, bool) {
	values, exists := t.ReadStream(cc, id)
	if exists && len(values) > 0 {
		return values[0], true
	}
	var zero T
	return zero, false
}

func (t *AsyncTopicCell[T]) Advance(id string, count int) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	before, ok := t.inner.Subscription(id)
	cursor := t.inner.Advance(id, count)
	if ok && before.Connected && cursor != before.Cursor {
		t.invalidateReadersLocked([]string{id})
	}
	return cursor
}

func (t *AsyncTopicCell[T]) GC() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inner.GC()
}

func (t *AsyncTopicCell[T]) BaseOffset() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inner.BaseOffset()
}

func (t *AsyncTopicCell[T]) TailOffset() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inner.TailOffset()
}

func (t *AsyncTopicCell[T]) Elements() []T {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inner.Elements()
}

func (t *AsyncTopicCell[T]) Subscription(id string) (TopicSubscriptionSnapshot, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inner.Subscription(id)
}

func (t *AsyncTopicCell[T]) ReaderHandle(id string) *AsyncSlotHandle[TopicRead[T]] {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ensureReaderLocked(id).slot
}

func (t *AsyncTopicCell[T]) Snapshot() TopicSnapshot[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inner.Snapshot()
}

func (t *AsyncTopicCell[T]) Restart() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inner.Restart()
}

// AsyncWorkQueueReaderHandles exposes the four lifecycle derives.
type AsyncWorkQueueReaderHandles struct {
	PendingLen    *AsyncSlotHandle[int]
	IsEmpty       *AsyncSlotHandle[bool]
	InFlightLen   *AsyncSlotHandle[int]
	DeadLetterLen *AsyncSlotHandle[int]
}

// AsyncWorkQueueCell is the AsyncContext competing-consumer flavor.
type AsyncWorkQueueCell[T any] struct {
	ctx   *AsyncContext
	mu    sync.Mutex
	inner *WorkQueueCell[T]

	versionValues [4]uint64
	versions      [4]*AsyncCellHandle[uint64]
	readers       AsyncWorkQueueReaderHandles
}

func NewAsyncWorkQueueCell[T any](
	ctx *AsyncContext,
	visibilityTimeout int64,
	maxDeliveries uint64,
) *AsyncWorkQueueCell[T] {
	q := &AsyncWorkQueueCell[T]{
		ctx: ctx,
		inner: NewWorkQueueCell[T](
			NewContext(), visibilityTimeout, maxDeliveries,
		),
	}
	for i := range q.versions {
		q.versions[i] = NewAsyncCell(ctx, uint64(0))
	}
	q.readers.PendingLen = NewAsyncSlot(ctx, func(cc *AsyncComputeContext) (int, error) {
		TrackCell(cc, q.versions[0])
		q.mu.Lock()
		defer q.mu.Unlock()
		return len(q.inner.pending), nil
	})
	q.readers.IsEmpty = NewAsyncSlot(ctx, func(cc *AsyncComputeContext) (bool, error) {
		TrackCell(cc, q.versions[1])
		q.mu.Lock()
		defer q.mu.Unlock()
		return len(q.inner.pending) == 0, nil
	})
	q.readers.InFlightLen = NewAsyncSlot(ctx, func(cc *AsyncComputeContext) (int, error) {
		TrackCell(cc, q.versions[2])
		q.mu.Lock()
		defer q.mu.Unlock()
		return len(q.inner.inFlight), nil
	})
	q.readers.DeadLetterLen = NewAsyncSlot(ctx, func(cc *AsyncComputeContext) (int, error) {
		TrackCell(cc, q.versions[3])
		q.mu.Lock()
		defer q.mu.Unlock()
		return len(q.inner.deadLetters), nil
	})
	return q
}

func (q *AsyncWorkQueueCell[T]) countsLocked() [3]int {
	return [3]int{
		len(q.inner.pending), len(q.inner.inFlight), len(q.inner.deadLetters),
	}
}

func (q *AsyncWorkQueueCell[T]) invalidateLocked(before [3]int) {
	after := q.countsLocked()
	changed := [4]bool{
		before[0] != after[0],
		(before[0] == 0) != (after[0] == 0),
		before[1] != after[1],
		before[2] != after[2],
	}
	q.ctx.Batch(func() {
		for i, didChange := range changed {
			if !didChange {
				continue
			}
			q.versionValues[i]++
			q.versions[i].Set(q.versionValues[i])
		}
	})
}

func (q *AsyncWorkQueueCell[T]) Push(value T) uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	before := q.countsLocked()
	result := q.inner.Push(value)
	q.invalidateLocked(before)
	return result
}

func (q *AsyncWorkQueueCell[T]) Claim(worker string, now int64) (WorkQueueDelivery[T], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	before := q.countsLocked()
	delivery, ok := q.inner.Claim(worker, now)
	q.invalidateLocked(before)
	return delivery, ok
}

func (q *AsyncWorkQueueCell[T]) Ack(worker string, deliveryID uint64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	before := q.countsLocked()
	ok := q.inner.Ack(worker, deliveryID)
	q.invalidateLocked(before)
	return ok
}

func (q *AsyncWorkQueueCell[T]) Nack(worker string, deliveryID uint64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	before := q.countsLocked()
	ok := q.inner.Nack(worker, deliveryID)
	q.invalidateLocked(before)
	return ok
}

func (q *AsyncWorkQueueCell[T]) ReapExpired(now int64) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	before := q.countsLocked()
	count := q.inner.ReapExpired(now)
	q.invalidateLocked(before)
	return count
}

func (q *AsyncWorkQueueCell[T]) PendingLen(cc *AsyncComputeContext) int {
	return resolveAsyncReader(cc, q.readers.PendingLen)
}

func (q *AsyncWorkQueueCell[T]) IsEmpty(cc *AsyncComputeContext) bool {
	return resolveAsyncReader(cc, q.readers.IsEmpty)
}

func (q *AsyncWorkQueueCell[T]) InFlightLen(cc *AsyncComputeContext) int {
	return resolveAsyncReader(cc, q.readers.InFlightLen)
}

func (q *AsyncWorkQueueCell[T]) DeadLetterLen(cc *AsyncComputeContext) int {
	return resolveAsyncReader(cc, q.readers.DeadLetterLen)
}

func (q *AsyncWorkQueueCell[T]) ReaderHandles() AsyncWorkQueueReaderHandles {
	return q.readers
}

func (q *AsyncWorkQueueCell[T]) PendingItems() []WorkQueueItem[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.inner.PendingItems()
}

func (q *AsyncWorkQueueCell[T]) InFlightDeliveries() []WorkQueueDelivery[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.inner.InFlightDeliveries()
}

func (q *AsyncWorkQueueCell[T]) DeadLetterItems() []WorkQueueDeadLetter[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.inner.DeadLetterItems()
}
