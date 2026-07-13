package lazily

import (
	"math"
	"sort"
)

// WorkQueueItem is a stable logical item. Attempts counts completed claims.
type WorkQueueItem[T any] struct {
	ItemID   uint64
	Value    T
	Attempts uint64
}

// WorkQueueDelivery is one exclusive leased delivery to a worker.
type WorkQueueDelivery[T any] struct {
	DeliveryID uint64
	ItemID     uint64
	Value      T
	Worker     string
	Attempt    uint64
	Deadline   int64
}

type WorkQueueDeadLetterReason string

const (
	WorkQueueDeadLetterNack    WorkQueueDeadLetterReason = "nack"
	WorkQueueDeadLetterExpired WorkQueueDeadLetterReason = "expired"
)

type WorkQueueDeadLetter[T any] struct {
	ItemID   uint64
	Value    T
	Attempts uint64
	Reason   WorkQueueDeadLetterReason
}

type WorkQueueReaderHandles struct {
	PendingLen    *Slot[int]
	IsEmpty       *Slot[bool]
	InFlightLen   *Slot[int]
	DeadLetterLen *Slot[int]
}

// WorkQueueCell is a process-local competing-consumer work queue.
//
// Item ids survive retries while every claim receives a fresh delivery id.
// Nack and expired deliveries requeue at the tail until MaxDeliveries is
// reached, then move to the dead-letter list. A lease is live at its deadline
// and expires strictly after it.
//
// This object is a local serialization point. Distributed/HA deployments must
// place a consensus-backed leader or adapter in front of it.
type WorkQueueCell[T any] struct {
	ctx               *Context
	VisibilityTimeout int64
	MaxDeliveries     uint64
	pending           []WorkQueueItem[T]
	inFlight          map[uint64]WorkQueueDelivery[T]
	deadLetters       []WorkQueueDeadLetter[T]
	nextItemID        uint64
	nextDeliveryID    uint64
	pendingLen        *Slot[int]
	isEmpty           *Slot[bool]
	inFlightLen       *Slot[int]
	deadLetterLen     *Slot[int]
}

// NewWorkQueueCell creates an empty queue. It panics for invalid configuration.
func NewWorkQueueCell[T any](ctx *Context, visibilityTimeout int64, maxDeliveries uint64) *WorkQueueCell[T] {
	if visibilityTimeout <= 0 {
		panic("visibilityTimeout must be positive")
	}
	if maxDeliveries < 1 {
		panic("maxDeliveries must be at least one")
	}
	q := &WorkQueueCell[T]{
		ctx:               ctx,
		VisibilityTimeout: visibilityTimeout,
		MaxDeliveries:     maxDeliveries,
		inFlight:          make(map[uint64]WorkQueueDelivery[T]),
	}
	q.pendingLen = NewSlot[int](ctx, func(*Context) int { return len(q.pending) })
	q.isEmpty = NewSlot[bool](ctx, func(*Context) bool { return len(q.pending) == 0 })
	q.inFlightLen = NewSlot[int](ctx, func(*Context) int { return len(q.inFlight) })
	q.deadLetterLen = NewSlot[int](ctx, func(*Context) int { return len(q.deadLetters) })
	return q
}

func (q *WorkQueueCell[T]) counts() (int, int, int) {
	return len(q.pending), len(q.inFlight), len(q.deadLetters)
}

func (q *WorkQueueCell[T]) invalidate(pendingBefore, inFlightBefore, deadBefore int) {
	pendingAfter, inFlightAfter, deadAfter := q.counts()
	q.ctx.Batch(func() {
		if pendingBefore != pendingAfter {
			q.pendingLen.invalidate()
		}
		if (pendingBefore == 0) != (pendingAfter == 0) {
			q.isEmpty.invalidate()
		}
		if inFlightBefore != inFlightAfter {
			q.inFlightLen.invalidate()
		}
		if deadBefore != deadAfter {
			q.deadLetterLen.invalidate()
		}
	})
}

func (q *WorkQueueCell[T]) Push(value T) uint64 {
	pendingBefore, inFlightBefore, deadBefore := q.counts()
	if q.nextItemID == math.MaxUint64 {
		panic("work queue item ids exhausted")
	}
	itemID := q.nextItemID
	q.nextItemID++
	q.pending = append(q.pending, WorkQueueItem[T]{ItemID: itemID, Value: value})
	q.invalidate(pendingBefore, inFlightBefore, deadBefore)
	return itemID
}

func (q *WorkQueueCell[T]) Claim(worker string, now int64) (WorkQueueDelivery[T], bool) {
	if now < 0 {
		panic("now must be non-negative")
	}
	if len(q.pending) == 0 {
		return WorkQueueDelivery[T]{}, false
	}
	if q.nextDeliveryID == math.MaxUint64 {
		panic("work queue delivery ids exhausted")
	}
	if now > math.MaxInt64-q.VisibilityTimeout {
		panic("work queue deadline overflow")
	}
	pendingBefore, inFlightBefore, deadBefore := q.counts()
	item := q.pending[0]
	q.pending = q.pending[1:]
	delivery := WorkQueueDelivery[T]{
		DeliveryID: q.nextDeliveryID,
		ItemID:     item.ItemID,
		Value:      item.Value,
		Worker:     worker,
		Attempt:    item.Attempts + 1,
		Deadline:   now + q.VisibilityTimeout,
	}
	q.nextDeliveryID++
	q.inFlight[delivery.DeliveryID] = delivery
	q.invalidate(pendingBefore, inFlightBefore, deadBefore)
	return delivery, true
}

func (q *WorkQueueCell[T]) Ack(worker string, deliveryID uint64) bool {
	delivery, ok := q.inFlight[deliveryID]
	if !ok || delivery.Worker != worker {
		return false
	}
	pendingBefore, inFlightBefore, deadBefore := q.counts()
	delete(q.inFlight, deliveryID)
	q.invalidate(pendingBefore, inFlightBefore, deadBefore)
	return true
}

func (q *WorkQueueCell[T]) fail(delivery WorkQueueDelivery[T], reason WorkQueueDeadLetterReason) {
	if delivery.Attempt >= q.MaxDeliveries {
		q.deadLetters = append(q.deadLetters, WorkQueueDeadLetter[T]{
			ItemID: delivery.ItemID, Value: delivery.Value,
			Attempts: delivery.Attempt, Reason: reason,
		})
		return
	}
	q.pending = append(q.pending, WorkQueueItem[T]{
		ItemID: delivery.ItemID, Value: delivery.Value, Attempts: delivery.Attempt,
	})
}

func (q *WorkQueueCell[T]) Nack(worker string, deliveryID uint64) bool {
	delivery, ok := q.inFlight[deliveryID]
	if !ok || delivery.Worker != worker {
		return false
	}
	pendingBefore, inFlightBefore, deadBefore := q.counts()
	delete(q.inFlight, deliveryID)
	q.fail(delivery, WorkQueueDeadLetterNack)
	q.invalidate(pendingBefore, inFlightBefore, deadBefore)
	return true
}

func (q *WorkQueueCell[T]) ReapExpired(now int64) int {
	if now < 0 {
		panic("now must be non-negative")
	}
	expired := make([]uint64, 0)
	for deliveryID, delivery := range q.inFlight {
		if delivery.Deadline < now {
			expired = append(expired, deliveryID)
		}
	}
	if len(expired) == 0 {
		return 0
	}
	sort.Slice(expired, func(i, j int) bool { return expired[i] < expired[j] })
	pendingBefore, inFlightBefore, deadBefore := q.counts()
	for _, deliveryID := range expired {
		delivery := q.inFlight[deliveryID]
		delete(q.inFlight, deliveryID)
		q.fail(delivery, WorkQueueDeadLetterExpired)
	}
	q.invalidate(pendingBefore, inFlightBefore, deadBefore)
	return len(expired)
}

func (q *WorkQueueCell[T]) PendingLen() int  { return q.pendingLen.Get() }
func (q *WorkQueueCell[T]) IsEmpty() bool    { return q.isEmpty.Get() }
func (q *WorkQueueCell[T]) InFlightLen() int { return q.inFlightLen.Get() }
func (q *WorkQueueCell[T]) DeadLetterLen() int {
	return q.deadLetterLen.Get()
}

func (q *WorkQueueCell[T]) ReaderHandles() WorkQueueReaderHandles {
	return WorkQueueReaderHandles{
		PendingLen: q.pendingLen, IsEmpty: q.isEmpty,
		InFlightLen: q.inFlightLen, DeadLetterLen: q.deadLetterLen,
	}
}

func (q *WorkQueueCell[T]) PendingItems() []WorkQueueItem[T] {
	return append([]WorkQueueItem[T](nil), q.pending...)
}

func (q *WorkQueueCell[T]) InFlightDeliveries() []WorkQueueDelivery[T] {
	result := make([]WorkQueueDelivery[T], 0, len(q.inFlight))
	for _, delivery := range q.inFlight {
		result = append(result, delivery)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].DeliveryID < result[j].DeliveryID })
	return result
}

func (q *WorkQueueCell[T]) DeadLetterItems() []WorkQueueDeadLetter[T] {
	return append([]WorkQueueDeadLetter[T](nil), q.deadLetters...)
}
