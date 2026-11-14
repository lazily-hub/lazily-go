// ThreadSafeEgressCell — the lock-serialized reactive egress shell (#lzegress),
// ported from lazily-rs `thread_safe_egress.rs`.
//
// Delivery authority stays in the shared EgressCore; this shell adds a mutex
// over the core and mints the four readers on the wrapped ThreadSafeContext's
// graph. It holds the same two invariants as ThreadSafeIngressCell:
//
//  1. The core mutex is NEVER held across graph work. A reader's compute takes
//     the context lock and then the core mutex; a transition that invalidated
//     while still holding the core mutex would invert that order. Lock order is
//     context-then-core, uniformly.
//
//  2. One transition is ONE frontier walk: every reader the core reported dirty
//     is cleared inside a single ts.Batch.
//
// Every READ takes the graph lock too — a Get of a stale reader recomputes, and
// a recompute writes the shared Context (see ThreadSafeIngressCell).

package lazily

import "sync"

// ThreadSafeEgressCell is the goroutine-safe reactive egress.
type ThreadSafeEgressCell[T any] struct {
	ts *ThreadSafeContext

	// mu guards core and is never held across graph work.
	mu   sync.Mutex
	core *EgressCore[T]

	readers EgressReaders[T]
}

// NewThreadSafeEgressCell builds a goroutine-safe reactive egress at generation.
func NewThreadSafeEgressCell[T any](ts *ThreadSafeContext, generation uint64, policy EgressPolicy) (*ThreadSafeEgressCell[T], error) {
	core, err := NewEgressCore[T](generation, policy)
	if err != nil {
		return nil, err
	}
	cell := &ThreadSafeEgressCell[T]{ts: ts, core: core}
	ts.WithLock(func(ctx *Context) {
		cell.readers = EgressReaders[T]{
			Pending: NewSlot(ctx, func(*Compute) []EgressEnvelope[T] {
				cell.mu.Lock()
				defer cell.mu.Unlock()
				return cell.core.Pending()
			}),
			Inflight: NewSlot(ctx, func(*Compute) []EgressEnvelope[T] {
				cell.mu.Lock()
				defer cell.mu.Unlock()
				return cell.core.Inflight()
			}),
			AckedThrough: NewSlot(ctx, func(*Compute) Opt[uint64] {
				cell.mu.Lock()
				defer cell.mu.Unlock()
				return cell.core.AckedThrough()
			}),
			Retry: NewSlot(ctx, func(*Compute) Opt[EgressRetry] {
				cell.mu.Lock()
				defer cell.mu.Unlock()
				return cell.core.Retry()
			}),
		}
	})
	return cell, nil
}

// transition runs op against the core under mu, RELEASES mu, then clears the
// reported readers inside the same ts.Batch.
func (e *ThreadSafeEgressCell[T]) transition(op func() EgressChange) {
	e.ts.Batch(func() {
		e.mu.Lock()
		change := op()
		e.mu.Unlock()
		if !change.IsEmpty() {
			invalidateEgressReaders(e.readers, change)
		}
	})
}

// Readers returns the four reader handles. Read them only under the context
// lock (ts.WithLock / Read) or through the methods below.
func (e *ThreadSafeEgressCell[T]) Readers() EgressReaders[T] { return e.readers }

// Generation is the current producer generation.
func (e *ThreadSafeEgressCell[T]) Generation() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.core.Generation()
}

// NextSequence is the sequence the next Enqueue will assign.
func (e *ThreadSafeEgressCell[T]) NextSequence() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.core.NextSequence()
}

// Enqueue assigns the next sequence to payload.
func (e *ThreadSafeEgressCell[T]) Enqueue(payload T) uint64 {
	var sequence uint64
	e.transition(func() EgressChange {
		change, next := e.core.Enqueue(payload)
		sequence = next
		return change
	})
	return sequence
}

// Claim moves one pending record into the window at the caller's generation.
func (e *ThreadSafeEgressCell[T]) Claim(generation uint64) EgressClaim[T] {
	var claim EgressClaim[T]
	e.transition(func() EgressChange {
		change, next := e.core.Claim(generation)
		claim = next
		return change
	})
	return claim
}

// Ack advances the cumulative acknowledgement watermark.
func (e *ThreadSafeEgressCell[T]) Ack(generation, through uint64) EgressAck {
	var ack EgressAck
	e.transition(func() EgressChange {
		change, next := e.core.Ack(generation, through)
		ack = next
		return change
	})
	return ack
}

// Fail reports a failed send of sequence.
func (e *ThreadSafeEgressCell[T]) Fail(generation, sequence uint64) EgressFailure {
	var failure EgressFailure
	e.transition(func() EgressChange {
		change, next := e.core.Fail(generation, sequence)
		failure = next
		return change
	})
	return failure
}

// RetryNow makes a parked failed record eligible again.
func (e *ThreadSafeEgressCell[T]) RetryNow(generation, sequence uint64) EgressRetryAction {
	var action EgressRetryAction
	e.transition(func() EgressChange {
		change, next := e.core.RetryNow(generation, sequence)
		action = next
		return change
	})
	return action
}

// Reconnect advances the producer generation, fencing the old attachment.
func (e *ThreadSafeEgressCell[T]) Reconnect(generation uint64) EgressReconnect {
	var reconnect EgressReconnect
	e.transition(func() EgressChange {
		change, next := e.core.Reconnect(generation)
		reconnect = next
		return change
	})
	return reconnect
}

// Pending is the reactive read of the pending projection. c is nil for an
// untracked read, or the *Compute of a reader running under this context.
func (e *ThreadSafeEgressCell[T]) Pending(c ComputeOps) []EgressEnvelope[T] {
	return Read(e.ts, func(ctx *Context) []EgressEnvelope[T] {
		return Get[[]EgressEnvelope[T]](tsReadSurface(c, ctx), e.readers.Pending)
	})
}

// Inflight is the reactive read of the unacknowledged window.
func (e *ThreadSafeEgressCell[T]) Inflight(c ComputeOps) []EgressEnvelope[T] {
	return Read(e.ts, func(ctx *Context) []EgressEnvelope[T] {
		return Get[[]EgressEnvelope[T]](tsReadSurface(c, ctx), e.readers.Inflight)
	})
}

// AckedThrough is the reactive read of the acknowledgement watermark.
func (e *ThreadSafeEgressCell[T]) AckedThrough(c ComputeOps) Opt[uint64] {
	return Read(e.ts, func(ctx *Context) Opt[uint64] {
		return Get[Opt[uint64]](tsReadSurface(c, ctx), e.readers.AckedThrough)
	})
}

// Retry is the reactive read of the live retry evidence.
func (e *ThreadSafeEgressCell[T]) Retry(c ComputeOps) Opt[EgressRetry] {
	return Read(e.ts, func(ctx *Context) Opt[EgressRetry] {
		return Get[Opt[EgressRetry]](tsReadSurface(c, ctx), e.readers.Retry)
	})
}

// tsReadSurface defaults a nil read surface to the locked Context (an untracked
// read).
func tsReadSurface(c ComputeOps, ctx *Context) ComputeOps {
	if c == nil {
		return ctx
	}
	return c
}

// AttachTransport attaches one transport Effect at the CURRENT producer
// generation. The Effect runs under the context lock; the transport's Send is
// called from inside it, exactly as the reference shell does.
func (e *ThreadSafeEgressCell[T]) AttachTransport(transport EgressTransport[T]) *Effect {
	generation := e.Generation()
	var effect *Effect
	e.ts.WithLock(func(ctx *Context) {
		effect = NewEffect(ctx, func(c *Compute) func() {
			Get[[]EgressEnvelope[T]](c, e.readers.Inflight)
			if len(Get[[]EgressEnvelope[T]](c, e.readers.Pending)) == 0 {
				return nil
			}
			drainEgressAttachment(generation, e.Claim, e.Fail, transport)
			return nil
		})
	})
	return effect
}

// DisposeTransport disposes an attachment under the context lock.
func (e *ThreadSafeEgressCell[T]) DisposeTransport(effect *Effect) {
	e.ts.WithLock(func(*Context) { effect.Dispose() })
}
