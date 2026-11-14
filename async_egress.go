// AsyncEgressCell — the AsyncContext reactive egress shell (#lzegress), ported
// from lazily-rs `async_egress.rs`.
//
// Delivery authority is synchronous: every transition below is an ordinary call
// returning a plain value, and the four delivery-state readers are synchronous
// computes living on the async graph. Only the transport attachment is
// async-coloured — it is an AsyncContext effect whose body runs off the event
// loop, so a transport may block or await inside Send without stalling the
// graph.
//
// The async graph has no "clear this computed" primitive; a reader is
// invalidated by writing an input it tracks. So each reader kind is paired with a
// monotone version AsyncSource that the reader tracks and the shell bumps — the
// same shape AsyncIngressCell uses. Every version a transition dirtied is
// published inside ONE AsyncContext.Batch, with the core lock released.

package lazily

import "sync"

// AsyncEgressReaders are the four delivery-state readers on the async graph.
type AsyncEgressReaders[T any] struct {
	Pending      *AsyncComputed[[]EgressEnvelope[T]]
	Inflight     *AsyncComputed[[]EgressEnvelope[T]]
	AckedThrough *AsyncComputed[Opt[uint64]]
	Retry        *AsyncComputed[Opt[EgressRetry]]
}

const (
	asyncEgressPendingSlot = iota
	asyncEgressInflightSlot
	asyncEgressAckedThroughSlot
	asyncEgressRetrySlot
)

// AsyncEgressCell is the AsyncContext reactive egress.
type AsyncEgressCell[T any] struct {
	ctx *AsyncContext

	// mu guards core and is never held across graph work.
	mu   sync.Mutex
	core *EgressCore[T]

	// versionMu orders version bumps so concurrent transitions publish
	// monotone versions.
	versionMu sync.Mutex
	versions  [4]*AsyncSource[uint64]
	counters  [4]uint64

	readers AsyncEgressReaders[T]
}

// NewAsyncEgressCell builds an async reactive egress at generation.
func NewAsyncEgressCell[T any](ctx *AsyncContext, generation uint64, policy EgressPolicy) (*AsyncEgressCell[T], error) {
	core, err := NewEgressCore[T](generation, policy)
	if err != nil {
		return nil, err
	}
	cell := &AsyncEgressCell[T]{ctx: ctx, core: core}
	for i := range cell.versions {
		cell.versions[i] = NewAsyncSource(ctx, uint64(0))
	}
	cell.readers = AsyncEgressReaders[T]{
		Pending: NewAsyncComputed(ctx, func(cc *AsyncComputeContext) ([]EgressEnvelope[T], error) {
			TrackSource(cc, cell.versions[asyncEgressPendingSlot])
			cell.mu.Lock()
			defer cell.mu.Unlock()
			return cell.core.Pending(), nil
		}),
		Inflight: NewAsyncComputed(ctx, func(cc *AsyncComputeContext) ([]EgressEnvelope[T], error) {
			TrackSource(cc, cell.versions[asyncEgressInflightSlot])
			cell.mu.Lock()
			defer cell.mu.Unlock()
			return cell.core.Inflight(), nil
		}),
		AckedThrough: NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (Opt[uint64], error) {
			TrackSource(cc, cell.versions[asyncEgressAckedThroughSlot])
			cell.mu.Lock()
			defer cell.mu.Unlock()
			return cell.core.AckedThrough(), nil
		}),
		Retry: NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (Opt[EgressRetry], error) {
			TrackSource(cc, cell.versions[asyncEgressRetrySlot])
			cell.mu.Lock()
			defer cell.mu.Unlock()
			return cell.core.Retry(), nil
		}),
	}
	return cell, nil
}

// publish bumps exactly the version inputs the core reported dirty, inside one
// batch so the whole transition is one frontier walk.
func (e *AsyncEgressCell[T]) publish(change EgressChange) {
	if change.IsEmpty() {
		return
	}
	dirty := [4]bool{change.Pending, change.Inflight, change.AckedThrough, change.Retry}
	e.versionMu.Lock()
	for slot, changed := range dirty {
		if changed {
			e.counters[slot]++
		}
	}
	next := e.counters
	e.versionMu.Unlock()
	e.ctx.Batch(func() {
		for slot, changed := range dirty {
			if changed {
				e.versions[slot].Set(next[slot])
			}
		}
	})
}

// transition runs one core mutation and publishes its invalidation set with the
// core lock released.
func (e *AsyncEgressCell[T]) transition(op func() EgressChange) {
	e.mu.Lock()
	change := op()
	e.mu.Unlock()
	e.publish(change)
}

// Readers returns the four reader handles.
func (e *AsyncEgressCell[T]) Readers() AsyncEgressReaders[T] { return e.readers }

// Generation is the current producer generation.
func (e *AsyncEgressCell[T]) Generation() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.core.Generation()
}

// NextSequence is the sequence the next Enqueue will assign.
func (e *AsyncEgressCell[T]) NextSequence() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.core.NextSequence()
}

// Enqueue assigns the next sequence to payload.
func (e *AsyncEgressCell[T]) Enqueue(payload T) uint64 {
	var sequence uint64
	e.transition(func() EgressChange {
		change, next := e.core.Enqueue(payload)
		sequence = next
		return change
	})
	return sequence
}

// Claim moves one pending record into the window at the caller's generation.
func (e *AsyncEgressCell[T]) Claim(generation uint64) EgressClaim[T] {
	var claim EgressClaim[T]
	e.transition(func() EgressChange {
		change, next := e.core.Claim(generation)
		claim = next
		return change
	})
	return claim
}

// Ack advances the cumulative acknowledgement watermark.
func (e *AsyncEgressCell[T]) Ack(generation, through uint64) EgressAck {
	var ack EgressAck
	e.transition(func() EgressChange {
		change, next := e.core.Ack(generation, through)
		ack = next
		return change
	})
	return ack
}

// Fail reports a failed send of sequence.
func (e *AsyncEgressCell[T]) Fail(generation, sequence uint64) EgressFailure {
	var failure EgressFailure
	e.transition(func() EgressChange {
		change, next := e.core.Fail(generation, sequence)
		failure = next
		return change
	})
	return failure
}

// RetryNow makes a parked failed record eligible again.
func (e *AsyncEgressCell[T]) RetryNow(generation, sequence uint64) EgressRetryAction {
	var action EgressRetryAction
	e.transition(func() EgressChange {
		change, next := e.core.RetryNow(generation, sequence)
		action = next
		return change
	})
	return action
}

// Reconnect advances the producer generation, fencing the old attachment.
func (e *AsyncEgressCell[T]) Reconnect(generation uint64) EgressReconnect {
	var reconnect EgressReconnect
	e.transition(func() EgressChange {
		change, next := e.core.Reconnect(generation)
		reconnect = next
		return change
	})
	return reconnect
}

// Pending reads the pending projection. A nil compute surface is an untracked
// top-level read. The read never awaits I/O: the reader is a synchronous compute.
func (e *AsyncEgressCell[T]) Pending(cc *AsyncComputeContext) []EgressEnvelope[T] {
	return resolveAsyncReader(cc, e.readers.Pending)
}

// Inflight reads the unacknowledged window.
func (e *AsyncEgressCell[T]) Inflight(cc *AsyncComputeContext) []EgressEnvelope[T] {
	return resolveAsyncReader(cc, e.readers.Inflight)
}

// AckedThrough reads the acknowledgement watermark.
func (e *AsyncEgressCell[T]) AckedThrough(cc *AsyncComputeContext) Opt[uint64] {
	return resolveAsyncReader(cc, e.readers.AckedThrough)
}

// Retry reads the live retry evidence.
func (e *AsyncEgressCell[T]) Retry(cc *AsyncComputeContext) Opt[EgressRetry] {
	return resolveAsyncReader(cc, e.readers.Retry)
}

// AttachTransport attaches one async transport effect at the CURRENT producer
// generation. The effect tracks the inflight and pending readers (synchronous
// computes) and runs its claim/send loop off the event loop, so Send may block.
func (e *AsyncEgressCell[T]) AttachTransport(transport EgressTransport[T]) *AsyncEffectHandle {
	generation := e.Generation()
	return e.ctx.EffectAsync(func(cc *AsyncComputeContext) func() {
		resolveAsyncReader(cc, e.readers.Inflight)
		if len(resolveAsyncReader(cc, e.readers.Pending)) == 0 {
			return nil
		}
		drainEgressAttachment(generation, e.Claim, e.Fail, transport)
		return nil
	})
}
