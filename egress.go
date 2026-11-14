// EgressCell — the single-threaded reactive egress shell (#lzegress), ported
// from lazily-rs `egress.rs`.
//
// The four projections (pending, inflight, acked_through, retry) are Computeds
// over the pure EgressCore. Transport I/O lives in exactly one Effect per
// attachment (AttachTransport); the core stays pure and is reused unchanged by
// ThreadSafeEgressCell and AsyncEgressCell.
//
// # Four reader kinds, cleared independently
//
// Every core transition reports the reader kinds it moved (EgressChange) and
// the shell clears exactly that set inside one batch. An ack that does not
// advance the watermark invalidates nothing; an enqueue invalidates only the
// pending reader. The readers are unguarded slots on purpose, like the ingress
// family's: "did this transition dirty this reader kind" is the contract the
// corpus asserts, so an invalidation must be observable as a recompute even
// when the recomputed value compares equal.
//
// # No observers
//
// There is no listener list here. A transport reacts by being an Effect over the
// pending and inflight readers; an acknowledgement reopens the send window by
// invalidating inflight, which re-runs that same Effect.

package lazily

// EgressTransport is the only transport seam egress owns. Durability and
// framing remain separate adapters around the envelope.
type EgressTransport[T any] interface {
	// Send reports whether the transport ACCEPTED the send attempt. Domain
	// delivery is proven later, by an acknowledgement.
	Send(envelope EgressEnvelope[T]) bool
}

// EgressTransportFunc adapts a function to EgressTransport.
type EgressTransportFunc[T any] func(envelope EgressEnvelope[T]) bool

// Send calls f.
func (f EgressTransportFunc[T]) Send(envelope EgressEnvelope[T]) bool { return f(envelope) }

// EgressReaders are the four delivery-state readers.
type EgressReaders[T any] struct {
	Pending      *Computed[[]EgressEnvelope[T]]
	Inflight     *Computed[[]EgressEnvelope[T]]
	AckedThrough *Computed[Opt[uint64]]
	Retry        *Computed[Opt[EgressRetry]]
}

// EgressCell is the single-threaded reactive egress.
type EgressCell[T any] struct {
	ctx     *Context
	core    *EgressCore[T]
	readers EgressReaders[T]
}

// NewEgressCell builds a reactive egress at generation under policy.
func NewEgressCell[T any](ctx *Context, generation uint64, policy EgressPolicy) (*EgressCell[T], error) {
	core, err := NewEgressCore[T](generation, policy)
	if err != nil {
		return nil, err
	}
	cell := &EgressCell[T]{ctx: ctx, core: core}
	cell.readers = EgressReaders[T]{
		Pending:      NewSlot(ctx, func(*Compute) []EgressEnvelope[T] { return core.Pending() }),
		Inflight:     NewSlot(ctx, func(*Compute) []EgressEnvelope[T] { return core.Inflight() }),
		AckedThrough: NewSlot(ctx, func(*Compute) Opt[uint64] { return core.AckedThrough() }),
		Retry:        NewSlot(ctx, func(*Compute) Opt[EgressRetry] { return core.Retry() }),
	}
	return cell, nil
}

// apply clears exactly the reader kinds one transition moved, in ONE batch so
// an Effect over several kinds runs once.
func (e *EgressCell[T]) apply(change EgressChange) {
	if change.IsEmpty() {
		return
	}
	e.ctx.Batch(func() { invalidateEgressReaders(e.readers, change) })
}

func invalidateEgressReaders[T any](readers EgressReaders[T], change EgressChange) {
	if change.Pending {
		readers.Pending.invalidate()
	}
	if change.Inflight {
		readers.Inflight.invalidate()
	}
	if change.AckedThrough {
		readers.AckedThrough.invalidate()
	}
	if change.Retry {
		readers.Retry.invalidate()
	}
}

// Readers returns the four reader handles.
func (e *EgressCell[T]) Readers() EgressReaders[T] { return e.readers }

// Generation is the current producer generation.
func (e *EgressCell[T]) Generation() uint64 { return e.core.Generation() }

// NextSequence is the sequence the next Enqueue will assign.
func (e *EgressCell[T]) NextSequence() uint64 { return e.core.NextSequence() }

// Enqueue assigns the next sequence to payload.
func (e *EgressCell[T]) Enqueue(payload T) uint64 {
	change, sequence := e.core.Enqueue(payload)
	e.apply(change)
	return sequence
}

// Claim moves one pending record into the window at the caller's generation.
func (e *EgressCell[T]) Claim(generation uint64) EgressClaim[T] {
	change, claim := e.core.Claim(generation)
	e.apply(change)
	return claim
}

// Ack advances the cumulative acknowledgement watermark.
func (e *EgressCell[T]) Ack(generation, through uint64) EgressAck {
	change, ack := e.core.Ack(generation, through)
	e.apply(change)
	return ack
}

// Fail reports a failed send of sequence.
func (e *EgressCell[T]) Fail(generation, sequence uint64) EgressFailure {
	change, failure := e.core.Fail(generation, sequence)
	e.apply(change)
	return failure
}

// RetryNow makes a parked failed record eligible again.
func (e *EgressCell[T]) RetryNow(generation, sequence uint64) EgressRetryAction {
	change, action := e.core.RetryNow(generation, sequence)
	e.apply(change)
	return action
}

// Reconnect advances the producer generation, fencing the old attachment.
func (e *EgressCell[T]) Reconnect(generation uint64) EgressReconnect {
	change, reconnect := e.core.Reconnect(generation)
	e.apply(change)
	return reconnect
}

// Pending is the reactive read of the pending projection. A *Compute registers a
// dependency edge; a *Context registers none.
func (e *EgressCell[T]) Pending(c ComputeOps) []EgressEnvelope[T] {
	return Get[[]EgressEnvelope[T]](c, e.readers.Pending)
}

// Inflight is the reactive read of the unacknowledged window.
func (e *EgressCell[T]) Inflight(c ComputeOps) []EgressEnvelope[T] {
	return Get[[]EgressEnvelope[T]](c, e.readers.Inflight)
}

// AckedThrough is the reactive read of the acknowledgement watermark.
func (e *EgressCell[T]) AckedThrough(c ComputeOps) Opt[uint64] {
	return Get[Opt[uint64]](c, e.readers.AckedThrough)
}

// Retry is the reactive read of the live retry evidence.
func (e *EgressCell[T]) Retry(c ComputeOps) Opt[EgressRetry] {
	return Get[Opt[EgressRetry]](c, e.readers.Retry)
}

// AttachTransport attaches one transport Effect at the CURRENT producer
// generation.
//
// A reconnect does not mutate this Effect's captured generation, so the old
// attachment becomes inert at the core fence; callers attach one replacement
// Effect for the new transport incarnation and Dispose the old one.
func (e *EgressCell[T]) AttachTransport(transport EgressTransport[T]) *Effect {
	generation := e.Generation()
	return NewEffect(e.ctx, func(c *Compute) func() {
		// inflight is a dependency even though the core remains the authority:
		// an acknowledgement reopens the send window and must re-run this same
		// attachment Effect.
		Get[[]EgressEnvelope[T]](c, e.readers.Inflight)
		if len(Get[[]EgressEnvelope[T]](c, e.readers.Pending)) == 0 {
			return nil
		}
		drainEgressAttachment(generation, e.Claim, e.Fail, transport)
		return nil
	})
}

// drainEgressAttachment claims and sends until the window fills, pending runs
// dry, the attachment is fenced, or a send is refused. Shared by every flavor so
// the attachment loop has one definition.
func drainEgressAttachment[T any](
	generation uint64,
	claim func(uint64) EgressClaim[T],
	fail func(uint64, uint64) EgressFailure,
	transport EgressTransport[T],
) {
	for {
		claimed := claim(generation)
		if claimed.Kind != EgressClaimClaimed {
			return
		}
		if !transport.Send(claimed.Envelope) {
			fail(generation, claimed.Envelope.Sequence)
			return
		}
	}
}
