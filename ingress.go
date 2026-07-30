// IngressCell — the single-threaded flavor of the transport-agnostic reactive
// ingress family (#designimplementtransport).
//
// The admission algebra lives in the flavor-neutral IngressCore; this shell adds
// only the reactivity — four memoized readers per keyed scope plus three receipt
// readers and a derived schedule, minted on *this* context's graph.
//
// # Readiness, authority, and retry are derives, not refresh calls
//
// The point of the family: nothing here polls a connection to find out whether it
// is healthy. Readiness, Authority, and Retry are Computed reads over scope
// state, so a consumer that reads readiness is a graph dependent of exactly the
// transitions that can change it — and a transition that cannot (a buffered
// out-of-order envelope, a tick inside the freshness horizon) invalidates
// nothing. IngressCore returns the invalidation set for every transition, and
// this shell clears precisely that set.
//
// # Why four reader kinds per scope and not one
//
// Collapsing them into one reader would make an error deepen a backoff *and*
// re-render a value that did not change. The four boundaries are distinct in the
// algebra (IngressScopeChange), so they are distinct here.
//
// # No observers
//
// There is no listener list, subscription set, or observer registry anywhere in
// this family. Anything that survived an invalidation would not be a graph edge.
// A consumer reacts by being an Effect over a reader handle.

package lazily

// IngressWindow is the value a scope's window reader carries: the coalesced
// payload plus whether one is present. The Go analogue of Option<T> for a
// non-comparable T (Opt requires comparable; the window payload need not be).
type IngressWindow[T any] struct {
	Present bool
	Value   T
}

// IngressScopeReaders are the four reader kinds one keyed scope exposes. They are
// unguarded slots on purpose: an invalidation must be observable as a recompute
// even when the recomputed value compares equal, because "did this transition
// dirty this reader kind" is the contract the corpus asserts.
type IngressScopeReaders[T any] struct {
	Value     *Computed[IngressWindow[T]]
	Readiness *Computed[IngressReadiness]
	Authority *Computed[Opt[IngressAuthority]]
	Retry     *Computed[Opt[IngressRetry]]
}

// IngressReceiptReaders are the three receipt channels, as three separate readers
// because they have three separate consumers: a projection wants accepts, a
// dashboard wants drops, a supervisor wants errors.
type IngressReceiptReaders[K comparable] struct {
	Accepted *Computed[[]IngressReceipt[K]]
	Dropped  *Computed[[]IngressReceipt[K]]
	Errors   *Computed[[]IngressReceipt[K]]
}

// IngressCell is a keyed, lifecycle-scoped reactive ingress: one admission plane
// per key, with readiness, authority, and retry as derives rather than calls.
type IngressCell[K comparable, T any] struct {
	ctx    *Context
	core   *IngressCore[K, T]
	scopes map[K]IngressScopeReaders[T]

	receipts IngressReceiptReaders[K]

	transportKind *Source[IngressTransportKind]
	pollInterval  *Source[uint64]
	schedule      *Computed[IngressSchedule]
}

// NewIngressCell builds an ingress over policy, folding the hot window under
// merge and delivering as kind.
//
// pollInterval is retained even for an event channel so a later SetTransport to
// BoundedPolling has a bound to fall back to rather than inventing one.
func NewIngressCell[K comparable, T any](
	ctx *Context,
	policy IngressPolicy,
	merge MergePolicy[T],
	kind IngressTransportKind,
	pollInterval uint64,
) (*IngressCell[K, T], error) {
	core, err := NewIngressCore[K, T](policy, merge)
	if err != nil {
		return nil, err
	}
	in := &IngressCell[K, T]{
		ctx:    ctx,
		core:   core,
		scopes: make(map[K]IngressScopeReaders[T]),
	}
	in.receipts = IngressReceiptReaders[K]{
		Accepted: in.receiptReader(IngressReceiptAccepted),
		Dropped:  in.receiptReader(IngressReceiptDropped),
		Errors:   in.receiptReader(IngressReceiptError),
	}
	in.transportKind = NewSource(ctx, kind)
	in.pollInterval = NewSource(ctx, pollInterval)
	in.schedule = NewSlot(ctx, func(c *Compute) IngressSchedule {
		return IngressScheduleFor(
			Get[IngressTransportKind](c, in.transportKind),
			Get[uint64](c, in.pollInterval),
		)
	})
	return in, nil
}

func (in *IngressCell[K, T]) receiptReader(
	channel IngressReceiptChannel,
) *Computed[[]IngressReceipt[K]] {
	return NewSlot(in.ctx, func(*Compute) []IngressReceipt[K] {
		return in.core.Receipts(channel)
	})
}

// ScopeReaders mints (or returns) one scope's four readers. Idempotent, so a
// consumer may hold handles for a key that has not opened yet — which is why an
// unknown scope reads Unknown/absent rather than erroring.
func (in *IngressCell[K, T]) ScopeReaders(key K) IngressScopeReaders[T] {
	if readers, ok := in.scopes[key]; ok {
		return readers
	}
	readers := IngressScopeReaders[T]{
		Value: NewSlot(in.ctx, func(*Compute) IngressWindow[T] {
			value, ok := in.core.Peek(key)
			return IngressWindow[T]{Present: ok, Value: value}
		}),
		Readiness: NewSlot(in.ctx, func(*Compute) IngressReadiness {
			return in.core.Readiness(key)
		}),
		Authority: NewSlot(in.ctx, func(*Compute) Opt[IngressAuthority] {
			authority, ok := in.core.Authority(key)
			return Opt[IngressAuthority]{Present: ok, Value: authority}
		}),
		Retry: NewSlot(in.ctx, func(*Compute) Opt[IngressRetry] {
			retry, ok := in.core.Retry(key)
			return Opt[IngressRetry]{Present: ok, Value: retry}
		}),
	}
	in.scopes[key] = readers
	return readers
}

// ReceiptReaders returns the three receipt-channel handles.
func (in *IngressCell[K, T]) ReceiptReaders() IngressReceiptReaders[K] { return in.receipts }

// ScheduleReader returns the derived-schedule handle.
func (in *IngressCell[K, T]) ScheduleReader() *Computed[IngressSchedule] { return in.schedule }

// apply applies one core-reported invalidation set. Every affected reader is
// cleared inside ONE batch, so the cascade and its effect flush happen once: no
// reader observes a partial fan-out, and a generation handoff is never visible as
// "new value, old authority".
func (in *IngressCell[K, T]) apply(change IngressChange[K]) {
	if change.IsEmpty() {
		return
	}
	in.ctx.Batch(func() {
		for _, delta := range change.Scopes {
			readers := in.ScopeReaders(delta.Key)
			if delta.Change.Value {
				readers.Value.invalidate()
			}
			if delta.Change.Readiness {
				readers.Readiness.invalidate()
			}
			if delta.Change.Authority {
				readers.Authority.invalidate()
			}
			if delta.Change.Retry {
				readers.Retry.invalidate()
			}
		}
		if change.AcceptedReceipts {
			in.receipts.Accepted.invalidate()
		}
		if change.DroppedReceipts {
			in.receipts.Dropped.invalidate()
		}
		if change.ErrorReceipts {
			in.receipts.Errors.invalidate()
		}
	})
}

// Open opens (or reopens) a keyed scope at generation.
func (in *IngressCell[K, T]) Open(key K, generation uint64) {
	in.apply(in.core.Open(key, generation))
}

// Admit admits one decoded envelope.
func (in *IngressCell[K, T]) Admit(envelope IngressEnvelope[K, T]) IngressAdmission {
	change, admission := in.core.Admit(envelope)
	in.apply(change)
	return admission
}

// Suspend suspends a scope, retaining its watermark. It returns the replay request
// a reconnect will need.
func (in *IngressCell[K, T]) Suspend(key K) (ReplayRequest, bool) {
	change, request, ok := in.core.Suspend(key)
	in.apply(change)
	return request, ok
}

// Reconnect reconnects a scope at generation, clearing its error streak.
func (in *IngressCell[K, T]) Reconnect(key K, generation uint64) ReplayRequest {
	change, request := in.core.Reconnect(key, generation)
	in.apply(change)
	return request
}

// Close closes a scope. It admits nothing and claims no authority until reopened.
func (in *IngressCell[K, T]) Close(key K) { in.apply(in.core.Close(key)) }

// Fail records a transport/decode failure, deepening the scope's backoff.
func (in *IngressCell[K, T]) Fail(key K, err IngressError) {
	in.apply(in.core.Fail(key, err))
}

// Tick advances logical time. Only scopes that crossed the freshness horizon are
// invalidated.
func (in *IngressCell[K, T]) Tick(now uint64) { in.apply(in.core.Tick(now)) }

// Drain drains a scope's coalesced window.
func (in *IngressCell[K, T]) Drain(key K) (T, bool) {
	change, value, ok := in.core.Drain(key)
	in.apply(change)
	return value, ok
}

// Pump admits everything transport has decoded, then asks it to replay any gap
// still open, returning the admission outcomes in arrival order.
//
// This is the only method that touches a transport, and it makes no decision of
// its own: the gap it replays is the one the algebra reports.
func (in *IngressCell[K, T]) Pump(transport IngressTransport[K, T]) []IngressAdmission {
	batch := transport.Drain()
	outcomes := make([]IngressAdmission, 0, len(batch))
	var touched []K
	for _, envelope := range batch {
		key := envelope.Key
		outcomes = append(outcomes, in.Admit(envelope))
		if !ingressContainsKey(touched, key) {
			touched = append(touched, key)
		}
	}
	for _, key := range touched {
		view, ok := in.core.View(key)
		if !ok || !view.HasGap() {
			continue
		}
		transport.RequestReplay(key, ReplayRequest{
			Generation:   view.Generation,
			FromSequence: view.ResumeFrom(),
		})
	}
	return outcomes
}

func ingressContainsKey[K comparable](keys []K, key K) bool {
	for _, seen := range keys {
		if seen == key {
			return true
		}
	}
	return false
}

// Value is the reactive read of the coalesced window awaiting drain. c is the
// value-threaded read surface: a *Compute registers a dependency edge, a *Context
// registers none.
func (in *IngressCell[K, T]) Value(c ComputeOps, key K) (T, bool) {
	window := Get[IngressWindow[T]](c, in.ScopeReaders(key).Value)
	return window.Value, window.Present
}

// Readiness is the reactive read of derived readiness.
func (in *IngressCell[K, T]) Readiness(c ComputeOps, key K) IngressReadiness {
	return Get[IngressReadiness](c, in.ScopeReaders(key).Readiness)
}

// Authority is the reactive read of derived authority.
func (in *IngressCell[K, T]) Authority(c ComputeOps, key K) (IngressAuthority, bool) {
	opt := Get[Opt[IngressAuthority]](c, in.ScopeReaders(key).Authority)
	return opt.Value, opt.Present
}

// Retry is the reactive read of the derived retry decision.
func (in *IngressCell[K, T]) Retry(c ComputeOps, key K) (IngressRetry, bool) {
	opt := Get[Opt[IngressRetry]](c, in.ScopeReaders(key).Retry)
	return opt.Value, opt.Present
}

// Accepted is the reactive read of accepted receipts, oldest first.
func (in *IngressCell[K, T]) Accepted(c ComputeOps) []IngressReceipt[K] {
	return Get[[]IngressReceipt[K]](c, in.receipts.Accepted)
}

// Dropped is the reactive read of dropped receipts, oldest first.
func (in *IngressCell[K, T]) Dropped(c ComputeOps) []IngressReceipt[K] {
	return Get[[]IngressReceipt[K]](c, in.receipts.Dropped)
}

// Errors is the reactive read of error receipts, oldest first.
func (in *IngressCell[K, T]) Errors(c ComputeOps) []IngressReceipt[K] {
	return Get[[]IngressReceipt[K]](c, in.receipts.Errors)
}

// Schedule is the reactive read of the derived delivery schedule.
func (in *IngressCell[K, T]) Schedule(c ComputeOps) IngressSchedule {
	return Get[IngressSchedule](c, in.schedule)
}

// SetTransport retunes the transport live: falling back from an event channel to
// bounded polling is a cell write, so every schedule dependent reacts.
func (in *IngressCell[K, T]) SetTransport(kind IngressTransportKind) {
	in.transportKind.Set(kind)
}

// SetPollInterval retunes the poll bound live.
func (in *IngressCell[K, T]) SetPollInterval(interval uint64) {
	in.pollInterval.Set(interval)
}

// View is the non-reactive projection of a scope, for assertions and diagnostics.
func (in *IngressCell[K, T]) View(key K) (IngressScopeView, bool) { return in.core.View(key) }

// Policy returns the bounds in force.
func (in *IngressCell[K, T]) Policy() IngressPolicy { return in.core.Policy() }

// ScopeKeys returns every known scope key.
func (in *IngressCell[K, T]) ScopeKeys() []K { return in.core.ScopeKeys() }
