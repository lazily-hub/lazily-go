// AsyncIngressCell — the AsyncContext flavor of the transport-agnostic reactive
// ingress family (#designimplementtransport).
//
// # Admission is not async-coloured
//
// Whether an envelope is admissible is a function of the fence, the watermark, the
// reorder buffer, and the observed clock — state the graph does not own and
// nothing has to await. So every mutator below is an ordinary synchronous call
// returning a plain value, exactly as in the other two flavors, and the reader
// kinds are synchronous computes living on the async graph. Awaiting belongs to
// the transport, and the transport is outside the primitive by construction.
//
// # How invalidation reaches an async reader
//
// The async graph has no "clear this computed" primitive; a reader is invalidated
// by writing an input it tracks. So each reader kind is paired with a monotone
// version AsyncSource that the reader tracks and the shell bumps — the same shape
// AsyncQueueCell and AsyncWorkQueueCell use. Every version a transition dirtied is
// published inside ONE AsyncContext.Batch, so a multi-kind transition propagates
// as a single frontier walk rather than one per reader.
//
// The core lock is released before publishing, so invalidation never runs inside
// the storage lock.

package lazily

import "sync"

// AsyncIngressScopeReaders are the four reader kinds one keyed scope exposes on
// the async graph.
type AsyncIngressScopeReaders[T any] struct {
	Value     *AsyncComputed[IngressWindow[T]]
	Readiness *AsyncComputed[IngressReadiness]
	Authority *AsyncComputed[Opt[IngressAuthority]]
	Retry     *AsyncComputed[Opt[IngressRetry]]
}

// AsyncIngressReceiptReaders are the three receipt channels on the async graph.
type AsyncIngressReceiptReaders[K comparable] struct {
	Accepted *AsyncComputed[[]IngressReceipt[K]]
	Dropped  *AsyncComputed[[]IngressReceipt[K]]
	Errors   *AsyncComputed[[]IngressReceipt[K]]
}

// asyncIngressScope pairs a scope's four readers with the four version inputs
// that invalidate them.
type asyncIngressScope[T any] struct {
	versions [4]*AsyncSource[uint64]
	counters [4]uint64
	readers  AsyncIngressScopeReaders[T]
}

const (
	asyncIngressValueSlot = iota
	asyncIngressReadinessSlot
	asyncIngressAuthoritySlot
	asyncIngressRetrySlot
)

// AsyncIngressCell is the AsyncContext ingress flavor.
type AsyncIngressCell[K comparable, T any] struct {
	ctx *AsyncContext

	// mu guards the core. Released before any version is published, so
	// invalidation never runs inside the storage lock.
	mu   sync.Mutex
	core *IngressCore[K, T]

	// readerMu guards the reader table. Disjoint from mu: a reader's compute
	// takes mu and never readerMu, so the two can never invert.
	readerMu sync.Mutex
	scopes   map[K]*asyncIngressScope[T]

	receiptVersions [3]*AsyncSource[uint64]
	receiptCounters [3]uint64
	receipts        AsyncIngressReceiptReaders[K]

	transportKind *AsyncSource[IngressTransportKind]
	pollInterval  *AsyncSource[uint64]
	schedule      *AsyncComputed[IngressSchedule]
}

// NewAsyncIngressCell builds an async ingress over policy, folding the hot window
// under merge and delivering as kind.
func NewAsyncIngressCell[K comparable, T any](
	ctx *AsyncContext,
	policy IngressPolicy,
	merge MergePolicy[T],
	kind IngressTransportKind,
	pollInterval uint64,
) (*AsyncIngressCell[K, T], error) {
	core, err := NewIngressCore[K, T](policy, merge)
	if err != nil {
		return nil, err
	}
	in := &AsyncIngressCell[K, T]{
		ctx:    ctx,
		core:   core,
		scopes: make(map[K]*asyncIngressScope[T]),
	}
	for i := range in.receiptVersions {
		in.receiptVersions[i] = NewAsyncSource(ctx, uint64(0))
	}
	in.receipts = AsyncIngressReceiptReaders[K]{
		Accepted: in.receiptReader(0, IngressReceiptAccepted),
		Dropped:  in.receiptReader(1, IngressReceiptDropped),
		Errors:   in.receiptReader(2, IngressReceiptError),
	}
	in.transportKind = NewAsyncSource(ctx, kind)
	in.pollInterval = NewAsyncSource(ctx, pollInterval)
	in.schedule = NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (IngressSchedule, error) {
		return IngressScheduleFor(
			TrackSource(cc, in.transportKind),
			TrackSource(cc, in.pollInterval),
		), nil
	})
	return in, nil
}

func (in *AsyncIngressCell[K, T]) receiptReader(
	slot int,
	channel IngressReceiptChannel,
) *AsyncComputed[[]IngressReceipt[K]] {
	return NewAsyncComputed(in.ctx,
		func(cc *AsyncComputeContext) ([]IngressReceipt[K], error) {
			TrackSource(cc, in.receiptVersions[slot])
			in.mu.Lock()
			defer in.mu.Unlock()
			return in.core.Receipts(channel), nil
		})
}

// ensureScope mints (or returns) one scope's version inputs and readers.
func (in *AsyncIngressCell[K, T]) ensureScope(key K) *asyncIngressScope[T] {
	in.readerMu.Lock()
	defer in.readerMu.Unlock()
	if scope, ok := in.scopes[key]; ok {
		return scope
	}
	scope := &asyncIngressScope[T]{}
	for i := range scope.versions {
		scope.versions[i] = NewAsyncSource(in.ctx, uint64(0))
	}
	scope.readers = AsyncIngressScopeReaders[T]{
		Value: NewAsyncComputed(in.ctx,
			func(cc *AsyncComputeContext) (IngressWindow[T], error) {
				TrackSource(cc, scope.versions[asyncIngressValueSlot])
				in.mu.Lock()
				defer in.mu.Unlock()
				value, ok := in.core.Peek(key)
				return IngressWindow[T]{Present: ok, Value: value}, nil
			}),
		Readiness: NewAsyncComputed(in.ctx,
			func(cc *AsyncComputeContext) (IngressReadiness, error) {
				TrackSource(cc, scope.versions[asyncIngressReadinessSlot])
				in.mu.Lock()
				defer in.mu.Unlock()
				return in.core.Readiness(key), nil
			}),
		Authority: NewAsyncComputed(in.ctx,
			func(cc *AsyncComputeContext) (Opt[IngressAuthority], error) {
				TrackSource(cc, scope.versions[asyncIngressAuthoritySlot])
				in.mu.Lock()
				defer in.mu.Unlock()
				authority, ok := in.core.Authority(key)
				return Opt[IngressAuthority]{Present: ok, Value: authority}, nil
			}),
		Retry: NewAsyncComputed(in.ctx,
			func(cc *AsyncComputeContext) (Opt[IngressRetry], error) {
				TrackSource(cc, scope.versions[asyncIngressRetrySlot])
				in.mu.Lock()
				defer in.mu.Unlock()
				retry, ok := in.core.Retry(key)
				return Opt[IngressRetry]{Present: ok, Value: retry}, nil
			}),
	}
	in.scopes[key] = scope
	return scope
}

// ScopeReaders mints (or returns) one scope's four readers.
func (in *AsyncIngressCell[K, T]) ScopeReaders(key K) AsyncIngressScopeReaders[T] {
	return in.ensureScope(key).readers
}

// ReceiptReaders returns the three receipt-channel handles.
func (in *AsyncIngressCell[K, T]) ReceiptReaders() AsyncIngressReceiptReaders[K] {
	return in.receipts
}

// ScheduleReader returns the derived-schedule handle.
func (in *AsyncIngressCell[K, T]) ScheduleReader() *AsyncComputed[IngressSchedule] {
	return in.schedule
}

// publish bumps exactly the version inputs the core reported dirty, inside one
// batch so the whole transition is one frontier walk.
func (in *AsyncIngressCell[K, T]) publish(change IngressChange[K]) {
	if change.IsEmpty() {
		return
	}
	// Mint every reader OUTSIDE the batch: ensureScope takes readerMu, and the
	// batch body should hold nothing but the graph's own bookkeeping.
	scopes := make([]*asyncIngressScope[T], len(change.Scopes))
	for i, delta := range change.Scopes {
		scopes[i] = in.ensureScope(delta.Key)
	}
	in.readerMu.Lock()
	dirty := make([][4]bool, len(change.Scopes))
	for i, delta := range change.Scopes {
		dirty[i] = [4]bool{
			delta.Change.Value,
			delta.Change.Readiness,
			delta.Change.Authority,
			delta.Change.Retry,
		}
		for slot, changed := range dirty[i] {
			if changed {
				scopes[i].counters[slot]++
			}
		}
	}
	receiptDirty := [3]bool{
		change.AcceptedReceipts, change.DroppedReceipts, change.ErrorReceipts,
	}
	for slot, changed := range receiptDirty {
		if changed {
			in.receiptCounters[slot]++
		}
	}
	// Snapshot the counters so the batch body reads no shared mutable state.
	next := make([][4]uint64, len(scopes))
	for i := range scopes {
		next[i] = scopes[i].counters
	}
	nextReceipts := in.receiptCounters
	in.readerMu.Unlock()

	in.ctx.Batch(func() {
		for i, scope := range scopes {
			for slot, changed := range dirty[i] {
				if changed {
					scope.versions[slot].Set(next[i][slot])
				}
			}
		}
		for slot, changed := range receiptDirty {
			if changed {
				in.receiptVersions[slot].Set(nextReceipts[slot])
			}
		}
	})
}

// transition runs one core mutation and publishes its invalidation set with the
// core lock released.
func (in *AsyncIngressCell[K, T]) transition(op func() IngressChange[K]) {
	in.mu.Lock()
	change := op()
	in.mu.Unlock()
	in.publish(change)
}

// Open opens (or reopens) a keyed scope at generation.
func (in *AsyncIngressCell[K, T]) Open(key K, generation uint64) {
	in.transition(func() IngressChange[K] { return in.core.Open(key, generation) })
}

// Admit admits one decoded envelope.
func (in *AsyncIngressCell[K, T]) Admit(envelope IngressEnvelope[K, T]) IngressAdmission {
	var admission IngressAdmission
	in.transition(func() IngressChange[K] {
		change, result := in.core.Admit(envelope)
		admission = result
		return change
	})
	return admission
}

// Suspend suspends a scope, retaining its watermark.
func (in *AsyncIngressCell[K, T]) Suspend(key K) (ReplayRequest, bool) {
	var request ReplayRequest
	var ok bool
	in.transition(func() IngressChange[K] {
		change, result, present := in.core.Suspend(key)
		request, ok = result, present
		return change
	})
	return request, ok
}

// Reconnect reconnects a scope at generation, clearing its error streak.
func (in *AsyncIngressCell[K, T]) Reconnect(key K, generation uint64) ReplayRequest {
	var request ReplayRequest
	in.transition(func() IngressChange[K] {
		change, result := in.core.Reconnect(key, generation)
		request = result
		return change
	})
	return request
}

// Close closes a scope.
func (in *AsyncIngressCell[K, T]) Close(key K) {
	in.transition(func() IngressChange[K] { return in.core.Close(key) })
}

// Fail records a transport/decode failure, deepening the scope's backoff.
func (in *AsyncIngressCell[K, T]) Fail(key K, err IngressError) {
	in.transition(func() IngressChange[K] { return in.core.Fail(key, err) })
}

// Tick advances logical time.
func (in *AsyncIngressCell[K, T]) Tick(now uint64) {
	in.transition(func() IngressChange[K] { return in.core.Tick(now) })
}

// Drain drains a scope's coalesced window.
func (in *AsyncIngressCell[K, T]) Drain(key K) (T, bool) {
	var value T
	var ok bool
	in.transition(func() IngressChange[K] {
		change, drained, present := in.core.Drain(key)
		value, ok = drained, present
		return change
	})
	return value, ok
}

// Pump admits everything transport has decoded, then asks it to replay any gap
// still open. Nothing here awaits: the transport already decoded.
func (in *AsyncIngressCell[K, T]) Pump(
	transport IngressTransport[K, T],
) []IngressAdmission {
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
		view, ok := in.View(key)
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

// Value reads the coalesced window awaiting drain. A nil compute surface is an
// untracked top-level read.
func (in *AsyncIngressCell[K, T]) Value(cc *AsyncComputeContext, key K) (T, bool) {
	window := resolveAsyncReader(cc, in.ScopeReaders(key).Value)
	return window.Value, window.Present
}

// Readiness reads derived readiness.
func (in *AsyncIngressCell[K, T]) Readiness(
	cc *AsyncComputeContext,
	key K,
) IngressReadiness {
	return resolveAsyncReader(cc, in.ScopeReaders(key).Readiness)
}

// Authority reads derived authority.
func (in *AsyncIngressCell[K, T]) Authority(
	cc *AsyncComputeContext,
	key K,
) (IngressAuthority, bool) {
	opt := resolveAsyncReader(cc, in.ScopeReaders(key).Authority)
	return opt.Value, opt.Present
}

// Retry reads the derived retry decision.
func (in *AsyncIngressCell[K, T]) Retry(
	cc *AsyncComputeContext,
	key K,
) (IngressRetry, bool) {
	opt := resolveAsyncReader(cc, in.ScopeReaders(key).Retry)
	return opt.Value, opt.Present
}

// Accepted reads accepted receipts, oldest first.
func (in *AsyncIngressCell[K, T]) Accepted(cc *AsyncComputeContext) []IngressReceipt[K] {
	return resolveAsyncReader(cc, in.receipts.Accepted)
}

// Dropped reads dropped receipts, oldest first.
func (in *AsyncIngressCell[K, T]) Dropped(cc *AsyncComputeContext) []IngressReceipt[K] {
	return resolveAsyncReader(cc, in.receipts.Dropped)
}

// Errors reads error receipts, oldest first.
func (in *AsyncIngressCell[K, T]) Errors(cc *AsyncComputeContext) []IngressReceipt[K] {
	return resolveAsyncReader(cc, in.receipts.Errors)
}

// Schedule reads the derived delivery schedule.
func (in *AsyncIngressCell[K, T]) Schedule(cc *AsyncComputeContext) IngressSchedule {
	return resolveAsyncReader(cc, in.schedule)
}

// SetTransport retunes the transport live.
func (in *AsyncIngressCell[K, T]) SetTransport(kind IngressTransportKind) {
	in.transportKind.Set(kind)
}

// SetPollInterval retunes the poll bound live.
func (in *AsyncIngressCell[K, T]) SetPollInterval(interval uint64) {
	in.pollInterval.Set(interval)
}

// View is the non-reactive projection of a scope.
func (in *AsyncIngressCell[K, T]) View(key K) (IngressScopeView, bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.core.View(key)
}

// Policy returns the bounds in force.
func (in *AsyncIngressCell[K, T]) Policy() IngressPolicy {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.core.Policy()
}

// ScopeKeys returns every known scope key.
func (in *AsyncIngressCell[K, T]) ScopeKeys() []K {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.core.ScopeKeys()
}
