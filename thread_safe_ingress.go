// ThreadSafeIngressCell — the lock-serialized flavor of the transport-agnostic
// reactive ingress family (#designimplementtransport).
//
// This shell owns its OWN IngressCore and its OWN readers, minted on the wrapped
// ThreadSafeContext's graph. It is not a delegating facade over the
// single-threaded cell, because the two invariants this flavor has to hold are
// its own:
//
//  1. Invalidation runs with the core lock RELEASED. A reader's compute takes the
//     context lock and then the core lock; an op that invalidated while still
//     holding the core lock would invert that order and deadlock the effect flush.
//     The ordering is therefore enforced by construction, not by convention.
//
//  2. One admission is ONE frontier walk. Every reader the core reported dirty is
//     cleared inside a single ts.Batch, so an Effect over several reader kinds
//     runs once — a generation handoff must never be observable as "new value,
//     old authority".
//
// # Every READ takes the graph lock too, not just every write
//
// This is the defect lazily-go already shipped once, in ThreadSafeReactiveMap: a
// read is not passive in this kernel. A Get of a stale derived reader runs
// refresh → recomputeNow → Context.newCompute, which bumps computeGen and
// cachedCount on the shared single-threaded Context. Two goroutines reading two
// *different* scopes therefore write the same Context fields, and `-race` reported
// exactly that. Even a Source read mutates the dependency edge set when the read
// surface is a *Compute. So every method below that touches the graph — read or
// write — goes through ts.WithLock / Read / ts.Batch. The lock is reentrant, so a
// reader compute that re-enters is safe.

package lazily

import "sync"

// ThreadSafeIngressCell is the Send-and-Sync-equivalent ingress flavor: safe for
// concurrent goroutines, with the admission plane serialized by an internal mutex
// and all graph work serialized by the owning ThreadSafeContext's lock.
type ThreadSafeIngressCell[K comparable, T any] struct {
	ts *ThreadSafeContext

	// mu guards the core. It is NEVER held across graph work — see the invariant
	// note above. Lock order is context-then-mu, uniformly.
	mu   sync.Mutex
	core *IngressCore[K, T]

	// scopes is the reader table, guarded by the context lock (it mints graph
	// nodes, so it can only be touched where the graph can).
	scopes map[K]IngressScopeReaders[T]

	receipts IngressReceiptReaders[K]

	transportKind *Source[IngressTransportKind]
	pollInterval  *Source[uint64]
	schedule      *Computed[IngressSchedule]
}

// NewThreadSafeIngressCell builds a lock-serialized ingress over policy, folding
// the hot window under merge and delivering as kind.
func NewThreadSafeIngressCell[K comparable, T any](
	ts *ThreadSafeContext,
	policy IngressPolicy,
	merge MergePolicy[T],
	kind IngressTransportKind,
	pollInterval uint64,
) (*ThreadSafeIngressCell[K, T], error) {
	core, err := NewIngressCore[K, T](policy, merge)
	if err != nil {
		return nil, err
	}
	in := &ThreadSafeIngressCell[K, T]{
		ts:     ts,
		core:   core,
		scopes: make(map[K]IngressScopeReaders[T]),
	}
	ts.WithLock(func(ctx *Context) {
		in.receipts = IngressReceiptReaders[K]{
			Accepted: in.receiptReader(ctx, IngressReceiptAccepted),
			Dropped:  in.receiptReader(ctx, IngressReceiptDropped),
			Errors:   in.receiptReader(ctx, IngressReceiptError),
		}
		in.transportKind = NewSource(ctx, kind)
		in.pollInterval = NewSource(ctx, pollInterval)
		in.schedule = NewSlot(ctx, func(c *Compute) IngressSchedule {
			return IngressScheduleFor(
				Get[IngressTransportKind](c, in.transportKind),
				Get[uint64](c, in.pollInterval),
			)
		})
	})
	return in, nil
}

func (in *ThreadSafeIngressCell[K, T]) receiptReader(
	ctx *Context,
	channel IngressReceiptChannel,
) *Computed[[]IngressReceipt[K]] {
	return NewSlot(ctx, func(*Compute) []IngressReceipt[K] {
		in.mu.Lock()
		defer in.mu.Unlock()
		return in.core.Receipts(channel)
	})
}

// scopeReadersLocked mints (or returns) one scope's four readers. The caller holds
// the context lock.
func (in *ThreadSafeIngressCell[K, T]) scopeReadersLocked(
	ctx *Context,
	key K,
) IngressScopeReaders[T] {
	if readers, ok := in.scopes[key]; ok {
		return readers
	}
	readers := IngressScopeReaders[T]{
		Value: NewSlot(ctx, func(*Compute) IngressWindow[T] {
			in.mu.Lock()
			defer in.mu.Unlock()
			value, ok := in.core.Peek(key)
			return IngressWindow[T]{Present: ok, Value: value}
		}),
		Readiness: NewSlot(ctx, func(*Compute) IngressReadiness {
			in.mu.Lock()
			defer in.mu.Unlock()
			return in.core.Readiness(key)
		}),
		Authority: NewSlot(ctx, func(*Compute) Opt[IngressAuthority] {
			in.mu.Lock()
			defer in.mu.Unlock()
			authority, ok := in.core.Authority(key)
			return Opt[IngressAuthority]{Present: ok, Value: authority}
		}),
		Retry: NewSlot(ctx, func(*Compute) Opt[IngressRetry] {
			in.mu.Lock()
			defer in.mu.Unlock()
			retry, ok := in.core.Retry(key)
			return Opt[IngressRetry]{Present: ok, Value: retry}
		}),
	}
	in.scopes[key] = readers
	return readers
}

// ScopeReaders mints (or returns) one scope's four readers, under the graph lock.
func (in *ThreadSafeIngressCell[K, T]) ScopeReaders(key K) IngressScopeReaders[T] {
	return Read(in.ts, func(ctx *Context) IngressScopeReaders[T] {
		return in.scopeReadersLocked(ctx, key)
	})
}

// ReceiptReaders returns the three receipt-channel handles.
func (in *ThreadSafeIngressCell[K, T]) ReceiptReaders() IngressReceiptReaders[K] {
	return in.receipts
}

// ScheduleReader returns the derived-schedule handle.
func (in *ThreadSafeIngressCell[K, T]) ScheduleReader() *Computed[IngressSchedule] {
	return in.schedule
}

// applyLocked clears exactly the readers the core reported dirty. The caller holds
// the context lock, is inside the batch, and has RELEASED the core lock.
func (in *ThreadSafeIngressCell[K, T]) applyLocked(ctx *Context, change IngressChange[K]) {
	for _, delta := range change.Scopes {
		readers := in.scopeReadersLocked(ctx, delta.Key)
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
}

// transition runs one core mutation and applies its invalidation set.
//
// The shape is the contract: ts.Batch takes the graph lock and opens ONE batch;
// the core lock is taken and released around the algebra alone; the invalidation
// then runs inside that batch with the core lock free, so the effect flush at
// batch exit can re-enter a reader compute without deadlocking.
func (in *ThreadSafeIngressCell[K, T]) transition(op func() IngressChange[K]) {
	in.ts.Batch(func() {
		in.mu.Lock()
		change := op()
		in.mu.Unlock()
		in.applyLocked(in.ts.Context(), change)
	})
}

// Open opens (or reopens) a keyed scope at generation.
func (in *ThreadSafeIngressCell[K, T]) Open(key K, generation uint64) {
	in.transition(func() IngressChange[K] { return in.core.Open(key, generation) })
}

// Admit admits one decoded envelope.
func (in *ThreadSafeIngressCell[K, T]) Admit(
	envelope IngressEnvelope[K, T],
) IngressAdmission {
	var admission IngressAdmission
	in.transition(func() IngressChange[K] {
		change, result := in.core.Admit(envelope)
		admission = result
		return change
	})
	return admission
}

// Suspend suspends a scope, retaining its watermark.
func (in *ThreadSafeIngressCell[K, T]) Suspend(key K) (ReplayRequest, bool) {
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
func (in *ThreadSafeIngressCell[K, T]) Reconnect(key K, generation uint64) ReplayRequest {
	var request ReplayRequest
	in.transition(func() IngressChange[K] {
		change, result := in.core.Reconnect(key, generation)
		request = result
		return change
	})
	return request
}

// Close closes a scope.
func (in *ThreadSafeIngressCell[K, T]) Close(key K) {
	in.transition(func() IngressChange[K] { return in.core.Close(key) })
}

// Fail records a transport/decode failure, deepening the scope's backoff.
func (in *ThreadSafeIngressCell[K, T]) Fail(key K, err IngressError) {
	in.transition(func() IngressChange[K] { return in.core.Fail(key, err) })
}

// Tick advances logical time.
func (in *ThreadSafeIngressCell[K, T]) Tick(now uint64) {
	in.transition(func() IngressChange[K] { return in.core.Tick(now) })
}

// Drain drains a scope's coalesced window.
func (in *ThreadSafeIngressCell[K, T]) Drain(key K) (T, bool) {
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
// still open.
func (in *ThreadSafeIngressCell[K, T]) Pump(
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

// Value is the reactive read of the coalesced window awaiting drain, under the
// graph lock.
func (in *ThreadSafeIngressCell[K, T]) Value(c ComputeOps, key K) (T, bool) {
	window := Read(in.ts, func(ctx *Context) IngressWindow[T] {
		return Get[IngressWindow[T]](c, in.scopeReadersLocked(ctx, key).Value)
	})
	return window.Value, window.Present
}

// Readiness is the reactive read of derived readiness, under the graph lock.
func (in *ThreadSafeIngressCell[K, T]) Readiness(c ComputeOps, key K) IngressReadiness {
	return Read(in.ts, func(ctx *Context) IngressReadiness {
		return Get[IngressReadiness](c, in.scopeReadersLocked(ctx, key).Readiness)
	})
}

// Authority is the reactive read of derived authority, under the graph lock.
func (in *ThreadSafeIngressCell[K, T]) Authority(
	c ComputeOps,
	key K,
) (IngressAuthority, bool) {
	opt := Read(in.ts, func(ctx *Context) Opt[IngressAuthority] {
		return Get[Opt[IngressAuthority]](c, in.scopeReadersLocked(ctx, key).Authority)
	})
	return opt.Value, opt.Present
}

// Retry is the reactive read of the derived retry decision, under the graph lock.
func (in *ThreadSafeIngressCell[K, T]) Retry(c ComputeOps, key K) (IngressRetry, bool) {
	opt := Read(in.ts, func(ctx *Context) Opt[IngressRetry] {
		return Get[Opt[IngressRetry]](c, in.scopeReadersLocked(ctx, key).Retry)
	})
	return opt.Value, opt.Present
}

// Accepted is the reactive read of accepted receipts, under the graph lock.
func (in *ThreadSafeIngressCell[K, T]) Accepted(c ComputeOps) []IngressReceipt[K] {
	return Read(in.ts, func(*Context) []IngressReceipt[K] {
		return Get[[]IngressReceipt[K]](c, in.receipts.Accepted)
	})
}

// Dropped is the reactive read of dropped receipts, under the graph lock.
func (in *ThreadSafeIngressCell[K, T]) Dropped(c ComputeOps) []IngressReceipt[K] {
	return Read(in.ts, func(*Context) []IngressReceipt[K] {
		return Get[[]IngressReceipt[K]](c, in.receipts.Dropped)
	})
}

// Errors is the reactive read of error receipts, under the graph lock.
func (in *ThreadSafeIngressCell[K, T]) Errors(c ComputeOps) []IngressReceipt[K] {
	return Read(in.ts, func(*Context) []IngressReceipt[K] {
		return Get[[]IngressReceipt[K]](c, in.receipts.Errors)
	})
}

// Schedule is the reactive read of the derived delivery schedule, under the graph
// lock.
func (in *ThreadSafeIngressCell[K, T]) Schedule(c ComputeOps) IngressSchedule {
	return Read(in.ts, func(*Context) IngressSchedule {
		return Get[IngressSchedule](c, in.schedule)
	})
}

// SetTransport retunes the transport live.
func (in *ThreadSafeIngressCell[K, T]) SetTransport(kind IngressTransportKind) {
	TSSetCell(in.ts, in.transportKind, kind)
}

// SetPollInterval retunes the poll bound live.
func (in *ThreadSafeIngressCell[K, T]) SetPollInterval(interval uint64) {
	TSSetCell(in.ts, in.pollInterval, interval)
}

// View is the non-reactive projection of a scope. It touches the core, not the
// graph, so it takes the core lock alone.
func (in *ThreadSafeIngressCell[K, T]) View(key K) (IngressScopeView, bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.core.View(key)
}

// Policy returns the bounds in force.
func (in *ThreadSafeIngressCell[K, T]) Policy() IngressPolicy {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.core.Policy()
}

// ScopeKeys returns every known scope key.
func (in *ThreadSafeIngressCell[K, T]) ScopeKeys() []K {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.core.ScopeKeys()
}
