// ThreadSafeEffectCas — the goroutine-safe reactive shell over EffectCasCore
// (#lzeffectcas), ported from lazily-rs `thread_safe_effect_cas.rs`.
//
// One state reader (a Computed snapshot) is invalidated only by a transition
// that reports a state change, and only AFTER the core lock is released: a
// stale generation, unknown attempt, or non-matching receipt changes no state
// and no reader version. Scheduling observes PendingKeys(now) and invokes the
// application's CompareAndApply adapter OUTSIDE the lock, then reports its
// outcome through RecordOutcome.

package lazily

import (
	"cmp"
	"sync"
)

// ThreadSafeEffectCas is the thread-safe reactive keyed effect-CAS authority.
type ThreadSafeEffectCas[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable] struct {
	ts *ThreadSafeContext

	// mu guards core and is never held across graph work.
	mu   sync.Mutex
	core *EffectCasCore[K, A, P, R, E]

	state *Computed[EffectCasSnapshot[K, A, P, R, E]]
}

// NewThreadSafeEffectCas builds an empty authority at actor generation.
func NewThreadSafeEffectCas[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable](ts *ThreadSafeContext, generation uint64) *ThreadSafeEffectCas[K, A, P, R, E] {
	return newThreadSafeEffectCasFromCore(ts, NewEffectCasCore[K, A, P, R, E](generation))
}

// HydrateThreadSafeEffectCas restores an authority from a durable image before
// the runtime becomes live.
func HydrateThreadSafeEffectCas[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable](
	ts *ThreadSafeContext,
	state EffectCasDurableState[K, A, P, R, E],
) (*ThreadSafeEffectCas[K, A, P, R, E], error) {
	core, err := HydrateEffectCasCore(state)
	if err != nil {
		return nil, err
	}
	return newThreadSafeEffectCasFromCore(ts, core), nil
}

func newThreadSafeEffectCasFromCore[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable](
	ts *ThreadSafeContext,
	core *EffectCasCore[K, A, P, R, E],
) *ThreadSafeEffectCas[K, A, P, R, E] {
	shell := &ThreadSafeEffectCas[K, A, P, R, E]{ts: ts, core: core}
	ts.WithLock(func(ctx *Context) {
		shell.state = NewSlot(ctx, func(*Compute) EffectCasSnapshot[K, A, P, R, E] {
			shell.mu.Lock()
			defer shell.mu.Unlock()
			return shell.core.Snapshot()
		})
	})
	return shell
}

func (s *ThreadSafeEffectCas[K, A, P, R, E]) transition(op func() EffectCasChange) {
	s.ts.Batch(func() {
		s.mu.Lock()
		change := op()
		s.mu.Unlock()
		if change.State {
			s.state.invalidate()
		}
	})
}

// StateHandle is the snapshot reader. Read it under the context lock or via
// Snapshot.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) StateHandle() *Computed[EffectCasSnapshot[K, A, P, R, E]] {
	return s.state
}

// Snapshot is the reactive read of the whole projection. c is nil for an
// untracked read.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) Snapshot(c ComputeOps) EffectCasSnapshot[K, A, P, R, E] {
	return Read(s.ts, func(ctx *Context) EffectCasSnapshot[K, A, P, R, E] {
		return Get[EffectCasSnapshot[K, A, P, R, E]](tsReadSurface(c, ctx), s.state)
	})
}

// Generation is the current actor generation.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) Generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.core.Generation()
}

// PendingKeys lists the keys with claimable work at logical time now.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) PendingKeys(now uint64) []K {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.core.PendingKeys(now)
}

// DurableState is the full-fidelity image for crash recovery.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) DurableState() EffectCasDurableState[K, A, P, R, E] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.core.DurableState()
}

// State is a non-reactive projection of one key.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) State(key K) (EffectCasKeyState[K, A, P, R, E], bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.core.State(key)
}

// UpsertDesired records a desired intent.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) UpsertDesired(intent EffectIntent[K, A, P]) EffectCasUpsert {
	var outcome EffectCasUpsert
	s.transition(func() EffectCasChange {
		change, next := s.core.UpsertDesired(intent)
		outcome = next
		return change
	})
	return outcome
}

// Claim moves eligible pending work for key into its in-flight slot.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) Claim(key K, generation, now uint64) EffectCasClaim[K, A, P, R] {
	var outcome EffectCasClaim[K, A, P, R]
	s.transition(func() EffectCasChange {
		change, next := s.core.Claim(key, generation, now)
		outcome = next
		return change
	})
	return outcome
}

// RecordApplied reports an exact applied receipt.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) RecordApplied(key K, attempt EffectAttemptIdentity, receipt R, authority A) EffectCasApplied {
	var outcome EffectCasApplied
	s.transition(func() EffectCasChange {
		change, next := s.core.RecordApplied(key, attempt, receipt, authority)
		outcome = next
		return change
	})
	return outcome
}

// RecordSuperseded reports a compare mismatch.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) RecordSuperseded(key K, attempt EffectAttemptIdentity, observed A) EffectCasFailure {
	var outcome EffectCasFailure
	s.transition(func() EffectCasChange {
		change, next := s.core.RecordSuperseded(key, attempt, observed)
		outcome = next
		return change
	})
	return outcome
}

// RecordRetryable reports a retryable failure at logical retry time retryAt.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) RecordRetryable(key K, attempt EffectAttemptIdentity, err E, retryAt uint64) EffectCasFailure {
	var outcome EffectCasFailure
	s.transition(func() EffectCasChange {
		change, next := s.core.RecordRetryable(key, attempt, err, retryAt)
		outcome = next
		return change
	})
	return outcome
}

// RecordRejected reports a permanent rejection.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) RecordRejected(key K, attempt EffectAttemptIdentity, err E) EffectCasFailure {
	var outcome EffectCasFailure
	s.transition(func() EffectCasChange {
		change, next := s.core.RecordRejected(key, attempt, err)
		outcome = next
		return change
	})
	return outcome
}

// RecordOutcome reports one adapter outcome.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) RecordOutcome(key K, attempt EffectAttemptIdentity, outcome EffectApply[R, A, E], retryAt uint64) EffectCasOutcome {
	var result EffectCasOutcome
	s.transition(func() EffectCasChange {
		change, next := s.core.RecordOutcome(key, attempt, outcome, retryAt)
		result = next
		return change
	})
	return result
}

// Reconnect advances the actor generation, fencing older receipts.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) Reconnect(generation uint64) EffectCasReconnect {
	var outcome EffectCasReconnect
	s.transition(func() EffectCasChange {
		change, next := s.core.Reconnect(generation)
		outcome = next
		return change
	})
	return outcome
}

// RunOnce claims key at logical time now, invokes adapter OUTSIDE every lock,
// and records its outcome (a Retryable outcome is parked until retryAt). It
// returns the claim and, when one was made, the recorded outcome.
func (s *ThreadSafeEffectCas[K, A, P, R, E]) RunOnce(
	key K,
	now, retryAt uint64,
	adapter CompareAndApply[K, A, P, R, E],
) (EffectCasClaim[K, A, P, R], EffectCasOutcome, bool) {
	claim := s.Claim(key, s.Generation(), now)
	if claim.Kind != EffectCasClaimClaimed {
		return claim, EffectCasOutcome{}, false
	}
	outcome := adapter.CompareAndApply(claim.Attempt)
	return claim, s.RecordOutcome(key, claim.Attempt.Identity, outcome, retryAt), true
}
