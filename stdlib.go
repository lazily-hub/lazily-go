package lazily

import (
	"math"
	"sync"
)

// TimerError is a typed failure reported by the portable logical-clock timer.
type TimerError string

const (
	TimerDeadlineOverflow TimerError = "deadline_overflow"
	TimerClockRegression  TimerError = "clock_regression"
)

func (e TimerError) Error() string { return string(e) }

// CheckedDeadline returns now+duration or a typed overflow error.
func CheckedDeadline(now, duration uint64) (uint64, error) {
	if duration > math.MaxUint64-now {
		return 0, TimerDeadlineOverflow
	}
	return now + duration, nil
}

// TimerObservation is the externally observable state of Timer.
type TimerObservation struct {
	Outcome  string
	Deadline uint64
	FiredAt  uint64
}

// Timer is a deterministic single-shot timer driven by caller-supplied ticks.
// Its mutex makes concurrent observations race-free while preserving the
// no-state-change rule for a regressing clock.
type Timer struct {
	mu       sync.Mutex
	deadline uint64
	lastNow  uint64
	firedAt  uint64
	fired    bool
}

func NewTimer(now, duration uint64) (*Timer, error) {
	deadline, err := CheckedDeadline(now, duration)
	if err != nil {
		return nil, err
	}
	return &Timer{deadline: deadline, lastNow: now}, nil
}

func (t *Timer) Observe(now uint64) (TimerObservation, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fired {
		return TimerObservation{Outcome: "fired", FiredAt: t.firedAt}, nil
	}
	if now < t.lastNow {
		return TimerObservation{Outcome: "unavailable", Deadline: t.deadline}, TimerClockRegression
	}
	t.lastNow = now
	if now >= t.deadline {
		t.fired = true
		t.firedAt = now
		return TimerObservation{Outcome: "fired", FiredAt: now}, nil
	}
	return TimerObservation{Outcome: "pending", Deadline: t.deadline}, nil
}

// TimeoutOperation is the result returned by one operation adapter poll.
type TimeoutOperation[T any] struct {
	State string
	Value T
}

func PendingOperation[T any]() TimeoutOperation[T] {
	return TimeoutOperation[T]{State: "pending"}
}

func CompletedOperation[T any](value T) TimeoutOperation[T] {
	return TimeoutOperation[T]{State: "completed", Value: value}
}

func UnavailableOperation[T any]() TimeoutOperation[T] {
	return TimeoutOperation[T]{State: "unavailable"}
}

// TimeoutCancellation is returned by a cancellation adapter owned by the
// caller. "unavailable" represents a foreign or unreadable cancellation seam.
type TimeoutCancellation string

const (
	CancellationPending     TimeoutCancellation = "pending"
	CancellationCancelled   TimeoutCancellation = "cancelled"
	CancellationUnavailable TimeoutCancellation = "unavailable"
)

// TimeoutObservation is the deterministic, terminal-latching timeout result.
type TimeoutObservation[T any] struct {
	Outcome  string
	Deadline uint64
	Value    T
	Reason   string
}

type Timeout[T any] struct {
	mu       sync.Mutex
	deadline uint64
	lastNow  uint64
	terminal *TimeoutObservation[T]
}

func NewTimeout[T any](now, duration uint64) (*Timeout[T], error) {
	deadline, err := CheckedDeadline(now, duration)
	if err != nil {
		return nil, err
	}
	return &Timeout[T]{deadline: deadline, lastNow: now}, nil
}

// Poll invokes both adapters exactly once before the deadline. Precedence is
// completion, unavailable operation, cancellation, then pending. At or after
// the deadline neither adapter is called. Terminal reads also call neither.
func (t *Timeout[T]) Poll(
	now uint64,
	operation func() TimeoutOperation[T],
	cancellation func() TimeoutCancellation,
) TimeoutObservation[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.terminal != nil {
		return *t.terminal
	}
	if now < t.lastNow {
		return t.latch(TimeoutObservation[T]{
			Outcome: "unavailable", Reason: string(TimerClockRegression),
		})
	}
	t.lastNow = now
	if now >= t.deadline {
		return t.latch(TimeoutObservation[T]{Outcome: "timed_out"})
	}
	op := operation()
	cancel := cancellation()
	switch op.State {
	case "completed":
		return t.latch(TimeoutObservation[T]{Outcome: "completed", Value: op.Value})
	case "unavailable":
		return t.latch(TimeoutObservation[T]{
			Outcome: "unavailable", Reason: "operation_unavailable",
		})
	}
	switch cancel {
	case CancellationCancelled:
		return t.latch(TimeoutObservation[T]{Outcome: "cancelled"})
	case CancellationUnavailable:
		return t.latch(TimeoutObservation[T]{
			Outcome: "unavailable", Reason: "cancellation_unavailable",
		})
	}
	return TimeoutObservation[T]{Outcome: "pending", Deadline: t.deadline}
}

func (t *Timeout[T]) latch(observation TimeoutObservation[T]) TimeoutObservation[T] {
	t.terminal = &observation
	return observation
}

// RevisionBarrierObservation is the portable logical observation of a barrier.
type RevisionBarrierObservation struct {
	Outcome    string
	Reason     string
	Revision   uint64
	Generation uint64
}

// RevisionBarrier separates the authoritative revision from its wake
// generation. Receipts may wake a waiter, but only an accepted revision advance
// mutates either observable counter.
type RevisionBarrier struct {
	mu               sync.Mutex
	revision         uint64
	requiredRevision uint64
	generation       uint64
	deadline         *uint64
	terminal         string
	terminalReason   string
}

func NewRevisionBarrier(revision, requiredRevision uint64, deadline *uint64) *RevisionBarrier {
	return &RevisionBarrier{
		revision: revision, requiredRevision: requiredRevision, deadline: deadline,
	}
}

func (b *RevisionBarrier) Observe(
	now uint64,
	predicate bool,
	cancellation func() TimeoutCancellation,
) RevisionBarrierObservation {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.terminal != "" {
		return b.snapshot()
	}
	if b.deadline != nil && now >= *b.deadline {
		return b.latch("timed_out", "")
	}
	if predicate && b.revision >= b.requiredRevision {
		return b.latch("satisfied", "")
	}
	switch cancellation() {
	case CancellationCancelled:
		return b.latch("cancelled", "")
	case CancellationUnavailable:
		return b.latch("unavailable", "cancellation_unavailable")
	}
	return b.snapshot()
}

// RegisterRecheck models register-then-recheck: a revision accepted during
// registration is applied before the predicate is checked.
func (b *RevisionBarrier) RegisterRecheck(
	now, observedRevision uint64,
	predicate bool,
) RevisionBarrierObservation {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.terminal != "" {
		return b.snapshot()
	}
	if b.deadline != nil && now >= *b.deadline {
		return b.latch("timed_out", "")
	}
	b.acceptRevision(observedRevision)
	if predicate && b.revision >= b.requiredRevision {
		return b.latch("satisfied", "")
	}
	return b.snapshot()
}

func (b *RevisionBarrier) Advance(revision uint64, predicate bool) RevisionBarrierObservation {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.terminal != "" {
		return b.snapshot()
	}
	b.acceptRevision(revision)
	if predicate && b.revision >= b.requiredRevision {
		return b.latch("satisfied", "")
	}
	return b.snapshot()
}

func (b *RevisionBarrier) Dispose() RevisionBarrierObservation {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.terminal == "" {
		return b.latch("disposed", "")
	}
	return b.snapshot()
}

// Receipt is deliberately not an authority for barrier progress.
func (b *RevisionBarrier) Receipt(string) RevisionBarrierObservation {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snapshot()
}

func (b *RevisionBarrier) acceptRevision(revision uint64) {
	if revision > b.revision {
		b.revision = revision
		b.generation++
	}
}

func (b *RevisionBarrier) latch(outcome, reason string) RevisionBarrierObservation {
	b.terminal = outcome
	b.terminalReason = reason
	return b.snapshot()
}

func (b *RevisionBarrier) snapshot() RevisionBarrierObservation {
	outcome := b.terminal
	if outcome == "" {
		outcome = "pending"
	}
	return RevisionBarrierObservation{
		Outcome: outcome, Reason: b.terminalReason,
		Revision: b.revision, Generation: b.generation,
	}
}
