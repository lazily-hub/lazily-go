// EgressCore — the transport-agnostic reactive egress delivery authority
// (#lzegress), ported from lazily-rs `egress_core.rs`.
//
// The core owns delivery authority: sequence assignment, the unacknowledged
// window, a monotone acknowledgement watermark, the retry budget, and the
// producer-generation fence. It owns no graph handles and performs no I/O.
// DurableOutbox/SpillStore remain the persistence layer; RelayCell remains the
// conflation/backpressure layer.
//
// Every transition returns the EgressChange it caused — the reader kinds whose
// projection moved — and the reactive shells (EgressCell,
// ThreadSafeEgressCell, AsyncEgressCell) clear exactly that set. A transition
// that changes nothing reports an empty change and invalidates nothing.

package lazily

import (
	"errors"
	"math"
	"sort"
)

// EgressPolicy bounds one egress. RetryBudget counts retries AFTER the first
// attempt, so a budget of 1 allows two attempts in total.
type EgressPolicy struct {
	InflightLimit int
	RetryBudget   uint32
	RetryBase     uint64
	RetryCeiling  uint64
}

// DefaultEgressPolicy mirrors lazily-rs `EgressPolicy::default()`.
func DefaultEgressPolicy() EgressPolicy {
	return EgressPolicy{InflightLimit: 64, RetryBudget: 3, RetryBase: 10, RetryCeiling: 1_000}
}

// Egress configuration errors, mirroring lazily-rs `EgressConfigError`.
var (
	ErrEgressZeroInflightLimit     = errors.New("egress: inflight limit must be non-zero")
	ErrEgressZeroRetryBase         = errors.New("egress: retry base must be non-zero")
	ErrEgressRetryCeilingBelowBase = errors.New("egress: retry ceiling must be at least the retry base")
)

// EgressEnvelope is one send attempt. The (Generation, Sequence) pair is the
// transport idempotency identity.
type EgressEnvelope[T any] struct {
	Generation uint64
	Sequence   uint64
	Attempt    uint32
	Payload    T
}

// EgressRetry is the retry evidence of the most recent failed send.
type EgressRetry struct {
	Sequence  uint64
	Attempt   uint32
	Backoff   uint64
	Exhausted bool
}

// EgressClaimKind discriminates an EgressClaim.
type EgressClaimKind string

const (
	EgressClaimClaimed         EgressClaimKind = "claimed"
	EgressClaimEmpty           EgressClaimKind = "empty"
	EgressClaimWindowFull      EgressClaimKind = "window_full"
	EgressClaimStaleGeneration EgressClaimKind = "stale_generation"
)

// EgressClaim is the outcome of moving one pending record into the window.
// Envelope is meaningful for Claimed; Current for StaleGeneration.
type EgressClaim[T any] struct {
	Kind     EgressClaimKind
	Envelope EgressEnvelope[T]
	Current  uint64
}

// EgressAckKind discriminates an EgressAck.
type EgressAckKind string

const (
	EgressAckAdvanced        EgressAckKind = "advanced"
	EgressAckUnchanged       EgressAckKind = "unchanged"
	EgressAckStaleGeneration EgressAckKind = "stale_generation"
)

// EgressAck is the outcome of a cumulative acknowledgement. Through is the
// watermark for Advanced/Unchanged; Current for StaleGeneration.
type EgressAck struct {
	Kind    EgressAckKind
	Through uint64
	Current uint64
}

// EgressFailureKind discriminates an EgressFailure.
type EgressFailureKind string

const (
	EgressFailureRetrying        EgressFailureKind = "retrying"
	EgressFailureExhausted       EgressFailureKind = "exhausted"
	EgressFailureUnknownSequence EgressFailureKind = "unknown_sequence"
	EgressFailureStaleGeneration EgressFailureKind = "stale_generation"
)

// EgressFailure is the outcome of reporting a failed send. Retry is meaningful
// for Retrying/Exhausted; Current for StaleGeneration.
type EgressFailure struct {
	Kind    EgressFailureKind
	Retry   EgressRetry
	Current uint64
}

// EgressRetryActionKind discriminates an EgressRetryAction.
type EgressRetryActionKind string

const (
	EgressRetryScheduled       EgressRetryActionKind = "scheduled"
	EgressRetryUnknownSequence EgressRetryActionKind = "unknown_sequence"
	EgressRetryStaleGeneration EgressRetryActionKind = "stale_generation"
)

// EgressRetryAction is the outcome of making a parked failed record eligible.
type EgressRetryAction struct {
	Kind     EgressRetryActionKind
	Sequence uint64
	Current  uint64
}

// EgressReconnectKind discriminates an EgressReconnect.
type EgressReconnectKind string

const (
	EgressReconnectAdvanced        EgressReconnectKind = "advanced"
	EgressReconnectUnchanged       EgressReconnectKind = "unchanged"
	EgressReconnectStaleGeneration EgressReconnectKind = "stale_generation"
)

// EgressReconnect is the outcome of advancing the producer generation.
type EgressReconnect struct {
	Kind       EgressReconnectKind
	Generation uint64
	Replayed   int
	Current    uint64
}

// EgressChange is the reader-kind invalidation set one transition emits.
type EgressChange struct {
	Pending      bool
	Inflight     bool
	AckedThrough bool
	Retry        bool
}

// IsEmpty reports whether the transition changed no reader projection.
func (c EgressChange) IsEmpty() bool { return c == EgressChange{} }

type egressRecord[T any] struct {
	generation uint64
	sequence   uint64
	attempt    uint32
	payload    T
}

func (r egressRecord[T]) envelope() EgressEnvelope[T] {
	return EgressEnvelope[T]{
		Generation: r.generation,
		Sequence:   r.sequence,
		Attempt:    r.attempt,
		Payload:    r.payload,
	}
}

// EgressCore is the pure egress delivery authority. It is NOT safe for
// concurrent use on its own; the thread-safe and async shells serialize it.
type EgressCore[T any] struct {
	policy       EgressPolicy
	generation   uint64
	nextSequence uint64
	ackedThrough Opt[uint64]
	pending      []egressRecord[T]
	// inflight and failed are kept sorted by sequence, the BTreeMap order of
	// the reference implementation.
	inflight []egressRecord[T]
	failed   []egressRecord[T]
	retry    Opt[EgressRetry]
}

// NewEgressCore validates policy and builds an empty core at generation.
func NewEgressCore[T any](generation uint64, policy EgressPolicy) (*EgressCore[T], error) {
	if policy.InflightLimit <= 0 {
		return nil, ErrEgressZeroInflightLimit
	}
	if policy.RetryBase == 0 {
		return nil, ErrEgressZeroRetryBase
	}
	if policy.RetryCeiling < policy.RetryBase {
		return nil, ErrEgressRetryCeilingBelowBase
	}
	return &EgressCore[T]{policy: policy, generation: generation}, nil
}

// Policy returns the bounds in force.
func (c *EgressCore[T]) Policy() EgressPolicy { return c.policy }

// Generation is the current producer generation.
func (c *EgressCore[T]) Generation() uint64 { return c.generation }

// NextSequence is the sequence the next Enqueue will assign.
func (c *EgressCore[T]) NextSequence() uint64 { return c.nextSequence }

// Pending is the ordered projection of records awaiting a claim.
func (c *EgressCore[T]) Pending() []EgressEnvelope[T] { return egressEnvelopes(c.pending) }

// Inflight is the unacknowledged window, ordered by sequence.
func (c *EgressCore[T]) Inflight() []EgressEnvelope[T] { return egressEnvelopes(c.inflight) }

// AckedThrough is the cumulative acknowledgement watermark.
func (c *EgressCore[T]) AckedThrough() Opt[uint64] { return c.ackedThrough }

// Retry is the most recent retry evidence, if any is live.
func (c *EgressCore[T]) Retry() Opt[EgressRetry] { return c.retry }

func egressEnvelopes[T any](records []egressRecord[T]) []EgressEnvelope[T] {
	out := make([]EgressEnvelope[T], len(records))
	for i, record := range records {
		out[i] = record.envelope()
	}
	return out
}

func saturatingAddU64(a, b uint64) uint64 {
	if a > math.MaxUint64-b {
		return math.MaxUint64
	}
	return a + b
}

func saturatingIncU32(a uint32) uint32 {
	if a == math.MaxUint32 {
		return a
	}
	return a + 1
}

// Enqueue assigns the next sequence and places payload in the pending
// projection.
func (c *EgressCore[T]) Enqueue(payload T) (EgressChange, uint64) {
	sequence := c.nextSequence
	c.nextSequence = saturatingAddU64(c.nextSequence, 1)
	c.pending = append(c.pending, egressRecord[T]{
		generation: c.generation,
		sequence:   sequence,
		payload:    payload,
	})
	return EgressChange{Pending: true}, sequence
}

// Claim moves one pending record into the unacknowledged window.
//
// The caller supplies the generation captured by its transport attachment. An
// attachment from a superseded incarnation cannot claim or replay.
func (c *EgressCore[T]) Claim(generation uint64) (EgressChange, EgressClaim[T]) {
	if generation != c.generation {
		return EgressChange{}, EgressClaim[T]{Kind: EgressClaimStaleGeneration, Current: c.generation}
	}
	if len(c.inflight) >= c.policy.InflightLimit {
		return EgressChange{}, EgressClaim[T]{Kind: EgressClaimWindowFull}
	}
	if len(c.pending) == 0 {
		return EgressChange{}, EgressClaim[T]{Kind: EgressClaimEmpty}
	}
	record := c.pending[0]
	c.pending = c.pending[1:]
	record.generation = c.generation
	record.attempt = saturatingIncU32(record.attempt)
	envelope := record.envelope()
	c.inflight = insertEgressOrdered(c.inflight, record)
	retryChanged := c.retry.Present && c.retry.Value.Sequence == envelope.Sequence
	if retryChanged {
		c.retry = None[EgressRetry]()
	}
	return EgressChange{Pending: true, Inflight: true, Retry: retryChanged},
		EgressClaim[T]{Kind: EgressClaimClaimed, Envelope: envelope}
}

// Ack advances the acknowledgement watermark and prunes acknowledged records.
func (c *EgressCore[T]) Ack(generation, through uint64) (EgressChange, EgressAck) {
	if generation != c.generation {
		return EgressChange{}, EgressAck{Kind: EgressAckStaleGeneration, Current: c.generation}
	}
	if c.ackedThrough.Present && through <= c.ackedThrough.Value {
		return EgressChange{}, EgressAck{Kind: EgressAckUnchanged, Through: c.ackedThrough.Value}
	}
	inflightBefore := len(c.inflight)
	pendingBefore := len(c.pending)
	c.ackedThrough = Some(through)
	keep := func(record egressRecord[T]) bool { return record.sequence > through }
	c.inflight = filterEgress(c.inflight, keep)
	c.failed = filterEgress(c.failed, keep)
	c.pending = filterEgress(c.pending, keep)
	retryChanged := c.retry.Present && c.retry.Value.Sequence <= through
	if retryChanged {
		c.retry = None[EgressRetry]()
	}
	return EgressChange{
			Pending:      len(c.pending) != pendingBefore,
			Inflight:     len(c.inflight) != inflightBefore,
			AckedThrough: true,
			Retry:        retryChanged,
		},
		EgressAck{Kind: EgressAckAdvanced, Through: through}
}

// Fail parks a failed in-flight record while budget remains, or records
// terminal exhaustion.
func (c *EgressCore[T]) Fail(generation, sequence uint64) (EgressChange, EgressFailure) {
	if generation != c.generation {
		return EgressChange{}, EgressFailure{Kind: EgressFailureStaleGeneration, Current: c.generation}
	}
	index, ok := findEgress(c.inflight, sequence)
	if !ok {
		return EgressChange{}, EgressFailure{Kind: EgressFailureUnknownSequence}
	}
	record := c.inflight[index]
	c.inflight = append(c.inflight[:index:index], c.inflight[index+1:]...)
	exhausted := record.attempt > c.policy.RetryBudget
	retry := EgressRetry{
		Sequence:  sequence,
		Attempt:   record.attempt,
		Backoff:   c.backoff(record.attempt),
		Exhausted: exhausted,
	}
	c.retry = Some(retry)
	if !exhausted {
		c.failed = insertEgressOrdered(c.failed, record)
	}
	kind := EgressFailureRetrying
	if exhausted {
		kind = EgressFailureExhausted
	}
	return EgressChange{Inflight: true, Retry: true}, EgressFailure{Kind: kind, Retry: retry}
}

// RetryNow makes a parked failed record eligible again (the caller decides when
// its derived backoff has elapsed). Order is preserved: the record re-enters
// pending at its sequence position.
func (c *EgressCore[T]) RetryNow(generation, sequence uint64) (EgressChange, EgressRetryAction) {
	if generation != c.generation {
		return EgressChange{}, EgressRetryAction{Kind: EgressRetryStaleGeneration, Current: c.generation}
	}
	index, ok := findEgress(c.failed, sequence)
	if !ok {
		return EgressChange{}, EgressRetryAction{Kind: EgressRetryUnknownSequence}
	}
	record := c.failed[index]
	c.failed = append(c.failed[:index:index], c.failed[index+1:]...)
	c.pending = insertEgressOrdered(c.pending, record)
	retryChanged := c.retry.Present && c.retry.Value.Sequence == sequence
	if retryChanged {
		c.retry = None[EgressRetry]()
	}
	return EgressChange{Pending: true, Retry: retryChanged},
		EgressRetryAction{Kind: EgressRetryScheduled, Sequence: sequence}
}

// Reconnect fences the previous attachment and replays every unacknowledged
// in-flight (and parked failed) record through the new generation. The ack
// watermark and the sequence space survive; only the producer incarnation
// changes.
func (c *EgressCore[T]) Reconnect(generation uint64) (EgressChange, EgressReconnect) {
	if generation < c.generation {
		return EgressChange{}, EgressReconnect{Kind: EgressReconnectStaleGeneration, Current: c.generation}
	}
	if generation == c.generation {
		return EgressChange{}, EgressReconnect{Kind: EgressReconnectUnchanged, Generation: c.generation}
	}
	c.generation = generation
	replayed := len(c.inflight) + len(c.failed)
	for _, record := range c.inflight {
		record.generation = generation
		c.pending = insertEgressOrdered(c.pending, record)
	}
	c.inflight = nil
	for _, record := range c.failed {
		record.generation = generation
		c.pending = insertEgressOrdered(c.pending, record)
	}
	c.failed = nil
	for i := range c.pending {
		c.pending[i].generation = generation
	}
	retryChanged := c.retry.Present
	c.retry = None[EgressRetry]()
	return EgressChange{Pending: replayed > 0, Inflight: replayed > 0, Retry: retryChanged},
		EgressReconnect{Kind: EgressReconnectAdvanced, Generation: generation, Replayed: replayed}
}

func (c *EgressCore[T]) backoff(attempt uint32) uint64 {
	shift := uint64(0)
	if attempt > 0 {
		shift = uint64(attempt - 1)
	}
	if shift > 63 {
		shift = 63
	}
	factor := uint64(1) << shift
	base := c.policy.RetryBase
	var delay uint64
	if base != 0 && factor > math.MaxUint64/base {
		delay = math.MaxUint64
	} else {
		delay = base * factor
	}
	if delay > c.policy.RetryCeiling {
		return c.policy.RetryCeiling
	}
	return delay
}

// insertEgressOrdered inserts record before the first record with a larger
// sequence, the reference implementation's `insert_pending_ordered`.
func insertEgressOrdered[T any](records []egressRecord[T], record egressRecord[T]) []egressRecord[T] {
	position := sort.Search(len(records), func(i int) bool { return records[i].sequence > record.sequence })
	records = append(records, egressRecord[T]{})
	copy(records[position+1:], records[position:])
	records[position] = record
	return records
}

func findEgress[T any](records []egressRecord[T], sequence uint64) (int, bool) {
	for i, record := range records {
		if record.sequence == sequence {
			return i, true
		}
	}
	return 0, false
}

func filterEgress[T any](records []egressRecord[T], keep func(egressRecord[T]) bool) []egressRecord[T] {
	out := records[:0:0]
	for _, record := range records {
		if keep(record) {
			out = append(out, record)
		}
	}
	return out
}
