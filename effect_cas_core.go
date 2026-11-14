// EffectCasCore — generic keyed compare-and-swap effect authority
// (#lzeffectcas), ported from lazily-rs `effect_cas_core.rs`.
//
// The pure core owns effect identity, keyed single-flight, latest-wins
// coalescing, logical-clock retry timing, reconnect fencing, exact receipt
// matching, and the cursor of an ordered effect plan. Backends remain
// application-owned: they observe an opaque authority token and execute one
// atomic compare-and-apply (CompareAndApply). The core owns no thread, executor,
// timer, or I/O; a reactive shell (ThreadSafeEffectCas) projects its state and
// invalidates readers only after releasing the core lock.
//
// Type parameters: K is the effect key, A the opaque backend authority token, P
// a plan-step payload, R a backend receipt, and E a backend error.

package lazily

import (
	"cmp"
	"errors"
	"slices"
)

// EffectIdentity is the stable, non-empty identity of one desired effect. Retry,
// reconnect, and crash recovery all preserve it; a crash MUST NOT mint a new
// identity for already-retained work.
type EffectIdentity string

// ErrEmptyEffectIdentity rejects an empty effect identity.
var ErrEmptyEffectIdentity = errors.New("effect identity must be non-empty")

// NewEffectIdentity validates a non-empty identity.
func NewEffectIdentity(value string) (EffectIdentity, error) {
	if value == "" {
		return "", ErrEmptyEffectIdentity
	}
	return EffectIdentity(value), nil
}

// String returns the identity text.
func (id EffectIdentity) String() string { return string(id) }

// ErrEmptyEffectPlan rejects a plan with no steps.
var ErrEmptyEffectPlan = errors.New("an effect plan must contain at least one step")

// EffectPlan is an ordered, non-empty sequence of idempotent effect payloads.
// It is immutable once built.
type EffectPlan[P comparable] struct {
	steps []P
}

// NewEffectPlan validates a non-empty plan. The steps are copied.
func NewEffectPlan[P comparable](steps []P) (EffectPlan[P], error) {
	if len(steps) == 0 {
		return EffectPlan[P]{}, ErrEmptyEffectPlan
	}
	return EffectPlan[P]{steps: slices.Clone(steps)}, nil
}

// SingleEffectPlan is a one-step plan.
func SingleEffectPlan[P comparable](payload P) EffectPlan[P] {
	return EffectPlan[P]{steps: []P{payload}}
}

// Len is the number of steps.
func (p EffectPlan[P]) Len() int { return len(p.steps) }

// Steps returns a copy of the ordered steps.
func (p EffectPlan[P]) Steps() []P { return slices.Clone(p.steps) }

// Equal reports step-wise equality.
func (p EffectPlan[P]) Equal(other EffectPlan[P]) bool { return slices.Equal(p.steps, other.steps) }

// EffectIntent is one immutable desired effect. One epoch names exactly one
// identity and plan.
type EffectIntent[K cmp.Ordered, A comparable, P comparable] struct {
	Identity EffectIdentity
	Epoch    uint64
	Key      K
	Expected A
	Plan     EffectPlan[P]
}

// Equal reports full equality, the plan compared step-wise.
func (i EffectIntent[K, A, P]) Equal(other EffectIntent[K, A, P]) bool {
	return i.Identity == other.Identity && i.Epoch == other.Epoch && i.Key == other.Key &&
		i.Expected == other.Expected && i.Plan.Equal(other.Plan)
}

// EffectAttemptIdentity is the exact identity of one adapter call.
type EffectAttemptIdentity struct {
	Effect     EffectIdentity
	Generation uint64
	Epoch      uint64
	Step       int
	Attempt    uint32
}

// EffectReceipt is a successful step receipt, retained as lineage for later
// plan steps.
type EffectReceipt[R comparable, A comparable] struct {
	Attempt   EffectAttemptIdentity
	Receipt   R
	Authority A
}

// EffectAttempt is the exact compare-and-apply call handed to an adapter.
type EffectAttempt[K cmp.Ordered, A comparable, P comparable, R comparable] struct {
	Identity      EffectAttemptIdentity
	Key           K
	Expected      A
	Payload       P
	PriorReceipts []EffectReceipt[R, A]
}

func (a EffectAttempt[K, A, P, R]) clone() EffectAttempt[K, A, P, R] {
	a.PriorReceipts = slices.Clone(a.PriorReceipts)
	return a
}

// EffectApplyKind discriminates EffectApply.
type EffectApplyKind string

const (
	EffectApplyApplied    EffectApplyKind = "applied"
	EffectApplySuperseded EffectApplyKind = "superseded"
	EffectApplyRetryable  EffectApplyKind = "retryable"
	EffectApplyRejected   EffectApplyKind = "rejected"
)

// EffectApply is one of the only four outcomes an adapter may return:
// Applied{Receipt, Authority}, Superseded{Observed}, Retryable(Error), or
// Rejected(Error).
type EffectApply[R comparable, A comparable, E comparable] struct {
	Kind      EffectApplyKind
	Receipt   R
	Authority A
	Observed  A
	Error     E
}

// CompareAndApply is the narrow application-owned atomic authority boundary.
// The adapter MUST NOT retry, choose a newer intent, clear retained work, or
// advance a frontier — the core decides all of that.
type CompareAndApply[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable] interface {
	// Observe is a side-effect-free read of the backend's opaque authority.
	Observe(key K) (A, error)
	// CompareAndApply executes one atomic compare-and-apply. A failed
	// comparison returns Superseded with zero backend mutation.
	CompareAndApply(attempt EffectAttempt[K, A, P, R]) EffectApply[R, A, E]
}

// EffectPending is retained work: an intent plus its plan cursor.
type EffectPending[K cmp.Ordered, A comparable, P comparable, R comparable] struct {
	Intent      EffectIntent[K, A, P]
	NextStep    int
	Expected    A
	Receipts    []EffectReceipt[R, A]
	NextAttempt uint32
	ReadyAt     Opt[uint64]
}

func (p EffectPending[K, A, P, R]) clone() EffectPending[K, A, P, R] {
	p.Receipts = slices.Clone(p.Receipts)
	return p
}

// EffectRetry is the evidence of the most recent retryable failure.
type EffectRetry[E comparable] struct {
	Attempt EffectAttemptIdentity
	Error   E
	RetryAt uint64
}

// EffectTerminalKind discriminates EffectTerminal.
type EffectTerminalKind string

const (
	EffectTerminalApplied    EffectTerminalKind = "applied"
	EffectTerminalSuperseded EffectTerminalKind = "superseded"
	EffectTerminalRejected   EffectTerminalKind = "rejected"
)

// EffectTerminal is the most recent terminal receipt of a key. Observed is set
// only for a superseded terminal that saw backend authority; Error only for
// Rejected.
type EffectTerminal[R comparable, A comparable, E comparable] struct {
	Kind     EffectTerminalKind
	Identity EffectIdentity
	Epoch    uint64
	Observed Opt[A]
	Error    E
	Receipts []EffectReceipt[R, A]
}

func (t EffectTerminal[R, A, E]) clone() EffectTerminal[R, A, E] {
	t.Receipts = slices.Clone(t.Receipts)
	return t
}

// EffectCasKeyState is the reader projection of one key.
type EffectCasKeyState[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable] struct {
	Key            K
	Pending        *EffectPending[K, A, P, R]
	Inflight       *EffectAttempt[K, A, P, R]
	Retry          *EffectRetry[E]
	AppliedThrough Opt[uint64]
	Terminal       *EffectTerminal[R, A, E]
}

// EffectCasSnapshot is the reader projection of the whole core; Keys is ordered
// by key.
type EffectCasSnapshot[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable] struct {
	Generation uint64
	Keys       []EffectCasKeyState[K, A, P, R, E]
}

// EffectCasUpsertKind discriminates EffectCasUpsert.
type EffectCasUpsertKind string

const (
	EffectCasUpsertAccepted       EffectCasUpsertKind = "accepted"
	EffectCasUpsertUnchanged      EffectCasUpsertKind = "unchanged"
	EffectCasUpsertAlreadyApplied EffectCasUpsertKind = "already_applied"
	EffectCasUpsertStaleEpoch     EffectCasUpsertKind = "stale_epoch"
	EffectCasUpsertEpochConflict  EffectCasUpsertKind = "epoch_conflict"
)

// EffectCasUpsert is the outcome of UpsertDesired.
type EffectCasUpsert struct {
	Kind           EffectCasUpsertKind
	AppliedThrough uint64
	Current        uint64
}

// EffectCasClaimKind discriminates EffectCasClaim.
type EffectCasClaimKind string

const (
	EffectCasClaimClaimed         EffectCasClaimKind = "claimed"
	EffectCasClaimEmpty           EffectCasClaimKind = "empty"
	EffectCasClaimBusy            EffectCasClaimKind = "busy"
	EffectCasClaimWaiting         EffectCasClaimKind = "waiting"
	EffectCasClaimStaleGeneration EffectCasClaimKind = "stale_generation"
)

// EffectCasClaim is the outcome of Claim.
type EffectCasClaim[K cmp.Ordered, A comparable, P comparable, R comparable] struct {
	Kind    EffectCasClaimKind
	Attempt EffectAttempt[K, A, P, R]
	RetryAt uint64
	Current uint64
}

// EffectCasAppliedKind discriminates EffectCasApplied.
type EffectCasAppliedKind string

const (
	EffectCasAppliedStepApplied     EffectCasAppliedKind = "step_applied"
	EffectCasAppliedCompleted       EffectCasAppliedKind = "completed"
	EffectCasAppliedSuperseded      EffectCasAppliedKind = "superseded"
	EffectCasAppliedDuplicate       EffectCasAppliedKind = "duplicate"
	EffectCasAppliedUnknownAttempt  EffectCasAppliedKind = "unknown_attempt"
	EffectCasAppliedStaleGeneration EffectCasAppliedKind = "stale_generation"
)

// EffectCasApplied is the outcome of RecordApplied.
type EffectCasApplied struct {
	Kind           EffectCasAppliedKind
	NextStep       int
	AppliedThrough uint64
	Current        uint64
}

// EffectCasFailureKind discriminates EffectCasFailure.
type EffectCasFailureKind string

const (
	EffectCasFailureSuperseded      EffectCasFailureKind = "superseded"
	EffectCasFailureRetrying        EffectCasFailureKind = "retrying"
	EffectCasFailureRejected        EffectCasFailureKind = "rejected"
	EffectCasFailureUnknownAttempt  EffectCasFailureKind = "unknown_attempt"
	EffectCasFailureStaleGeneration EffectCasFailureKind = "stale_generation"
)

// EffectCasFailure is the outcome of RecordSuperseded / RecordRetryable /
// RecordRejected.
type EffectCasFailure struct {
	Kind    EffectCasFailureKind
	RetryAt uint64
	Current uint64
}

// EffectCasReconnectKind discriminates EffectCasReconnect.
type EffectCasReconnectKind string

const (
	EffectCasReconnectAdvanced        EffectCasReconnectKind = "advanced"
	EffectCasReconnectUnchanged       EffectCasReconnectKind = "unchanged"
	EffectCasReconnectStaleGeneration EffectCasReconnectKind = "stale_generation"
)

// EffectCasReconnect is the outcome of Reconnect.
type EffectCasReconnect struct {
	Kind       EffectCasReconnectKind
	Generation uint64
	Requeued   int
	Superseded int
	Current    uint64
}

// EffectCasOutcome is the outcome of RecordOutcome: exactly one of Applied (when
// IsApplied) or Failure.
type EffectCasOutcome struct {
	IsApplied bool
	Applied   EffectCasApplied
	Failure   EffectCasFailure
}

// EffectCasChange reports whether a transition changed any reader-visible
// state. A stale generation, unknown attempt, or non-matching receipt reports
// no change.
type EffectCasChange struct {
	State bool
}

type effectCasInflight[K cmp.Ordered, A comparable, P comparable, R comparable] struct {
	retained EffectPending[K, A, P, R]
	attempt  EffectAttempt[K, A, P, R]
}

type effectCasEntry[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable] struct {
	pending        *EffectPending[K, A, P, R]
	inflight       *effectCasInflight[K, A, P, R]
	retry          *EffectRetry[E]
	appliedThrough Opt[uint64]
	terminal       *EffectTerminal[R, A, E]
	terminalIntent *EffectIntent[K, A, P]
}

// EffectCasCore is the pure keyed effect-CAS orchestration authority. It is NOT
// safe for concurrent use on its own; ThreadSafeEffectCas serializes it.
type EffectCasCore[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable] struct {
	generation uint64
	keys       map[K]*effectCasEntry[K, A, P, R, E]
}

// NewEffectCasCore builds an empty core at actor generation.
func NewEffectCasCore[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable](generation uint64) *EffectCasCore[K, A, P, R, E] {
	return &EffectCasCore[K, A, P, R, E]{generation: generation, keys: map[K]*effectCasEntry[K, A, P, R, E]{}}
}

// Generation is the current actor generation.
func (c *EffectCasCore[K, A, P, R, E]) Generation() uint64 { return c.generation }

func (c *EffectCasCore[K, A, P, R, E]) sortedKeys() []K {
	keys := make([]K, 0, len(c.keys))
	for key := range c.keys {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func ptrClone[T any](value *T, clone func(T) T) *T {
	if value == nil {
		return nil
	}
	out := clone(*value)
	return &out
}

func effectCasCopy[T any](value T) T { return value }

func (c *EffectCasCore[K, A, P, R, E]) keyState(key K, entry *effectCasEntry[K, A, P, R, E]) EffectCasKeyState[K, A, P, R, E] {
	state := EffectCasKeyState[K, A, P, R, E]{
		Key:            key,
		Pending:        ptrClone(entry.pending, EffectPending[K, A, P, R].clone),
		Retry:          ptrClone(entry.retry, effectCasCopy[EffectRetry[E]]),
		AppliedThrough: entry.appliedThrough,
		Terminal:       ptrClone(entry.terminal, EffectTerminal[R, A, E].clone),
	}
	if entry.inflight != nil {
		attempt := entry.inflight.attempt.clone()
		state.Inflight = &attempt
	}
	return state
}

// Snapshot is the reader projection of every key, ordered by key.
func (c *EffectCasCore[K, A, P, R, E]) Snapshot() EffectCasSnapshot[K, A, P, R, E] {
	snapshot := EffectCasSnapshot[K, A, P, R, E]{Generation: c.generation}
	for _, key := range c.sortedKeys() {
		snapshot.Keys = append(snapshot.Keys, c.keyState(key, c.keys[key]))
	}
	return snapshot
}

// State is the reader projection of one key.
func (c *EffectCasCore[K, A, P, R, E]) State(key K) (EffectCasKeyState[K, A, P, R, E], bool) {
	entry, ok := c.keys[key]
	if !ok {
		return EffectCasKeyState[K, A, P, R, E]{}, false
	}
	return c.keyState(key, entry), true
}

// PendingKeys lists, in key order, every key with claimable work at logical
// time now. Scheduling observes this and invokes an adapter outside the lock.
func (c *EffectCasCore[K, A, P, R, E]) PendingKeys(now uint64) []K {
	var out []K
	for _, key := range c.sortedKeys() {
		entry := c.keys[key]
		if entry.inflight != nil || entry.pending == nil {
			continue
		}
		if entry.pending.ReadyAt.Present && entry.pending.ReadyAt.Value > now {
			continue
		}
		out = append(out, key)
	}
	return out
}

// UpsertDesired accepts only a strictly newer epoch. An equal immutable intent
// is idempotent, an equal epoch with a different identity/authority/plan is a
// conflict, and an epoch at or below applied_through is already applied.
// Replacing pending work records explicit supersession and keeps its receipts.
func (c *EffectCasCore[K, A, P, R, E]) UpsertDesired(intent EffectIntent[K, A, P]) (EffectCasChange, EffectCasUpsert) {
	entry, ok := c.keys[intent.Key]
	if !ok {
		entry = &effectCasEntry[K, A, P, R, E]{}
		c.keys[intent.Key] = entry
	}
	if entry.appliedThrough.Present && intent.Epoch <= entry.appliedThrough.Value {
		return EffectCasChange{}, EffectCasUpsert{Kind: EffectCasUpsertAlreadyApplied, AppliedThrough: entry.appliedThrough.Value}
	}

	var candidates []*EffectIntent[K, A, P]
	if entry.pending != nil {
		candidates = append(candidates, &entry.pending.Intent)
	}
	if entry.inflight != nil {
		candidates = append(candidates, &entry.inflight.retained.Intent)
	}
	current, hasCurrent := uint64(0), false
	for _, candidate := range candidates {
		if !hasCurrent || candidate.Epoch > current {
			current, hasCurrent = candidate.Epoch, true
		}
	}
	if entry.terminalIntent != nil && (!hasCurrent || entry.terminalIntent.Epoch > current) {
		current, hasCurrent = entry.terminalIntent.Epoch, true
	}
	if hasCurrent {
		if intent.Epoch < current {
			return EffectCasChange{}, EffectCasUpsert{Kind: EffectCasUpsertStaleEpoch, Current: current}
		}
		if intent.Epoch == current {
			same := false
			for _, candidate := range candidates {
				if candidate.Equal(intent) {
					same = true
				}
			}
			if entry.terminalIntent != nil && entry.terminalIntent.Equal(intent) {
				same = true
			}
			if same {
				return EffectCasChange{}, EffectCasUpsert{Kind: EffectCasUpsertUnchanged}
			}
			return EffectCasChange{}, EffectCasUpsert{Kind: EffectCasUpsertEpochConflict}
		}
	}

	if superseded := entry.pending; superseded != nil {
		entry.pending = nil
		terminalIntent := superseded.Intent
		entry.terminalIntent = &terminalIntent
		entry.terminal = &EffectTerminal[R, A, E]{
			Kind:     EffectTerminalSuperseded,
			Identity: superseded.Intent.Identity,
			Epoch:    superseded.Intent.Epoch,
			Receipts: superseded.Receipts,
		}
	}
	entry.pending = &EffectPending[K, A, P, R]{
		Intent:      intent,
		Expected:    intent.Expected,
		NextAttempt: 1,
	}
	entry.retry = nil
	return EffectCasChange{State: true}, EffectCasUpsert{Kind: EffectCasUpsertAccepted}
}

// Claim moves eligible pending work into the key's sole in-flight slot, at the
// current generation and logical clock now. Otherwise it returns Busy, Empty,
// Waiting{RetryAt}, or StaleGeneration without mutation.
func (c *EffectCasCore[K, A, P, R, E]) Claim(key K, generation, now uint64) (EffectCasChange, EffectCasClaim[K, A, P, R]) {
	if generation != c.generation {
		return EffectCasChange{}, EffectCasClaim[K, A, P, R]{Kind: EffectCasClaimStaleGeneration, Current: c.generation}
	}
	entry, ok := c.keys[key]
	if !ok {
		return EffectCasChange{}, EffectCasClaim[K, A, P, R]{Kind: EffectCasClaimEmpty}
	}
	if entry.inflight != nil {
		return EffectCasChange{}, EffectCasClaim[K, A, P, R]{Kind: EffectCasClaimBusy}
	}
	if entry.pending == nil {
		return EffectCasChange{}, EffectCasClaim[K, A, P, R]{Kind: EffectCasClaimEmpty}
	}
	if ready := entry.pending.ReadyAt; ready.Present && now < ready.Value {
		return EffectCasChange{}, EffectCasClaim[K, A, P, R]{Kind: EffectCasClaimWaiting, RetryAt: ready.Value}
	}

	pending := *entry.pending
	entry.pending = nil
	attempt := EffectAttempt[K, A, P, R]{
		Identity: EffectAttemptIdentity{
			Effect:     pending.Intent.Identity,
			Generation: generation,
			Epoch:      pending.Intent.Epoch,
			Step:       pending.NextStep,
			Attempt:    pending.NextAttempt,
		},
		Key:           pending.Intent.Key,
		Expected:      pending.Expected,
		Payload:       pending.Intent.Plan.steps[pending.NextStep],
		PriorReceipts: slices.Clone(pending.Receipts),
	}
	entry.inflight = &effectCasInflight[K, A, P, R]{retained: pending, attempt: attempt.clone()}
	return EffectCasChange{State: true}, EffectCasClaim[K, A, P, R]{Kind: EffectCasClaimClaimed, Attempt: attempt}
}

func terminalHasReceipt[R comparable, A comparable, E comparable](terminal *EffectTerminal[R, A, E], attempt EffectAttemptIdentity, receipt R) bool {
	if terminal == nil {
		return false
	}
	for _, known := range terminal.Receipts {
		if known.Attempt == attempt && known.Receipt == receipt {
			return true
		}
	}
	return false
}

// RecordApplied matches the exact attempt, retains the receipt, and either
// advances the plan cursor (the returned authority becomes the next step's
// expected token) or completes and monotonically advances applied_through. A
// duplicate terminal receipt is idempotent; a newer desired intent stops the old
// plan immediately.
func (c *EffectCasCore[K, A, P, R, E]) RecordApplied(key K, attempt EffectAttemptIdentity, receipt R, authority A) (EffectCasChange, EffectCasApplied) {
	if attempt.Generation != c.generation {
		return EffectCasChange{}, EffectCasApplied{Kind: EffectCasAppliedStaleGeneration, Current: c.generation}
	}
	entry, ok := c.keys[key]
	if !ok {
		return EffectCasChange{}, EffectCasApplied{Kind: EffectCasAppliedUnknownAttempt}
	}
	if terminalHasReceipt(entry.terminal, attempt, receipt) {
		return EffectCasChange{}, EffectCasApplied{Kind: EffectCasAppliedDuplicate}
	}
	if entry.inflight == nil || entry.inflight.attempt.Identity != attempt {
		return EffectCasChange{}, EffectCasApplied{Kind: EffectCasAppliedUnknownAttempt}
	}

	inflight := entry.inflight
	entry.inflight = nil
	retained := inflight.retained
	retained.Receipts = append(slices.Clone(retained.Receipts), EffectReceipt[R, A]{
		Attempt:   attempt,
		Receipt:   receipt,
		Authority: authority,
	})
	entry.retry = nil

	if entry.pending != nil && entry.pending.Intent.Epoch > retained.Intent.Epoch {
		terminalIntent := retained.Intent
		entry.terminalIntent = &terminalIntent
		entry.terminal = &EffectTerminal[R, A, E]{
			Kind:     EffectTerminalSuperseded,
			Identity: retained.Intent.Identity,
			Epoch:    retained.Intent.Epoch,
			Observed: Some(authority),
			Receipts: retained.Receipts,
		}
		return EffectCasChange{State: true}, EffectCasApplied{Kind: EffectCasAppliedSuperseded}
	}

	nextStep := retained.NextStep + 1
	if nextStep < retained.Intent.Plan.Len() {
		retained.NextStep = nextStep
		retained.Expected = authority
		retained.ReadyAt = None[uint64]()
		retained.NextAttempt = 1
		entry.pending = &retained
		return EffectCasChange{State: true}, EffectCasApplied{Kind: EffectCasAppliedStepApplied, NextStep: nextStep}
	}

	epoch := retained.Intent.Epoch
	if !entry.appliedThrough.Present || epoch > entry.appliedThrough.Value {
		entry.appliedThrough = Some(epoch)
	}
	terminalIntent := retained.Intent
	entry.terminalIntent = &terminalIntent
	entry.terminal = &EffectTerminal[R, A, E]{
		Kind:     EffectTerminalApplied,
		Identity: retained.Intent.Identity,
		Epoch:    epoch,
		Receipts: retained.Receipts,
	}
	return EffectCasChange{State: true}, EffectCasApplied{Kind: EffectCasAppliedCompleted, AppliedThrough: entry.appliedThrough.Value}
}

type effectCasFailureKind uint8

const (
	effectCasFailSuperseded effectCasFailureKind = iota
	effectCasFailRetryable
	effectCasFailRejected
)

// RecordSuperseded matches and terminally supersedes the attempt without
// advancing applied_through; the adapter mutated nothing.
func (c *EffectCasCore[K, A, P, R, E]) RecordSuperseded(key K, attempt EffectAttemptIdentity, observed A) (EffectCasChange, EffectCasFailure) {
	var zero E
	return c.finishFailed(key, attempt, effectCasFailSuperseded, observed, zero, 0)
}

// RecordRetryable matches and restores the same effect identity and cursor with
// an incremented attempt number and the caller-supplied logical retry time,
// unless a newer desired epoch already supersedes it.
func (c *EffectCasCore[K, A, P, R, E]) RecordRetryable(key K, attempt EffectAttemptIdentity, err E, retryAt uint64) (EffectCasChange, EffectCasFailure) {
	var zero A
	return c.finishFailed(key, attempt, effectCasFailRetryable, zero, err, retryAt)
}

// RecordRejected matches and terminally rejects the intent. Rejection is
// permanent and never aliases retry.
func (c *EffectCasCore[K, A, P, R, E]) RecordRejected(key K, attempt EffectAttemptIdentity, err E) (EffectCasChange, EffectCasFailure) {
	var zero A
	return c.finishFailed(key, attempt, effectCasFailRejected, zero, err, 0)
}

// RecordOutcome routes one adapter outcome to the matching Record* transition.
func (c *EffectCasCore[K, A, P, R, E]) RecordOutcome(key K, attempt EffectAttemptIdentity, outcome EffectApply[R, A, E], retryAt uint64) (EffectCasChange, EffectCasOutcome) {
	switch outcome.Kind {
	case EffectApplyApplied:
		change, applied := c.RecordApplied(key, attempt, outcome.Receipt, outcome.Authority)
		return change, EffectCasOutcome{IsApplied: true, Applied: applied}
	case EffectApplySuperseded:
		change, failure := c.RecordSuperseded(key, attempt, outcome.Observed)
		return change, EffectCasOutcome{Failure: failure}
	case EffectApplyRetryable:
		change, failure := c.RecordRetryable(key, attempt, outcome.Error, retryAt)
		return change, EffectCasOutcome{Failure: failure}
	case EffectApplyRejected:
		change, failure := c.RecordRejected(key, attempt, outcome.Error)
		return change, EffectCasOutcome{Failure: failure}
	}
	panic("lazily: unknown EffectApply kind " + string(outcome.Kind))
}

// Reconnect fences older receipts and requeues each in-flight cursor with the
// same effect identity and completed-step lineage, unless a newer desired intent
// supersedes it.
func (c *EffectCasCore[K, A, P, R, E]) Reconnect(newGeneration uint64) (EffectCasChange, EffectCasReconnect) {
	if newGeneration < c.generation {
		return EffectCasChange{}, EffectCasReconnect{Kind: EffectCasReconnectStaleGeneration, Current: c.generation}
	}
	if newGeneration == c.generation {
		return EffectCasChange{}, EffectCasReconnect{Kind: EffectCasReconnectUnchanged, Generation: c.generation}
	}
	c.generation = newGeneration
	requeued, superseded := 0, 0
	for _, key := range c.sortedKeys() {
		entry := c.keys[key]
		if entry.inflight == nil {
			continue
		}
		retained := entry.inflight.retained
		entry.inflight = nil
		if entry.pending != nil && entry.pending.Intent.Epoch > retained.Intent.Epoch {
			terminalIntent := retained.Intent
			entry.terminalIntent = &terminalIntent
			entry.terminal = &EffectTerminal[R, A, E]{
				Kind:     EffectTerminalSuperseded,
				Identity: retained.Intent.Identity,
				Epoch:    retained.Intent.Epoch,
				Receipts: retained.Receipts,
			}
			superseded++
			continue
		}
		retained.NextAttempt = saturatingIncU32(retained.NextAttempt)
		retained.ReadyAt = None[uint64]()
		entry.pending = &retained
		requeued++
	}
	return EffectCasChange{State: true}, EffectCasReconnect{
		Kind:       EffectCasReconnectAdvanced,
		Generation: newGeneration,
		Requeued:   requeued,
		Superseded: superseded,
	}
}

func (c *EffectCasCore[K, A, P, R, E]) finishFailed(
	key K,
	attempt EffectAttemptIdentity,
	kind effectCasFailureKind,
	observed A,
	err E,
	retryAt uint64,
) (EffectCasChange, EffectCasFailure) {
	if attempt.Generation != c.generation {
		return EffectCasChange{}, EffectCasFailure{Kind: EffectCasFailureStaleGeneration, Current: c.generation}
	}
	entry, ok := c.keys[key]
	if !ok {
		return EffectCasChange{}, EffectCasFailure{Kind: EffectCasFailureUnknownAttempt}
	}
	if entry.inflight == nil || entry.inflight.attempt.Identity != attempt {
		return EffectCasChange{}, EffectCasFailure{Kind: EffectCasFailureUnknownAttempt}
	}

	retained := entry.inflight.retained
	entry.inflight = nil
	newerPending := entry.pending != nil && entry.pending.Intent.Epoch > retained.Intent.Epoch
	terminal := func(kind EffectTerminalKind, observed Opt[A], err E) {
		entry.retry = nil
		terminalIntent := retained.Intent
		entry.terminalIntent = &terminalIntent
		entry.terminal = &EffectTerminal[R, A, E]{
			Kind:     kind,
			Identity: retained.Intent.Identity,
			Epoch:    retained.Intent.Epoch,
			Observed: observed,
			Error:    err,
			Receipts: retained.Receipts,
		}
	}
	var zeroErr E
	switch {
	case kind == effectCasFailSuperseded:
		terminal(EffectTerminalSuperseded, Some(observed), zeroErr)
		return EffectCasChange{State: true}, EffectCasFailure{Kind: EffectCasFailureSuperseded}
	case kind == effectCasFailRetryable && !newerPending:
		entry.retry = &EffectRetry[E]{Attempt: attempt, Error: err, RetryAt: retryAt}
		retained.NextAttempt = saturatingIncU32(retained.NextAttempt)
		retained.ReadyAt = Some(retryAt)
		entry.pending = &retained
		return EffectCasChange{State: true}, EffectCasFailure{Kind: EffectCasFailureRetrying, RetryAt: retryAt}
	case kind == effectCasFailRetryable:
		terminal(EffectTerminalSuperseded, None[A](), zeroErr)
		return EffectCasChange{State: true}, EffectCasFailure{Kind: EffectCasFailureSuperseded}
	default:
		terminal(EffectTerminalRejected, None[A](), err)
		return EffectCasChange{State: true}, EffectCasFailure{Kind: EffectCasFailureRejected}
	}
}

// --- durable image (#lzeffectcasdurable) -------------------------------------

// EffectCasDurableInflight is the durable image of an in-flight attempt and the
// retained work behind it.
type EffectCasDurableInflight[K cmp.Ordered, A comparable, P comparable, R comparable] struct {
	Retained EffectPending[K, A, P, R]
	Attempt  EffectAttempt[K, A, P, R]
}

// EffectCasDurableKey is the durable image of one key.
type EffectCasDurableKey[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable] struct {
	Key            K
	Pending        *EffectPending[K, A, P, R]
	Inflight       *EffectCasDurableInflight[K, A, P, R]
	Retry          *EffectRetry[E]
	AppliedThrough Opt[uint64]
	Terminal       *EffectTerminal[R, A, E]
	TerminalIntent *EffectIntent[K, A, P]
}

// EffectCasDurableState is the full-fidelity image of an EffectCasCore.
//
// EffectCasSnapshot is a reader projection: it omits the retained intent behind
// an in-flight attempt and the last terminal intent, so it cannot restore the
// core. This image carries every field the core needs to resume after a crash or
// in another process. Keys are a list ordered by key.
type EffectCasDurableState[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable] struct {
	Generation uint64
	Keys       []EffectCasDurableKey[K, A, P, R, E]
}

// Hydration refusals. Hydration never repairs an image: a corrupt image is
// refused so no adapter call runs on invented state.
var (
	ErrEffectCasDuplicateKey     = errors.New("durable effect-CAS image repeats a key")
	ErrEffectCasKeyMismatch      = errors.New("durable effect-CAS intent belongs to another key")
	ErrEffectCasCursorOutOfRange = errors.New("durable effect-CAS plan cursor is out of range")
	ErrEffectCasAttemptMismatch  = errors.New("durable effect-CAS attempt does not match its retained cursor")
	ErrEffectCasFutureGeneration = errors.New("durable effect-CAS attempt is newer than the image generation")
	ErrEffectCasEmptyPlan        = errors.New("durable effect-CAS intent carries an empty plan")
)

// DurableState is the full-fidelity image for crash recovery and cross-process
// persistence.
func (c *EffectCasCore[K, A, P, R, E]) DurableState() EffectCasDurableState[K, A, P, R, E] {
	state := EffectCasDurableState[K, A, P, R, E]{Generation: c.generation}
	for _, key := range c.sortedKeys() {
		entry := c.keys[key]
		durable := EffectCasDurableKey[K, A, P, R, E]{
			Key:            key,
			Pending:        ptrClone(entry.pending, EffectPending[K, A, P, R].clone),
			Retry:          ptrClone(entry.retry, effectCasCopy[EffectRetry[E]]),
			AppliedThrough: entry.appliedThrough,
			Terminal:       ptrClone(entry.terminal, EffectTerminal[R, A, E].clone),
			TerminalIntent: ptrClone(entry.terminalIntent, effectCasCopy[EffectIntent[K, A, P]]),
		}
		if entry.inflight != nil {
			durable.Inflight = &EffectCasDurableInflight[K, A, P, R]{
				Retained: entry.inflight.retained.clone(),
				Attempt:  entry.inflight.attempt.clone(),
			}
		}
		state.Keys = append(state.Keys, durable)
	}
	return state
}

// HydrateEffectCasCore restores a core from DurableState before the runtime
// becomes live. The restored core is observably identical to the one that wrote
// the image. A recovering actor then calls Reconnect with a newer generation to
// fence the crashed actor and replay its retained work under the SAME effect
// identity.
func HydrateEffectCasCore[K cmp.Ordered, A comparable, P comparable, R comparable, E comparable](
	state EffectCasDurableState[K, A, P, R, E],
) (*EffectCasCore[K, A, P, R, E], error) {
	core := NewEffectCasCore[K, A, P, R, E](state.Generation)
	for _, durable := range state.Keys {
		if durable.Pending != nil {
			if err := validateEffectRetained(durable.Key, *durable.Pending); err != nil {
				return nil, err
			}
		}
		if durable.TerminalIntent != nil {
			if durable.TerminalIntent.Key != durable.Key {
				return nil, ErrEffectCasKeyMismatch
			}
			if durable.TerminalIntent.Plan.Len() == 0 {
				return nil, ErrEffectCasEmptyPlan
			}
		}
		entry := &effectCasEntry[K, A, P, R, E]{
			pending:        ptrClone(durable.Pending, EffectPending[K, A, P, R].clone),
			retry:          ptrClone(durable.Retry, effectCasCopy[EffectRetry[E]]),
			appliedThrough: durable.AppliedThrough,
			terminal:       ptrClone(durable.Terminal, EffectTerminal[R, A, E].clone),
			terminalIntent: ptrClone(durable.TerminalIntent, effectCasCopy[EffectIntent[K, A, P]]),
		}
		if inflight := durable.Inflight; inflight != nil {
			if err := validateEffectRetained(durable.Key, inflight.Retained); err != nil {
				return nil, err
			}
			attempt, retained := inflight.Attempt, inflight.Retained
			if attempt.Identity.Generation > state.Generation {
				return nil, ErrEffectCasFutureGeneration
			}
			if attempt.Key != durable.Key ||
				attempt.Identity.Effect != retained.Intent.Identity ||
				attempt.Identity.Epoch != retained.Intent.Epoch ||
				attempt.Identity.Step != retained.NextStep ||
				attempt.Identity.Attempt != retained.NextAttempt ||
				attempt.Expected != retained.Expected ||
				attempt.Payload != retained.Intent.Plan.steps[retained.NextStep] ||
				!slices.Equal(attempt.PriorReceipts, retained.Receipts) {
				return nil, ErrEffectCasAttemptMismatch
			}
			entry.inflight = &effectCasInflight[K, A, P, R]{retained: retained.clone(), attempt: attempt.clone()}
		}
		if _, dup := core.keys[durable.Key]; dup {
			return nil, ErrEffectCasDuplicateKey
		}
		core.keys[durable.Key] = entry
	}
	return core, nil
}

func validateEffectRetained[K cmp.Ordered, A comparable, P comparable, R comparable](key K, retained EffectPending[K, A, P, R]) error {
	if retained.Intent.Key != key {
		return ErrEffectCasKeyMismatch
	}
	if retained.Intent.Plan.Len() == 0 {
		return ErrEffectCasEmptyPlan
	}
	if retained.NextStep < 0 || retained.NextStep >= retained.Intent.Plan.Len() {
		return ErrEffectCasCursorOutOfRange
	}
	return nil
}
