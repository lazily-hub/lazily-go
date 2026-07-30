// IngressCore — the graph-agnostic admission algebra behind every ingress
// flavor (#designimplementtransport).
//
// Spec:   lazily-spec/docs/transport-ingress.md
// Formal: lazily-formal/LazilyFormal/Ingress.lean
// Rust reference: lazily-rs/src/ingress_core.rs
//
// Same split keyedOrder makes for the map family and for the same reason:
// deciding whether an inbound envelope is *admissible* touches no reactive node
// and awaits nothing, so the single-threaded, thread-safe, and async shells share
// it verbatim — while reactivity deliberately stays OUT. Invalidation is a graph
// write, so each flavor mints its own per-scope readers on its own graph and
// clears them with the storage lock released.
//
// Every mutator therefore returns an IngressChange — *which* reader kinds the
// transition dirtied — rather than performing the invalidation itself. That
// return value is the whole contract between the core and a shell, and it is a
// pure function of the transition, which is what makes the plane portable across
// flavors without re-deriving values per flavor.
//
// # Transport-agnostic by construction
//
// The core never touches a transport. An envelope is a value
// (IngressEnvelope) carrying its own provenance — generation, sequence,
// stamped_at — so a WebSocket frame, an RPC response, and a polled page are the
// SAME input once decoded. That is what makes stale rejection, dedupe, reorder,
// freshness, and backpressure independent of how the bytes arrived, and it is why
// IngressTransportKind exists only to derive a *schedule*.
//
// # What is a derive and what is a call
//
// Readiness, authority, and retry are not imperative refresh calls. They are pure
// functions of scope state (IngressScopeView.Readiness / Authority / Retry) that
// each shell exposes as a Computed. Freshness is time-dependent, so it enters
// through an explicit Tick rather than a hidden clock read — the same discipline
// TimerCell.Tick uses, and the reason staleness transitions are deterministic in
// fixtures.

package lazily

import "math"

// --- transport ---------------------------------------------------------------

// IngressTransportKind is how envelopes reach a scope. Event delivery is the
// default and needs no schedule; the other two exist so a binding without an
// event channel still has a *bounded* fallback rather than an unbounded refresh
// loop.
type IngressTransportKind string

const (
	// IngressTransportEventChannel is server-initiated delivery (WebSocket, SSE,
	// in-proc channel). Preferred.
	IngressTransportEventChannel IngressTransportKind = "EventChannel"
	// IngressTransportRpcTriggered is client-initiated but triggered by an
	// out-of-band event rather than a timer — an RPC issued *because* something
	// happened.
	IngressTransportRpcTriggered IngressTransportKind = "RpcTriggered"
	// IngressTransportBoundedPolling is client-initiated on a bounded interval.
	// The fallback of last resort.
	IngressTransportBoundedPolling IngressTransportKind = "BoundedPolling"
)

// IngressSchedule is when, if ever, a scope should ask the transport for more
// data.
//
// PollInterval is present only for IngressTransportBoundedPolling — making "we
// polled a transport that pushes" unrepresentable rather than merely discouraged.
type IngressSchedule struct {
	// Kind is the transport this schedule was derived from.
	Kind IngressTransportKind
	// PollInterval is the bounded poll period, absent when delivery is
	// event-driven.
	PollInterval Opt[uint64]
}

// IngressScheduleFor derives the schedule for kind. A poll interval is offered
// only where event delivery is unavailable, and never zero.
func IngressScheduleFor(kind IngressTransportKind, pollInterval uint64) IngressSchedule {
	if kind != IngressTransportBoundedPolling {
		return IngressSchedule{Kind: kind}
	}
	if pollInterval < 1 {
		pollInterval = 1
	}
	return IngressSchedule{Kind: kind, PollInterval: Some(pollInterval)}
}

// IngressEnvelope is one decoded inbound message, with the provenance admission
// needs.
//
// Generation fences a producer incarnation (a reconnect, a redeploy, a build
// skew); Sequence orders within a generation; StampedAt is the producer's logical
// time, which is what freshness is measured against.
type IngressEnvelope[K comparable, T any] struct {
	// Key is the lifecycle-scoped identity this envelope belongs to.
	Key K
	// Generation is the producer incarnation. Monotone per key; a higher value
	// fences lower ones.
	Generation uint64
	// Sequence is the position within Generation, starting at 0.
	Sequence uint64
	// StampedAt is the producer's logical timestamp, compared against the
	// freshness horizon.
	StampedAt uint64
	// Payload is the decoded value.
	Payload T
}

// NewIngressEnvelope builds an envelope at generation/sequence stamped stampedAt.
func NewIngressEnvelope[K comparable, T any](
	key K,
	generation, sequence, stampedAt uint64,
	payload T,
) IngressEnvelope[K, T] {
	return IngressEnvelope[K, T]{
		Key:        key,
		Generation: generation,
		Sequence:   sequence,
		StampedAt:  stampedAt,
		Payload:    payload,
	}
}

// IngressReplayRecord is one replay request a transport was asked to carry.
type IngressReplayRecord[K comparable] struct {
	Key     K
	Request ReplayRequest
}

// IngressTransport is a decoded source of envelopes.
//
// The core never calls this — a shell's Pump does — which is exactly what keeps
// admission independent of delivery. Implementations decode; they do not decide.
type IngressTransport[K comparable, T any] interface {
	// Kind reports how this transport delivers. Drives IngressSchedule and
	// nothing else.
	Kind() IngressTransportKind
	// Drain takes everything decoded since the last call. Never blocks.
	Drain() []IngressEnvelope[K, T]
	// RequestReplay asks the producer to resend from request.FromSequence. It
	// reports whether the transport could carry the request — a polling
	// transport that cannot address history answers false, which is what makes
	// "this gap will never close" observable rather than silent.
	RequestReplay(key K, request ReplayRequest) bool
}

// InProcIngress is an in-process event channel: the reference IngressTransport,
// and the one the conformance corpus replays against.
//
// Kind is configurable so one implementation exercises all three delivery modes —
// including the BoundedPolling case that cannot serve a replay.
type InProcIngress[K comparable, T any] struct {
	kind    IngressTransportKind
	inbound []IngressEnvelope[K, T]
	replays []IngressReplayRecord[K]
}

// NewInProcIngress creates an empty channel delivering as kind.
func NewInProcIngress[K comparable, T any](kind IngressTransportKind) *InProcIngress[K, T] {
	return &InProcIngress[K, T]{kind: kind}
}

// Kind reports the configured delivery mode.
func (t *InProcIngress[K, T]) Kind() IngressTransportKind { return t.kind }

// Push queues one envelope for the next Drain.
func (t *InProcIngress[K, T]) Push(envelope IngressEnvelope[K, T]) {
	t.inbound = append(t.inbound, envelope)
}

// Drain takes every queued envelope, oldest first.
func (t *InProcIngress[K, T]) Drain() []IngressEnvelope[K, T] {
	batch := t.inbound
	t.inbound = nil
	return batch
}

// RequestReplay records the request, unless this transport is a bounded poll.
func (t *InProcIngress[K, T]) RequestReplay(key K, request ReplayRequest) bool {
	// A bounded poll has no addressable history: it can only wait for the next
	// page, so it cannot honour a replay.
	if t.kind == IngressTransportBoundedPolling {
		return false
	}
	t.replays = append(t.replays, IngressReplayRecord[K]{Key: key, Request: request})
	return true
}

// Replays returns the replay requests observed so far, oldest first.
func (t *InProcIngress[K, T]) Replays() []IngressReplayRecord[K] {
	return append([]IngressReplayRecord[K](nil), t.replays...)
}

// --- decisions ---------------------------------------------------------------

// IngressDropReason is why an envelope was refused. Every value is a *decision*,
// not a failure — dropping a superseded envelope is correct behaviour and is
// receipted as such.
type IngressDropReason string

const (
	// IngressDropStaleGeneration — generation is below the scope's fence: a
	// zombie producer.
	IngressDropStaleGeneration IngressDropReason = "StaleGeneration"
	// IngressDropDuplicateSequence — the sequence was already delivered in this
	// generation.
	IngressDropDuplicateSequence IngressDropReason = "DuplicateSequence"
	// IngressDropDuplicateBuffered — the sequence is already sitting in the
	// reorder buffer.
	IngressDropDuplicateBuffered IngressDropReason = "DuplicateBuffered"
	// IngressDropReorderWindowOverflow — the reorder buffer is at ReorderWindow
	// and this envelope does not fill the gap.
	IngressDropReorderWindowOverflow IngressDropReason = "ReorderWindowOverflow"
	// IngressDropExpired — now - stamped_at exceeds the freshness horizon.
	IngressDropExpired IngressDropReason = "Expired"
	// IngressDropBackpressure — the hot window is at HighWater under a bounding
	// overflow policy.
	IngressDropBackpressure IngressDropReason = "Backpressure"
	// IngressDropScopeClosed — the scope is closed; it admits nothing until
	// reopened.
	IngressDropScopeClosed IngressDropReason = "ScopeClosed"
)

// IngressError is a transport- or decode-level failure attributed to a scope.
// Distinct from a drop: an error means we could not *decide*, so it drives retry.
type IngressError string

const (
	// IngressErrorTransportClosed — the transport closed or reset under us.
	IngressErrorTransportClosed IngressError = "TransportClosed"
	// IngressErrorDecodeFailed — the frame could not be decoded into an envelope.
	IngressErrorDecodeFailed IngressError = "DecodeFailed"
	// IngressErrorAuthorityLost — the producer reported that our generation is no
	// longer authoritative.
	IngressErrorAuthorityLost IngressError = "AuthorityLost"
)

// IngressAdmissionKind discriminates IngressAdmission.
//
// The constants carry the Admission prefix because relay.go already owns
// IngressAccepted / IngressConflated / IngressDropped / IngressBlocked for its
// own IngressOutcome — a flat Go package has one namespace.
type IngressAdmissionKind string

const (
	// IngressAdmissionAccepted — delivered in order, window holds this op alone.
	IngressAdmissionAccepted IngressAdmissionKind = "Accepted"
	// IngressAdmissionConflated — delivered in order and coalesced with at least
	// one other op, either a prior undrained op or a buffered successor this
	// delivery flushed.
	IngressAdmissionConflated IngressAdmissionKind = "Conflated"
	// IngressAdmissionBuffered — held pending an earlier sequence. Nothing is
	// visible yet.
	IngressAdmissionBuffered IngressAdmissionKind = "Buffered"
	// IngressAdmissionGenerationHandoff — a newer producer incarnation took
	// over: sequence expectations reset and the envelope was delivered.
	IngressAdmissionGenerationHandoff IngressAdmissionKind = "GenerationHandoff"
	// IngressAdmissionDropped — refused, with the reason receipted.
	IngressAdmissionDropped IngressAdmissionKind = "Dropped"
	// IngressAdmissionBlocked — refused by OverflowBlock; the producer must retry
	// after a drain.
	IngressAdmissionBlocked IngressAdmissionKind = "Blocked"
)

// IngressAdmission is the outcome of admitting one envelope. It is a comparable
// tagged record rather than a Rust-style enum, so fixtures can compare it with ==.
type IngressAdmission struct {
	Kind IngressAdmissionKind
	// DeliveredThrough is the highest in-order sequence now delivered
	// (Accepted / Conflated / GenerationHandoff).
	DeliveredThrough uint64
	// GapFrom is the first sequence still missing (Buffered).
	GapFrom uint64
	// From and To are the fence we held and the fence we now hold
	// (GenerationHandoff).
	From, To uint64
	// Reason is the refusal class (Dropped).
	Reason IngressDropReason
}

// IngressAdmitted builds an Accepted outcome.
func IngressAdmitted(deliveredThrough uint64) IngressAdmission {
	return IngressAdmission{Kind: IngressAdmissionAccepted, DeliveredThrough: deliveredThrough}
}

// IngressCoalesced builds a Conflated outcome.
func IngressCoalesced(deliveredThrough uint64) IngressAdmission {
	return IngressAdmission{Kind: IngressAdmissionConflated, DeliveredThrough: deliveredThrough}
}

// IngressHeld builds a Buffered outcome.
func IngressHeld(gapFrom uint64) IngressAdmission {
	return IngressAdmission{Kind: IngressAdmissionBuffered, GapFrom: gapFrom}
}

// IngressHandedOff builds a GenerationHandoff outcome.
//
// It deliberately does NOT carry DeliveredThrough: the fixtures pin a handoff by
// its two fences, and folding the watermark in would make == comparison against a
// fixture-built expectation depend on a field the corpus never states.
func IngressHandedOff(from, to uint64) IngressAdmission {
	return IngressAdmission{Kind: IngressAdmissionGenerationHandoff, From: from, To: to}
}

// IngressRefused builds a Dropped outcome.
func IngressRefused(reason IngressDropReason) IngressAdmission {
	return IngressAdmission{Kind: IngressAdmissionDropped, Reason: reason}
}

// IngressBackpressured builds a Blocked outcome.
func IngressBackpressured() IngressAdmission {
	return IngressAdmission{Kind: IngressAdmissionBlocked}
}

// IsDelivered reports whether the envelope became visible to readers.
func (a IngressAdmission) IsDelivered() bool {
	switch a.Kind {
	case IngressAdmissionAccepted, IngressAdmissionConflated,
		IngressAdmissionGenerationHandoff:
		return true
	default:
		return false
	}
}

// --- lifecycle and derives ---------------------------------------------------

// IngressLifecycle is where a scope is in its lifecycle. Scopes are keyed and
// independent: closing one never touches another.
type IngressLifecycle string

const (
	// IngressLifecycleOpening — opened, nothing delivered yet.
	IngressLifecycleOpening IngressLifecycle = "Opening"
	// IngressLifecycleLive — delivering.
	IngressLifecycleLive IngressLifecycle = "Live"
	// IngressLifecycleSuspended — disconnected but retained: state and cursors
	// survive for replay.
	IngressLifecycleSuspended IngressLifecycle = "Suspended"
	// IngressLifecycleClosed — terminal until reopened. Admits nothing.
	IngressLifecycleClosed IngressLifecycle = "Closed"
)

// IngressReadiness is the derived answer to "can a consumer trust this scope
// right now?".
type IngressReadiness string

const (
	// IngressReadinessUnknown — no such scope.
	IngressReadinessUnknown IngressReadiness = "Unknown"
	// IngressReadinessWarming — open, nothing delivered yet.
	IngressReadinessWarming IngressReadiness = "Warming"
	// IngressReadinessReady — delivered and inside the freshness horizon.
	IngressReadinessReady IngressReadiness = "Ready"
	// IngressReadinessStale — delivered, but the newest accepted stamp is older
	// than the horizon.
	IngressReadinessStale IngressReadiness = "Stale"
	// IngressReadinessSuspended — disconnected; retained state may be replayed.
	IngressReadinessSuspended IngressReadiness = "Suspended"
	// IngressReadinessClosed — terminal.
	IngressReadinessClosed IngressReadiness = "Closed"
)

// IngressAuthority is what the scope currently claims authority over — the fence
// plus the in-order watermark a replay request must resume from.
type IngressAuthority struct {
	// Generation is the generation fence currently held.
	Generation uint64
	// DeliveredThrough is the highest in-order sequence delivered, absent before
	// first delivery.
	DeliveredThrough Opt[uint64]
	// StampedAt is the producer stamp of the newest delivered envelope.
	StampedAt uint64
}

// IngressRetry is the derived retry decision for a scope that has errored.
type IngressRetry struct {
	// Attempt counts consecutive errors since the last delivery.
	Attempt uint32
	// Backoff is the exponential backoff, clamped to the policy ceiling.
	Backoff uint64
	// ResumeFrom is the sequence a replay should resume from.
	ResumeFrom uint64
}

// ReplayRequest is what a reconnect needs from the transport to close its gap.
type ReplayRequest struct {
	// Generation being resumed.
	Generation uint64
	// FromSequence is the first sequence the consumer has not delivered.
	FromSequence uint64
}

// --- policy ------------------------------------------------------------------

// IngressPolicy carries the bounds and taxes, all flavor-neutral.
type IngressPolicy struct {
	// ReorderWindow is how many out-of-order envelopes may be held per scope.
	// 0 disables reordering: a gap drops immediately.
	ReorderWindow int
	// FreshnessHorizon — now - stamped_at above this marks a scope Stale; an
	// *arriving* envelope that old is dropped as Expired.
	FreshnessHorizon uint64
	// HighWater is the merged-op count at which Overflow engages.
	HighWater uint64
	// Overflow is what to do at HighWater. Reuses the relay algebra's policy.
	Overflow Overflow
	// ReceiptCapacity is the retained receipt count, oldest evicted first.
	ReceiptCapacity int
	// RetryBase is the first retry backoff; it doubles per consecutive error.
	RetryBase uint64
	// RetryCeiling clamps the backoff.
	RetryCeiling uint64
}

// DefaultIngressPolicy is the reference tuning.
func DefaultIngressPolicy() IngressPolicy {
	return IngressPolicy{
		ReorderWindow:    8,
		FreshnessHorizon: 1000,
		HighWater:        64,
		Overflow:         OverflowConflate,
		ReceiptCapacity:  256,
		RetryBase:        10,
		RetryCeiling:     10000,
	}
}

// IngressConfigError is why a policy was refused at construction time.
type IngressConfigError struct{ msg string }

func (e *IngressConfigError) Error() string { return "lazily: ingress config: " + e.msg }

// ErrIngressConflateNotBounding is returned when OverflowConflate is chosen for a
// non-conflating merge algebra. Conflate bounds nothing when ⊕ does not conflate —
// validated exactly as NewRelayCell validates it.
var ErrIngressConflateNotBounding = &IngressConfigError{msg: "ConflateNotBounding"}

// ErrIngressZeroReceiptCapacity is returned for a zero receipt capacity, which
// would discard every receipt it just minted.
var ErrIngressZeroReceiptCapacity = &IngressConfigError{msg: "ZeroReceiptCapacity"}

// --- receipts ----------------------------------------------------------------

// IngressReceiptChannel is which receipt channel a receipt belongs to. The three
// are separate reader kinds because they have separate consumers: a projection
// wants accepts, a dashboard wants drops, a supervisor wants errors.
type IngressReceiptChannel string

const (
	// IngressReceiptAccepted — delivered.
	IngressReceiptAccepted IngressReceiptChannel = "Accepted"
	// IngressReceiptDropped — refused by a decision.
	IngressReceiptDropped IngressReceiptChannel = "Dropped"
	// IngressReceiptError — could not be decided.
	IngressReceiptError IngressReceiptChannel = "Error"
)

// IngressReceiptOutcome is the decision a receipt records. Channel is the tag.
type IngressReceiptOutcome struct {
	// Channel is which of the three logs this outcome belongs to.
	Channel IngressReceiptChannel
	// DeliveredThrough is the resulting watermark (Accepted).
	DeliveredThrough uint64
	// Conflated reports whether the payload coalesced into a non-empty window
	// (Accepted).
	Conflated bool
	// Reason is the refusal class (Dropped).
	Reason IngressDropReason
	// Err is the failure (Error).
	Err IngressError
}

// IngressReceipt is one durable record of an admission decision.
type IngressReceipt[K comparable] struct {
	// Offset is a monotone receipt offset, stable across eviction — so a consumer
	// can tell "I have seen everything" from "the log wrapped".
	Offset uint64
	// Key is the scope the decision was made for.
	Key K
	// Generation is the generation the decision was made under.
	Generation uint64
	// Sequence is the sequence the decision was made for, when there was one.
	Sequence Opt[uint64]
	// Outcome is the decision.
	Outcome IngressReceiptOutcome
}

// Channel reports which channel this receipt is read from.
func (r IngressReceipt[K]) Channel() IngressReceiptChannel { return r.Outcome.Channel }

// --- the change set ----------------------------------------------------------

// IngressScopeChange is which of a scope's reader kinds a transition dirtied.
//
// Four kinds exist because they have four different invalidation boundaries: a
// buffered envelope moves nothing but its own gap, a Tick across the horizon
// moves only readiness, and an error moves only retry.
type IngressScopeChange struct {
	// Value — the coalesced window changed.
	Value bool
	// Readiness — IngressReadiness changed.
	Readiness bool
	// Authority — IngressAuthority changed.
	Authority bool
	// Retry — IngressRetry changed.
	Retry bool
}

// IsEmpty reports that nothing changed — the shell must not clear a reader.
func (c IngressScopeChange) IsEmpty() bool {
	return !(c.Value || c.Readiness || c.Authority || c.Retry)
}

func ingressScopeChangeAll() IngressScopeChange {
	return IngressScopeChange{Value: true, Readiness: true, Authority: true, Retry: true}
}

func ingressScopeChangeReadinessOnly() IngressScopeChange {
	return IngressScopeChange{Readiness: true}
}

func ingressScopeChangeValueOnly() IngressScopeChange {
	return IngressScopeChange{Value: true}
}

func ingressScopeChangeRetryOnly() IngressScopeChange {
	return IngressScopeChange{Retry: true}
}

// ingressScopeChangeCreation is what materializing a previously-unknown scope
// changes: an unknown scope reads Unknown / absent, so its first appearance moves
// readiness and authority — and nothing else. A reader that observed a key before
// it opened must learn that it did.
func ingressScopeChangeCreation() IngressScopeChange {
	return IngressScopeChange{Readiness: true, Authority: true}
}

func (c IngressScopeChange) union(other IngressScopeChange) IngressScopeChange {
	return IngressScopeChange{
		Value:     c.Value || other.Value,
		Readiness: c.Readiness || other.Readiness,
		Authority: c.Authority || other.Authority,
		Retry:     c.Retry || other.Retry,
	}
}

// IngressScopeDelta pairs a scope key with its dirtied reader kinds.
type IngressScopeDelta[K comparable] struct {
	Key    K
	Change IngressScopeChange
}

// IngressChange is the pure invalidation set of one transition: the whole
// contract between the core and a flavor shell.
type IngressChange[K comparable] struct {
	// Scopes lists per-scope dirty reader kinds, in transition order.
	Scopes []IngressScopeDelta[K]
	// AcceptedReceipts — the accepted-receipt reader grew.
	AcceptedReceipts bool
	// DroppedReceipts — the dropped-receipt reader grew.
	DroppedReceipts bool
	// ErrorReceipts — the error-receipt reader grew.
	ErrorReceipts bool
}

// IsEmpty reports whether this transition dirtied nothing at all.
func (c IngressChange[K]) IsEmpty() bool {
	return len(c.Scopes) == 0 &&
		!c.AcceptedReceipts && !c.DroppedReceipts && !c.ErrorReceipts
}

func (c *IngressChange[K]) mark(key K, change IngressScopeChange) {
	if change.IsEmpty() {
		return
	}
	c.Scopes = append(c.Scopes, IngressScopeDelta[K]{Key: key, Change: change})
}

func (c *IngressChange[K]) markChannel(channel IngressReceiptChannel) {
	switch channel {
	case IngressReceiptAccepted:
		c.AcceptedReceipts = true
	case IngressReceiptDropped:
		c.DroppedReceipts = true
	case IngressReceiptError:
		c.ErrorReceipts = true
	}
}

// --- the scope projection ----------------------------------------------------

// IngressScopeView is a read-only projection of one scope, from which every
// derive is computed.
//
// A shell's reader closures call these and nothing else, which is why the three
// flavors cannot disagree about readiness, authority, or retry.
//
// Named IngressScopeView rather than the Rust reference's bare ScopeView: a flat
// Go package has one namespace and "ScopeView" claims a word the ingress family
// does not own.
type IngressScopeView struct {
	// Lifecycle position.
	Lifecycle IngressLifecycle
	// Generation fence.
	Generation uint64
	// DeliveredThrough is the in-order watermark.
	DeliveredThrough Opt[uint64]
	// StampedAt is the producer stamp of the newest delivered envelope.
	StampedAt uint64
	// Buffered counts out-of-order envelopes held.
	Buffered int
	// WindowDepth counts merged ops in the hot window.
	WindowDepth uint64
	// ConsecutiveErrors counts errors since the last delivery.
	ConsecutiveErrors uint32
	// ObservedNow is logical now, as of the last Tick.
	ObservedNow uint64
	// Policy in force.
	Policy IngressPolicy
}

func ingressElapsed(now, stamp uint64) uint64 {
	if now < stamp {
		return 0
	}
	return now - stamp
}

// IsFresh reports whether the newest delivered stamp is inside the freshness
// horizon.
func (v IngressScopeView) IsFresh() bool {
	return ingressElapsed(v.ObservedNow, v.StampedAt) <= v.Policy.FreshnessHorizon
}

// Readiness is the derived readiness. A scope that has never delivered is
// Warming, not Stale, because there is no stamp to be old.
func (v IngressScopeView) Readiness() IngressReadiness {
	switch v.Lifecycle {
	case IngressLifecycleClosed:
		return IngressReadinessClosed
	case IngressLifecycleSuspended:
		return IngressReadinessSuspended
	case IngressLifecycleOpening:
		return IngressReadinessWarming
	default:
		if !v.DeliveredThrough.Present {
			return IngressReadinessWarming
		}
		if v.IsFresh() {
			return IngressReadinessReady
		}
		return IngressReadinessStale
	}
}

// Authority is the derived authority. A closed scope claims none.
func (v IngressScopeView) Authority() (IngressAuthority, bool) {
	if v.Lifecycle == IngressLifecycleClosed {
		return IngressAuthority{}, false
	}
	return IngressAuthority{
		Generation:       v.Generation,
		DeliveredThrough: v.DeliveredThrough,
		StampedAt:        v.StampedAt,
	}, true
}

// ResumeFrom is the first sequence not yet delivered in order.
func (v IngressScopeView) ResumeFrom() uint64 {
	if !v.DeliveredThrough.Present {
		return 0
	}
	return v.DeliveredThrough.Value + 1
}

// HasGap reports whether the scope is holding a gap open — an out-of-order
// buffer that a replay, not a retry, is the fix for.
func (v IngressScopeView) HasGap() bool { return v.Buffered > 0 }

// Retry is the derived retry decision. Absent while no error is outstanding — a
// healthy scope has no backoff, rather than a zero one.
func (v IngressScopeView) Retry() (IngressRetry, bool) {
	if v.ConsecutiveErrors == 0 {
		return IngressRetry{}, false
	}
	shift := uint(v.ConsecutiveErrors - 1)
	if shift > 31 {
		shift = 31
	}
	factor := uint64(1) << shift
	backoff := v.Policy.RetryBase
	if factor != 0 && backoff > math.MaxUint64/factor {
		backoff = math.MaxUint64
	} else {
		backoff *= factor
	}
	if backoff > v.Policy.RetryCeiling {
		backoff = v.Policy.RetryCeiling
	}
	return IngressRetry{
		Attempt:    v.ConsecutiveErrors,
		Backoff:    backoff,
		ResumeFrom: v.ResumeFrom(),
	}, true
}

// --- internal scope state ----------------------------------------------------

type ingressPending[T any] struct {
	payload   T
	stampedAt uint64
}

type ingressScope[T any] struct {
	lifecycle         IngressLifecycle
	generation        uint64
	deliveredThrough  Opt[uint64]
	stampedAt         uint64
	pending           map[uint64]ingressPending[T]
	window            T
	hasWindow         bool
	windowDepth       uint64
	consecutiveErrors uint32
}

func newIngressScope[T any](generation uint64) *ingressScope[T] {
	return &ingressScope[T]{
		lifecycle:  IngressLifecycleOpening,
		generation: generation,
		pending:    make(map[uint64]ingressPending[T]),
	}
}

func (s *ingressScope[T]) view(observedNow uint64, policy IngressPolicy) IngressScopeView {
	return IngressScopeView{
		Lifecycle:         s.lifecycle,
		Generation:        s.generation,
		DeliveredThrough:  s.deliveredThrough,
		StampedAt:         s.stampedAt,
		Buffered:          len(s.pending),
		WindowDepth:       s.windowDepth,
		ConsecutiveErrors: s.consecutiveErrors,
		ObservedNow:       observedNow,
		Policy:            policy,
	}
}

func (s *ingressScope[T]) nextExpected() uint64 {
	if !s.deliveredThrough.Present {
		return 0
	}
	return s.deliveredThrough.Value + 1
}

// ingressStamp is everything a reader can observe *about shape rather than
// payload*. The buffered path diffs these to derive its invalidation set, so "a
// buffered envelope invalidates nothing" is a computed fact rather than a claim —
// and the handoff-then-buffer case (which clears the window) cannot slip through.
type ingressStamp struct {
	lifecycle        IngressLifecycle
	generation       uint64
	deliveredThrough Opt[uint64]
	hasWindow        bool
}

func (s *ingressScope[T]) stamp() ingressStamp {
	return ingressStamp{
		lifecycle:        s.lifecycle,
		generation:       s.generation,
		deliveredThrough: s.deliveredThrough,
		hasWindow:        s.hasWindow,
	}
}

func (s *ingressScope[T]) liveOrOpening() IngressLifecycle {
	if s.deliveredThrough.Present {
		return IngressLifecycleLive
	}
	return IngressLifecycleOpening
}

func (s *ingressScope[T]) clearWindow() {
	var zero T
	s.window = zero
	s.hasWindow = false
	s.windowDepth = 0
}

// ingressDecision is what the admission algebra decided, before any receipt is
// minted. Splitting the decision from its bookkeeping is what keeps the scope
// mutation from overlapping the receipt log.
type ingressDecisionKind int

const (
	ingressDecisionRefuse ingressDecisionKind = iota
	ingressDecisionBlock
	ingressDecisionBuffered
	ingressDecisionDelivered
)

type ingressDecision struct {
	kind             ingressDecisionKind
	reason           IngressDropReason
	gapFrom          uint64
	deliveredThrough uint64
	conflated        bool
	handoff          bool
	handoffFrom      uint64
	handoffTo        uint64
}

// --- the core ----------------------------------------------------------------

// IngressCore holds keyed lifecycle scopes, the admission algebra, and a bounded
// receipt log. No reactive nodes, no context, no invalidation — each flavor wraps
// this in its own lock and owns its own graph.
type IngressCore[K comparable, T any] struct {
	policy            IngressPolicy
	merge             MergePolicy[T]
	scopes            map[K]*ingressScope[T]
	receipts          []IngressReceipt[K]
	nextReceiptOffset uint64
	observedNow       uint64
}

// NewIngressCore builds a core over policy, validating the overflow choice
// against the merge algebra the way NewRelayCell does: Conflate bounds nothing
// for a non-conflating ⊕.
func NewIngressCore[K comparable, T any](
	policy IngressPolicy,
	merge MergePolicy[T],
) (*IngressCore[K, T], error) {
	if policy.Overflow == OverflowConflate && !merge.Conflates {
		return nil, ErrIngressConflateNotBounding
	}
	if policy.ReceiptCapacity == 0 {
		return nil, ErrIngressZeroReceiptCapacity
	}
	return &IngressCore[K, T]{
		policy: policy,
		merge:  merge,
		scopes: make(map[K]*ingressScope[T]),
	}, nil
}

// Policy returns the bounds in force.
func (c *IngressCore[K, T]) Policy() IngressPolicy { return c.policy }

// Merge returns the merge algebra the hot window folds under.
func (c *IngressCore[K, T]) Merge() MergePolicy[T] { return c.merge }

// ScopeKeys returns every known scope key, for a shell rebuilding its reader
// table.
func (c *IngressCore[K, T]) ScopeKeys() []K {
	keys := make([]K, 0, len(c.scopes))
	for key := range c.scopes {
		keys = append(keys, key)
	}
	return keys
}

// View returns a read-only projection of one scope, or false when unknown.
func (c *IngressCore[K, T]) View(key K) (IngressScopeView, bool) {
	scope, ok := c.scopes[key]
	if !ok {
		return IngressScopeView{}, false
	}
	return scope.view(c.observedNow, c.policy), true
}

// Readiness reports the readiness of a scope. An unknown scope is Unknown rather
// than an error: a reader may legitimately observe a key before it opens.
func (c *IngressCore[K, T]) Readiness(key K) IngressReadiness {
	view, ok := c.View(key)
	if !ok {
		return IngressReadinessUnknown
	}
	return view.Readiness()
}

// Authority reports the authority claimed by a scope.
func (c *IngressCore[K, T]) Authority(key K) (IngressAuthority, bool) {
	view, ok := c.View(key)
	if !ok {
		return IngressAuthority{}, false
	}
	return view.Authority()
}

// Retry reports the retry decision for a scope.
func (c *IngressCore[K, T]) Retry(key K) (IngressRetry, bool) {
	view, ok := c.View(key)
	if !ok {
		return IngressRetry{}, false
	}
	return view.Retry()
}

// Peek returns the coalesced window awaiting drain.
func (c *IngressCore[K, T]) Peek(key K) (T, bool) {
	scope, ok := c.scopes[key]
	if !ok || !scope.hasWindow {
		var zero T
		return zero, false
	}
	return scope.window, true
}

// Receipts returns the receipts on one channel, oldest first.
func (c *IngressCore[K, T]) Receipts(channel IngressReceiptChannel) []IngressReceipt[K] {
	out := make([]IngressReceipt[K], 0, len(c.receipts))
	for _, receipt := range c.receipts {
		if receipt.Channel() == channel {
			out = append(out, receipt)
		}
	}
	return out
}

// Open opens (or reopens) a scope at generation.
//
// Reopening a suspended scope preserves its watermark so a replay can resume from
// the gap; reopening a *closed* scope resets it, because a closed scope's
// producer is gone and its sequence space is not resumable.
func (c *IngressCore[K, T]) Open(key K, generation uint64) IngressChange[K] {
	var change IngressChange[K]
	scope, exists := c.scopes[key]
	if !exists {
		c.scopes[key] = newIngressScope[T](generation)
		change.mark(key, ingressScopeChangeCreation())
		return change
	}
	before := scope.stamp()
	if scope.lifecycle == IngressLifecycleClosed {
		scope = newIngressScope[T](generation)
		c.scopes[key] = scope
	} else {
		scope.lifecycle = scope.liveOrOpening()
		if generation > scope.generation {
			scope.generation = generation
			scope.deliveredThrough = None[uint64]()
			clear(scope.pending)
		}
	}
	after := scope.stamp()
	if before.lifecycle != after.lifecycle ||
		before.generation != after.generation ||
		before.deliveredThrough != after.deliveredThrough {
		change.mark(key, IngressScopeChange{
			Readiness: before.lifecycle != after.lifecycle,
			Authority: true,
		})
	}
	return change
}

// Suspend suspends a scope: retain state and cursors, stop delivering. It returns
// the replay request a reconnect will need, or false when there was nothing to
// suspend.
func (c *IngressCore[K, T]) Suspend(key K) (IngressChange[K], ReplayRequest, bool) {
	var change IngressChange[K]
	scope, ok := c.scopes[key]
	if !ok {
		return change, ReplayRequest{}, false
	}
	if scope.lifecycle == IngressLifecycleSuspended ||
		scope.lifecycle == IngressLifecycleClosed {
		return change, ReplayRequest{}, false
	}
	scope.lifecycle = IngressLifecycleSuspended
	request := ReplayRequest{
		Generation:   scope.generation,
		FromSequence: scope.nextExpected(),
	}
	change.mark(key, ingressScopeChangeReadinessOnly())
	return change, request, true
}

// Reconnect reconnects a scope at generation, clearing the error streak.
//
// A higher generation is a producer handoff: the sequence space restarts, so the
// buffered reorder window and the coalesced value are discarded rather than
// replayed against a fence they no longer belong to.
func (c *IngressCore[K, T]) Reconnect(key K, generation uint64) (IngressChange[K], ReplayRequest) {
	var change IngressChange[K]
	scope, exists := c.scopes[key]
	created := !exists
	if !exists {
		scope = newIngressScope[T](generation)
		c.scopes[key] = scope
	}
	handoff := generation > scope.generation
	hadWindow := scope.hasWindow
	if handoff {
		scope.generation = generation
		scope.deliveredThrough = None[uint64]()
		clear(scope.pending)
		scope.clearWindow()
	}
	beforeLifecycle := scope.lifecycle
	scope.lifecycle = scope.liveOrOpening()
	hadErrors := scope.consecutiveErrors > 0
	scope.consecutiveErrors = 0
	request := ReplayRequest{
		Generation:   scope.generation,
		FromSequence: scope.nextExpected(),
	}
	base := IngressScopeChange{
		Value:     handoff && hadWindow,
		Readiness: beforeLifecycle != scope.lifecycle,
		Authority: handoff,
		Retry:     hadErrors,
	}
	if created {
		base = base.union(ingressScopeChangeCreation())
	}
	change.mark(key, base)
	return change, request
}

// Close closes a scope. It admits nothing and claims no authority until reopened.
func (c *IngressCore[K, T]) Close(key K) IngressChange[K] {
	var change IngressChange[K]
	scope, ok := c.scopes[key]
	if !ok || scope.lifecycle == IngressLifecycleClosed {
		return change
	}
	hadWindow := scope.hasWindow
	hadErrors := scope.consecutiveErrors > 0
	scope.lifecycle = IngressLifecycleClosed
	clear(scope.pending)
	scope.clearWindow()
	scope.consecutiveErrors = 0
	change.mark(key, IngressScopeChange{
		Value:     hadWindow,
		Readiness: true,
		Authority: true,
		Retry:     hadErrors,
	})
	return change
}

// Tick advances logical time. Only scopes that *crossed* the freshness horizon
// are dirtied — a tick inside the horizon invalidates nothing, which is what
// keeps a polling shell from re-rendering on every tick.
func (c *IngressCore[K, T]) Tick(now uint64) IngressChange[K] {
	var change IngressChange[K]
	if now == c.observedNow {
		return change
	}
	before := c.observedNow
	c.observedNow = now
	for key, scope := range c.scopes {
		if scope.view(before, c.policy).Readiness() !=
			scope.view(now, c.policy).Readiness() {
			change.mark(key, ingressScopeChangeReadinessOnly())
		}
	}
	return change
}

// Fail records a transport/decode failure against a scope, deepening its backoff.
func (c *IngressCore[K, T]) Fail(key K, err IngressError) IngressChange[K] {
	var change IngressChange[K]
	scope, exists := c.scopes[key]
	created := !exists
	if !exists {
		scope = newIngressScope[T](0)
		c.scopes[key] = scope
	}
	if scope.consecutiveErrors < math.MaxUint32 {
		scope.consecutiveErrors++
	}
	base := ingressScopeChangeRetryOnly()
	if created {
		base = base.union(ingressScopeChangeCreation())
	}
	change.mark(key, base)
	channel := c.pushReceipt(IngressReceipt[K]{
		Key:        key,
		Generation: scope.generation,
		Outcome: IngressReceiptOutcome{
			Channel: IngressReceiptError,
			Err:     err,
		},
	})
	change.markChannel(channel)
	return change
}

// Drain drains a scope's coalesced window, resetting its depth. It returns false
// for an empty window and dirties nothing.
//
// A drain is an *egress*, not an ack: it never moves the watermark, so a replay
// after a drain still resumes from the same sequence.
func (c *IngressCore[K, T]) Drain(key K) (IngressChange[K], T, bool) {
	var change IngressChange[K]
	var zero T
	scope, ok := c.scopes[key]
	if !ok || !scope.hasWindow {
		return change, zero, false
	}
	value := scope.window
	scope.clearWindow()
	change.mark(key, ingressScopeChangeValueOnly())
	return change, value, true
}

// Admit admits one envelope, applying — in this order — scope lifecycle, the
// generation fence, freshness, the generation handoff, dedupe, ordering,
// backpressure, and the merge.
//
// The order is the contract: a zombie generation is rejected before its stale
// sequence is consulted, and an expired envelope is rejected before it can occupy
// a reorder slot.
func (c *IngressCore[K, T]) Admit(
	envelope IngressEnvelope[K, T],
) (IngressChange[K], IngressAdmission) {
	key := envelope.Key
	scope, exists := c.scopes[key]
	created := !exists
	var before ingressStamp
	if exists {
		before = scope.stamp()
	} else {
		scope = newIngressScope[T](envelope.Generation)
		c.scopes[key] = scope
	}
	decision := c.decide(scope, envelope)

	// A refused envelope must not leave a scope behind: an expired or blocked
	// message for a key we do not track is not an admission plane, and
	// materializing one would report a readiness change that never happened.
	admitted := decision.kind == ingressDecisionBuffered ||
		decision.kind == ingressDecisionDelivered
	if created && !admitted {
		delete(c.scopes, key)
	}

	var change IngressChange[K]
	fence := envelope.Generation
	if survivor, ok := c.scopes[key]; ok {
		fence = survivor.generation
	}

	switch decision.kind {
	case ingressDecisionRefuse:
		channel := c.pushReceipt(IngressReceipt[K]{
			Key:        key,
			Generation: fence,
			Sequence:   Some(envelope.Sequence),
			Outcome: IngressReceiptOutcome{
				Channel: IngressReceiptDropped,
				Reason:  decision.reason,
			},
		})
		change.markChannel(channel)
		return change, IngressRefused(decision.reason)

	case ingressDecisionBlock:
		channel := c.pushReceipt(IngressReceipt[K]{
			Key:        key,
			Generation: fence,
			Sequence:   Some(envelope.Sequence),
			Outcome: IngressReceiptOutcome{
				Channel: IngressReceiptDropped,
				Reason:  IngressDropBackpressure,
			},
		})
		change.markChannel(channel)
		return change, IngressBackpressured()

	case ingressDecisionBuffered:
		// A buffered envelope mints no receipt, and for an already-current scope
		// it dirties no reader, because nothing a reader can observe moved. Two
		// cases are NOT invisible and are derived rather than assumed: the
		// scope's own first appearance (it moves off Unknown), and a generation
		// handoff that buffers — which resets the fence, the watermark, and the
		// window before parking the envelope.
		scopeChange := IngressScopeChange{}
		if created {
			scopeChange = ingressScopeChangeCreation()
		} else {
			after := scope.stamp()
			scopeChange = scopeChange.union(IngressScopeChange{
				Value: before.hasWindow != after.hasWindow,
				Readiness: before.lifecycle != after.lifecycle ||
					before.deliveredThrough.Present != after.deliveredThrough.Present,
				Authority: before.generation != after.generation ||
					before.deliveredThrough != after.deliveredThrough,
			})
		}
		change.mark(key, scopeChange)
		return change, IngressHeld(decision.gapFrom)

	default:
		change.mark(key, ingressScopeChangeAll())
		channel := c.pushReceipt(IngressReceipt[K]{
			Key:        key,
			Generation: fence,
			Sequence:   Some(envelope.Sequence),
			Outcome: IngressReceiptOutcome{
				Channel:          IngressReceiptAccepted,
				DeliveredThrough: decision.deliveredThrough,
				Conflated:        decision.conflated,
			},
		})
		change.markChannel(channel)
		switch {
		case decision.handoff:
			return change, IngressHandedOff(decision.handoffFrom, decision.handoffTo)
		case decision.conflated:
			return change, IngressCoalesced(decision.deliveredThrough)
		default:
			return change, IngressAdmitted(decision.deliveredThrough)
		}
	}
}

// decide is the admission algebra proper: pure over one scope, mutating only that
// scope, minting nothing.
func (c *IngressCore[K, T]) decide(
	scope *ingressScope[T],
	envelope IngressEnvelope[K, T],
) ingressDecision {
	// 1. lifecycle.
	if scope.lifecycle == IngressLifecycleClosed {
		return ingressDecision{kind: ingressDecisionRefuse, reason: IngressDropScopeClosed}
	}
	// 2. generation fence. It OUTRANKS dedupe on purpose: a zombie producer
	// replaying old sequences under an old generation must be distinguishable
	// from a legitimate retry, and testing the sequence first would report
	// DuplicateSequence and hide the zombie.
	if envelope.Generation < scope.generation {
		return ingressDecision{kind: ingressDecisionRefuse, reason: IngressDropStaleGeneration}
	}
	// 3. freshness. It OUTRANKS ordering on purpose: an expired envelope must
	// never occupy a reorder slot, or a slow zombie can exhaust the buffer and
	// starve live data.
	if ingressElapsed(c.observedNow, envelope.StampedAt) > c.policy.FreshnessHorizon {
		return ingressDecision{kind: ingressDecisionRefuse, reason: IngressDropExpired}
	}

	// 4. generation handoff — a baseline reset, not a continuation. The new
	// incarnation's first envelope is authoritative, so the old incarnation's
	// undrained window AND buffered successors are discarded rather than folded
	// into it. Merging a superseded delta into a fresh baseline is exactly the
	// build-skew corruption the generation fence exists to prevent, and it is the
	// same rule Reconnect at a higher generation applies.
	handoff := false
	handoffFrom, handoffTo := uint64(0), uint64(0)
	if envelope.Generation > scope.generation {
		handoff = true
		handoffFrom, handoffTo = scope.generation, envelope.Generation
		scope.generation = envelope.Generation
		scope.deliveredThrough = None[uint64]()
		clear(scope.pending)
		scope.clearWindow()
	}

	// 5. dedupe.
	expected := scope.nextExpected()
	if envelope.Sequence < expected {
		return ingressDecision{kind: ingressDecisionRefuse, reason: IngressDropDuplicateSequence}
	}
	// 6. ordering.
	if envelope.Sequence > expected {
		if _, buffered := scope.pending[envelope.Sequence]; buffered {
			return ingressDecision{
				kind: ingressDecisionRefuse, reason: IngressDropDuplicateBuffered,
			}
		}
		if len(scope.pending) >= c.policy.ReorderWindow {
			return ingressDecision{
				kind: ingressDecisionRefuse, reason: IngressDropReorderWindowOverflow,
			}
		}
		scope.pending[envelope.Sequence] = ingressPending[T]{
			payload: envelope.Payload, stampedAt: envelope.StampedAt,
		}
		return ingressDecision{kind: ingressDecisionBuffered, gapFrom: expected}
	}

	// 7. backpressure. Checked here and not earlier: refusing an in-order
	// envelope leaves a gap the reorder buffer cannot close, so Block must be
	// observable by the producer as its own outcome.
	if scope.windowDepth >= c.policy.HighWater {
		switch c.policy.Overflow {
		case OverflowBlock:
			// Refuses WITHOUT advancing the watermark, which is what makes the
			// producer's retry in-order rather than a duplicate.
			return ingressDecision{kind: ingressDecisionBlock}
		case OverflowDropNewest:
			return ingressDecision{
				kind: ingressDecisionRefuse, reason: IngressDropBackpressure,
			}
		case OverflowDropOldest:
			scope.clearWindow()
		default:
			// Conflate *is* the bound; Spill degrades to it until a durable tail
			// is wired, exactly as RelayCell does.
		}
	}

	// 8. merge.
	conflated := c.mergeInto(scope, envelope.Payload, envelope.StampedAt)
	scope.deliveredThrough = Some(envelope.Sequence)
	scope.lifecycle = IngressLifecycleLive
	scope.consecutiveErrors = 0
	deliveredThrough := envelope.Sequence

	// Flush every buffered successor this delivery unblocked. One invalidation
	// covers the whole flush: readers observe the coalesced window, never a
	// partial replay. The buffer replays in SEQUENCE order, which is why a merely
	// associative ⊕ converges to the in-order fold (reorder_needs_no_commutativity).
	for {
		next := scope.nextExpected()
		held, ok := scope.pending[next]
		if !ok {
			break
		}
		delete(scope.pending, next)
		if c.mergeInto(scope, held.payload, held.stampedAt) {
			conflated = true
		}
		scope.deliveredThrough = Some(next)
		deliveredThrough = next
	}

	return ingressDecision{
		kind:             ingressDecisionDelivered,
		deliveredThrough: deliveredThrough,
		conflated:        conflated,
		handoff:          handoff,
		handoffFrom:      handoffFrom,
		handoffTo:        handoffTo,
	}
}

// mergeInto folds one payload into a scope's hot head, reporting whether it
// coalesced with an existing window.
func (c *IngressCore[K, T]) mergeInto(
	scope *ingressScope[T],
	payload T,
	stampedAt uint64,
) bool {
	conflated := false
	if !scope.hasWindow {
		scope.window = payload
		scope.hasWindow = true
	} else {
		scope.window = c.merge.Merge(scope.window, payload)
		conflated = true
	}
	scope.windowDepth++
	if stampedAt > scope.stampedAt {
		scope.stampedAt = stampedAt
	}
	return conflated
}

func (c *IngressCore[K, T]) pushReceipt(receipt IngressReceipt[K]) IngressReceiptChannel {
	receipt.Offset = c.nextReceiptOffset
	c.nextReceiptOffset++
	c.receipts = append(c.receipts, receipt)
	if len(c.receipts) > c.policy.ReceiptCapacity {
		drop := len(c.receipts) - c.policy.ReceiptCapacity
		c.receipts = append(c.receipts[:0], c.receipts[drop:]...)
	}
	return receipt.Channel()
}
