package lazily

// Unit tests for the transport-agnostic reactive ingress family
// (#designimplementtransport). Each test names one invariant of
// lazily-spec/docs/transport-ingress.md; the cross-language corpus replay and the
// enforced flavor ledger live in ingress_family_conformance_test.go.

import (
	"reflect"
	"testing"
)

func testIngressCore(t *testing.T, policy IngressPolicy) *IngressCore[string, uint64] {
	t.Helper()
	core, err := NewIngressCore[string, uint64](policy, Sum[uint64]())
	if err != nil {
		t.Fatalf("build core: %v", err)
	}
	return core
}

func ingressEnv(
	key string,
	generation, sequence, stampedAt, payload uint64,
) IngressEnvelope[string, uint64] {
	return NewIngressEnvelope(key, generation, sequence, stampedAt, payload)
}

// --- construction ------------------------------------------------------------

func TestIngressConflateIsRejectedForANonConflatingAlgebra(t *testing.T) {
	policy := DefaultIngressPolicy()
	policy.Overflow = OverflowConflate
	// RawFifo carries order and multiplicity as meaning, so coalescence bounds
	// nothing — the same validation NewRelayCell performs.
	if _, err := NewIngressCore[string, []uint64](policy, RawFifo[uint64]()); err !=
		ErrIngressConflateNotBounding {
		t.Fatalf("err=%v, want ErrIngressConflateNotBounding", err)
	}
}

func TestIngressZeroReceiptCapacityIsRejected(t *testing.T) {
	policy := DefaultIngressPolicy()
	policy.ReceiptCapacity = 0
	if _, err := NewIngressCore[string, uint64](policy, Sum[uint64]()); err !=
		ErrIngressZeroReceiptCapacity {
		t.Fatalf("err=%v, want ErrIngressZeroReceiptCapacity", err)
	}
}

// --- admission ---------------------------------------------------------------

func TestIngressInOrderDeliveryConflatesAndReceipts(t *testing.T) {
	core := testIngressCore(t, DefaultIngressPolicy())
	change, admission := core.Admit(ingressEnv("a", 1, 0, 0, 5))
	if admission != IngressAdmitted(0) {
		t.Fatalf("admission=%+v, want accepted@0", admission)
	}
	if !change.AcceptedReceipts {
		t.Fatal("a delivery must dirty the accepted-receipt reader")
	}
	if want := []IngressScopeDelta[string]{{"a", ingressScopeChangeAll()}}; !reflect.DeepEqual(
		change.Scopes, want) {
		t.Fatalf("scopes=%+v, want %+v", change.Scopes, want)
	}

	_, admission = core.Admit(ingressEnv("a", 1, 1, 0, 7))
	if admission != IngressCoalesced(1) {
		t.Fatalf("admission=%+v, want conflated@1", admission)
	}
	if window, ok := core.Peek("a"); !ok || window != 12 {
		t.Fatalf("window=(%d,%v), want 12", window, ok)
	}
	if got := len(core.Receipts(IngressReceiptAccepted)); got != 2 {
		t.Fatalf("accepted receipts=%d, want 2", got)
	}
	if got := len(core.Receipts(IngressReceiptDropped)); got != 0 {
		t.Fatalf("dropped receipts=%d, want 0", got)
	}
}

func TestIngressReorderBuffersThenFlushesInOneInvalidation(t *testing.T) {
	core := testIngressCore(t, DefaultIngressPolicy())
	change, admission := core.Admit(ingressEnv("a", 1, 2, 0, 4))
	if admission != IngressHeld(0) {
		t.Fatalf("admission=%+v, want buffered@0", admission)
	}
	// A buffered envelope mints no receipt and moves no value. The scope's FIRST
	// appearance does move it off Unknown, and saying so is the difference between
	// a sound invalidation set and a reader stuck on Unknown forever.
	if change.AcceptedReceipts || change.DroppedReceipts || change.ErrorReceipts {
		t.Fatal("a buffered envelope must mint no receipt")
	}
	if want := []IngressScopeDelta[string]{
		{"a", ingressScopeChangeCreation()},
	}; !reflect.DeepEqual(change.Scopes, want) {
		t.Fatalf("scopes=%+v, want %+v", change.Scopes, want)
	}
	if _, ok := core.Peek("a"); ok {
		t.Fatal("a buffered envelope must not be visible")
	}

	change, admission = core.Admit(ingressEnv("a", 1, 1, 0, 2))
	if admission != IngressHeld(0) {
		t.Fatalf("admission=%+v, want buffered@0", admission)
	}
	// Now the scope exists, so a second buffered envelope really is invisible.
	if !change.IsEmpty() {
		t.Fatalf("change=%+v, want empty", change)
	}

	_, admission = core.Admit(ingressEnv("a", 1, 0, 0, 1))
	// Three ops coalesced, so the delivery reports Conflated even though the
	// window it started from was empty.
	if admission != IngressCoalesced(2) {
		t.Fatalf("admission=%+v, want conflated@2", admission)
	}
	if window, _ := core.Peek("a"); window != 7 {
		t.Fatalf("window=%d, want 1+2+4", window)
	}
	view, _ := core.View("a")
	if view.Buffered != 0 {
		t.Fatalf("buffered=%d, want 0", view.Buffered)
	}
	// Exactly one accepted receipt for the delivery that unblocked the flush.
	if got := len(core.Receipts(IngressReceiptAccepted)); got != 1 {
		t.Fatalf("accepted receipts=%d, want 1", got)
	}
}

func TestIngressDuplicatesAreDroppedAfterDeliveryAndWhileBuffered(t *testing.T) {
	core := testIngressCore(t, DefaultIngressPolicy())
	core.Admit(ingressEnv("a", 1, 0, 0, 1))
	if _, admission := core.Admit(ingressEnv("a", 1, 0, 0, 1)); admission !=
		IngressRefused(IngressDropDuplicateSequence) {
		t.Fatalf("admission=%+v, want duplicate_sequence", admission)
	}
	core.Admit(ingressEnv("a", 1, 5, 0, 1))
	if _, admission := core.Admit(ingressEnv("a", 1, 5, 0, 1)); admission !=
		IngressRefused(IngressDropDuplicateBuffered) {
		t.Fatalf("admission=%+v, want duplicate_buffered", admission)
	}
	if window, _ := core.Peek("a"); window != 1 {
		t.Fatalf("window=%d, want 1", window)
	}
}

func TestIngressReorderWindowOverflowDropsRatherThanGrowing(t *testing.T) {
	policy := DefaultIngressPolicy()
	policy.ReorderWindow = 2
	core := testIngressCore(t, policy)
	core.Admit(ingressEnv("a", 1, 1, 0, 1))
	core.Admit(ingressEnv("a", 1, 2, 0, 1))
	if _, admission := core.Admit(ingressEnv("a", 1, 3, 0, 1)); admission !=
		IngressRefused(IngressDropReorderWindowOverflow) {
		t.Fatalf("admission=%+v, want reorder_window_overflow", admission)
	}
	if view, _ := core.View("a"); view.Buffered != 2 {
		t.Fatalf("buffered=%d, want 2", view.Buffered)
	}
}

func TestIngressAZeroReorderWindowDropsEveryGapImmediately(t *testing.T) {
	policy := DefaultIngressPolicy()
	policy.ReorderWindow = 0
	core := testIngressCore(t, policy)
	if _, admission := core.Admit(ingressEnv("a", 1, 1, 0, 1)); admission !=
		IngressRefused(IngressDropReorderWindowOverflow) {
		t.Fatalf("admission=%+v, want reorder_window_overflow", admission)
	}
}

func TestIngressAStaleGenerationIsFencedBeforeItsSequenceIsConsulted(t *testing.T) {
	core := testIngressCore(t, DefaultIngressPolicy())
	core.Admit(ingressEnv("a", 2, 0, 0, 1))
	// Sequence 0 would be a duplicate; generation 1 is stale. The fence wins,
	// which is what makes a zombie producer distinguishable from a retry.
	if _, admission := core.Admit(ingressEnv("a", 1, 0, 0, 9)); admission !=
		IngressRefused(IngressDropStaleGeneration) {
		t.Fatalf("admission=%+v, want stale_generation", admission)
	}
	if window, _ := core.Peek("a"); window != 1 {
		t.Fatalf("window=%d, want 1", window)
	}
}

func TestIngressANewerGenerationHandsOffAndResetsTheSequenceSpace(t *testing.T) {
	core := testIngressCore(t, DefaultIngressPolicy())
	core.Admit(ingressEnv("a", 1, 0, 0, 1))
	core.Admit(ingressEnv("a", 1, 7, 0, 1))
	if _, admission := core.Admit(ingressEnv("a", 2, 0, 0, 4)); admission !=
		IngressHandedOff(1, 2) {
		t.Fatalf("admission=%+v, want handoff 1->2", admission)
	}
	view, _ := core.View("a")
	if view.Generation != 2 {
		t.Fatalf("generation=%d, want 2", view.Generation)
	}
	if view.DeliveredThrough != Some[uint64](0) {
		t.Fatalf("watermark=%+v, want 0", view.DeliveredThrough)
	}
	// The old generation's buffered successor is not replayed under the new
	// fence — its sequence numbers mean something else now.
	if view.Buffered != 0 {
		t.Fatalf("buffered=%d, want 0", view.Buffered)
	}
	// Nor is its undrained window folded into the new baseline.
	if window, _ := core.Peek("a"); window != 4 {
		t.Fatalf("window=%d, want 4 (the new baseline alone)", window)
	}
}

func TestIngressAHandoffThatBuffersStillReportsTheBaselineReset(t *testing.T) {
	// A NEWER generation arriving out of order resets the fence, the watermark,
	// AND the window before parking the envelope. Reporting that as "buffered,
	// nothing changed" would leave every reader showing the superseded
	// generation's value forever.
	core := testIngressCore(t, DefaultIngressPolicy())
	core.Admit(ingressEnv("a", 1, 0, 0, 5))
	change, admission := core.Admit(ingressEnv("a", 2, 3, 0, 9))
	if admission != IngressHeld(0) {
		t.Fatalf("admission=%+v, want buffered@0", admission)
	}
	want := []IngressScopeDelta[string]{{
		"a", IngressScopeChange{Value: true, Readiness: true, Authority: true},
	}}
	if !reflect.DeepEqual(change.Scopes, want) {
		t.Fatalf("scopes=%+v, want %+v", change.Scopes, want)
	}
	if _, ok := core.Peek("a"); ok {
		t.Fatal("the superseded window must be discarded")
	}
	view, _ := core.View("a")
	if view.Generation != 2 || view.DeliveredThrough.Present || view.Buffered != 1 {
		t.Fatalf("view=%+v, want generation 2 / no watermark / 1 buffered", view)
	}
	// A buffered envelope under the SAME generation is still invisible.
	change, _ = core.Admit(ingressEnv("a", 2, 4, 0, 1))
	if !change.IsEmpty() {
		t.Fatalf("change=%+v, want empty", change)
	}
}

func TestIngressAnExpiredEnvelopeNeverOccupiesAReorderSlot(t *testing.T) {
	policy := DefaultIngressPolicy()
	policy.FreshnessHorizon = 10
	policy.ReorderWindow = 1
	core := testIngressCore(t, policy)
	core.Tick(100)
	if _, admission := core.Admit(ingressEnv("a", 1, 3, 50, 1)); admission !=
		IngressRefused(IngressDropExpired) {
		t.Fatalf("admission=%+v, want expired", admission)
	}
	// A refused envelope leaves no scope behind: an expired message for an
	// untracked key is not an admission plane.
	if _, ok := core.View("a"); ok {
		t.Fatal("a refused envelope must not materialize a scope")
	}
	// The slot is still free for a fresh out-of-order envelope.
	if _, admission := core.Admit(ingressEnv("a", 1, 3, 95, 1)); admission !=
		IngressHeld(0) {
		t.Fatalf("admission=%+v, want buffered@0", admission)
	}
}

// --- backpressure ------------------------------------------------------------

func TestIngressBlockOverflowRefusesWithoutLosingTheWindow(t *testing.T) {
	policy := DefaultIngressPolicy()
	policy.HighWater = 1
	policy.Overflow = OverflowBlock
	core, err := NewIngressCore[string, uint64](policy, KeepLatest[uint64]())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	core.Admit(ingressEnv("a", 1, 0, 0, 5))
	change, admission := core.Admit(ingressEnv("a", 1, 1, 0, 9))
	if admission != IngressBackpressured() {
		t.Fatalf("admission=%+v, want blocked", admission)
	}
	if !change.DroppedReceipts {
		t.Fatal("a block must be receipted on the dropped channel")
	}
	if window, _ := core.Peek("a"); window != 5 {
		t.Fatalf("window=%d, want 5 (intact)", window)
	}
	// The blocked envelope did not advance the watermark, so a producer retry
	// after a drain is still in order rather than a duplicate.
	if view, _ := core.View("a"); view.DeliveredThrough != Some[uint64](0) {
		t.Fatalf("watermark=%+v, want 0", view.DeliveredThrough)
	}
	core.Drain("a")
	if _, admission := core.Admit(ingressEnv("a", 1, 1, 0, 9)); admission !=
		IngressAdmitted(1) {
		t.Fatalf("admission=%+v, want accepted@1", admission)
	}
}

func TestIngressDropOldestRestartsTheWindowAtTheIncomingOp(t *testing.T) {
	policy := DefaultIngressPolicy()
	policy.HighWater = 2
	policy.Overflow = OverflowDropOldest
	core := testIngressCore(t, policy)
	core.Admit(ingressEnv("a", 1, 0, 0, 1))
	core.Admit(ingressEnv("a", 1, 1, 0, 2))
	if _, admission := core.Admit(ingressEnv("a", 1, 2, 0, 30)); admission !=
		IngressAdmitted(2) {
		t.Fatalf("admission=%+v, want accepted@2", admission)
	}
	if window, _ := core.Peek("a"); window != 30 {
		t.Fatalf("window=%d, want 30", window)
	}
}

func TestIngressDropNewestKeepsTheWindowAndReceiptsTheDrop(t *testing.T) {
	policy := DefaultIngressPolicy()
	policy.HighWater = 1
	policy.Overflow = OverflowDropNewest
	core := testIngressCore(t, policy)
	core.Admit(ingressEnv("a", 1, 0, 0, 5))
	change, admission := core.Admit(ingressEnv("a", 1, 1, 0, 9))
	if admission != IngressRefused(IngressDropBackpressure) {
		t.Fatalf("admission=%+v, want backpressure", admission)
	}
	if !change.DroppedReceipts {
		t.Fatal("a backpressure drop must be receipted")
	}
	if window, _ := core.Peek("a"); window != 5 {
		t.Fatalf("window=%d, want 5", window)
	}
}

// --- derives -----------------------------------------------------------------

func TestIngressReadinessDerivesFromLifecycleAndFreshness(t *testing.T) {
	policy := DefaultIngressPolicy()
	policy.FreshnessHorizon = 10
	core := testIngressCore(t, policy)
	if got := core.Readiness("a"); got != IngressReadinessUnknown {
		t.Fatalf("readiness=%s, want unknown", got)
	}
	core.Open("a", 1)
	if got := core.Readiness("a"); got != IngressReadinessWarming {
		t.Fatalf("readiness=%s, want warming", got)
	}
	core.Admit(ingressEnv("a", 1, 0, 0, 1))
	if got := core.Readiness("a"); got != IngressReadinessReady {
		t.Fatalf("readiness=%s, want ready", got)
	}
	// Crossing the horizon is a readiness-only transition.
	change := core.Tick(50)
	want := []IngressScopeDelta[string]{{"a", ingressScopeChangeReadinessOnly()}}
	if !reflect.DeepEqual(change.Scopes, want) {
		t.Fatalf("scopes=%+v, want %+v", change.Scopes, want)
	}
	if got := core.Readiness("a"); got != IngressReadinessStale {
		t.Fatalf("readiness=%s, want stale", got)
	}
	// A further tick inside the same readiness dirties nothing.
	if change := core.Tick(60); !change.IsEmpty() {
		t.Fatalf("change=%+v, want empty", change)
	}
}

func TestIngressSuspendRetainsTheWatermarkAndReconnectReplaysTheGap(t *testing.T) {
	core := testIngressCore(t, DefaultIngressPolicy())
	core.Admit(ingressEnv("a", 1, 0, 0, 1))
	core.Admit(ingressEnv("a", 1, 1, 0, 1))
	_, request, ok := core.Suspend("a")
	if !ok || request != (ReplayRequest{Generation: 1, FromSequence: 2}) {
		t.Fatalf("replay=(%+v,%v), want generation 1 from 2", request, ok)
	}
	if got := core.Readiness("a"); got != IngressReadinessSuspended {
		t.Fatalf("readiness=%s, want suspended", got)
	}
	// The coalesced window survives a disconnect; only readiness changed.
	if window, _ := core.Peek("a"); window != 2 {
		t.Fatalf("window=%d, want 2", window)
	}
	// Suspending twice is idempotent and dirties nothing.
	change, _, ok := core.Suspend("a")
	if !change.IsEmpty() || ok {
		t.Fatalf("second suspend: change=%+v ok=%v, want empty/false", change, ok)
	}
	_, request = core.Reconnect("a", 1)
	if request != (ReplayRequest{Generation: 1, FromSequence: 2}) {
		t.Fatalf("replay=%+v, want generation 1 from 2", request)
	}
	if got := core.Readiness("a"); got != IngressReadinessReady {
		t.Fatalf("readiness=%s, want ready", got)
	}
}

func TestIngressReconnectAtAHigherGenerationDiscardsTheStaleWindow(t *testing.T) {
	core := testIngressCore(t, DefaultIngressPolicy())
	core.Admit(ingressEnv("a", 1, 0, 0, 5))
	core.Suspend("a")
	change, request := core.Reconnect("a", 3)
	if request != (ReplayRequest{Generation: 3, FromSequence: 0}) {
		t.Fatalf("replay=%+v, want generation 3 from 0", request)
	}
	found := false
	for _, delta := range change.Scopes {
		if delta.Change.Value && delta.Change.Authority {
			found = true
		}
	}
	if !found {
		t.Fatalf("scopes=%+v, want a value+authority change", change.Scopes)
	}
	if _, ok := core.Peek("a"); ok {
		t.Fatal("a higher-generation reconnect must discard the stale window")
	}
}

func TestIngressErrorsDeepenBackoffAndADeliveryClearsIt(t *testing.T) {
	policy := DefaultIngressPolicy()
	policy.RetryBase = 10
	policy.RetryCeiling = 25
	core := testIngressCore(t, policy)
	core.Open("a", 1)
	if _, ok := core.Retry("a"); ok {
		t.Fatal("a healthy scope has no backoff, not a zero one")
	}

	core.Fail("a", IngressErrorTransportClosed)
	retry, ok := core.Retry("a")
	if !ok || retry != (IngressRetry{Attempt: 1, Backoff: 10, ResumeFrom: 0}) {
		t.Fatalf("retry=(%+v,%v), want attempt 1 backoff 10", retry, ok)
	}
	core.Fail("a", IngressErrorTransportClosed)
	if retry, _ := core.Retry("a"); retry.Backoff != 20 {
		t.Fatalf("backoff=%d, want 20", retry.Backoff)
	}
	// Clamped, not doubled past the ceiling.
	core.Fail("a", IngressErrorTransportClosed)
	if retry, _ := core.Retry("a"); retry.Backoff != 25 {
		t.Fatalf("backoff=%d, want the 25 ceiling", retry.Backoff)
	}
	if got := len(core.Receipts(IngressReceiptError)); got != 3 {
		t.Fatalf("error receipts=%d, want 3", got)
	}

	core.Admit(ingressEnv("a", 1, 0, 0, 1))
	if _, ok := core.Retry("a"); ok {
		t.Fatal("a delivery clears the error streak")
	}
}

func TestIngressAReconnectClearsTheErrorStreakWithoutADelivery(t *testing.T) {
	core := testIngressCore(t, DefaultIngressPolicy())
	core.Open("a", 1)
	core.Fail("a", IngressErrorAuthorityLost)
	change, _ := core.Reconnect("a", 1)
	found := false
	for _, delta := range change.Scopes {
		if delta.Change.Retry {
			found = true
		}
	}
	if !found {
		t.Fatalf("scopes=%+v, want a retry change", change.Scopes)
	}
	if _, ok := core.Retry("a"); ok {
		t.Fatal("a reconnect clears the streak")
	}
}

func TestIngressClosedScopesAdmitNothingAndClaimNoAuthority(t *testing.T) {
	core := testIngressCore(t, DefaultIngressPolicy())
	core.Admit(ingressEnv("a", 1, 0, 0, 1))
	core.Close("a")
	if _, ok := core.Authority("a"); ok {
		t.Fatal("a closed scope claims no authority")
	}
	if _, admission := core.Admit(ingressEnv("a", 1, 1, 0, 1)); admission !=
		IngressRefused(IngressDropScopeClosed) {
		t.Fatalf("admission=%+v, want scope_closed", admission)
	}
	// Reopening a closed scope restarts its sequence space.
	core.Open("a", 1)
	if _, admission := core.Admit(ingressEnv("a", 1, 0, 0, 4)); admission !=
		IngressAdmitted(0) {
		t.Fatalf("admission=%+v, want accepted@0", admission)
	}
}

func TestIngressScopesAreIndependent(t *testing.T) {
	core := testIngressCore(t, DefaultIngressPolicy())
	core.Admit(ingressEnv("a", 1, 0, 0, 1))
	change, _ := core.Admit(ingressEnv("b", 1, 0, 0, 2))
	if len(change.Scopes) != 1 || change.Scopes[0].Key != "b" {
		t.Fatalf("scopes=%+v, want only b", change.Scopes)
	}
	core.Close("b")
	if got := core.Readiness("a"); got != IngressReadinessReady {
		t.Fatalf("readiness(a)=%s, want ready", got)
	}
	if window, _ := core.Peek("a"); window != 1 {
		t.Fatalf("window(a)=%d, want 1", window)
	}
}

func TestIngressReceiptsAreBoundedAndOffsetsStayMonotone(t *testing.T) {
	policy := DefaultIngressPolicy()
	policy.ReceiptCapacity = 2
	core := testIngressCore(t, policy)
	for seq := uint64(0); seq < 4; seq++ {
		core.Admit(ingressEnv("a", 1, seq, 0, 1))
	}
	accepted := core.Receipts(IngressReceiptAccepted)
	if len(accepted) != 2 {
		t.Fatalf("accepted receipts=%d, want the 2-receipt bound", len(accepted))
	}
	// The offsets survive eviction, so a consumer can tell "I have seen
	// everything" from "the log wrapped".
	if accepted[0].Offset != 2 || accepted[1].Offset != 3 {
		t.Fatalf("offsets=%d,%d, want 2,3", accepted[0].Offset, accepted[1].Offset)
	}
}

func TestIngressAScheduleOffersAPollIntervalOnlyWithoutEventDelivery(t *testing.T) {
	if got := IngressScheduleFor(IngressTransportEventChannel, 50).PollInterval; got.Present {
		t.Fatalf("poll interval=%+v, want absent for an event channel", got)
	}
	if got := IngressScheduleFor(IngressTransportRpcTriggered, 50).PollInterval; got.Present {
		t.Fatalf("poll interval=%+v, want absent for an RPC trigger", got)
	}
	if got := IngressScheduleFor(IngressTransportBoundedPolling, 50).PollInterval; got !=
		Some[uint64](50) {
		t.Fatalf("poll interval=%+v, want 50", got)
	}
	// A zero interval would be an unbounded refresh loop.
	if got := IngressScheduleFor(IngressTransportBoundedPolling, 0).PollInterval; got !=
		Some[uint64](1) {
		t.Fatalf("poll interval=%+v, want the 1 floor", got)
	}
}

func TestIngressDrainIsAValueOnlyTransitionAndEmptyDrainsDirtyNothing(t *testing.T) {
	core := testIngressCore(t, DefaultIngressPolicy())
	core.Admit(ingressEnv("a", 1, 0, 0, 3))
	change, value, ok := core.Drain("a")
	if !ok || value != 3 {
		t.Fatalf("drained=(%d,%v), want 3", value, ok)
	}
	want := []IngressScopeDelta[string]{{"a", ingressScopeChangeValueOnly()}}
	if !reflect.DeepEqual(change.Scopes, want) {
		t.Fatalf("scopes=%+v, want %+v", change.Scopes, want)
	}
	change, _, ok = core.Drain("a")
	if ok || !change.IsEmpty() {
		t.Fatalf("empty drain: ok=%v change=%+v, want false/empty", ok, change)
	}
	// Draining does not move the watermark: a drain is an egress, not an ack.
	if view, _ := core.View("a"); view.DeliveredThrough != Some[uint64](0) {
		t.Fatalf("watermark=%+v, want 0", view.DeliveredThrough)
	}
}

func TestIngressOutOfOrderArrivalConvergesToTheInOrderFold(t *testing.T) {
	// The reordering tax is paid by the buffer, not by the algebra: for any
	// arrival permutation of a contiguous run, the drained window equals the
	// in-order fold. This is the operational reading of the formal model's
	// reorder_needs_no_commutativity.
	permutations := [][]uint64{
		{0, 1, 2, 3},
		{3, 2, 1, 0},
		{1, 0, 3, 2},
		{2, 0, 1, 3},
		{0, 3, 1, 2},
	}
	for _, order := range permutations {
		core := testIngressCore(t, DefaultIngressPolicy())
		for _, seq := range order {
			core.Admit(ingressEnv("a", 1, seq, 0, uint64(1)<<seq))
		}
		if window, _ := core.Peek("a"); window != 1+2+4+8 {
			t.Fatalf("order %v: window=%d, want 15", order, window)
		}
		if view, _ := core.View("a"); view.DeliveredThrough != Some[uint64](3) {
			t.Fatalf("order %v: watermark=%+v, want 3", order, view.DeliveredThrough)
		}
	}
}

// --- the single-threaded shell ----------------------------------------------

func testIngressCell(t *testing.T, ctx *Context, policy IngressPolicy) *IngressCell[string, uint64] {
	t.Helper()
	cell, err := NewIngressCell[string, uint64](
		ctx, policy, Sum[uint64](), IngressTransportEventChannel, 25)
	if err != nil {
		t.Fatalf("build cell: %v", err)
	}
	return cell
}

func TestIngressDeliveryIsVisibleThroughTheValueReader(t *testing.T) {
	ctx := NewContext()
	cell := testIngressCell(t, ctx, DefaultIngressPolicy())
	if _, ok := cell.Value(ctx, "a"); ok {
		t.Fatal("an unknown scope has no window")
	}
	cell.Admit(ingressEnv("a", 1, 0, 0, 5))
	if window, ok := cell.Value(ctx, "a"); !ok || window != 5 {
		t.Fatalf("window=(%d,%v), want 5", window, ok)
	}
	cell.Admit(ingressEnv("a", 1, 1, 0, 7))
	if window, _ := cell.Value(ctx, "a"); window != 12 {
		t.Fatalf("window=%d, want 12", window)
	}
	if window, ok := cell.Drain("a"); !ok || window != 12 {
		t.Fatalf("drained=(%d,%v), want 12", window, ok)
	}
	if _, ok := cell.Value(ctx, "a"); ok {
		t.Fatal("a drained window is empty")
	}
}

func TestIngressABufferedEnvelopeRerunsNoEffect(t *testing.T) {
	ctx := NewContext()
	cell := testIngressCell(t, ctx, DefaultIngressPolicy())
	cell.Open("a", 1)

	value := cell.ScopeReaders("a").Value
	runs := 0
	var observed []IngressWindow[uint64]
	effect := NewEffect(ctx, func(c *Compute) func() {
		runs++
		observed = append(observed, Get[IngressWindow[uint64]](c, value))
		return nil
	})
	defer effect.Dispose()
	if runs != 1 {
		t.Fatalf("runs=%d, want 1", runs)
	}

	// Out of order: nothing observable moved, so the value effect must not run.
	cell.Admit(ingressEnv("a", 1, 2, 0, 4))
	cell.Admit(ingressEnv("a", 1, 1, 0, 2))
	if runs != 1 {
		t.Fatalf("runs=%d after two buffered envelopes, want 1", runs)
	}

	// The delivery that closes the gap flushes all three as ONE value change.
	cell.Admit(ingressEnv("a", 1, 0, 0, 1))
	if runs != 2 {
		t.Fatalf("runs=%d after the unblocking delivery, want 2", runs)
	}
	want := []IngressWindow[uint64]{{}, {Present: true, Value: 7}}
	if !reflect.DeepEqual(observed, want) {
		t.Fatalf("observed=%+v, want %+v", observed, want)
	}
}

func TestIngressATickInsideTheHorizonRerunsNoReadinessEffect(t *testing.T) {
	ctx := NewContext()
	policy := DefaultIngressPolicy()
	policy.FreshnessHorizon = 100
	cell := testIngressCell(t, ctx, policy)
	cell.Admit(ingressEnv("a", 1, 0, 0, 1))

	readiness := cell.ScopeReaders("a").Readiness
	runs := 0
	effect := NewEffect(ctx, func(c *Compute) func() {
		runs++
		_ = Get[IngressReadiness](c, readiness)
		return nil
	})
	defer effect.Dispose()
	if runs != 1 {
		t.Fatalf("runs=%d, want 1", runs)
	}
	cell.Tick(50)
	if runs != 1 {
		t.Fatalf("runs=%d, want 1: a tick inside the horizon is not a change", runs)
	}
	cell.Tick(500)
	if runs != 2 {
		t.Fatalf("runs=%d, want 2: crossing the horizon is a change", runs)
	}
}

func TestIngressAnErrorMovesRetryWithoutTouchingTheValue(t *testing.T) {
	ctx := NewContext()
	cell := testIngressCell(t, ctx, DefaultIngressPolicy())
	cell.Admit(ingressEnv("a", 1, 0, 0, 9))

	value := cell.ScopeReaders("a").Value
	runs := 0
	effect := NewEffect(ctx, func(c *Compute) func() {
		runs++
		_ = Get[IngressWindow[uint64]](c, value)
		return nil
	})
	defer effect.Dispose()
	cell.Fail("a", IngressErrorTransportClosed)
	if runs != 1 {
		t.Fatalf("runs=%d, want 1: an error must not re-render the value", runs)
	}
	if retry, ok := cell.Retry(ctx, "a"); !ok || retry.Attempt != 1 {
		t.Fatalf("retry=(%+v,%v), want attempt 1", retry, ok)
	}
	if window, _ := cell.Value(ctx, "a"); window != 9 {
		t.Fatalf("window=%d, want 9", window)
	}
}

func TestIngressReceiptChannelsAreIndependentReaders(t *testing.T) {
	ctx := NewContext()
	cell := testIngressCell(t, ctx, DefaultIngressPolicy())
	cell.Admit(ingressEnv("a", 2, 0, 0, 1))
	if got := len(cell.Accepted(ctx)); got != 1 {
		t.Fatalf("accepted=%d, want 1", got)
	}
	if got := len(cell.Dropped(ctx)); got != 0 {
		t.Fatalf("dropped=%d, want 0", got)
	}
	if got := len(cell.Errors(ctx)); got != 0 {
		t.Fatalf("errors=%d, want 0", got)
	}

	// A fenced zombie shows up only on the dropped channel.
	cell.Admit(ingressEnv("a", 1, 0, 0, 1))
	if got := len(cell.Accepted(ctx)); got != 1 {
		t.Fatalf("accepted=%d, want 1: a drop must not touch the accept channel", got)
	}
	dropped := cell.Dropped(ctx)
	if len(dropped) != 1 || dropped[0].Outcome.Reason != IngressDropStaleGeneration {
		t.Fatalf("dropped=%+v, want one stale_generation", dropped)
	}

	cell.Fail("a", IngressErrorDecodeFailed)
	if got := len(cell.Errors(ctx)); got != 1 {
		t.Fatalf("errors=%d, want 1", got)
	}
	if got := len(cell.Dropped(ctx)); got != 1 {
		t.Fatalf("dropped=%d, want 1: an error must not touch the drop channel", got)
	}
}

func TestIngressTheScheduleDerivesFromTheTransportAndRetunesLive(t *testing.T) {
	ctx := NewContext()
	cell := testIngressCell(t, ctx, DefaultIngressPolicy())
	if got := cell.Schedule(ctx).PollInterval; got.Present {
		t.Fatalf("poll interval=%+v, want absent", got)
	}
	cell.SetTransport(IngressTransportBoundedPolling)
	if got := cell.Schedule(ctx).PollInterval; got != Some[uint64](25) {
		t.Fatalf("poll interval=%+v, want the retained 25", got)
	}
	cell.SetPollInterval(200)
	if got := cell.Schedule(ctx).PollInterval; got != Some[uint64](200) {
		t.Fatalf("poll interval=%+v, want 200", got)
	}
	cell.SetTransport(IngressTransportRpcTriggered)
	if got := cell.Schedule(ctx).PollInterval; got.Present {
		t.Fatalf("poll interval=%+v, want absent", got)
	}
}

func TestIngressPumpAdmitsABatchAndRequestsReplayForASurvivingGap(t *testing.T) {
	ctx := NewContext()
	cell := testIngressCell(t, ctx, DefaultIngressPolicy())
	transport := NewInProcIngress[string, uint64](IngressTransportEventChannel)
	transport.Push(ingressEnv("a", 1, 0, 0, 1))
	transport.Push(ingressEnv("a", 1, 2, 0, 4))

	outcomes := cell.Pump(transport)
	if len(outcomes) != 2 || !outcomes[0].IsDelivered() || outcomes[1] != IngressHeld(1) {
		t.Fatalf("outcomes=%+v, want delivered then buffered@1", outcomes)
	}
	want := []IngressReplayRecord[string]{
		{Key: "a", Request: ReplayRequest{Generation: 1, FromSequence: 1}},
	}
	if !reflect.DeepEqual(transport.Replays(), want) {
		t.Fatalf("replays=%+v, want %+v", transport.Replays(), want)
	}

	// The replay closes the gap, and a second pump asks for nothing more.
	transport.Push(ingressEnv("a", 1, 1, 0, 2))
	cell.Pump(transport)
	if window, _ := cell.Value(ctx, "a"); window != 7 {
		t.Fatalf("window=%d, want 7", window)
	}
	if got := len(transport.Replays()); got != 1 {
		t.Fatalf("replays=%d, want 1", got)
	}
}

func TestIngressAPollingTransportCannotServeAReplay(t *testing.T) {
	ctx := NewContext()
	cell := testIngressCell(t, ctx, DefaultIngressPolicy())
	transport := NewInProcIngress[string, uint64](IngressTransportBoundedPolling)
	transport.Push(ingressEnv("a", 1, 3, 0, 1))
	cell.Pump(transport)
	// A bounded poll has no addressable history, so "this gap will never close"
	// is observable rather than silent.
	if got := transport.Replays(); len(got) != 0 {
		t.Fatalf("replays=%+v, want none", got)
	}
}

func TestIngressScopesDoNotInvalidateEachOther(t *testing.T) {
	ctx := NewContext()
	cell := testIngressCell(t, ctx, DefaultIngressPolicy())
	cell.Admit(ingressEnv("a", 1, 0, 0, 1))

	value := cell.ScopeReaders("a").Value
	runs := 0
	effect := NewEffect(ctx, func(c *Compute) func() {
		runs++
		_ = Get[IngressWindow[uint64]](c, value)
		return nil
	})
	defer effect.Dispose()
	if runs != 1 {
		t.Fatalf("runs=%d, want 1", runs)
	}
	cell.Admit(ingressEnv("b", 1, 0, 0, 2))
	cell.Close("b")
	if runs != 1 {
		t.Fatalf("runs=%d, want 1: closing b must not touch a", runs)
	}
	if window, _ := cell.Value(ctx, "a"); window != 1 {
		t.Fatalf("window(a)=%d, want 1", window)
	}
}

func TestIngressSuspendAndReconnectMoveReadinessAndReportTheGap(t *testing.T) {
	ctx := NewContext()
	cell := testIngressCell(t, ctx, DefaultIngressPolicy())
	cell.Admit(ingressEnv("a", 1, 0, 0, 1))
	cell.Admit(ingressEnv("a", 1, 1, 0, 1))

	request, ok := cell.Suspend("a")
	if !ok || request != (ReplayRequest{Generation: 1, FromSequence: 2}) {
		t.Fatalf("replay=(%+v,%v), want generation 1 from 2", request, ok)
	}
	if got := cell.Readiness(ctx, "a"); got != IngressReadinessSuspended {
		t.Fatalf("readiness=%s, want suspended", got)
	}
	if window, _ := cell.Value(ctx, "a"); window != 2 {
		t.Fatalf("window=%d, want 2 (retained across the disconnect)", window)
	}
	if got := cell.Reconnect("a", 1); got.FromSequence != 2 {
		t.Fatalf("replay from=%d, want 2", got.FromSequence)
	}
	if got := cell.Readiness(ctx, "a"); got != IngressReadinessReady {
		t.Fatalf("readiness=%s, want ready", got)
	}
}

// --- the thread-safe shell ---------------------------------------------------

// TestIngressOneAdmissionIsOneFrontierWalk is the thread-safe flavor's own gate.
//
// A generation handoff dirties value AND authority. If the shell cleared them
// outside one batch, an Effect reading both would run twice and would observe the
// intermediate "new value, old authority" — the partial fan-out the spec forbids.
func TestIngressOneAdmissionIsOneFrontierWalk(t *testing.T) {
	ts := NewThreadSafeContext()
	cell, err := NewThreadSafeIngressCell[string, uint64](
		ts, DefaultIngressPolicy(), Sum[uint64](), IngressTransportEventChannel, 25)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	cell.Admit(ingressEnv("a", 1, 0, 0, 5))

	readers := cell.ScopeReaders("a")
	runs := 0
	var seen [][2]uint64
	var effect *Effect
	ts.WithLock(func(ctx *Context) {
		effect = NewEffect(ctx, func(c *Compute) func() {
			runs++
			window := Get[IngressWindow[uint64]](c, readers.Value)
			authority := Get[Opt[IngressAuthority]](c, readers.Authority)
			seen = append(seen, [2]uint64{window.Value, authority.Value.Generation})
			return nil
		})
	})
	defer ts.WithLock(func(*Context) { effect.Dispose() })
	if runs != 1 {
		t.Fatalf("runs=%d, want 1", runs)
	}

	cell.Admit(ingressEnv("a", 2, 0, 0, 9))
	if runs != 2 {
		t.Fatalf("runs=%d, want 2: one admission is ONE frontier walk", runs)
	}
	want := [][2]uint64{{5, 1}, {9, 2}}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("seen=%+v, want %+v: value and authority must land together", seen, want)
	}
}

func TestIngressThreadSafeFlavorMatchesTheSingleThreadedDerives(t *testing.T) {
	ts := NewThreadSafeContext()
	policy := DefaultIngressPolicy()
	policy.FreshnessHorizon = 10
	cell, err := NewThreadSafeIngressCell[string, uint64](
		ts, policy, Sum[uint64](), IngressTransportEventChannel, 25)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx := ts.Context()
	if got := cell.Readiness(ctx, "a"); got != IngressReadinessUnknown {
		t.Fatalf("readiness=%s, want unknown", got)
	}
	cell.Open("a", 4)
	if got := cell.Readiness(ctx, "a"); got != IngressReadinessWarming {
		t.Fatalf("readiness=%s, want warming", got)
	}
	cell.Admit(ingressEnv("a", 4, 0, 5, 1))
	authority, ok := cell.Authority(ctx, "a")
	want := IngressAuthority{
		Generation: 4, DeliveredThrough: Some[uint64](0), StampedAt: 5,
	}
	if !ok || authority != want {
		t.Fatalf("authority=(%+v,%v), want %+v", authority, ok, want)
	}
	if got := cell.Readiness(ctx, "a"); got != IngressReadinessReady {
		t.Fatalf("readiness=%s, want ready", got)
	}
	cell.Tick(100)
	if got := cell.Readiness(ctx, "a"); got != IngressReadinessStale {
		t.Fatalf("readiness=%s, want stale", got)
	}
}

// --- the async shell ---------------------------------------------------------

func TestIngressAsyncFlavorIsNotAsyncColoured(t *testing.T) {
	ctx := NewAsyncContext()
	defer func() { _ = ctx.Close() }()
	cell, err := NewAsyncIngressCell[string, uint64](
		ctx, DefaultIngressPolicy(), Sum[uint64](), IngressTransportEventChannel, 25)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// Every mutator returns a plain value: an admission decision is a function of
	// the fence, the watermark, the reorder buffer, and the observed clock, so
	// there is nothing to await.
	if got := cell.Admit(ingressEnv("a", 1, 0, 0, 5)); got != IngressAdmitted(0) {
		t.Fatalf("admission=%+v, want accepted@0", got)
	}
	if got := cell.Admit(ingressEnv("a", 1, 2, 0, 4)); got != IngressHeld(1) {
		t.Fatalf("admission=%+v, want buffered@1", got)
	}
	if window, ok := cell.Value(nil, "a"); !ok || window != 5 {
		t.Fatalf("window=(%d,%v), want 5", window, ok)
	}
	if got := cell.Admit(ingressEnv("a", 1, 1, 0, 2)); got != IngressCoalesced(2) {
		t.Fatalf("admission=%+v, want conflated@2", got)
	}
	if window, _ := cell.Value(nil, "a"); window != 11 {
		t.Fatalf("window=%d, want 5+2+4", window)
	}
	if got := cell.Readiness(nil, "a"); got != IngressReadinessReady {
		t.Fatalf("readiness=%s, want ready", got)
	}
	if got := len(cell.Accepted(nil)); got != 2 {
		t.Fatalf("accepted=%d, want 2", got)
	}
	if got := cell.Schedule(nil).PollInterval; got.Present {
		t.Fatalf("poll interval=%+v, want absent", got)
	}
}
