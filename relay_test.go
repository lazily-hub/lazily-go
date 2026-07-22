// RelayCell Phases 2–6 spike (#relaycell) for the Go binding. The doc §8 mandate
// is that Go converges identically to lazily-rs — these prove the operational
// invariants: relay_converges, transport_independent, spill_lossless,
// spill_replay_idempotent, plus overflow behaviour, roles, and the Phase-6
// policies. Mirrors lazily-js test/relay.test.js and lazily-kt RelayTest.kt.

package lazily

import "testing"

// relayFor builds a Sum/Max/etc relay with a high default high-water so a whole
// op stream coalesces into one window.
func relayFor(t *testing.T, ctx *Context, merge MergePolicy[int], highWater uint64, overflow Overflow) *RelayCell[int] {
	t.Helper()
	if highWater == 0 {
		highWater = 1_000_000
	}
	bp := NewBackpressurePolicy(ctx, BoundCount, highWater, highWater/2, overflow)
	r, err := NewRelayCell(ctx, bp, merge)
	if err != nil {
		t.Fatalf("NewRelayCell: %v", err)
	}
	return r
}

// -- Phase 2 -----------------------------------------------------------------

func TestRelayConvergedEgressIndependentOfDrainSchedule(t *testing.T) {
	policies := map[string]MergePolicy[int]{"Sum": Sum[int](), "Max": Max[int]()}
	ops := []int{3, 1, 4, 1, 5, 9, 2, 6}
	for name, policy := range policies {
		flat := ops[0]
		for _, op := range ops[1:] {
			flat = policy.Merge(flat, op)
		}

		// Drain after every op, folding into an egress accumulator.
		ctxA := NewContext()
		rA := relayFor(t, ctxA, policy, 0, OverflowConflate)
		accSet := false
		acc := 0
		for _, op := range ops {
			rA.Ingress(op)
			d, ok := rA.Drain()
			if !ok {
				continue
			}
			if !accSet {
				acc, accSet = d, true
			} else {
				acc = policy.Merge(acc, d)
			}
		}
		if acc != flat {
			t.Fatalf("%s drain-every: got %d want %d", name, acc, flat)
		}

		// Drain once at the end.
		ctxB := NewContext()
		rB := relayFor(t, ctxB, policy, 0, OverflowConflate)
		for _, op := range ops {
			rB.Ingress(op)
		}
		d, _ := rB.Drain()
		if d != flat {
			t.Fatalf("%s drain-once: got %d want %d", name, d, flat)
		}
	}
}

func TestRelayReactiveDepthIsFullIsEmpty(t *testing.T) {
	ctx := NewContext()
	r := relayFor(t, ctx, Sum[int](), 3, OverflowConflate)
	if !r.IsEmpty() || r.Depth() != 0 || r.IsFull() {
		t.Fatalf("fresh relay: empty=%v depth=%d full=%v", r.IsEmpty(), r.Depth(), r.IsFull())
	}

	r.Ingress(1)
	r.Ingress(1)
	if r.IsEmpty() || r.Depth() != 2 || r.IsFull() {
		t.Fatalf("after 2: empty=%v depth=%d full=%v", r.IsEmpty(), r.Depth(), r.IsFull())
	}

	r.Ingress(1)
	if r.Depth() != 3 || !r.IsFull() {
		t.Fatalf("after 3: depth=%d full=%v", r.Depth(), r.IsFull())
	}

	r.Drain()
	if !r.IsEmpty() || r.Depth() != 0 {
		t.Fatalf("after drain: empty=%v depth=%d", r.IsEmpty(), r.Depth())
	}
}

func TestRelayReactiveReadersInvalidateEffect(t *testing.T) {
	ctx := NewContext()
	r := relayFor(t, ctx, Sum[int](), 4, OverflowConflate)
	runs := 0
	lastDepth := uint64(0)
	NewEffect(ctx, func(c *Compute) func() {
		lastDepth = Get(c, r.DepthSlot())
		runs++
		return nil
	})
	if runs != 1 {
		t.Fatalf("initial effect run: %d", runs)
	}
	r.Ingress(1)
	r.Ingress(2)
	if runs < 2 || lastDepth != 2 {
		t.Fatalf("effect after ingress: runs=%d depth=%d", runs, lastDepth)
	}
}

func TestRelayBlockOverflowRefusesIngress(t *testing.T) {
	ctx := NewContext()
	r := relayFor(t, ctx, Sum[int](), 2, OverflowBlock)
	if got := r.Ingress(1); got != IngressAccepted {
		t.Fatalf("first: %v", got)
	}
	if got := r.Ingress(1); got != IngressConflated {
		t.Fatalf("second: %v", got)
	}
	if got := r.Ingress(1); got != IngressBlocked {
		t.Fatalf("third should block: %v", got)
	}
	if d, _ := r.Drain(); d != 2 {
		t.Fatalf("drain: %d", d)
	}
}

func TestRelayDropNewestAndDropOldest(t *testing.T) {
	ctxN := NewContext()
	rn := relayFor(t, ctxN, Sum[int](), 2, OverflowDropNewest)
	rn.Ingress(1)
	rn.Ingress(1)
	if got := rn.Ingress(9); got != IngressDropped {
		t.Fatalf("DropNewest: %v", got)
	}
	if d, _ := rn.Drain(); d != 2 {
		t.Fatalf("DropNewest drain: %d", d)
	}

	ctxO := NewContext()
	ro := relayFor(t, ctxO, Sum[int](), 2, OverflowDropOldest)
	ro.Ingress(1)
	ro.Ingress(1)
	if got := ro.Ingress(9); got != IngressDropped {
		t.Fatalf("DropOldest: %v", got)
	}
	if d, _ := ro.Drain(); d != 9 {
		t.Fatalf("DropOldest drain: %d", d)
	}
}

func TestRelayConstructionRejectsConflateForNonConflating(t *testing.T) {
	ctx := NewContext()
	// RawFifo is the canonical non-conflating policy, but it is slice-valued and
	// Go slices are not comparable, so a RelayCell cannot be built over it. Use a
	// comparable non-conflating policy to exercise the identical construction
	// guard (Conflates=false, mirroring RawFifo's flag).
	nonConflating := MergePolicy[int]{
		Name:        "NonConflatingFifo",
		Merge:       func(_ int, op int) int { return op },
		Commutative: false,
		Idempotent:  false,
		Conflates:   false,
	}
	bp := NewBackpressurePolicy(ctx, BoundCount, 4, 2, OverflowConflate)
	if _, err := NewRelayCell(ctx, bp, nonConflating); err != ErrConflateNotBounding {
		t.Fatalf("want ErrConflateNotBounding, got %v", err)
	}
	// A conflating policy with the same overflow is accepted.
	bp2 := NewBackpressurePolicy(ctx, BoundCount, 4, 2, OverflowConflate)
	if _, err := NewRelayCell(ctx, bp2, Sum[int]()); err != nil {
		t.Fatalf("Sum+Conflate should be legal: %v", err)
	}
}

// -- Phase 3 -----------------------------------------------------------------

func TestSpillLosslessBothModes(t *testing.T) {
	for _, mode := range []SpillMode{SpillCompactOnWrite, SpillAppendCompact} {
		store := NewSpillStore(mode, 2, Sum[int]())
		windows := []int{1, 2, 3, 4, 5}
		for _, w := range windows {
			store.Spill(w, 1)
		}
		hot := 10
		flat := hot
		for _, w := range windows {
			flat += w
		}
		if got := store.Reconstruct(0, hot, true); got != flat {
			t.Fatalf("%s reconstruct: got %d want %d", mode, got, flat)
		}
	}
}

func TestSpillReplayIdempotentForIdempotentPolicy(t *testing.T) {
	store := NewSpillStore(SpillAppendCompact, 1, Max[int]())
	for _, w := range []int{3, 7, 5} {
		store.Spill(w, 1)
	}
	once := store.ReplayUnacked(0)
	twice := store.ReplayUnacked(once)
	if once != twice {
		t.Fatalf("replay not idempotent: %d vs %d", once, twice)
	}
	if once != 7 {
		t.Fatalf("max replay: got %d want 7", once)
	}
}

func TestCompactOnWriteBoundsPagesAndAckReclaims(t *testing.T) {
	store := NewSpillStore(SpillCompactOnWrite, 2, Sum[int]())
	for i := 0; i < 5; i++ { // page size 2 → 3 pages
		store.Spill(1, 1)
	}
	if store.PageCount() != 3 {
		t.Fatalf("page count: %d want 3", store.PageCount())
	}
	firstID := store.Manifest()[0].ID
	store.AckThrough(firstID)
	if len(store.PendingPages()) != 2 {
		t.Fatalf("pending after ack: %d want 2", len(store.PendingPages()))
	}
	store.Reclaim()
	if store.PageCount() != 2 {
		t.Fatalf("page count after reclaim: %d want 2", store.PageCount())
	}
}

// -- Phase 4 -----------------------------------------------------------------

func TestTransportIndependentAcrossFraming(t *testing.T) {
	policies := map[string]MergePolicy[int]{
		"Sum": Sum[int](), "Max": Max[int](), "KeepLatest": KeepLatest[int](),
	}
	ops := []int{3, 1, 4, 1, 5, 9}
	for name, policy := range policies {
		flat := ops[0]
		for _, op := range ops[1:] {
			flat = policy.Merge(flat, op)
		}
		transports := []Transport[int]{
			NewInProcTransport[int](),
			NewFramedTransport[int](2),
			NewFramedTransport[int](3),
		}
		for _, transport := range transports {
			for _, op := range ops {
				transport.Deliver(op)
			}
			ctx := NewContext()
			r := relayFor(t, ctx, policy, 0, OverflowConflate)
			for transport.HasPending() {
				for _, op := range transport.Poll() {
					r.Ingress(op)
				}
			}
			if d, _ := r.Drain(); d != flat {
				t.Fatalf("%s: got %d want %d", name, d, flat)
			}
		}
	}
}

// -- Phase 5 -----------------------------------------------------------------

func TestOutboxConflatesStateBroadcast(t *testing.T) {
	ctx := NewContext()
	out, err := NewOutbox(ctx, 8, KeepLatest[int]())
	if err != nil {
		t.Fatal(err)
	}
	out.Send(1)
	out.Send(2)
	out.Send(3)
	if d, _ := out.Drain(); d != 3 {
		t.Fatalf("outbox drain: %d want 3", d)
	}
}

func TestInboxCreditMetersRemote(t *testing.T) {
	ctx := NewContext()
	inbox, err := NewInbox(ctx, 100, 2, Sum[int]())
	if err != nil {
		t.Fatal(err)
	}
	if !inbox.Ready() {
		t.Fatal("inbox should start ready")
	}
	inbox.Receive(5)
	inbox.Receive(5)
	if inbox.Ready() {
		t.Fatal("inbox should be out of credits")
	}
	if d, _ := inbox.Consume(2); d != 10 {
		t.Fatalf("consume: %d want 10", d)
	}
	if !inbox.Ready() {
		t.Fatal("inbox should be ready after replenish")
	}
}

func TestOutboxToInboxLinkConverges(t *testing.T) {
	ctx := NewContext()
	out, err := NewOutbox(ctx, 64, Sum[int]())
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := NewInbox(ctx, 64, 64, Sum[int]())
	if err != nil {
		t.Fatal(err)
	}
	transport := NewInProcTransport[int]()
	ops := []int{1, 2, 3, 4}
	for _, op := range ops {
		out.Send(op)
	}
	if d, ok := out.Drain(); ok {
		transport.Deliver(d)
	}
	for transport.HasPending() {
		for _, frame := range transport.Poll() {
			inbox.Receive(frame)
		}
	}
	want := 0
	for _, op := range ops {
		want += op
	}
	if d, _ := inbox.Consume(64); d != want {
		t.Fatalf("link converge: %d want %d", d, want)
	}
}

func TestOutboxBlockBackpressuresProducer(t *testing.T) {
	ctx := NewContext()
	out, err := NewOutboxWithOverflow(ctx, BoundCount, 2, OverflowBlock, Sum[int]())
	if err != nil {
		t.Fatal(err)
	}
	out.Send(1)
	out.Send(1)
	if out.Send(1) != IngressBlocked {
		t.Fatal("outbox should backpressure at high water")
	}
	if !out.IsFull() {
		t.Fatal("outbox IsFull should be true at high water")
	}
}

// -- Phase 6 -----------------------------------------------------------------

func TestRatePolicyTokenBucket(t *testing.T) {
	rate := NewRatePolicy(2, 1)
	if !rate.TryEgress() || !rate.TryEgress() {
		t.Fatal("first two egresses should pass")
	}
	if rate.TryEgress() {
		t.Fatal("bucket should be empty")
	}
	rate.Tick()
	if !rate.TryEgress() {
		t.Fatal("egress should pass after refill tick")
	}
}

func TestWindowPolicyFlushOnFillAndTick(t *testing.T) {
	window := NewWindowPolicy(3)
	if window.OnIngress() || window.OnIngress() {
		t.Fatal("window should not flush before fill")
	}
	if !window.OnIngress() {
		t.Fatal("window should flush on fill")
	}
	if window.OnIngress() {
		t.Fatal("window resets after flush")
	}
	if !window.Tick() {
		t.Fatal("tick should flush pending")
	}
	if window.Tick() {
		t.Fatal("tick on empty should not flush")
	}
}

func TestExpiryPolicyDropsAged(t *testing.T) {
	expiry := NewExpiryPolicy(5)
	expiry.Advance(10)
	batch := []TimedValue[string]{
		{At: 3, Value: "old"},
		{At: 7, Value: "fresh"},
		{At: 10, Value: "now"},
	}
	got := RetainLive(expiry, batch)
	want := []string{"fresh", "now"}
	if len(got) != len(want) {
		t.Fatalf("retain live len: %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("retain live: %v want %v", got, want)
		}
	}
}

func TestPriorityStoragePopsHighestFirstFIFOWithin(t *testing.T) {
	pq := NewPriorityStorage[string]()
	pq.Push(1, "low")
	pq.Push(3, "highA")
	pq.Push(2, "mid")
	pq.Push(3, "highB")
	want := []string{"highA", "highB", "mid", "low"}
	for _, w := range want {
		got, ok := pq.Pop()
		if !ok || got != w {
			t.Fatalf("pop: got %q ok=%v want %q", got, ok, w)
		}
	}
	if _, ok := pq.Pop(); ok {
		t.Fatal("empty pop should return false")
	}
}

func TestKeyedRelayShardsPerKey(t *testing.T) {
	ctx := NewContext()
	keyed, err := NewKeyedRelay[string](ctx, 64, OverflowConflate, Sum[int]())
	if err != nil {
		t.Fatal(err)
	}
	keyed.Ingress("a", 1)
	keyed.Ingress("b", 10)
	keyed.Ingress("a", 2)
	if d, _ := keyed.Drain("a"); d != 3 {
		t.Fatalf("key a: %d want 3", d)
	}
	if d, _ := keyed.Drain("b"); d != 10 {
		t.Fatalf("key b: %d want 10", d)
	}
	keys := keyed.Keys()
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("keys: %v want [a b]", keys)
	}
}

func TestKeyedRelayRejectsConflateForNonConflating(t *testing.T) {
	ctx := NewContext()
	nonConflating := MergePolicy[int]{Name: "NonConflating", Merge: func(_ int, op int) int { return op }, Conflates: false}
	if _, err := NewKeyedRelay[string](ctx, 8, OverflowConflate, nonConflating); err != ErrConflateNotBounding {
		t.Fatalf("want ErrConflateNotBounding, got %v", err)
	}
}
