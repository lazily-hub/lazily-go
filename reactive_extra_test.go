package lazily

import "testing"

// Ported from lazily-dart test/reactive_core_test.dart and
// test/reactive_properties_test.dart — the cases NOT already covered by
// core_test.go (effect cleanup, dispose, unrelated-cell isolation, nested
// batch, no-op/equal-write absorption, memo chains, signal materialization,
// diamond dependency).

// --- Effect (reactive_core_test.dart § Effect) -----------------------------

func TestReactiveEffectRerunsOnDependencyChange(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 1)
	var log []int
	NewEffect(ctx, func(*Context) func() {
		log = append(log, a.Get())
		return nil
	})
	if got := log; len(got) != 1 || got[0] != 1 {
		t.Fatalf("log = %v, want [1]", log)
	}
	a.Set(2)
	a.Set(3)
	if len(log) != 3 || log[1] != 2 || log[2] != 3 {
		t.Fatalf("log = %v, want [1 2 3]", log)
	}
}

func TestReactiveEffectCleanupBeforeRerunAndOnDispose(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 0)
	var cleanups []int
	effect := NewEffect(ctx, func(*Context) func() {
		seen := a.Get()
		return func() { cleanups = append(cleanups, seen) }
	})
	a.Set(1)
	// cleanup for the first run (seen=0) fired before the rerun.
	if len(cleanups) != 1 || cleanups[0] != 0 {
		t.Fatalf("cleanups after rerun = %v, want [0]", cleanups)
	}
	effect.Dispose()
	// cleanup for the second run (seen=1) fired on dispose.
	if len(cleanups) != 2 || cleanups[1] != 1 {
		t.Fatalf("cleanups after dispose = %v, want [0 1]", cleanups)
	}
	a.Set(2)
	if len(cleanups) != 2 {
		t.Fatalf("cleanups after post-dispose write = %v, want no rerun", cleanups)
	}
}

func TestReactiveEffectIgnoresUnrelatedCell(t *testing.T) {
	ctx := NewContext()
	tracked := NewCell(ctx, 10)
	untracked := NewCell(ctx, 100)
	runs := 0
	NewEffect(ctx, func(*Context) func() {
		_ = tracked.Get()
		runs++
		return nil
	})
	if runs != 1 {
		t.Fatalf("runs = %d, want 1", runs)
	}
	untracked.Set(200)
	if runs != 1 {
		t.Fatalf("runs after unrelated write = %d, want 1", runs)
	}
	tracked.Set(20)
	if runs != 2 {
		t.Fatalf("runs after tracked write = %d, want 2", runs)
	}
}

func TestReactiveEffectIsActiveAfterDispose(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 0)
	effect := NewEffect(ctx, func(*Context) func() {
		_ = a.Get()
		return nil
	})
	if !effect.IsActive() {
		t.Fatal("effect should be active before dispose")
	}
	effect.Dispose()
	if effect.IsActive() {
		t.Fatal("effect should be inactive after dispose")
	}
	// Dispose is idempotent.
	effect.Dispose()
}

// --- Memo (reactive_core_test.dart § Memo) ---------------------------------

func TestReactiveMemoReturnsCachedValue(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 2)
	b := NewCell(ctx, 3)
	sum := NewMemo(ctx, func(*Context) int { return a.Get() + b.Get() })
	if got := sum.Get(); got != 5 {
		t.Fatalf("sum = %d, want 5", got)
	}
	a.Set(10)
	if got := sum.Get(); got != 13 {
		t.Fatalf("sum after set = %d, want 13", got)
	}
}

func TestReactiveMemoChainCascades(t *testing.T) {
	ctx := NewContext()
	src := NewCell(ctx, 1)
	doubled := NewMemo(ctx, func(*Context) int { return src.Get() * 2 })
	quadrupled := NewMemo(ctx, func(*Context) int { return doubled.Get() * 2 })
	if got := quadrupled.Get(); got != 4 {
		t.Fatalf("quadrupled = %d, want 4", got)
	}
	src.Set(5)
	if got := quadrupled.Get(); got != 20 {
		t.Fatalf("quadrupled after set = %d, want 20", got)
	}
}

// --- batch (reactive_core_test.dart § batch) -------------------------------

func TestReactiveNestedBatchDefersToOutermost(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 0)
	runs := 0
	NewEffect(ctx, func(*Context) func() {
		_ = a.Get()
		runs++
		return nil
	})
	if runs != 1 {
		t.Fatalf("runs = %d, want 1", runs)
	}
	ctx.Batch(func() {
		a.Set(1)
		ctx.Batch(func() {
			a.Set(2)
		})
		if runs != 1 {
			t.Fatalf("runs inside outer batch = %d, want 1 (deferred)", runs)
		}
		a.Set(3)
	})
	if runs != 2 {
		t.Fatalf("runs after nested batch = %d, want 2", runs)
	}
}

func TestReactiveBatchWritesVisibleImmediately(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 0)
	ctx.Batch(func() {
		a.Set(42)
		if got := a.Peek(); got != 42 {
			t.Fatalf("peek inside batch = %d, want 42", got)
		}
	})
}

func TestReactiveNoOpBatchNoSpuriousEffect(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 1)
	runs := 0
	NewEffect(ctx, func(*Context) func() {
		_ = a.Get()
		runs++
		return nil
	})
	if runs != 1 {
		t.Fatalf("runs = %d, want 1", runs)
	}
	ctx.Batch(func() {})
	if runs != 1 {
		t.Fatalf("runs after no-op batch = %d, want 1", runs)
	}
}

func TestReactiveEqualWriteInsideBatchAbsorbed(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 5)
	runs := 0
	NewEffect(ctx, func(*Context) func() {
		_ = a.Get()
		runs++
		return nil
	})
	if runs != 1 {
		t.Fatalf("runs = %d, want 1", runs)
	}
	ctx.Batch(func() {
		a.Set(5) // equal — absorbed
	})
	if runs != 1 {
		t.Fatalf("runs after equal batch write = %d, want 1", runs)
	}
}

// --- properties (reactive_properties_test.dart, mirroring Lean theorems) ---

// Lean setCell_equal_preserves_graph: an equal setCell invalidates no dependent
// — neither the lazy slot recomputes nor the eager effect reruns. Both arms are
// dependency edges declared by reading the cell, which is the only way to
// observe a Cell.
func TestReactiveEqualSetPreservesGraph(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 2)
	slotFires := 0
	dependent := NewSlot(ctx, func(*Context) int {
		slotFires++
		return a.Get()
	})
	effectFires := 0
	NewEffect(ctx, func(*Context) func() {
		effectFires++
		a.Get()
		return nil
	})

	if got := dependent.Get(); got != 2 {
		t.Fatalf("dependent = %d, want 2", got)
	}
	slotBefore, effectBefore := slotFires, effectFires

	a.Set(2) // equal — must be a no-op

	if got := dependent.Get(); got != 2 {
		t.Fatalf("dependent after equal set = %d, want 2", got)
	}
	if slotFires != slotBefore {
		t.Fatalf("slot recomputed on equal set: %d != %d", slotFires, slotBefore)
	}
	if effectFires != effectBefore {
		t.Fatalf("effect reran on equal set: %d != %d", effectFires, effectBefore)
	}
}

// Lean setCell_different_invalidates_dependents: a strictly-different write
// marks every direct dependent (lazy slot + eager signal) dirty.
func TestReactiveDifferentSetInvalidatesDependents(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 1)
	lazy := NewSlot(ctx, func(*Context) int { return a.Get() + 1 })
	eager := NewSignal(ctx, func(*Context) int { return a.Get() * 10 })
	if got := lazy.Get(); got != 2 {
		t.Fatalf("lazy = %d, want 2", got)
	}
	if got := eager.Get(); got != 10 {
		t.Fatalf("eager = %d, want 10", got)
	}
	a.Set(99)
	if got := lazy.Get(); got != 100 {
		t.Fatalf("lazy after set = %d, want 100", got)
	}
	if got := eager.Get(); got != 990 {
		t.Fatalf("eager after set = %d, want 990", got)
	}
}

// Lean recomputeSlot_equal_preserves_dependents, applied to a Signal, does NOT
// hold in this binding: Signal is backed by a plain Slot (core.go), so an equal
// recompute does not suppress the downstream cascade. The property is recorded
// as a known divergence, asserted bidirectionally, in
// TestKnownDivergenceSignalEqualRecomputeDoesNotSuppressDownstream
// (known_divergences_test.go), which also carries the mechanism and the
// conditions for restoring the original assertion.
//
// The same Lean property DOES hold for Memo itself — see
// TestMemoEqualitySuppression in core_test.go, which is unaffected.

// Lean recomputeSlot_different_invalidates_dependents: a strictly-different
// signal recompute invalidates every direct dependent.
func TestReactiveSignalDifferentRecomputeInvalidatesDependents(t *testing.T) {
	ctx := NewContext()
	src := NewCell(ctx, 1)
	sig := NewSignal(ctx, func(*Context) int { return src.Get() * 2 })
	lazyChild := NewSlot(ctx, func(*Context) int { return sig.Get() + 1 })
	if got := lazyChild.Get(); got != 3 {
		t.Fatalf("lazyChild = %d, want 3", got)
	}
	src.Set(5)
	if got := sig.Get(); got != 10 {
		t.Fatalf("sig = %d, want 10", got)
	}
	if got := lazyChild.Get(); got != 11 {
		t.Fatalf("lazyChild after set = %d, want 11", got)
	}
}

// Lean signal_materialized_after_recompute: a signal reflects the new input the
// instant a dependency changes (never an unset intermediate), across repeated
// changes.
func TestReactiveSignalMaterializedAfterRecompute(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 1)
	sig := NewSignal(ctx, func(*Context) int { return a.Get() + 100 })
	if got := sig.Get(); got != 101 {
		t.Fatalf("sig = %d, want 101", got)
	}
	a.Set(7)
	if got := sig.Get(); got != 107 {
		t.Fatalf("sig after set = %d, want 107", got)
	}
	a.Set(8)
	if got := sig.Get(); got != 108 {
		t.Fatalf("sig after second set = %d, want 108", got)
	}
}

// --- extra structural coverage --------------------------------------------

// Diamond dependency: a source feeds two memos that both feed one effect. A
// single source write must trigger exactly one downstream effect rerun (the
// cascade coalesces at the shared sink).
func TestReactiveDiamondDependencySingleRerun(t *testing.T) {
	ctx := NewContext()
	src := NewCell(ctx, 1)
	left := NewMemo(ctx, func(*Context) int { return src.Get() + 1 })
	right := NewMemo(ctx, func(*Context) int { return src.Get() * 2 })
	runs := 0
	var lastSum int
	NewEffect(ctx, func(*Context) func() {
		lastSum = left.Get() + right.Get()
		runs++
		return nil
	})
	if runs != 1 || lastSum != (1+1)+(1*2) {
		t.Fatalf("initial runs=%d sum=%d, want 1 and 4", runs, lastSum)
	}
	src.Set(10)
	if runs != 2 {
		t.Fatalf("runs after diamond source write = %d, want 2 (single coalesced rerun)", runs)
	}
	if lastSum != (10+1)+(10*2) {
		t.Fatalf("sum after set = %d, want 31", lastSum)
	}
}

// Signal.Dispose reverts the signal to lazy behavior but keeps the last value
// readable.
func TestReactiveSignalDisposeRevertsToLazy(t *testing.T) {
	ctx := NewContext()
	a := NewCell(ctx, 2)
	sig := NewSignal(ctx, func(*Context) int { return a.Get() * 5 })
	if got := sig.Get(); got != 10 {
		t.Fatalf("sig = %d, want 10", got)
	}
	if !sig.IsActive() {
		t.Fatal("signal should be active before dispose")
	}
	sig.Dispose()
	if sig.IsActive() {
		t.Fatal("signal should be inactive after dispose")
	}
	// Still readable; lazy backing recomputes on read after a source change.
	a.Set(4)
	if got := sig.Get(); got != 20 {
		t.Fatalf("sig after dispose+set = %d, want 20 (lazy recompute)", got)
	}
}

// --- state machine (state_machine.go, ported from state_machine.dart) ------

func TestReactiveStateMachineTransitions(t *testing.T) {
	ctx := NewContext()
	// turnstile: locked --coin--> unlocked --push--> locked
	m := NewStateMachine(ctx, "locked", func(state, event string) (string, bool) {
		switch {
		case state == "locked" && event == "coin":
			return "unlocked", true
		case state == "unlocked" && event == "push":
			return "locked", true
		default:
			return state, false // reject
		}
	})
	if m.State() != "locked" {
		t.Fatalf("initial state = %q, want locked", m.State())
	}
	if !m.Send("coin") {
		t.Fatal("coin should be accepted from locked")
	}
	if m.State() != "unlocked" {
		t.Fatalf("state after coin = %q, want unlocked", m.State())
	}
	// Rejected event leaves state unchanged.
	if m.Send("coin") {
		t.Fatal("coin should be rejected from unlocked")
	}
	if m.State() != "unlocked" {
		t.Fatalf("state after rejected coin = %q, want unlocked", m.State())
	}
	if !m.Send("push") {
		t.Fatal("push should be accepted from unlocked")
	}
	if m.State() != "locked" {
		t.Fatalf("state after push = %q, want locked", m.State())
	}
}

func TestReactiveStateMachineDrivesReactiveGraph(t *testing.T) {
	ctx := NewContext()
	m := NewStateMachine(ctx, 0, func(state, event int) (int, bool) {
		return state + event, true
	})
	runs := 0
	var last int
	NewEffect(ctx, func(*Context) func() {
		last = m.State()
		runs++
		return nil
	})
	if runs != 1 || last != 0 {
		t.Fatalf("initial runs=%d last=%d, want 1 and 0", runs, last)
	}
	m.Send(5)
	if runs != 2 || last != 5 {
		t.Fatalf("after send(5) runs=%d last=%d, want 2 and 5", runs, last)
	}
	// A self-transition to an equal state is accepted but suppressed.
	if !m.Send(0) {
		t.Fatal("send(0) should be accepted")
	}
	if runs != 2 {
		t.Fatalf("runs after equal-state transition = %d, want 2 (suppressed)", runs)
	}
}

func TestReactiveStateMachineOnTransition(t *testing.T) {
	ctx := NewContext()
	m := NewStateMachine(ctx, "a", func(state, event string) (string, bool) {
		return event, true // event names the target state
	})
	type edge struct{ from, to string }
	var edges []edge
	dispose := m.OnTransition(func(oldState, newState string) {
		edges = append(edges, edge{oldState, newState})
	})
	m.Send("b")
	m.Send("b") // equal — no transition callback (cell != guard)
	m.Send("c")
	dispose()
	m.Send("d") // after dispose — no callback
	if len(edges) != 2 {
		t.Fatalf("edges = %v, want 2 transitions", edges)
	}
	if edges[0] != (edge{"a", "b"}) || edges[1] != (edge{"b", "c"}) {
		t.Fatalf("edges = %v, want [{a b} {b c}]", edges)
	}
}

// OnTransition is an effect, so a batch that walks several states reports one
// transition from the pre-batch state to the settled state. Intermediate states
// are not observable — that is what a batch asserts. This pins the behavior
// change from the removed Cell observer registry, which delivered per-write.
func TestReactiveStateMachineOnTransitionCoalescesUnderBatch(t *testing.T) {
	ctx := NewContext()
	m := NewStateMachine(ctx, "a", func(state, event string) (string, bool) {
		return event, true
	})
	type edge struct{ from, to string }
	var edges []edge
	defer m.OnTransition(func(oldState, newState string) {
		edges = append(edges, edge{oldState, newState})
	})()

	ctx.Batch(func() {
		m.Send("b")
		m.Send("c")
	})

	if len(edges) != 1 || edges[0] != (edge{"a", "c"}) {
		t.Fatalf("edges = %v, want [{a c}] — a batch settles to one transition", edges)
	}
}
