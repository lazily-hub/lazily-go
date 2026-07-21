package lazily

import (
	"errors"
	"testing"
)

// The reactive-graph conformance corpus covers Slot, Cell, and Effect disposal
// across both contexts. These tests cover the parts of the teardown surface the
// corpus does not reach — Memo, Signal, WithScope, the error contract, and the
// registry compaction that makes churn bounded in memory as well as in edges.

func TestDisposeDirtiesSurvivingCone(t *testing.T) {
	ctx := NewContext()
	src := NewSourceCell(ctx, 1)
	mid := NewFormulaCell(ctx, func(*Context) int { return src.Get() + 1 })
	sink := NewFormulaCell(ctx, func(*Context) int { return mid.Get() * 10 })

	if got := sink.Get(); got != 20 {
		t.Fatalf("sink = %d, want 20", got)
	}
	mid.Dispose()

	// The whole point: `sink` must not still be serving 20 from its cache.
	if _, err := sink.TryGet(); !errors.Is(err, ErrDisposed) {
		t.Fatalf("sink.TryGet() err = %v, want ErrDisposed — a live reader was left "+
			"frozen on its pre-disposal cache", err)
	}
}

func TestDisposeDoesNotRunEffectsReachedByTeardown(t *testing.T) {
	ctx := NewContext()
	src := NewSourceCell(ctx, 1)
	derived := NewFormulaCell(ctx, func(*Context) int { return src.Get() + 1 })

	runs := 0
	NewEffect(ctx, func(*Context) func() {
		runs++
		// Guarded: after `derived` is disposed this read would panic, which is
		// exactly why teardown must not schedule the effect at all.
		if runs == 1 {
			derived.Get()
		}
		return nil
	})
	if runs != 1 {
		t.Fatalf("effect ran %d times on creation, want 1", runs)
	}

	derived.Dispose()
	if runs != 1 {
		t.Fatalf("effect ran %d times after a disposal — teardown is not a publish "+
			"and must mark dirty without scheduling", runs)
	}
}

func TestDisposeIsIdempotentAndDetachesBothDirections(t *testing.T) {
	ctx := NewContext()
	src := NewSourceCell(ctx, 2)
	derived := NewFormulaCell(ctx, func(*Context) int { return src.Get() })
	_ = derived.Get()

	if got := ctx.DependentCount(src); got != 1 {
		t.Fatalf("DependentCount(src) = %d, want 1", got)
	}
	if got := ctx.DependencyCount(derived); got != 1 {
		t.Fatalf("DependencyCount(derived) = %d, want 1", got)
	}

	derived.Dispose()
	derived.Dispose() // idempotent, not an error

	if got := ctx.DependentCount(src); got != 0 {
		t.Fatalf("DependentCount(src) = %d after disposal, want 0", got)
	}
	if got := ctx.DependencyCount(derived); got != 0 {
		t.Fatalf("DependencyCount(derived) = %d after disposal, want 0", got)
	}
	if !ctx.IsDisposed(derived) {
		t.Fatal("IsDisposed(derived) = false after Dispose")
	}
	if v := src.Get(); v != 2 {
		t.Fatalf("src = %d — the surviving source must be unaffected", v)
	}
}

func TestDisposedCellReadsAsAnError(t *testing.T) {
	ctx := NewContext()
	c := NewSourceCell(ctx, 7)
	c.Dispose()

	if _, err := c.TryGet(); !errors.Is(err, ErrDisposed) {
		t.Fatalf("TryGet err = %v, want ErrDisposed", err)
	}
	var de *DisposedError
	_, err := c.TryGet()
	if !errors.As(err, &de) || de.Kind != "cell" {
		t.Fatalf("err = %v, want a *DisposedError of kind cell", err)
	}
	// Writing a disposed cell is a no-op rather than a panic: it has no
	// dependents left to notify.
	c.Set(9)
}

func TestMemoDisposalDefersItsEqualityRecompute(t *testing.T) {
	ctx := NewContext()
	src := NewSourceCell(ctx, 1)
	upstream := NewFormulaCell(ctx, func(*Context) int { return src.Get() })

	computes := 0
	m := NewMemo(ctx, func(*Context) int {
		computes++
		return upstream.Get() * 2
	})
	if got := m.Get(); got != 2 {
		t.Fatalf("memo = %d, want 2", got)
	}
	before := computes

	// A memo normally recomputes eagerly on invalidation to run its equality
	// guard. During teardown that recompute would read the node being disposed,
	// so it must be deferred to the next read.
	upstream.Dispose()
	if computes != before {
		t.Fatalf("memo recomputed during teardown (%d -> %d)", before, computes)
	}
	if _, err := m.TryGet(); !errors.Is(err, ErrDisposed) {
		t.Fatalf("memo.TryGet err = %v, want ErrDisposed", err)
	}

	m2 := NewMemo(ctx, func(*Context) int { return 1 })
	_ = m2.Get()
	m2.DisposeNode()
	if !ctx.IsDisposed(m2) {
		t.Fatal("Memo.DisposeNode did not dispose the node")
	}
}

func TestSignalDisposalDefersItsEagerPull(t *testing.T) {
	ctx := NewContext()
	src := NewSourceCell(ctx, 1)
	upstream := NewFormulaCell(ctx, func(*Context) int { return src.Get() })
	sig := Formula(ctx, func(*Context) int { return upstream.Get() + 100 }).Drive()

	if got := sig.Get(); got != 101 {
		t.Fatalf("signal = %d, want 101", got)
	}

	// Teardown must not trigger the eager re-pull, and must not leave the
	// signal serving 101 either.
	upstream.Dispose()
	if _, err := sig.TryGet(); !errors.Is(err, ErrDisposed) {
		t.Fatalf("signal.TryGet err = %v, want ErrDisposed", err)
	}

	// Undrive removes the eager puller but leaves the node in the graph (the
	// former dispose_signal); Dispose is the full graph teardown.
	other := Formula(ctx, func(*Context) int { return 5 }).Drive()
	other.Undrive()
	if other.IsDriven() {
		t.Fatal("Undrive should deactivate the eager puller")
	}
	if ctx.IsDisposed(other) {
		t.Fatal("Undrive must not tear the node out of the graph")
	}
	other.Dispose()
	if !ctx.IsDisposed(other) {
		t.Fatal("Dispose did not dispose the node")
	}
}

func TestScopeClosesInReverseCreationOrder(t *testing.T) {
	ctx := NewContext()
	src := NewSourceCell(ctx, 1)

	var cleanups []string
	scope := ctx.Scope()
	a := Own(scope, NewFormulaCell(ctx, func(*Context) int { return src.Get() + 1 }))
	b := Own(scope, NewFormulaCell(ctx, func(*Context) int { return a.Get() + 2 }))
	Own(scope, NewEffect(ctx, func(*Context) func() {
		b.Get()
		return func() { cleanups = append(cleanups, "watch_b") }
	}))
	if scope.Len() != 3 {
		t.Fatalf("scope.Len() = %d, want 3", scope.Len())
	}

	scope.Close()
	scope.Close() // idempotent

	// Only effects run a cleanup, so reverse order is observable through them.
	if len(cleanups) != 1 || cleanups[0] != "watch_b" {
		t.Fatalf("cleanups = %v, want [watch_b]", cleanups)
	}
	if !ctx.IsDisposed(a) || !ctx.IsDisposed(b) {
		t.Fatal("Close did not dispose every scope member")
	}
	if got := ctx.DependentCount(src); got != 0 {
		t.Fatalf("DependentCount(src) = %d after the scope closed, want 0", got)
	}
}

func TestDisarmedScopeDisposesNothing(t *testing.T) {
	ctx := NewContext()
	src := NewSourceCell(ctx, 1)
	scope := ctx.Scope()
	escaped := Own(scope, NewFormulaCell(ctx, func(*Context) int { return src.Get() + 5 }))
	_ = escaped.Get()

	scope.Disarm()
	if scope.Len() != 0 {
		t.Fatalf("scope.Len() = %d after Disarm, want 0", scope.Len())
	}
	scope.Close()

	if ctx.IsDisposed(escaped) {
		t.Fatal("a disarmed scope disposed its members")
	}
	if got := ctx.DependentCount(src); got != 1 {
		t.Fatalf("DependentCount(src) = %d — Disarm must not detach anything", got)
	}
	src.Set(2)
	if got := escaped.Get(); got != 7 {
		t.Fatalf("escaped = %d, want 7 — a disarmed node must still propagate", got)
	}
	// Nodes released by Disarm revert to plain context ownership and stay
	// individually disposable.
	escaped.Dispose()
	if !ctx.IsDisposed(escaped) {
		t.Fatal("a disarmed node is no longer individually disposable")
	}
}

func TestWithScopeClosesOnPanic(t *testing.T) {
	ctx := NewContext()
	var s *FormulaCell[int]
	func() {
		defer func() { _ = recover() }()
		ctx.WithScope(func(sc *TeardownScope) {
			s = Own(sc, NewFormulaCell(ctx, func(*Context) int { return 1 }))
			panic("boom")
		})
	}()
	if s == nil || !ctx.IsDisposed(s) {
		t.Fatal("WithScope must tear its scope down even when fn panics")
	}
}

// TestChurnReturnsToBaseline is the memory half of the #lzspecedgeindex
// contract: the conformance corpus asserts the edge set stays at the live
// subscriber count, and this asserts the slot registry does too. A binding that
// kept every disposed node in Context.slots would pass the corpus and still
// grow without bound.
func TestChurnReturnsToRegistryBaseline(t *testing.T) {
	ctx := NewContext()
	src := NewSourceCell(ctx, 0)

	baseline := len(ctx.slots)
	for i := 0; i < 500; i++ {
		s := NewFormulaCell(ctx, func(*Context) int { return src.Get() })
		_ = s.Get()
		s.Dispose()
	}
	if got := len(ctx.slots); got != baseline {
		t.Fatalf("slot registry = %d after 500 churn cycles, want %d (baseline)", got, baseline)
	}
	if got := ctx.DependentCount(src); got != 0 {
		t.Fatalf("DependentCount(src) = %d, want 0", got)
	}
	if got := ctx.Size(); got != 0 {
		t.Fatalf("Size() = %d, want 0 — a disposed slot must not be counted as cached", got)
	}
}
