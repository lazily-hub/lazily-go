package lazily

import "testing"

// The fortified Compute view is the sole tracking surface (#lzcellkernel).
//
// These tests pin the two halves of the fortification contract, mirroring
// lazily-rs tests/compute_fortification.rs:
//
//  1. A TRACKED read through the *Compute handed to a compute/effect closure
//     (Get(c, handle)) registers a dependency edge against the RECOMPUTING node,
//     so a change to the dependency recomputes the dependent.
//  2. The explicit UNTRACKED escape (Get(c.Untracked(), handle)) registers NO
//     edge, so the dependent neither gains a dependency nor recomputes.
//
// The recomputing node id is threaded as a VALUE (Compute.node), not an ambient
// stack frame, so the attribution is correct by construction. A third test
// covers the Go-specific runtime half of the non-escapability guarantee: a view
// used after its recompute returns fails fast instead of misattributing.

func TestTrackedReadRegistersEdgeAgainstRecomputingNode(t *testing.T) {
	ctx := NewContext()
	a := NewSource(ctx, 1)

	calls := 0
	b := NewComputedC(ctx, func(c *Compute) int {
		calls++
		// Tracked read: the edge must attribute to b, the node being recomputed —
		// not to any ambient frame.
		return Get[int](c, a) * 10
	})

	if got := b.Get(); got != 10 {
		t.Fatalf("b = %d, want 10", got)
	}
	if calls != 1 {
		t.Fatalf("first read computes once, calls = %d", calls)
	}

	// Structural: the edge exists in both directions.
	if n := ctx.DependentCount(a); n != 1 {
		t.Fatalf("a must have b as its single tracked dependent, got %d", n)
	}
	if n := ctx.DependencyCount(b); n != 1 {
		t.Fatalf("b must depend on a, got %d", n)
	}

	// Behavioural: changing a recomputes b.
	a.Set(5)
	if got := b.Get(); got != 50 {
		t.Fatalf("b = %d, want 50", got)
	}
	if calls != 2 {
		t.Fatalf("changing the tracked dependency recomputes b, calls = %d", calls)
	}
}

func TestUntrackedReadRegistersNoEdgeAndDoesNotRecompute(t *testing.T) {
	ctx := NewContext()
	a := NewSource(ctx, 1)

	calls := 0
	d := NewComputedC(ctx, func(c *Compute) int {
		calls++
		// The explicit untracked escape: read a through the owning Context
		// (c.Untracked()), which forms no dependency edge.
		return Get[int](c.Untracked(), a) * 10
	})

	if got := d.Get(); got != 10 {
		t.Fatalf("d = %d, want 10", got)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}

	// Structural: no edge was formed by the untracked read.
	if n := ctx.DependentCount(a); n != 0 {
		t.Fatalf("an untracked read must not register a dependent, got %d", n)
	}
	if n := ctx.DependencyCount(d); n != 0 {
		t.Fatalf("d must have acquired no dependency, got %d", n)
	}

	// Behavioural: changing a does NOT recompute d — its cached value stands.
	a.Set(5)
	if got := d.Get(); got != 10 {
		t.Fatalf("untracked dependent keeps its stale value, got %d", got)
	}
	if calls != 1 {
		t.Fatalf("untracked dependent never recomputes, calls = %d", calls)
	}
}

func TestEffectTracksThroughItsComputeView(t *testing.T) {
	ctx := NewContext()
	a := NewSource(ctx, 1)

	runs := 0
	watch := NewEffectC(ctx, func(c *Compute) func() {
		runs++
		_ = Get[int](c, a)
		return nil
	})
	defer watch.Dispose()

	if runs != 1 {
		t.Fatalf("effect runs once on creation, runs = %d", runs)
	}
	if n := ctx.DependentCount(a); n != 1 {
		t.Fatalf("effect owns the edge to a, got %d", n)
	}

	a.Set(2)
	if runs != 2 {
		t.Fatalf("a change reruns the tracking effect, runs = %d", runs)
	}
}

// TestStaleComputeViewPanics covers the Go-specific runtime fortification. Go
// cannot bind the view by lifetime the way lazily-rs does (!Send + a borrow), so
// non-escapability is enforced at runtime: a Compute captured out of its
// recompute and used afterward panics with *StaleComputeError instead of
// silently registering an edge against a node that is no longer recomputing.
func TestStaleComputeViewPanics(t *testing.T) {
	ctx := NewContext()
	a := NewSource(ctx, 1)

	var escaped *Compute
	b := NewComputedC(ctx, func(c *Compute) int {
		escaped = c // smuggle the view out of its recompute
		return Get[int](c, a)
	})
	_ = b.Get() // drives the recompute; escaped is now dead

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("a tracked read through an escaped Compute must panic")
		}
		if _, ok := r.(*StaleComputeError); !ok {
			t.Fatalf("want *StaleComputeError, got %T: %v", r, r)
		}
	}()
	// Using the escaped view after its recompute returned must fail fast.
	Get[int](escaped, a)
}

// TestComputeUntrackedIsTheOwningContext pins that the untracked escape is the
// owning Context itself (idempotent), so both surfaces satisfy ComputeOps and a
// bridged (func(*Context)) reader and a value-threaded (func(*Compute)) reader
// can coexist over the same graph.
func TestComputeViewAndContextBothSatisfyComputeOps(t *testing.T) {
	ctx := NewContext()
	a := NewSource(ctx, 3)

	// A value-threaded reader and a legacy bridged reader over the same cell.
	viaCompute := NewComputedC(ctx, func(c *Compute) int { return Get[int](c, a) + 1 })
	viaBridge := NewComputed(ctx, func(*Context) int { return a.Get() + 100 })

	if got := viaCompute.Get(); got != 4 {
		t.Fatalf("viaCompute = %d, want 4", got)
	}
	if got := viaBridge.Get(); got != 103 {
		t.Fatalf("viaBridge = %d, want 103", got)
	}

	// Both formed a real edge to a; a change recomputes both.
	if n := ctx.DependentCount(a); n != 2 {
		t.Fatalf("a must have two dependents, got %d", n)
	}
	a.Set(10)
	if got := viaCompute.Get(); got != 11 {
		t.Fatalf("viaCompute = %d, want 11", got)
	}
	if got := viaBridge.Get(); got != 110 {
		t.Fatalf("viaBridge = %d, want 110", got)
	}

	// ComputeOps is satisfied by both concrete surfaces.
	var _ ComputeOps = ctx
	var _ ComputeOps = ctx.newCompute(a)
}
