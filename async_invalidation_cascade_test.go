package lazily

import (
	"context"
	"sync/atomic"
	"testing"
)

// Regression coverage for the async invalidation cascade (#lzgoasynccascade).
//
// A slot invalidated *as a dependent* must itself invalidate its own
// dependents. The pull chain cannot compensate, because GetAsync short-circuits
// on AsyncComputedResolved and never re-reads its dependencies. These tests build
// explicit chains, prime the caches, write the root cell, and assert freshness
// at every downstream level.

// asyncChain builds cell -> slot1 -> slot2 -> ... -> slotN, where each slot
// multiplies its upstream by 10, and returns the cell plus the terminal slot.
// Each slot's compute increments its own counter so recomputes are observable.
func asyncChain(ctx *AsyncContext, depth int, counters []*int64) (*AsyncSource[int], *AsyncComputed[int]) {
	cell := NewAsyncSource(ctx, 1)
	head := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		atomic.AddInt64(counters[0], 1)
		return TrackSource(cc, cell) * 10, nil
	})
	prev := head
	for i := 1; i < depth; i++ {
		upstream := prev
		n := i
		prev = NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
			atomic.AddInt64(counters[n], 1)
			v, err := TrackComputed(cc, upstream)
			if err != nil {
				return 0, err
			}
			return v * 10, nil
		})
	}
	return cell, prev
}

func TestAsyncCascadeDepth2(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	counters := []*int64{new(int64), new(int64)}
	cell, tail := asyncChain(ctx, 2, counters)

	if v, err := tail.GetAsync(context.Background()); err != nil || v != 100 {
		t.Fatalf("primed GetAsync = (%d, %v), want (100, nil)", v, err)
	}
	cell.Set(2)
	if v, err := tail.GetAsync(context.Background()); err != nil || v != 200 {
		t.Fatalf("after cell.Set(2), depth-2 tail GetAsync = (%d, %v), want (200, nil) — stale cascade", v, err)
	}
}

func TestAsyncCascadeDepth3(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	counters := []*int64{new(int64), new(int64), new(int64)}
	cell, tail := asyncChain(ctx, 3, counters)

	if v, err := tail.GetAsync(context.Background()); err != nil || v != 1000 {
		t.Fatalf("primed GetAsync = (%d, %v), want (1000, nil)", v, err)
	}
	cell.Set(2)
	if v, err := tail.GetAsync(context.Background()); err != nil || v != 2000 {
		t.Fatalf("after cell.Set(2), depth-3 tail GetAsync = (%d, %v), want (2000, nil) — cascade stopped short", v, err)
	}
	// Every level must have recomputed exactly twice: once to prime, once
	// after the write. A cascade that over-propagates would show more.
	for i, c := range counters {
		if got := atomic.LoadInt64(c); got != 2 {
			t.Errorf("slot %d compute count = %d, want 2", i, got)
		}
	}
}

func TestAsyncCascadeDepth4(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	counters := []*int64{new(int64), new(int64), new(int64), new(int64)}
	cell, tail := asyncChain(ctx, 4, counters)

	if v, err := tail.GetAsync(context.Background()); err != nil || v != 10000 {
		t.Fatalf("primed GetAsync = (%d, %v), want (10000, nil)", v, err)
	}
	cell.Set(3)
	if v, err := tail.GetAsync(context.Background()); err != nil || v != 30000 {
		t.Fatalf("after cell.Set(3), depth-4 tail GetAsync = (%d, %v), want (30000, nil)", v, err)
	}
}

// A batched write must cascade the full cone exactly once at batch exit.
func TestAsyncCascadeUnderBatch(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	counters := []*int64{new(int64), new(int64), new(int64)}
	cell, tail := asyncChain(ctx, 3, counters)

	if v, err := tail.GetAsync(context.Background()); err != nil || v != 1000 {
		t.Fatalf("primed GetAsync = (%d, %v), want (1000, nil)", v, err)
	}
	ctx.Batch(func() {
		cell.Set(2)
		cell.Set(4)
	})
	if v, err := tail.GetAsync(context.Background()); err != nil || v != 4000 {
		t.Fatalf("after batched write, tail GetAsync = (%d, %v), want (4000, nil)", v, err)
	}
	for i, c := range counters {
		if got := atomic.LoadInt64(c); got != 2 {
			t.Errorf("slot %d compute count = %d, want 2 (batch coalesced)", i, got)
		}
	}
}

// A diamond (cell -> a; a -> b, a -> c; b,c -> d) must deliver a fresh tail and
// must not recompute any node twice for a single write.
func TestAsyncCascadeDiamond(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	var dRuns int64
	cell := NewAsyncSource(ctx, 1)
	a := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		return TrackSource(cc, cell), nil
	})
	b := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		v, err := TrackComputed(cc, a)
		return v * 2, err
	})
	cc3 := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		v, err := TrackComputed(cc, a)
		return v * 3, err
	})
	d := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		atomic.AddInt64(&dRuns, 1)
		x, err := TrackComputed(cc, b)
		if err != nil {
			return 0, err
		}
		y, err := TrackComputed(cc, cc3)
		if err != nil {
			return 0, err
		}
		return x + y, nil
	})
	if v, err := d.GetAsync(context.Background()); err != nil || v != 5 {
		t.Fatalf("primed diamond = (%d, %v), want (5, nil)", v, err)
	}
	cell.Set(2)
	if v, err := d.GetAsync(context.Background()); err != nil || v != 10 {
		t.Fatalf("after cell.Set(2), diamond tail = (%d, %v), want (10, nil)", v, err)
	}
	if got := atomic.LoadInt64(&dRuns); got != 2 {
		t.Errorf("diamond tail compute count = %d, want 2", got)
	}
}

// An async effect must keep reacting across MORE THAN ONE successive write.
// One rerun is not enough to detect an effect that fails to re-register its
// dependencies (the "deaf after one rerun" defect).
//
// The counter is bumped AFTER the tracking read, and that ordering is the whole
// reason this test is deterministic (#lzgoasynclostwake). runEffect detaches the
// effect's edges on the owner goroutine and the body re-registers them from a
// separate goroutine, so a counter bumped at the TOP of the body says only that
// the body started — not that it has subscribed. A write landing in that gap is
// absorbed into the run already in flight: TrackSource registers the edge and
// reads the value in one c.do() closure, so the body sees the NEW value and the
// run count never reaches the next number. That coalescing is correct — the
// effect is never left holding a stale value, which is the property that matters
// — but an exact run-count assertion is not entitled to it. Bumping after the
// read makes runs==N evidence that the edge exists, so every later write must
// schedule a real rerun.
func TestAsyncEffectRerunsAcrossTwoWrites(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	cell := NewAsyncSource(ctx, 1)
	var runs int64
	var last atomic.Int64
	eff := ctx.EffectAsync(func(cc *AsyncComputeContext) func() {
		last.Store(int64(TrackSource(cc, cell)))
		atomic.AddInt64(&runs, 1)
		return nil
	})
	defer eff.DisposeAsync()

	waitFor(t, func() bool { return atomic.LoadInt64(&runs) == 1 })
	cell.Set(2)
	waitFor(t, func() bool { return atomic.LoadInt64(&runs) == 2 && last.Load() == 2 })
	cell.Set(3)
	waitFor(t, func() bool { return atomic.LoadInt64(&runs) == 3 && last.Load() == 3 })
	cell.Set(4)
	waitFor(t, func() bool { return atomic.LoadInt64(&runs) == 4 && last.Load() == 4 })
}

// An effect downstream of a SLOT (not directly on the cell) must be reached by
// the cascade and rerun, across two successive writes.
func TestAsyncEffectDownstreamOfSlotCascade(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	cell := NewAsyncSource(ctx, 1)
	slot := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		return TrackSource(cc, cell) * 10, nil
	})
	var runs int64
	var last atomic.Int64
	// Counter after the tracking read, for the reason spelled out on
	// TestAsyncEffectRerunsAcrossTwoWrites (#lzgoasynclostwake).
	eff := ctx.EffectAsync(func(cc *AsyncComputeContext) func() {
		v, err := TrackComputed(cc, slot)
		if err == nil {
			last.Store(int64(v))
		}
		atomic.AddInt64(&runs, 1)
		return nil
	})
	defer eff.DisposeAsync()

	waitFor(t, func() bool { return atomic.LoadInt64(&runs) == 1 && last.Load() == 10 })
	cell.Set(2)
	waitFor(t, func() bool { return last.Load() == 20 })
	cell.Set(3)
	waitFor(t, func() bool { return last.Load() == 30 })
}

// A cyclic async dependency must not spin the invalidation walk. The cycle is
// built by having two slots track each other; the walk must terminate.
func TestAsyncCascadeCycleTerminates(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	cell := NewAsyncSource(ctx, 1)
	var a, b *AsyncComputed[int]
	a = NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		return TrackSource(cc, cell), nil
	})
	b = NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		v, err := TrackComputed(cc, a)
		return v + 1, err
	})
	// Manually close the cycle at the edge level: register a as a dependent of
	// b, which is what a genuine mutual-tracking graph would produce.
	ctx.do(func() { ctx.addDependent(b.node, a.node) })

	if v, err := b.GetAsync(context.Background()); err != nil || v != 2 {
		t.Fatalf("primed cycle = (%d, %v), want (2, nil)", v, err)
	}
	// Must return rather than spin.
	cell.Set(5)
	if v, err := b.GetAsync(context.Background()); err != nil || v != 6 {
		t.Fatalf("after cell.Set(5), cyclic tail = (%d, %v), want (6, nil)", v, err)
	}
}
