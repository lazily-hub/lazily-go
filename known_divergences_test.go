package lazily

import (
	"context"
	"testing"
)

// Known divergences: the async memo guard (#lzsignaleager)
// ===========================================================================
//
// THE DIVERGENCE TEST BELOW ASSERTS A DEFECT, NOT INTENDED BEHAVIOR. It pins
// the CURRENT behavior and documents the TARGET behavior beside it. The
// assertion is bidirectional on purpose: it fails if the divergence stops
// reproducing, so whoever changes the async plane gets a red test telling them
// to update this ledger instead of a silent pass.
//
// --- What this ledger used to say, and what changed -------------------------
//
// This file used to record a SECOND divergence: lazily-go's `Memo` implemented
// its `==` guard by RECOMPUTING DURING INVALIDATION, which meant `Memo` could
// not back a `Signal`, which in turn cost `Signal` the guard's downstream
// suppression. That is fixed on the SYNC plane and the entry is retired; the
// restored assertion lives in reactive_extra_test.go as
// TestReactiveSignalEqualRecomputePreservesDependents.
//
// The fix was not to the guard's timing alone. The blocker was that the
// cascade CONSUMED THE REVERSE EDGE as it walked (`clear(b.dependents)`) and
// terminated BY that consumption. A dependent "marked clean without
// recomputing" — which pull-time suppression requires — never re-registered,
// so its source could no longer reach it and the next genuine change was lost
// at depth two: `hybrid_serves_stale_value_at_depth_two` in ../lazily-formal.
//
// So `Context` now propagates the way lazily-rs does:
//
//   - `reactiveBase.invalidate` is a NON-CONSUMING mark-frontier walk
//     (`markCone`, core.go) that terminates on a staleness short-circuit
//     instead of on edge consumption.
//   - Staleness has two levels, `dirty` ("check me") and `forceRecompute`
//     ("I am definitely wrong"), matching rs's SlotNode.{dirty,force_recompute}.
//   - The cache is three-state (`hasValue` / `cached`), so an invalidated slot
//     keeps its previous value to compare a recompute against.
//   - `Memo` is now literally "a `Slot` whose `equals` is set" and overrides
//     nothing; the guard is applied in `Slot.recomputeNow`, at pull time.
//   - Effects carry rs's `force_run`: one scheduled only transitively checks
//     its dependencies at flush time and does not rerun if they all recompute
//     equal. That is how the guard reaches effects.
//   - `Effect.relinkDependencies`, which existed solely to repair edges the
//     consuming cascade ate during teardown, is gone — a non-consuming walk
//     leaves nothing to repair.
//   - `Signal` is back on `Memo` backing, the spec-literal construction.
//
// The rejected shortcut is worth naming, since this ledger once warned about
// it: keeping the consuming cascade, marking in `Memo.invalidate`, deferring
// the guard to the effect flush and un-marking via `relinkDependencies`. It
// passes every fixture while being a third variant of one-level-marking-plus-
// edge-repair — the exact family `hybrid_serves_stale_value_at_depth_two` is
// about. It was not taken.

// TestKnownDivergenceAsyncMemoGuardSuppressesValueNotDownstream records that
// the sync and async planes disagree about what the memo `==` guard means.
//
// TARGET: both planes suppress the downstream cascade on an equal recompute.
//
// CURRENT:
//   - `Context`      — suppresses downstream. The guard is a pull-time check:
//     invalidation only marks the cone, and the read that recomputes the memo
//     compares before anything downstream is treated as stale. Downstream
//     recomputes 0 extra times. Covered by TestMemoEqualitySuppression.
//   - `AsyncContext` — suppresses only the VALUE. `propagate` walks the full
//     transitive cone up front, so by the time `onComplete` applies `eq`
//     (async_context.go, "Memo equality suppression: keep the cached value, no
//     cascade") downstream is already marked. It recomputes 1 extra time.
//
// THIS IS A DELIBERATE SCOPING DECISION, NOT AN OVERSIGHT. The propagation
// migration that gave `Context` its non-consuming mark-frontier walk and
// pull-time guard was scoped to the SYNC plane only, and `AsyncContext` was
// explicitly left alone: its `propagate` FSM, its edge consumption, and its
// value-only suppression are all unchanged. The reason is that the two planes
// are not symmetric here. On the sync plane pull-time suppression RELOCATES an
// existing behavior; on the async plane it would ADD one that has never
// existed, which is a semantic change to AsyncContext independent of Signal.
// Migrating it is a separate piece of work with its own risk, and the full
// reactive-graph corpus is green on both planes as they stand.
//
// So this entry stays until someone deliberately decides to align the async
// plane. If it goes red, AsyncContext gained downstream suppression: confirm
// that was intended, align it with the sync plane, and delete this entry.
func TestKnownDivergenceAsyncMemoGuardSuppressesValueNotDownstream(t *testing.T) {
	ctx := NewAsyncContext()
	defer func() { _ = ctx.Close() }()

	src := NewAsyncCell(ctx, 2)
	m := NewAsyncMemo(ctx, func(cc *AsyncComputeContext) (int, error) {
		return TrackCell(cc, src) % 2, nil // 0 for every even input
	}, func(a, b int) bool { return a == b })

	downstreamFires := 0
	downstream := NewAsyncSlot(ctx, func(cc *AsyncComputeContext) (int, error) {
		downstreamFires++
		v, err := TrackAsync(cc, m)
		return v + 10, err
	})
	if v, err := downstream.GetAsync(context.Background()); err != nil || v != 10 {
		t.Fatalf("setup: downstream = %d, err = %v; want 10, nil", v, err)
	}
	before := downstreamFires

	src.Set(4) // memo recomputes to an equal value (0)

	if v, err := downstream.GetAsync(context.Background()); err != nil || v != 10 {
		t.Fatalf("downstream = %d, err = %v; want 10, nil", v, err)
	}

	const divergentExtraRecomputes = 1
	got := downstreamFires - before
	if got != divergentExtraRecomputes {
		t.Fatalf("async downstream recomputed %d extra times on an equal memo recompute, "+
			"ledger records %d.\nIf this is now 0, AsyncContext gained downstream "+
			"suppression: confirm it was intended, align it with the sync plane, and "+
			"delete this ledger entry. See the ledger comment at the top of this file.",
			got, divergentExtraRecomputes)
	}
}

// TestTargetPullTimeMemoGuard is the target test the propagation migration was
// written against.
//
// It is deliberately paired: the first subtest pins the behavior that was
// ADDED (invalidation computes nothing), the second pins what must NOT break
// while adding it (suppression, and the depth-two write that a half-migrated
// model loses). The second passed BEFORE the migration too — eager marking is
// a correct model — which is what made it the regression guard rather than a
// wish. It must stay green at every intermediate state of any future change to
// propagation; a red here means a hybrid model.
func TestTargetPullTimeMemoGuard(t *testing.T) {
	t.Run("invalidation computes nothing", func(t *testing.T) {
		ctx := NewContext()
		src := NewCell(ctx, 1)
		n := 0
		m := NewMemo(ctx, func(*Context) int { n++; return src.Get() % 2 })
		_ = m.Get()
		if n != 1 {
			t.Fatalf("setup: computes = %d, want 1", n)
		}

		src.Set(3) // invalidating write, no read

		if n != 1 {
			t.Errorf("invalidating write computed %d times, want 0 — the cone should be "+
				"marked dirty and nothing recomputed until a read", n-1)
		}
		if got := m.Get(); got != 1 {
			t.Errorf("value = %d, want 1", got)
		}
		if n != 2 {
			t.Errorf("computes after the read = %d, want 2", n)
		}
	})

	t.Run("suppression survives and no write is lost at depth two", func(t *testing.T) {
		ctx := NewContext()
		src := NewCell(ctx, 2)
		m := NewMemo(ctx, func(*Context) int { return src.Get() % 2 }) // 0 for evens
		downstreamFires := 0
		d := NewSlot(ctx, func(*Context) int { downstreamFires++; return m.Get() + 10 })
		if got := d.Get(); got != 10 {
			t.Fatalf("setup: d = %d, want 10", got)
		}
		before := downstreamFires

		src.Set(4) // memo recomputes equal → downstream must NOT recompute
		if got := d.Get(); got != 10 {
			t.Fatalf("d = %d, want 10", got)
		}
		if downstreamFires != before {
			t.Errorf("downstream recomputed on an equal memo recompute (%d → %d)",
				before, downstreamFires)
		}

		// hybrid_serves_stale_value_at_depth_two: after a suppressed cascade the
		// src → m → d edges must survive, or the next real change is lost.
		src.Set(5) // memo now 1 → d must observe 11
		if got := d.Get(); got != 11 {
			t.Errorf("write lost at depth two: d = %d, want 11", got)
		}
	})
}
