package lazily

import (
	"context"
	"testing"
)

// Known divergences: lazily-go's eager memo guard (#lzsignaleager)
// ===========================================================================
//
// THESE TESTS ASSERT DEFECTS, NOT INTENDED BEHAVIOR. Each one pins the CURRENT
// divergent behavior and documents the TARGET behavior beside it. The
// assertions are bidirectional on purpose: they fail if the divergence stops
// reproducing, so whoever lands the propagation migration gets a red test
// telling them to update this ledger instead of a silent pass.
//
// --- The divergence ---------------------------------------------------------
//
// lazily-go's `Memo` implements its `==` guard by RECOMPUTING DURING
// INVALIDATION and only then deciding whether to cascade. The rest of the
// family decides at pull time: invalidation marks the dependent cone dirty and
// computes nothing; a read recomputes a dirty memo, compares to the cached
// value, and if equal marks dependents clean without recomputing them.
//
// The consequence is that `Memo` cannot back a `Signal`. Backing `Signal` with
// it re-materializes once per invalidating source inside a batch (breaking
// #lzsignaleager clause 3) and keeps re-materializing after the puller Effect
// is disposed (breaking clause 4). So `Signal` is built on a plain `Slot` plus
// a puller Effect (see core.go), which satisfies all four clauses but loses the
// `==` guard's downstream suppression — the divergence pinned below.
//
// --- Why this is not a small fix (the mechanism) ----------------------------
//
// The blocker is not the guard's timing. It is that BOTH cascades CONSUME THE
// REVERSE EDGE as they walk:
//
//   - sync:  `reactiveBase.invalidate`  — core.go:116, `clear(b.dependents)`
//   - async: `AsyncContext.propagate`   — async_context.go:618,
//            `delete(c.dependents, cur)`
//
// Dependents re-register only when they recompute and re-run `track`. So a
// dependent "marked clean without recomputing" — which pull-time suppression
// requires — never re-registers, its source can no longer reach it, and the
// NEXT genuine change is lost at depth two, through the memo. That is
// `hybrid_serves_stale_value_at_depth_two` in ../lazily-formal: eager marking
// is correct and lazy pull is correct, but one-level marking plus a
// cache-trusting read is not.
//
// So the guard cannot move to pull time until the cascade becomes
// non-consuming (a mark-frontier walk, the lazily-rs model). That is a change
// to how this binding propagates, not a change to `Memo`. Note also:
//
//   - Both walks TERMINATE by edge consumption (async_context.go:618 says so
//     explicitly), so a non-consuming walk needs a visited/dirty short-circuit
//     added to both.
//   - `Effect.relinkDependencies` (core.go) exists SOLELY to repair edges the
//     consuming cascade ate during teardown. Its own comment notes lazily-rs
//     has nothing to do there "because its mark-frontier walk does not consume
//     edges" — this binding took the consuming variant deliberately, so that
//     whole compensation layer is obsoleted by the migration and has to be
//     re-derived rather than kept.
//   - `cached bool` conflates "no value" with "dirty"; pull-time checking needs
//     a third state. ~60 occurrences across 9 non-test files, plus
//     `Context.cachedCount` behind the public `Size()`.
//   - `DependentCount` is a public observable calibrated to the consuming
//     cascade (see its doc comment), and 30 `dependents_of` assertions across 8
//     reactive-graph fixtures are written against those post-cascade readings.
//
// DO NOT take the shortcut of deferring the guard to the effect flush and
// un-marking dependents via `relinkDependencies`. It would pass every fixture
// while being a third variant of one-level-marking-plus-edge-repair — the exact
// family `hybrid_serves_stale_value_at_depth_two` is about.
//
// The target test is `TestTargetPullTimeMemoGuard` below; it is skipped, not
// deleted, so the migration starts from a written test.

// TestKnownDivergenceSignalEqualRecomputeDoesNotSuppressDownstream records that
// an equal `Signal` recompute does NOT suppress the downstream cascade in this
// binding.
//
// TARGET (lazily-formal `recomputeSlot_equal_preserves_dependents`, and the
// original assertion this test replaced): downstream recomputes 0 additional
// times — a recompute yielding an equal value leaves dependents untouched.
//
// CURRENT: downstream recomputes exactly 1 additional time. `Signal` is backed
// by a plain `Slot`, so the cascade reaches downstream before the recomputed
// value is known and there is nothing to suppress it with. Backing it with
// `Memo` instead would restore suppression at the cost of clauses 3 and 4 of
// #lzsignaleager — see the ledger above.
//
// When the propagation migration lands, `Signal` should move back onto `Memo`
// and this test should fail. Restore the original assertion (0 additional
// recomputes) and delete this entry; do not relax it.
func TestKnownDivergenceSignalEqualRecomputeDoesNotSuppressDownstream(t *testing.T) {
	ctx := NewContext()
	toggle := NewCell(ctx, "x")
	stable := NewSignal(ctx, func(*Context) int {
		_ = toggle.Get() // register the edge; output is constant
		return 42
	})
	downstreamFires := 0
	downstream := NewSlot(ctx, func(*Context) int {
		downstreamFires++
		return stable.Get()
	})
	if got := downstream.Get(); got != 42 {
		t.Fatalf("downstream = %d, want 42", got)
	}
	before := downstreamFires

	toggle.Set("y") // input flips → signal recomputes → equal output

	// Values stay correct either way: this divergence is a cost defect, not a
	// wrong answer, which is exactly why it needs a count assertion to pin it.
	if got := stable.Get(); got != 42 {
		t.Fatalf("stable = %d, want 42", got)
	}
	if got := downstream.Get(); got != 42 {
		t.Fatalf("downstream = %d, want 42", got)
	}

	const divergentExtraRecomputes = 1
	got := downstreamFires - before
	if got != divergentExtraRecomputes {
		t.Fatalf("downstream recomputed %d extra times on an equal signal recompute, "+
			"ledger records %d.\nIf this is now 0, the pull-time memo guard has landed: "+
			"switch Signal back onto Memo, restore the original assertion "+
			"(recomputeSlot_equal_preserves_dependents — 0 extra recomputes), and delete "+
			"this ledger entry. See the ledger comment at the top of this file.",
			got, divergentExtraRecomputes)
	}
}

// TestKnownDivergenceAsyncMemoGuardSuppressesValueNotDownstream records a
// SECOND, independent divergence found while assessing the first: the sync and
// async planes disagree about what the memo `==` guard even means.
//
// TARGET: both planes suppress the downstream cascade on an equal recompute.
//
// CURRENT:
//   - `Context`      — suppresses downstream (eager guard decides BEFORE
//     propagating), so downstream recomputes 0 extra times. Covered by
//     TestMemoEqualitySuppression.
//   - `AsyncContext` — suppresses only the VALUE. `propagate` walks the full
//     transitive cone up front, so by the time `onComplete` applies `eq`
//     (async_context.go, "Memo equality suppression: keep the cached value, no
//     cascade") downstream is already marked. It recomputes 1 extra time.
//
// This matters for the migration: on the sync plane pull-time suppression
// RELOCATES an existing behavior, but on the async plane it ADDS one that has
// never existed. That is a semantic change to AsyncContext independent of
// Signal, and should be decided deliberately rather than absorbed.
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

// TestTargetPullTimeMemoGuard is the written starting point for the propagation
// migration. SKIPPED — it asserts the target semantics, which this binding does
// not implement yet.
//
// Un-skip it as the first step of the migration. It is deliberately paired: the
// first half pins the behavior to ADD (invalidation computes nothing), the
// second half pins what must NOT break while adding it (suppression, and the
// depth-two write that a half-migrated model loses). The second half passes
// TODAY — eager marking is a correct model — so it is the regression guard for
// the migration, not a wish.
func TestTargetPullTimeMemoGuard(t *testing.T) {
	t.Skip("target semantics for the pull-time memo guard; not implemented — " +
		"see the known-divergence ledger at the top of known_divergences_test.go")

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
