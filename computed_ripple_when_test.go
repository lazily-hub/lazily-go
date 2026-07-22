// NewComputedRippleWhen (#lzcellkernel) — a guarded computed with an explicit,
// PURE change predicate (true = propagate). Mirrors lazily-rs
// tests/computed_ripple_when.rs: a custom significance policy, "propagate every
// N" where the increment evidence lives in the value (so the predicate stays
// pure), and the identities NewComputed(f) == RippleWhen(f, !=) and
// NewSlot(f) = always-propagate. Adds a Go-specific case: a non-comparable
// []string computed guarded via slices.Equal.
package lazily

import (
	"slices"
	"testing"
)

func TestComputedRippleWhenCustomSignificancePropagatesOnProxyChange(t *testing.T) {
	ctx := NewContext()
	input := NewSource(ctx, uint64(0))

	// Derived value carries a bucket proxy; propagate only when the bucket
	// changes, ignoring the raw payload.
	type payload struct {
		value  uint64
		bucket uint64
	}
	derived := NewComputedRippleWhen(ctx, func(*Context) payload {
		v := input.Get()
		return payload{value: v, bucket: v / 10}
	}, func(old, next payload) bool { return old.bucket != next.bucket })

	recomputes := 0
	observer := NewComputed(ctx, func(*Context) uint64 {
		recomputes++
		return derived.Get().value
	})

	if got := observer.Get(); got != 0 {
		t.Fatalf("initial: got %d, want 0", got)
	}
	base := recomputes

	// Same bucket (0..9): dependent stays cached.
	input.Set(3)
	if got := observer.Get(); got != 0 {
		t.Fatalf("suppressed: got %d, want 0 (proxy bucket unchanged)", got)
	}
	if recomputes != base {
		t.Fatalf("no dependent recompute within a bucket: got %d, want %d", recomputes, base)
	}

	// Crossing a bucket boundary propagates.
	input.Set(12)
	if got := observer.Get(); got != 12 {
		t.Fatalf("propagated: got %d, want 12 (bucket changed)", got)
	}
	if recomputes != base+1 {
		t.Fatalf("expected one dependent recompute: got %d, want %d", recomputes, base+1)
	}
}

func TestComputedRippleWhenPropagateEveryNViaValueCarriedCounter(t *testing.T) {
	ctx := NewContext()
	input := NewSource(ctx, uint64(0))

	// "Propagate every 3rd increment" — evidence (the counter) is IN the value,
	// so the predicate is a pure function of (old, new): propagate only when the
	// count crosses a size-3 window boundary.
	sampled := NewComputedRippleWhen(ctx, func(*Context) uint64 {
		return input.Get()
	}, func(old, next uint64) bool { return next/3 != old/3 })

	seen := 0
	observer := NewComputed(ctx, func(*Context) uint64 {
		seen++
		return sampled.Get()
	})

	if got := observer.Get(); got != 0 {
		t.Fatalf("initial: got %d, want 0", got)
	}
	base := seen

	// 0 -> 1 -> 2 stay in window [0,3): suppressed.
	input.Set(1)
	input.Set(2)
	if got := observer.Get(); got != 0 {
		t.Fatalf("suppressed: got %d, want 0", got)
	}
	if seen != base {
		t.Fatalf("window not crossed yet: got %d, want %d", seen, base)
	}

	// 3 crosses into [3,6): propagate.
	input.Set(3)
	if got := observer.Get(); got != 3 {
		t.Fatalf("propagated: got %d, want 3", got)
	}
	if seen != base+1 {
		t.Fatalf("expected one dependent recompute: got %d, want %d", seen, base+1)
	}
}

func TestComputedIsComputedRippleWhenNotEqual(t *testing.T) {
	// NewComputed(f) behaves as NewComputedRippleWhen(f, func(o, n) bool { return o != n }).
	ctx := NewContext()
	input := NewSource(ctx, int64(0))

	viaComputed := NewComputed(ctx, func(*Context) int64 { return min64(input.Get(), 1) })
	viaWhen := NewComputedRippleWhen(ctx, func(*Context) int64 {
		return min64(input.Get(), 1)
	}, func(o, n int64) bool { return o != n })

	ca, cb := 0, 0
	obsA := NewComputed(ctx, func(*Context) int64 {
		ca++
		return viaComputed.Get()
	})
	obsB := NewComputed(ctx, func(*Context) int64 {
		cb++
		return viaWhen.Get()
	})
	if obsA.Get() != 0 || obsB.Get() != 0 {
		t.Fatalf("initial reads: got (%d, %d), want (0, 0)", obsA.Get(), obsB.Get())
	}
	baseA, baseB := ca, cb

	// 0 -> 5 both clamp to 1: both guards suppress identically (value changes 0->1).
	input.Set(5)
	if obsA.Get() != 1 || obsB.Get() != 1 {
		t.Fatalf("after 5: got (%d, %d), want (1, 1)", obsA.Get(), obsB.Get())
	}
	if ca != baseA+1 || cb != baseB+1 {
		t.Fatalf("after 5 recomputes: got (%d, %d), want (%d, %d)", ca, cb, baseA+1, baseB+1)
	}

	// 5 -> 9 both stay 1: both suppress the dependent.
	input.Set(9)
	if obsA.Get() != 1 || obsB.Get() != 1 {
		t.Fatalf("after 9: got (%d, %d), want (1, 1)", obsA.Get(), obsB.Get())
	}
	if ca != baseA+1 {
		t.Fatalf("computed suppressed equal recompute: got %d, want %d", ca, baseA+1)
	}
	if cb != baseB+1 {
		t.Fatalf("computed_ripple_when(!=) matches computed: got %d, want %d", cb, baseB+1)
	}
}

func TestSlotIsPassThroughAlwaysPropagates(t *testing.T) {
	ctx := NewContext()
	input := NewSource(ctx, uint64(0))
	// NewSlot installs no guard: even an equal recompute propagates. This is
	// exactly NewComputedRippleWhen(f, func(_, _) bool { return true }).
	passthrough := NewSlot(ctx, func(*Context) uint64 {
		_ = input.Get() // depend on input, but always yield the same value
		return 0
	})

	recomputes := 0
	observer := NewComputed(ctx, func(*Context) uint64 {
		recomputes++
		return passthrough.Get()
	})

	if got := observer.Get(); got != 0 {
		t.Fatalf("initial: got %d, want 0", got)
	}
	base := recomputes

	// Value stays 0, but slot has no guard, so the dependent re-fires.
	input.Set(5)
	if got := observer.Get(); got != 0 {
		t.Fatalf("after set: got %d, want 0", got)
	}
	if recomputes <= base {
		t.Fatalf("pass-through slot propagates even when the value is unchanged: got %d, want > %d", recomputes, base)
	}
}

func TestSlotEqualsAlwaysPropagateRippleWhen(t *testing.T) {
	// The pass-through identity spelled explicitly: NewSlot(f) ==
	// NewComputedRippleWhen(f, alwaysTrue). Both re-fire the dependent on an
	// equal recompute.
	ctx := NewContext()
	input := NewSource(ctx, uint64(0))
	always := NewComputedRippleWhen(ctx, func(*Context) uint64 {
		_ = input.Get()
		return 0
	}, func(_, _ uint64) bool { return true })

	recomputes := 0
	observer := NewComputed(ctx, func(*Context) uint64 {
		recomputes++
		return always.Get()
	})
	if got := observer.Get(); got != 0 {
		t.Fatalf("initial: got %d, want 0", got)
	}
	base := recomputes
	input.Set(7)
	if got := observer.Get(); got != 0 {
		t.Fatalf("after set: got %d, want 0", got)
	}
	if recomputes <= base {
		t.Fatalf("always-propagate ripple-when re-fires the dependent: got %d, want > %d", recomputes, base)
	}
}

func TestComputedRippleWhenNonComparableSliceGuardedBySlicesEqual(t *testing.T) {
	// Go-specific: a []string computed is not comparable, so NewComputed does not
	// even compile for it. NewComputedRippleWhen supplies the predicate, so a
	// slice value can still be change-guarded via slices.Equal.
	ctx := NewContext()
	n := NewSource(ctx, 0)

	tags := NewComputedRippleWhen(ctx, func(*Context) []string {
		// Value depends on n but only its parity determines the slice, so two
		// consecutive even (or odd) n produce an equal slice.
		if n.Get()%2 == 0 {
			return []string{"a", "b"}
		}
		return []string{"a", "b", "c"}
	}, func(old, next []string) bool { return !slices.Equal(old, next) })

	recomputes := 0
	observer := NewComputed(ctx, func(*Context) int {
		recomputes++
		return len(tags.Get())
	})

	if got := observer.Get(); got != 2 {
		t.Fatalf("initial: got %d, want 2", got)
	}
	base := recomputes

	// 0 -> 2: still even, slice equal — suppressed.
	n.Set(2)
	if got := observer.Get(); got != 2 {
		t.Fatalf("suppressed (equal slice): got %d, want 2", got)
	}
	if recomputes != base {
		t.Fatalf("no dependent recompute for an equal slice: got %d, want %d", recomputes, base)
	}

	// 2 -> 3: parity flips, slice changes — propagate.
	n.Set(3)
	if got := observer.Get(); got != 3 {
		t.Fatalf("propagated (slice changed): got %d, want 3", got)
	}
	if recomputes != base+1 {
		t.Fatalf("expected one dependent recompute: got %d, want %d", recomputes, base+1)
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
