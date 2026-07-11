package lazily

import (
	"sync"
	"testing"
)

// Tests mirror lazily-zig thread_safe_reactive_family.zig / lazily-rs
// thread_safe_reactive_family.rs, naming the lazily-formal Materialization
// theorems (incl. the confluence pair) each assertion rests on.

func TestTSReactiveFamilyDefaultModeIsEager(t *testing.T) {
	if DefaultMaterializationMode() != Eager {
		t.Fatalf("default mode = %v, want eager", DefaultMaterializationMode())
	}
}

func TestTSReactiveFamilyEagerCellMaterializesAllAtBuild(t *testing.T) {
	ctx := NewContext()
	fam := ThreadSafeEagerCellFamily(ctx, []uint32{1, 2, 3}, func(uint32) bool { return true })

	if fam.EntryKind() != EntryKindCell {
		t.Fatalf("kind = %v, want cell", fam.EntryKind())
	}
	if fam.Mode() != Eager {
		t.Fatalf("mode = %v, want eager", fam.Mode())
	}
	if got := fam.PresentCount(); got != 3 {
		t.Fatalf("present count = %d, want 3", got)
	}
	if !fam.IsPresent(1) || !fam.IsPresent(2) || !fam.IsPresent(3) {
		t.Fatalf("expected 1,2,3 present")
	}
	if got := fam.PresentKeys(); !equalU32(got, []uint32{1, 2, 3}) {
		t.Fatalf("present keys = %v, want [1 2 3]", got)
	}
}

func TestTSReactiveFamilyLazySlotDefersUntilRead(t *testing.T) {
	ctx := NewContext()
	fam := ThreadSafeLazySlotFamily(ctx, nil, func(k uint32) uint32 { return k * 10 })

	if fam.Mode() != Lazy {
		t.Fatalf("mode = %v, want lazy", fam.Mode())
	}
	if got := fam.PresentCount(); got != 0 {
		t.Fatalf("present count = %d, want 0", got)
	}
	if fam.IsPresent(2) {
		t.Fatalf("key 2 should not be present before read")
	}
	if got := fam.Observe(2); got != 20 {
		t.Fatalf("observe(2) = %d, want 20", got)
	}
	if !fam.IsPresent(2) {
		t.Fatalf("key 2 should be present after read")
	}
	if got := fam.PresentCount(); got != 1 {
		t.Fatalf("present count = %d, want 1", got)
	}
}

func TestTSReactiveFamilyLazyCellStillMaterializesAtBuild(t *testing.T) {
	ctx := NewContext()
	fam := ThreadSafeLazyCellFamily(ctx, []uint32{7, 8}, func(uint32) bool { return true })
	if got := fam.PresentCount(); got != 2 {
		t.Fatalf("present count = %d, want 2", got)
	}
}

func TestTSReactiveFamilyObservationalTransparency(t *testing.T) {
	ctx := NewContext()
	eager := ThreadSafeEagerSlotFamily(ctx, []uint32{1, 2, 3}, func(k uint32) uint32 { return k * 2 })
	lazy := ThreadSafeLazySlotFamily(ctx, []uint32{1, 2, 3}, func(k uint32) uint32 { return k * 2 })
	for _, k := range []uint32{1, 2, 3} {
		if eager.Observe(k) != lazy.Observe(k) {
			t.Fatalf("transparency broke at k=%d", k)
		}
	}
}

func TestTSReactiveFamilyPresentSetGrowsMonotonically(t *testing.T) {
	ctx := NewContext()
	fam := ThreadSafeLazySlotFamily(ctx, nil, func(k uint32) uint32 { return k })
	_ = fam.Observe(5)
	_ = fam.Observe(5) // repeat: no growth
	_ = fam.Observe(9)
	if got := fam.PresentCount(); got != 2 {
		t.Fatalf("present count = %d, want 2", got)
	}
	if got := fam.PresentKeys(); !equalU32(got, []uint32{5, 9}) {
		t.Fatalf("present keys = %v, want [5 9]", got)
	}
}

func TestTSReactiveFamilyCellSetOverwrites(t *testing.T) {
	ctx := NewContext()
	fam := ThreadSafeEagerCellFamily(ctx, []uint32{10, 20}, func(uint32) bool { return true })
	if !fam.Observe(20) {
		t.Fatalf("observe(20) = false, want true")
	}
	if !fam.Set(20, false) {
		t.Fatalf("set(20) reported failure on a cell family")
	}
	if fam.Observe(20) {
		t.Fatalf("observe(20) = true after set(false)")
	}
	if got := fam.PresentCount(); got != 2 {
		t.Fatalf("present count = %d, want 2 (no re-order)", got)
	}
}

func TestTSReactiveFamilySetFailsOnSlotFamily(t *testing.T) {
	ctx := NewContext()
	fam := ThreadSafeEagerSlotFamily(ctx, []uint32{1}, func(k uint32) uint32 { return k })
	if fam.Set(1, 99) {
		t.Fatalf("set on a slot family should report false")
	}
}

// Confluence soak: N goroutines materialize an overlapping key space
// concurrently. The present SET and every observed value must be independent of
// interleaving (materialize_present_comm / materialize_observe_comm).
func TestTSReactiveFamilyConcurrentMaterializationIsConfluent(t *testing.T) {
	ctx := NewContext()
	fam := ThreadSafeLazySlotFamily(ctx, nil, func(k uint32) uint32 { return k * 2 })

	const n = 4
	const span uint32 = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		lo := uint32(i) * span
		wg.Add(1)
		go func(lo uint32) {
			defer wg.Done()
			for k := lo; k < lo+span; k++ {
				// Observe twice, out of order, to stress the warm-read path.
				_ = fam.Observe(k)
				_ = fam.Observe((lo + span - 1) - (k - lo))
			}
		}(lo)
	}
	wg.Wait()

	if got := fam.PresentCount(); got != n*int(span) {
		t.Fatalf("present count = %d, want %d", got, n*int(span))
	}
	for k := uint32(0); k < n*span; k++ {
		if !fam.IsPresent(k) {
			t.Fatalf("key %d not present after soak", k)
		}
		if got := fam.Observe(k); got != k*2 {
			t.Fatalf("observe(%d) = %d, want %d", k, got, k*2)
		}
	}
}

func equalU32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
