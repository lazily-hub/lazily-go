package lazily

import (
	"sync"
	"testing"
)

// Tests mirror lazily-rs thread_safe_reactive_family.rs, naming the lazily-formal
// Materialization theorems (incl. the confluence pair) each assertion rests on.
// The unified model (#reactivemap): ThreadSafeComputedMap (derived slots, lazy
// GetOrInsertWith mint-on-access + eager MaterializeAll pre-mint, no Set) and
// ThreadSafeSourceMap (input cells, Set). No eager/lazy mode flag.

func doubleU32(k uint32) uint32 { return k * 2 }

func TestTSSourceMapEntryKindAndSet(t *testing.T) {
	ts := NewThreadSafeContext()
	fam := NewThreadSafeSourceMap[uint32, bool](ts)
	if fam.EntryKind() != EntryKindSource {
		t.Fatalf("kind = %v, want cell", fam.EntryKind())
	}
	for _, k := range []uint32{1, 2, 3} {
		fam.Set(k, true)
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

func TestTSComputedMapEagerMaterializesAllAtBuild(t *testing.T) {
	ts := NewThreadSafeContext()
	fam := NewThreadSafeComputedMap[uint32, uint32](ts)
	fam.MaterializeAll([]uint32{1, 2, 3}, doubleU32)
	if fam.EntryKind() != EntryKindComputed {
		t.Fatalf("kind = %v, want slot", fam.EntryKind())
	}
	if got := fam.PresentCount(); got != 3 {
		t.Fatalf("present count = %d, want 3", got)
	}
	if got := fam.PresentKeys(); !equalU32(got, []uint32{1, 2, 3}) {
		t.Fatalf("present keys = %v, want [1 2 3]", got)
	}
}

func TestTSComputedMapLazyDefersUntilRead(t *testing.T) {
	ts := NewThreadSafeContext()
	fam := NewThreadSafeComputedMap[uint32, uint32](ts)
	if got := fam.PresentCount(); got != 0 {
		t.Fatalf("present count = %d, want 0", got)
	}
	if fam.IsPresent(2) {
		t.Fatalf("key 2 should not be present before read")
	}
	if got := fam.GetOrInsertWith(ts.Context(), 2, func(k uint32) uint32 { return k * 10 }); got != 20 {
		t.Fatalf("GetOrInsertWith(2) = %d, want 20", got)
	}
	if !fam.IsPresent(2) {
		t.Fatalf("key 2 should be present after read")
	}
	if got := fam.PresentCount(); got != 1 {
		t.Fatalf("present count = %d, want 1", got)
	}
}

func TestTSComputedMapObservationalTransparency(t *testing.T) {
	ts := NewThreadSafeContext()
	eager := NewThreadSafeComputedMap[uint32, uint32](ts)
	eager.MaterializeAll([]uint32{1, 2, 3}, doubleU32)
	lazy := NewThreadSafeComputedMap[uint32, uint32](ts)
	for _, k := range []uint32{1, 2, 3} {
		ev, _ := eager.Observe(ts.Context(), k)
		lv := lazy.GetOrInsertWith(ts.Context(), k, doubleU32)
		if ev != lv {
			t.Fatalf("transparency broke at k=%d (eager=%d lazy=%d)", k, ev, lv)
		}
	}
}

func TestTSComputedMapPresentSetGrowsMonotonically(t *testing.T) {
	ts := NewThreadSafeContext()
	fam := NewThreadSafeComputedMap[uint32, uint32](ts)
	id := func(k uint32) uint32 { return k }
	_ = fam.GetOrInsertWith(ts.Context(), 5, id)
	_ = fam.GetOrInsertWith(ts.Context(), 5, id) // repeat: no growth
	_ = fam.GetOrInsertWith(ts.Context(), 9, id)
	if got := fam.PresentCount(); got != 2 {
		t.Fatalf("present count = %d, want 2", got)
	}
	if got := fam.PresentKeys(); !equalU32(got, []uint32{5, 9}) {
		t.Fatalf("present keys = %v, want [5 9]", got)
	}
}

func TestTSSourceMapSetOverwrites(t *testing.T) {
	ts := NewThreadSafeContext()
	fam := NewThreadSafeSourceMap[uint32, bool](ts)
	fam.Set(10, true)
	fam.Set(20, true)
	if v, ok := fam.Observe(ts.Context(), 20); !ok || !v {
		t.Fatalf("observe(20) = (%v,%v), want (true,true)", v, ok)
	}
	fam.Set(20, false)
	if v, _ := fam.Observe(ts.Context(), 20); v {
		t.Fatalf("observe(20) = true after set(false)")
	}
	if got := fam.PresentCount(); got != 2 {
		t.Fatalf("present count = %d, want 2 (no re-order)", got)
	}
}

// Confluence soak: N goroutines materialize an overlapping key space
// concurrently. The present SET and every observed value must be independent of
// interleaving (materialize_present_comm / materialize_observe_comm).
func TestTSComputedMapConcurrentMaterializationIsConfluent(t *testing.T) {
	ts := NewThreadSafeContext()
	fam := NewThreadSafeComputedMap[uint32, uint32](ts)

	const n = 4
	const span uint32 = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		lo := uint32(i) * span
		wg.Add(1)
		go func(lo uint32) {
			defer wg.Done()
			for k := lo; k < lo+span; k++ {
				// Read twice, out of order, to stress the warm-read path.
				_ = fam.GetOrInsertWith(ts.Context(), k, doubleU32)
				_ = fam.GetOrInsertWith(ts.Context(), (lo+span-1)-(k-lo), doubleU32)
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
		if got, _ := fam.Observe(ts.Context(), k); got != k*2 {
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
