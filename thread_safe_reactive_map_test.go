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

// The tracked reads race too, and nothing was driving them concurrently.
//
// The confluence soak above only exercises GetOrInsertWith, so when every graph
// read moved under the context lock it covered exactly one of the five call
// sites. Keys / Len / ContainsKey read the order and membership signals, and a
// signal read mutates the dependency edge set — they were racing on the same
// Context fields and no test could see it.
//
// Readers and a mutator run together on purpose: the mutator drives real
// invalidation, so the readers hit refresh/recompute rather than a warm cache,
// which is the path that writes computeGen.
func TestTSComputedMapConcurrentTrackedReadsAreRaceFree(t *testing.T) {
	ts := NewThreadSafeContext()
	fam := NewThreadSafeComputedMap[uint32, uint32](ts)

	const keys uint32 = 40
	for k := uint32(0); k < keys; k++ {
		_ = fam.GetOrInsertWith(ts.Context(), k, doubleU32)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(seed uint32) {
			defer wg.Done()
			for r := uint32(0); r < 100; r++ {
				k := (seed*7 + r) % keys
				_ = fam.Keys(ts.Context())
				_ = fam.Len(ts.Context())
				_ = fam.ContainsKey(ts.Context(), k)
				_, _ = fam.Observe(ts.Context(), k)
				_ = fam.GetOrInsertWith(ts.Context(), keys+k, doubleU32)
			}
		}(uint32(i))
	}
	// The mutator: reordering bumps the order signal, so Keys readers really
	// recompute instead of returning a cached list.
	//
	// The destination index must not be the key's current one. A first draft used
	// MoveTo(k, k), which is a no-op on an insertion-ordered map — the order
	// signal never moved, and unlocking the Keys read then raced with nothing.
	// The stride keeps every move a real reorder.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 0; r < 100; r++ {
			fam.MoveTo(uint32(r)%keys, (r*13+1)%int(keys))
		}
	}()
	wg.Wait()

	// Confluence: the mutator only reordered and the readers only minted the
	// upper half, so the final present set is exact regardless of interleaving.
	if got := fam.PresentCount(); got != int(keys*2) {
		t.Fatalf("present count = %d, want %d", got, keys*2)
	}
	if got := fam.Len(ts.Context()); got != int(keys*2) {
		t.Fatalf("reactive Len = %d, want %d", got, keys*2)
	}
	if got := len(fam.Keys(ts.Context())); got != int(keys*2) {
		t.Fatalf("Keys length = %d, want %d", got, keys*2)
	}
	for k := uint32(0); k < keys*2; k++ {
		if got, ok := fam.Observe(ts.Context(), k); !ok || got != k*2 {
			t.Fatalf("observe(%d) = (%d, %v), want (%d, true)", k, got, ok, k*2)
		}
	}
}

// Observe's lock is only load-bearing when an entry node is concurrently
// WRITTEN, and the computed-map soak never dirties one: its entries compute once
// and keep their value forever, so an unlocked read of a clean cached node races
// with nothing. Unlocking Observe was invisible until this test existed.
//
// A source map is the shape that exposes it. Set invalidates the entry under the
// context lock while readers observe the same key, so the reader touches
// value/cached exactly while the writer is rewriting them.
func TestTSSourceMapConcurrentSetAndObserveAreRaceFree(t *testing.T) {
	ts := NewThreadSafeContext()
	fam := NewThreadSafeSourceMap[uint32, uint32](ts)

	// A small hot key set on purpose. Readers and the writer must contend on the
	// SAME entry node for the read to overlap the write — spreading them over a
	// wide key space makes the collision rare enough that the detector misses it,
	// which is how an unlocked Observe first passed this test.
	const keys uint32 = 4
	for k := uint32(0); k < keys; k++ {
		fam.Set(k, k)
	}

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(uint32) {
			defer wg.Done()
			for r := uint32(0); r < 500; r++ {
				k := r % keys
				_, _ = fam.Observe(ts.Context(), k)
				_ = fam.GetOrInsertWith(ts.Context(), k, doubleU32)
			}
		}(uint32(i))
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := uint32(0); r < 500; r++ {
			fam.Set(r%keys, r)
		}
	}()
	wg.Wait()

	// Quiesce to a known state, then prove the map still reads correctly — a
	// race-free run that lost writes would not be a pass.
	for k := uint32(0); k < keys; k++ {
		fam.Set(k, k*2)
	}
	if got := fam.Len(ts.Context()); got != int(keys) {
		t.Fatalf("reactive Len = %d, want %d", got, keys)
	}
	for k := uint32(0); k < keys; k++ {
		if got, ok := fam.Observe(ts.Context(), k); !ok || got != k*2 {
			t.Fatalf("observe(%d) = (%d, %v), want (%d, true)", k, got, ok, k*2)
		}
	}
}
