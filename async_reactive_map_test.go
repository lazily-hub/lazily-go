package lazily

import "testing"

// Tests mirror lazily-rs async_reactive_family.rs, naming the lazily-formal
// AsyncMaterialization theorems each assertion rests on. The unified model
// (#reactivemap, async): AsyncComputedMap (derived slots — minted pending, driven to
// resolution; MaterializeAll pre-mint; no Set) and AsyncSourceMap (input cells —
// always resolved; Set).

func TestAsyncSourceMapResolvesImmediately(t *testing.T) {
	fam := NewAsyncSourceMap[uint32, bool]()
	if fam.EntryKind() != EntryKindCell {
		t.Fatalf("kind = %v, want cell", fam.EntryKind())
	}
	for _, k := range []uint32{1, 2, 3} {
		fam.Set(k, true)
	}
	if got := fam.PresentCount(); got != 3 {
		t.Fatalf("present count = %d, want 3", got)
	}
	// cell_resolved_at_build: observe returns a value immediately.
	if v, ok := fam.Observe(2, nil); !ok || v != true {
		t.Fatalf("observe(2) = (%v,%v), want (true,true)", v, ok)
	}
	if got := fam.PresentKeys(); !equalU32(got, []uint32{1, 2, 3}) {
		t.Fatalf("present keys = %v, want [1 2 3]", got)
	}
}

func TestAsyncComputedMapLazyDefersThenResolves(t *testing.T) {
	fam := NewAsyncComputedMap[uint32, uint32]()
	factory := func(k uint32) uint32 { return k * 10 }
	if fam.EntryKind() != EntryKindSlot {
		t.Fatalf("kind = %v, want slot", fam.EntryKind())
	}
	if got := fam.PresentCount(); got != 0 {
		t.Fatalf("present count = %d, want 0", got)
	}
	// Observe allocates the entry (present) but it is pending → not ok.
	if v, ok := fam.Observe(4, factory); ok {
		t.Fatalf("observe(4) = (%v,true), want pending", v)
	}
	if !fam.IsPresent(4) {
		t.Fatalf("key 4 should be present after observe")
	}
	if fam.IsResolved(4) {
		t.Fatalf("key 4 should be pending, not resolved")
	}
	if got := fam.PresentCount(); got != 1 {
		t.Fatalf("present count = %d, want 1", got)
	}
	// Drive resolves it → canonical value.
	if got := fam.Drive(4, factory); got != 40 {
		t.Fatalf("drive(4) = %d, want 40", got)
	}
	if !fam.IsResolved(4) {
		t.Fatalf("key 4 should be resolved after drive")
	}
	if v, ok := fam.Observe(4, factory); !ok || v != 40 {
		t.Fatalf("observe(4) = (%d,%v), want (40,true)", v, ok)
	}
}

func TestAsyncComputedMapPendingReadIsNone(t *testing.T) {
	fam := NewAsyncComputedMap[uint32, uint32]()
	factory := func(k uint32) uint32 { return k * 2 }
	fam.MaterializeAll([]uint32{5, 6}, factory)
	// Eager allocates the slots (present) but they start pending.
	if got := fam.PresentCount(); got != 2 {
		t.Fatalf("present count = %d, want 2", got)
	}
	if _, ok := fam.Observe(5, factory); ok {
		t.Fatalf("observe(5) should be pending")
	}
	// Driving resolves; eventual transparency.
	if got := fam.Drive(5, factory); got != 10 {
		t.Fatalf("drive(5) = %d, want 10", got)
	}
	if v, ok := fam.Observe(5, factory); !ok || v != 10 {
		t.Fatalf("observe(5) = (%d,%v), want (10,true)", v, ok)
	}
}

func TestAsyncComputedMapEventualTransparency(t *testing.T) {
	factory := func(k uint32) uint32 { return k * 2 }
	eager := NewAsyncComputedMap[uint32, uint32]()
	eager.MaterializeAll([]uint32{1, 2, 3}, factory)
	lazy := NewAsyncComputedMap[uint32, uint32]()
	for _, k := range []uint32{1, 2, 3} {
		if eager.Drive(k, factory) != lazy.Drive(k, factory) {
			t.Fatalf("eventual transparency broke at k=%d", k)
		}
	}
}

func TestAsyncComputedMapPresentSetGrowsMonotonically(t *testing.T) {
	fam := NewAsyncComputedMap[uint32, uint32]()
	id := func(k uint32) uint32 { return k }
	_ = fam.Drive(5, id)
	_ = fam.Drive(5, id) // repeat: no growth
	_ = fam.Drive(9, id)
	if got := fam.PresentCount(); got != 2 {
		t.Fatalf("present count = %d, want 2", got)
	}
	if got := fam.PresentKeys(); !equalU32(got, []uint32{5, 9}) {
		t.Fatalf("present keys = %v, want [5 9]", got)
	}
}

func TestAsyncSourceMapReactsToSet(t *testing.T) {
	fam := NewAsyncSourceMap[uint32, bool]()
	fam.Set(10, true)
	fam.Set(20, true)
	if v, ok := fam.Observe(20, nil); !ok || v != true {
		t.Fatalf("observe(20) = (%v,%v), want (true,true)", v, ok)
	}
	fam.Set(20, false)
	if v, ok := fam.Observe(20, nil); !ok || v != false {
		t.Fatalf("observe(20) = (%v,%v), want (false,true)", v, ok)
	}
}

func TestAsyncComputedMapResolveOneNeverDisturbsAnother(t *testing.T) {
	fam := NewAsyncComputedMap[uint32, uint32]()
	factory := func(k uint32) uint32 { return k * 2 }
	fam.MaterializeAll([]uint32{1, 2}, factory)
	if got := fam.Drive(1, factory); got != 2 {
		t.Fatalf("drive(1) = %d, want 2", got)
	}
	// Driving 2 leaves 1's resolved value intact (resolve_preserves_observe).
	if got := fam.Drive(2, factory); got != 4 {
		t.Fatalf("drive(2) = %d, want 4", got)
	}
	if v, ok := fam.Observe(1, factory); !ok || v != 2 {
		t.Fatalf("observe(1) = (%d,%v), want (2,true)", v, ok)
	}
}
