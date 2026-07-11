package lazily

import "testing"

// Tests mirror lazily-zig async_reactive_family.zig / lazily-rs
// async_reactive_family.rs, naming the lazily-formal AsyncMaterialization
// theorems each assertion rests on.

func TestAsyncReactiveFamilyEagerCellResolvesImmediately(t *testing.T) {
	ctx := NewContext()
	fam := AsyncEagerCellFamily(ctx, []uint32{1, 2, 3}, func(uint32) bool { return true })

	if fam.EntryKind() != EntryKindCell {
		t.Fatalf("kind = %v, want cell", fam.EntryKind())
	}
	if got := fam.PresentCount(); got != 3 {
		t.Fatalf("present count = %d, want 3", got)
	}
	// cell_resolved_at_build: observe returns a value immediately.
	if v, ok := fam.Observe(2); !ok || v != true {
		t.Fatalf("observe(2) = (%v,%v), want (true,true)", v, ok)
	}
	if got := fam.PresentKeys(); !equalU32(got, []uint32{1, 2, 3}) {
		t.Fatalf("present keys = %v, want [1 2 3]", got)
	}
}

func TestAsyncReactiveFamilyLazySlotDefersThenResolves(t *testing.T) {
	ctx := NewContext()
	fam := AsyncLazySlotFamily(ctx, nil, func(k uint32) uint32 { return k * 10 })

	if fam.Mode() != Lazy {
		t.Fatalf("mode = %v, want lazy", fam.Mode())
	}
	if got := fam.PresentCount(); got != 0 {
		t.Fatalf("present count = %d, want 0", got)
	}
	// observe allocates the entry (present) but it is pending → not ok.
	if v, ok := fam.Observe(4); ok {
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
	// drive resolves it → canonical value.
	if got := fam.Drive(4); got != 40 {
		t.Fatalf("drive(4) = %d, want 40", got)
	}
	if !fam.IsResolved(4) {
		t.Fatalf("key 4 should be resolved after drive")
	}
	if v, ok := fam.Observe(4); !ok || v != 40 {
		t.Fatalf("observe(4) = (%d,%v), want (40,true)", v, ok)
	}
}

func TestAsyncReactiveFamilyPendingReadIsNone(t *testing.T) {
	ctx := NewContext()
	fam := AsyncEagerSlotFamily(ctx, []uint32{5, 6}, func(k uint32) uint32 { return k * 2 })
	// Eager allocates the slots (present) but they start pending.
	if got := fam.PresentCount(); got != 2 {
		t.Fatalf("present count = %d, want 2", got)
	}
	if _, ok := fam.Observe(5); ok {
		t.Fatalf("observe(5) should be pending")
	}
	// Driving resolves; eventual transparency.
	if got := fam.Drive(5); got != 10 {
		t.Fatalf("drive(5) = %d, want 10", got)
	}
	if v, ok := fam.Observe(5); !ok || v != 10 {
		t.Fatalf("observe(5) = (%d,%v), want (10,true)", v, ok)
	}
}

func TestAsyncReactiveFamilyEventualTransparency(t *testing.T) {
	ctxE := NewContext()
	eager := AsyncEagerSlotFamily(ctxE, []uint32{1, 2, 3}, func(k uint32) uint32 { return k * 2 })
	ctxL := NewContext()
	lazy := AsyncLazySlotFamily(ctxL, []uint32{1, 2, 3}, func(k uint32) uint32 { return k * 2 })
	for _, k := range []uint32{1, 2, 3} {
		if eager.Drive(k) != lazy.Drive(k) {
			t.Fatalf("eventual transparency broke at k=%d", k)
		}
	}
}

func TestAsyncReactiveFamilyPresentSetGrowsMonotonically(t *testing.T) {
	ctx := NewContext()
	fam := AsyncLazySlotFamily(ctx, nil, func(k uint32) uint32 { return k })
	_ = fam.Drive(5)
	_ = fam.Drive(5) // repeat: no growth
	_ = fam.Drive(9)
	if got := fam.PresentCount(); got != 2 {
		t.Fatalf("present count = %d, want 2", got)
	}
	if got := fam.PresentKeys(); !equalU32(got, []uint32{5, 9}) {
		t.Fatalf("present keys = %v, want [5 9]", got)
	}
}

func TestAsyncReactiveFamilyCellReactsToSet(t *testing.T) {
	ctx := NewContext()
	fam := AsyncEagerCellFamily(ctx, []uint32{10, 20}, func(uint32) bool { return true })
	if v, ok := fam.Observe(20); !ok || v != true {
		t.Fatalf("observe(20) = (%v,%v), want (true,true)", v, ok)
	}
	if !fam.Set(20, false) {
		t.Fatalf("set(20) reported failure on a cell family")
	}
	if v, ok := fam.Observe(20); !ok || v != false {
		t.Fatalf("observe(20) = (%v,%v), want (false,true)", v, ok)
	}
}

func TestAsyncReactiveFamilyResolveOneNeverDisturbsAnother(t *testing.T) {
	ctx := NewContext()
	fam := AsyncEagerSlotFamily(ctx, []uint32{1, 2}, func(k uint32) uint32 { return k * 2 })
	if got := fam.Drive(1); got != 2 {
		t.Fatalf("drive(1) = %d, want 2", got)
	}
	// Driving 2 leaves 1's resolved value intact (resolve_preserves_observe).
	if got := fam.Drive(2); got != 4 {
		t.Fatalf("drive(2) = %d, want 4", got)
	}
	if v, ok := fam.Observe(1); !ok || v != 2 {
		t.Fatalf("observe(1) = (%d,%v), want (2,true)", v, ok)
	}
}

func TestAsyncReactiveFamilySetFailsOnSlotFamily(t *testing.T) {
	ctx := NewContext()
	fam := AsyncEagerSlotFamily(ctx, []uint32{1}, func(k uint32) uint32 { return k })
	if fam.Set(1, 99) {
		t.Fatalf("set on a slot family should report false")
	}
}
