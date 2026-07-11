package lazily

// ReactiveFamily materialization-mode conformance (#lzmatmode) — replays the
// shared lazily-spec/conformance/materialization/*.json fixtures and mirrors the
// lazily-rs materialization_conformance.rs + reactive_family.rs test suites.
// Each test names the law it exercises (proven in lazily-formal's Materialization
// module).

import (
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixture loading (materialization/*.json compute fixtures)
// ---------------------------------------------------------------------------

type matEntry struct {
	Kind string `json:"kind"`
	Val  int    `json:"val"`
}

type matFixture struct {
	Spec struct {
		Val     map[string]int      `json:"val"`
		Entries map[string]matEntry `json:"entries"`
	} `json:"spec"`
	Reads    []string `json:"reads"`
	Expected struct {
		DefaultMode           string         `json:"default_mode"`
		Observe               map[string]int `json:"observe"`
		EagerPresent          []string       `json:"eager_present"`
		PresentAfterEachRead  []int          `json:"present_after_each_read"`
		LazyPresentAfterReads []string       `json:"lazy_present_after_reads"`
		LazyPresentAtBuild    []string       `json:"lazy_present_at_build"`
	} `json:"expected"`
}

func loadMatFixture(t *testing.T, name string) matFixture {
	t.Helper()
	p := findFixture("materialization/" + name)
	if p == "" {
		t.Skipf("materialization fixture %q not found (lazily-spec checkout absent)", name)
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("reading %s: %v", p, err)
	}
	var f matFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("decoding %s: %v", p, err)
	}
	return f
}

func strSet(keys []string) map[string]struct{} {
	s := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		s[k] = struct{}{}
	}
	return s
}

func assertSameSet(t *testing.T, got []string, want []string, label string) {
	t.Helper()
	g, w := strSet(got), strSet(want)
	if len(g) != len(w) {
		t.Fatalf("%s: set size %d != %d (got %v want %v)", label, len(g), len(w), got, want)
	}
	for k := range w {
		if _, ok := g[k]; !ok {
			t.Fatalf("%s: missing key %q (got %v want %v)", label, k, got, want)
		}
	}
}

func matSortedKeys(m map[string]int) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// ---------------------------------------------------------------------------
// Fixture replays
// ---------------------------------------------------------------------------

// observe_canonical / eager_lazy_observationally_equivalent / default_mode_eager:
// a keyed family of derived cells built eager vs lazy returns identical values;
// eager materializes every key up front, lazy only the read keys.
func TestMaterializationObservationalTransparency(t *testing.T) {
	f := loadMatFixture(t, "observational_transparency.json")
	if f.Expected.DefaultMode != "eager" || DefaultMaterializationMode() != Eager {
		t.Fatalf("default mode must be eager (fixture=%q lib=%v)", f.Expected.DefaultMode, DefaultMaterializationMode())
	}
	keys := matSortedKeys(f.Spec.Val)
	factory := func(k string) int { return f.Spec.Val[k] }

	ctx := NewContext()
	eager := EagerSlotFamily(ctx, keys, factory)
	if eager.Mode() != Eager || eager.EntryKind() != EntryKindSlot {
		t.Fatalf("eager slot family mis-tagged: mode=%v kind=%v", eager.Mode(), eager.EntryKind())
	}
	if eager.PresentCount() != len(keys) {
		t.Fatalf("eager present count %d != %d (eager_materializes_all)", eager.PresentCount(), len(keys))
	}
	assertSameSet(t, eager.PresentKeys(), f.Expected.EagerPresent, "eager_present")

	// Lazy defers every derived slot: nothing present at build.
	lazy := LazySlotFamily(ctx, keys, factory)
	if lazy.PresentCount() != 0 {
		t.Fatalf("lazy present count %d != 0 (lazy_defers_slots)", lazy.PresentCount())
	}

	// Identical observed values under either mode.
	for k, want := range f.Expected.Observe {
		if got := eager.Observe(k); got != want {
			t.Fatalf("eager.Observe(%q)=%d want %d", k, got, want)
		}
		if got := lazy.Observe(k); got != want {
			t.Fatalf("lazy.Observe(%q)=%d want %d", k, got, want)
		}
	}

	// Replay the read sequence on a FRESH lazy family; the present set is
	// exactly the read keys.
	fresh := LazySlotFamily(ctx, keys, factory)
	for _, k := range f.Reads {
		fresh.Observe(k)
	}
	assertSameSet(t, fresh.PresentKeys(), f.Expected.LazyPresentAfterReads, "lazy_present_after_reads")
}

// materialize_present_monotone / lazy_present_subset_eager: the present set only
// grows and is unchanged by a re-read; the lazy present set is a subset of eager.
func TestMaterializationDeferralNotDeallocation(t *testing.T) {
	f := loadMatFixture(t, "deferral_not_deallocation.json")
	keys := matSortedKeys(f.Spec.Val)
	factory := func(k string) int { return f.Spec.Val[k] }

	ctx := NewContext()
	lazy := LazySlotFamily(ctx, keys, factory)

	var sizes []int
	for _, k := range f.Reads {
		lazy.Observe(k)
		sizes = append(sizes, lazy.PresentCount())
	}
	if len(sizes) != len(f.Expected.PresentAfterEachRead) {
		t.Fatalf("present_after_each_read length %d != %d", len(sizes), len(f.Expected.PresentAfterEachRead))
	}
	for i, want := range f.Expected.PresentAfterEachRead {
		if sizes[i] != want {
			t.Fatalf("present_after_each_read[%d]=%d want %d (monotone, re-read is a no-op)", i, sizes[i], want)
		}
	}

	assertSameSet(t, lazy.PresentKeys(), f.Expected.LazyPresentAfterReads, "lazy_present_after_reads")
	// lazy present set ⊆ eager present set.
	eagerSet := strSet(f.Expected.EagerPresent)
	for _, k := range lazy.PresentKeys() {
		if _, ok := eagerSet[k]; !ok {
			t.Fatalf("lazy present key %q not in eager_present (lazy_present_subset_eager)", k)
		}
	}
}

// cell_entries_materialized_in_every_mode / slot_entries_deferred_under_lazy:
// entry kind is orthogonal to mode. A mixed-kind key space is modelled by a cell
// family over the cell entries and a slot family over the slot entries.
func TestMaterializationEntryKindOrthogonalToMode(t *testing.T) {
	f := loadMatFixture(t, "entry_kind_orthogonal_to_mode.json")
	if f.Expected.DefaultMode != "eager" {
		t.Fatalf("default mode must be eager, got %q", f.Expected.DefaultMode)
	}

	var cellKeys, slotKeys []string
	vals := make(map[string]int)
	for key, e := range f.Spec.Entries {
		vals[key] = e.Val
		switch e.Kind {
		case "cell":
			cellKeys = append(cellKeys, key)
		case "slot":
			slotKeys = append(slotKeys, key)
		default:
			t.Fatalf("unknown entry kind %q", e.Kind)
		}
	}
	factory := func(k string) int { return vals[k] }

	ctx := NewContext()

	// Eager build: every entry present (cells + slots).
	eagerCells := EagerCellFamily(ctx, cellKeys, factory)
	eagerSlots := EagerSlotFamily(ctx, slotKeys, factory)
	if eagerCells.EntryKind() != EntryKindCell || eagerSlots.EntryKind() != EntryKindSlot {
		t.Fatalf("entry kinds mis-tagged")
	}
	eagerPresent := append(eagerCells.PresentKeys(), eagerSlots.PresentKeys()...)
	assertSameSet(t, eagerPresent, f.Expected.EagerPresent, "eager_present")

	// Lazy build: cells present at build, slots deferred.
	lazyCells := LazyCellFamily(ctx, cellKeys, factory)
	lazySlots := LazySlotFamily(ctx, slotKeys, factory)
	if lazySlots.PresentCount() != 0 {
		t.Fatalf("slots must defer at build, present=%d", lazySlots.PresentCount())
	}
	assertSameSet(t, lazyCells.PresentKeys(), f.Expected.LazyPresentAtBuild, "lazy_present_at_build")

	// Reads (slot pulls) grow only the slot present set.
	slotSet := strSet(slotKeys)
	for _, k := range f.Reads {
		if _, ok := slotSet[k]; ok {
			lazySlots.Observe(k)
		} else {
			lazyCells.Observe(k)
		}
	}
	lazyAfter := append(lazyCells.PresentKeys(), lazySlots.PresentKeys()...)
	assertSameSet(t, lazyAfter, f.Expected.LazyPresentAfterReads, "lazy_present_after_reads")

	// Observational transparency across kinds.
	cellSet := strSet(cellKeys)
	for k, want := range f.Expected.Observe {
		if _, isCell := cellSet[k]; isCell {
			if got := eagerCells.Observe(k); got != want {
				t.Fatalf("eagerCells.Observe(%q)=%d want %d", k, got, want)
			}
			if got := lazyCells.Observe(k); got != want {
				t.Fatalf("lazyCells.Observe(%q)=%d want %d", k, got, want)
			}
		} else {
			if got := eagerSlots.Observe(k); got != want {
				t.Fatalf("eagerSlots.Observe(%q)=%d want %d", k, got, want)
			}
			if got := lazySlots.Observe(k); got != want {
				t.Fatalf("lazySlots.Observe(%q)=%d want %d", k, got, want)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Unit tests mirroring lazily-rs/src/reactive_family.rs
// ---------------------------------------------------------------------------

func TestReactiveFamilyDefaultModeIsEager(t *testing.T) {
	var zero MaterializationMode
	if zero != Eager || DefaultMaterializationMode() != Eager {
		t.Fatalf("default materialization mode must be eager")
	}
}

func TestReactiveFamilyEagerMaterializesAll(t *testing.T) {
	ctx := NewContext()
	fam := EagerSlotFamily(ctx, []int{0, 1, 2, 5, 9}, func(k int) int { return k * 3 })
	if fam.PresentCount() != 5 {
		t.Fatalf("eager present count %d != 5", fam.PresentCount())
	}
	for _, k := range []int{0, 1, 2, 5, 9} {
		if !fam.IsPresent(k) {
			t.Fatalf("key %d not present under eager", k)
		}
	}
}

func TestReactiveFamilyLazyDefersSlots(t *testing.T) {
	ctx := NewContext()
	fam := LazySlotFamily(ctx, []int{0, 1, 2, 5, 9}, func(k int) int { return k * 3 })
	if fam.PresentCount() != 0 || fam.IsPresent(5) {
		t.Fatalf("lazy must defer slots; present=%d", fam.PresentCount())
	}
	if got := fam.Observe(5); got != 15 {
		t.Fatalf("Observe(5)=%d want 15", got)
	}
	if !fam.IsPresent(5) {
		t.Fatalf("key 5 must be present after read")
	}
	assertSameSet(t, keysToStr(fam.PresentKeys()), []string{"5"}, "present after single read")
}

func TestReactiveFamilyPresentSetMonotone(t *testing.T) {
	ctx := NewContext()
	fam := LazySlotFamily(ctx, []int{1, 2, 3, 4, 5}, func(k int) int { return k * 2 })
	var sizes []int
	for _, k := range []int{2, 4, 2, 5} {
		fam.Observe(k)
		sizes = append(sizes, fam.PresentCount())
	}
	want := []int{1, 2, 2, 3}
	for i := range want {
		if sizes[i] != want[i] {
			t.Fatalf("sizes[%d]=%d want %d", i, sizes[i], want[i])
		}
	}
}

func TestReactiveFamilyCellMaterializedInEveryMode(t *testing.T) {
	ctx := NewContext()
	keys := []string{"a", "b", "c"}
	for _, lazyMode := range []bool{false, true} {
		var fam *ReactiveFamily[string, int]
		if lazyMode {
			fam = LazyCellFamily(ctx, keys, func(string) int { return 0 })
		} else {
			fam = EagerCellFamily(ctx, keys, func(string) int { return 0 })
		}
		if fam.EntryKind() != EntryKindCell {
			t.Fatalf("expected cell entry kind")
		}
		if fam.PresentCount() != 3 {
			t.Fatalf("cells must be present at build in every mode; present=%d (lazy=%v)", fam.PresentCount(), lazyMode)
		}
	}
}

func TestReactiveFamilyCellEntriesAreWritableInputs(t *testing.T) {
	ctx := NewContext()
	fam := EagerCellFamily(ctx, []int{7}, func(k int) int { return k })
	cell, ok := fam.GetCell(7)
	if !ok || cell.Get() != 7 {
		t.Fatalf("GetCell(7) ok=%v val=%d want 7", ok, cell.Get())
	}
	if !fam.Set(7, 100) {
		t.Fatalf("Set on a cell family must succeed")
	}
	if got := fam.Observe(7); got != 100 {
		t.Fatalf("Observe(7)=%d want 100 after Set", got)
	}
	// A slot family rejects Set / GetCell.
	slotFam := EagerSlotFamily(ctx, []int{1}, func(k int) int { return k })
	if _, ok := slotFam.GetCell(1); ok {
		t.Fatalf("GetCell must fail on a slot family")
	}
	if slotFam.Set(1, 2) {
		t.Fatalf("Set must fail on a slot family")
	}
}

func keysToStr(ks []int) []string {
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = strconv.Itoa(k)
	}
	return out
}
