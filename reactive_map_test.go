package lazily

// ReactiveMap materialization conformance (#reactivemap) — replays the shared
// lazily-spec/conformance/materialization/*.json fixtures (model "SlotMap") and
// mirrors the lazily-rs materialization_conformance.rs + cell_family.rs test
// suites. Each test names the law it exercises (proven in lazily-formal's
// Materialization module).
//
// The unified model: SlotMap over derived slots — eager materialization is a
// pre-mint loop (MaterializeAll); lazy is mint-on-access (GetOrInsertWith). There
// is no eager/lazy mode flag. CellMap over input cells always materializes an
// entry on mint (Entry / Set).

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
	Model string `json:"model"`
	Spec  struct {
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
// a keyed SlotMap of derived slots built eager (MaterializeAll pre-mint) vs lazy
// (GetOrInsertWith mint-on-access) returns identical values; eager materializes
// every key up front, lazy only the read keys.
func TestMaterializationObservationalTransparency(t *testing.T) {
	f := loadMatFixture(t, "observational_transparency.json")
	if f.Expected.DefaultMode != "eager" {
		t.Fatalf("default strategy must be eager (fixture=%q)", f.Expected.DefaultMode)
	}
	if f.Model != "SlotMap" {
		t.Fatalf("fixture model = %q, want SlotMap", f.Model)
	}
	keys := matSortedKeys(f.Spec.Val)
	factory := func(k string) int { return f.Spec.Val[k] }

	ctx := NewContext()
	eager := NewSlotMap[string, int](ctx)
	eager.MaterializeAll(keys, factory)
	if eager.EntryKind() != EntryKindSlot {
		t.Fatalf("slot map mis-tagged: kind=%v", eager.EntryKind())
	}
	if eager.PresentCount() != len(keys) {
		t.Fatalf("eager present count %d != %d (eager_materializes_all)", eager.PresentCount(), len(keys))
	}
	assertSameSet(t, eager.PresentKeys(), f.Expected.EagerPresent, "eager_present")

	// Lazy defers every derived slot: nothing present at build.
	lazy := NewSlotMap[string, int](ctx)
	if lazy.PresentCount() != 0 {
		t.Fatalf("lazy present count %d != 0 (lazy_defers_slots)", lazy.PresentCount())
	}

	// Identical observed values under either strategy.
	for k, want := range f.Expected.Observe {
		if got, _ := eager.Observe(k); got != want {
			t.Fatalf("eager.Observe(%q)=%d want %d", k, got, want)
		}
		if got := lazy.GetOrInsertWith(k, factory); got != want {
			t.Fatalf("lazy.GetOrInsertWith(%q)=%d want %d", k, got, want)
		}
	}

	// Replay the read sequence on a FRESH lazy map; the present set is exactly
	// the read keys.
	fresh := NewSlotMap[string, int](ctx)
	for _, k := range f.Reads {
		fresh.GetOrInsertWith(k, factory)
	}
	assertSameSet(t, fresh.PresentKeys(), f.Expected.LazyPresentAfterReads, "lazy_present_after_reads")
}

// materialize_present_monotone / lazy_present_subset_eager: the present set only
// grows and is unchanged by a re-read; the lazy present set is a subset of eager.
func TestMaterializationDeferralNotDeallocation(t *testing.T) {
	f := loadMatFixture(t, "deferral_not_deallocation.json")
	factory := func(k string) int { return f.Spec.Val[k] }

	ctx := NewContext()
	lazy := NewSlotMap[string, int](ctx)

	var sizes []int
	for _, k := range f.Reads {
		lazy.GetOrInsertWith(k, factory)
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
// entry kind is orthogonal to strategy. A mixed-kind key space is modelled by a
// CellMap over the cell entries and a SlotMap over the slot entries.
func TestMaterializationEntryKindOrthogonalToStrategy(t *testing.T) {
	f := loadMatFixture(t, "entry_kind_orthogonal_to_mode.json")
	if f.Expected.DefaultMode != "eager" {
		t.Fatalf("default strategy must be eager, got %q", f.Expected.DefaultMode)
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

	// Eager build: cells pre-minted (always materialized) + slots pre-minted.
	eagerCells := NewCellMap[string, int](ctx)
	for _, k := range cellKeys {
		eagerCells.Entry(k, vals[k])
	}
	eagerSlots := NewSlotMap[string, int](ctx)
	eagerSlots.MaterializeAll(slotKeys, factory)
	if eagerCells.EntryKind() != EntryKindCell || eagerSlots.EntryKind() != EntryKindSlot {
		t.Fatalf("entry kinds mis-tagged")
	}
	eagerPresent := append(eagerCells.PresentKeys(), eagerSlots.PresentKeys()...)
	assertSameSet(t, eagerPresent, f.Expected.EagerPresent, "eager_present")

	// Lazy build: cells present at build (always materialized), slots deferred.
	lazyCells := NewCellMap[string, int](ctx)
	for _, k := range cellKeys {
		lazyCells.Entry(k, vals[k])
	}
	lazySlots := NewSlotMap[string, int](ctx)
	if lazySlots.PresentCount() != 0 {
		t.Fatalf("slots must defer at build, present=%d", lazySlots.PresentCount())
	}
	assertSameSet(t, lazyCells.PresentKeys(), f.Expected.LazyPresentAtBuild, "lazy_present_at_build")

	// Reads (slot pulls) grow only the slot present set.
	slotSet := strSet(slotKeys)
	for _, k := range f.Reads {
		if _, ok := slotSet[k]; ok {
			lazySlots.GetOrInsertWith(k, factory)
		} else {
			lazyCells.Entry(k, vals[k])
		}
	}
	lazyAfter := append(lazyCells.PresentKeys(), lazySlots.PresentKeys()...)
	assertSameSet(t, lazyAfter, f.Expected.LazyPresentAfterReads, "lazy_present_after_reads")

	// Observational transparency across kinds.
	cellSet := strSet(cellKeys)
	for k, want := range f.Expected.Observe {
		if _, isCell := cellSet[k]; isCell {
			if got, _ := eagerCells.Read(k); got != want {
				t.Fatalf("eagerCells.Read(%q)=%d want %d", k, got, want)
			}
			if got, _ := lazyCells.Read(k); got != want {
				t.Fatalf("lazyCells.Read(%q)=%d want %d", k, got, want)
			}
		} else {
			if got, _ := eagerSlots.Observe(k); got != want {
				t.Fatalf("eagerSlots.Observe(%q)=%d want %d", k, got, want)
			}
			if got := lazySlots.GetOrInsertWith(k, factory); got != want {
				t.Fatalf("lazySlots.GetOrInsertWith(%q)=%d want %d", k, got, want)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Unit tests mirroring lazily-rs/src/cell_family.rs
// ---------------------------------------------------------------------------

func TestSlotMapEagerMaterializesAll(t *testing.T) {
	ctx := NewContext()
	m := NewSlotMap[int, int](ctx)
	m.MaterializeAll([]int{0, 1, 2, 5, 9}, func(k int) int { return k * 3 })
	if m.PresentCount() != 5 {
		t.Fatalf("eager present count %d != 5", m.PresentCount())
	}
	for _, k := range []int{0, 1, 2, 5, 9} {
		if !m.IsPresent(k) {
			t.Fatalf("key %d not present after MaterializeAll", k)
		}
	}
}

func TestSlotMapLazyDefersThenMintsOnAccess(t *testing.T) {
	ctx := NewContext()
	m := NewSlotMap[int, int](ctx)
	if m.PresentCount() != 0 || m.IsPresent(5) {
		t.Fatalf("lazy must defer slots; present=%d", m.PresentCount())
	}
	if got := m.GetOrInsertWith(5, func(k int) int { return k * 3 }); got != 15 {
		t.Fatalf("GetOrInsertWith(5)=%d want 15", got)
	}
	if !m.IsPresent(5) {
		t.Fatalf("key 5 must be present after mint-on-access")
	}
	// Same key -> same slot; factory not re-run.
	if got := m.GetOrInsertWith(5, func(k int) int { return k * 999 }); got != 15 {
		t.Fatalf("GetOrInsertWith(5) re-run=%d want cached 15", got)
	}
	assertSameSet(t, keysToStr(m.PresentKeys()), []string{"5"}, "present after single read")
}

func TestSlotMapPresentSetMonotone(t *testing.T) {
	ctx := NewContext()
	m := NewSlotMap[int, int](ctx)
	factory := func(k int) int { return k * 2 }
	var sizes []int
	for _, k := range []int{2, 4, 2, 5} {
		m.GetOrInsertWith(k, factory)
		sizes = append(sizes, m.PresentCount())
	}
	want := []int{1, 2, 2, 3}
	for i := range want {
		if sizes[i] != want[i] {
			t.Fatalf("sizes[%d]=%d want %d", i, sizes[i], want[i])
		}
	}
}

func TestCellMapEntryCachesOneCellPerKey(t *testing.T) {
	ctx := NewContext()
	m := NewCellMap[string, int](ctx)
	a1 := m.Entry("a", 1)
	a2 := m.Entry("a", 999)
	// Same key -> same cell; the second default is ignored.
	if a1 != a2 {
		t.Fatalf("Entry must cache one cell per key")
	}
	if a1.Get() != 1 {
		t.Fatalf("Entry(a)=%d want 1", a1.Get())
	}
	if m.LenUntracked() != 1 {
		t.Fatalf("len untracked %d want 1", m.LenUntracked())
	}
}

func TestCellMapSetIsCellOnly(t *testing.T) {
	ctx := NewContext()
	m := NewCellMap[int, int](ctx)
	m.Set(7, 42)
	if got, ok := m.Read(7); !ok || got != 42 {
		t.Fatalf("Read(7)=(%d,%v) want (42,true)", got, ok)
	}
	m.Set(7, 100)
	if got, _ := m.Read(7); got != 100 {
		t.Fatalf("Read(7)=%d want 100 after Set", got)
	}
	if m.EntryKind() != EntryKindCell {
		t.Fatalf("CellMap kind = %v, want cell", m.EntryKind())
	}
}

func keysToStr(ks []int) []string {
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = strconv.Itoa(k)
	}
	return out
}
