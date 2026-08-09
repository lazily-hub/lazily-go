package lazily

// ReactiveMap materialization conformance (#reactivemap) — replays the shared
// lazily-spec/conformance/materialization/*.json fixtures (model "ComputedMap") and
// mirrors the lazily-rs materialization_conformance.rs + cell_family.rs test
// suites. Each test names the law it exercises (proven in lazily-formal's
// Materialization module).
//
// The unified model: ComputedMap over derived slots — eager materialization is a
// pre-mint loop (MaterializeAll); lazy is mint-on-access (GetOrInsertWith). There
// is no eager/lazy mode flag. SourceMap over input cells always materializes an
// entry on mint (Entry / Set).

import (
	"fmt"
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
	conformanceMeta
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
	raw, err := specReadFile(p)
	if err != nil {
		t.Fatalf("reading %s: %v", p, err)
	}
	var f matFixture
	mustStrictJSON(t, "materialization/"+name, raw, &f)
	return f
}

// isSourceMapModel / isComputedMapModel accept both the current fixture
// spelling ("SourceMap" / "ComputedMap", emitted by lazily-spec since the v2
// kernel rename) and the pre-rename spelling ("CellMap" / "SlotMap"), so this
// runner replays against either a current or an older lazily-spec checkout.
func isSourceMapModel(model string) bool {
	return model == "SourceMap" || model == "CellMap"
}

func isComputedMapModel(model string) bool {
	return model == "ComputedMap" || model == "SlotMap"
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

// assertDefaultModeBuild proves the materialization corpus's `default_mode_eager`
// clause behaviourally (#lzdefaultmodeuniform).
//
// This binding has no mode flag to read back: eager vs lazy IS which call the
// caller makes — MaterializeAll pre-mint versus GetOrInsertWith mint-on-access.
// So the fixture's value SELECTS the build, and the fact asserted is the one the
// clause names: *a map built the way the fixture names its default is fully
// materialized at build time.* The mode string is dispatched on ONLY to choose
// the construction; the count it must reach is the same either way.
//
// The shape this replaces was `if f.Expected.DefaultMode != "eager" { Fatalf }`.
// Deserializing the key and comparing it to a hardcoded literal asserts only that
// the fixture equals itself (#lzconsumednotasserted): it reddens when the corpus
// flips, but a binding whose eager build materialized NOTHING passed it. Mirrors
// lazily-rs materialization_conformance.rs, lazily-cpp
// test_materialization_replay.hpp, and lazily-cs MaterializationConformanceTests.
//
// eager/lazy each build the map that way and return its present-at-build count.
// declared is the fixture's full declared key count — the count a FULLY
// materialized build has. The comparison is against `declared` in BOTH arms,
// because that is the clause: the default build is fully materialized. Only the
// eager build satisfies it, so flipping the fixture to "lazy" reddens (the lazy
// build defers its computed entries) and an eager build that materializes nothing
// reddens too. An unknown mode string is a hard failure, never a skip.
func assertDefaultModeBuild(
	t *testing.T,
	label string,
	mode string,
	eager func() int,
	lazy func() int,
	declared int,
) {
	t.Helper()
	var present int
	switch mode {
	case "eager":
		present = eager()
	case "lazy":
		present = lazy()
	default:
		t.Fatalf("%s: unknown default_mode %q (want \"eager\" or \"lazy\")", label, mode)
	}
	if present != declared {
		t.Fatalf("%s: a map built the fixture's default way (%s) has %d of %d declared entries "+
			"present at build — default_mode_eager says the default build is fully materialized",
			label, mode, present, declared)
	}
}

// ---------------------------------------------------------------------------
// Fixture replays
// ---------------------------------------------------------------------------

// observe_canonical / eager_lazy_observationally_equivalent / default_mode_eager:
// a keyed ComputedMap of derived slots built eager (MaterializeAll pre-mint) vs lazy
// (GetOrInsertWith mint-on-access) returns identical values; eager materializes
// every key up front, lazy only the read keys.
func TestMaterializationObservationalTransparency(t *testing.T) {
	f := loadMatFixture(t, "observational_transparency.json")
	if !isComputedMapModel(f.Model) {
		t.Fatalf("fixture model = %q, want ComputedMap (or the deprecated SlotMap spelling)", f.Model)
	}
	keys := matSortedKeys(f.Spec.Val)
	factory := func(k string) int { return f.Spec.Val[k] }

	ctx := NewContext()

	// default_mode_eager, asserted behaviourally: build the map the way the
	// fixture names its default, then require that build to hold every declared
	// key at build time.
	assertDefaultModeBuild(t, "observational_transparency.json", f.Expected.DefaultMode,
		func() int {
			m := NewComputedMap[string, int](ctx)
			m.MaterializeAll(ctx, keys, factory)
			return m.PresentCount()
		},
		func() int { return NewComputedMap[string, int](ctx).PresentCount() },
		len(keys))

	eager := NewComputedMap[string, int](ctx)
	eager.MaterializeAll(ctx, keys, factory)
	if eager.EntryKind() != EntryKindComputed {
		t.Fatalf("slot map mis-tagged: kind=%v", eager.EntryKind())
	}
	if eager.PresentCount() != len(keys) {
		t.Fatalf("eager present count %d != %d (eager_materializes_all)", eager.PresentCount(), len(keys))
	}
	assertSameSet(t, eager.PresentKeys(), f.Expected.EagerPresent, "eager_present")

	// Lazy defers every derived slot: nothing present at build.
	lazy := NewComputedMap[string, int](ctx)
	if lazy.PresentCount() != 0 {
		t.Fatalf("lazy present count %d != 0 (lazy_defers_slots)", lazy.PresentCount())
	}

	// Identical observed values under either strategy.
	for k, want := range f.Expected.Observe {
		if got, _ := eager.Observe(ctx, k); got != want {
			t.Fatalf("eager.Observe(ctx, %q)=%d want %d", k, got, want)
		}
		if got := lazy.GetOrInsertWith(ctx, k, factory); got != want {
			t.Fatalf("lazy.GetOrInsertWith(ctx, %q)=%d want %d", k, got, want)
		}
	}

	// Replay the read sequence on a FRESH lazy map; the present set is exactly
	// the read keys.
	fresh := NewComputedMap[string, int](ctx)
	for _, k := range f.Reads {
		fresh.GetOrInsertWith(ctx, k, factory)
	}
	assertSameSet(t, fresh.PresentKeys(), f.Expected.LazyPresentAfterReads, "lazy_present_after_reads")
}

// materialize_present_monotone / lazy_present_subset_eager: the present set only
// grows and is unchanged by a re-read; the lazy present set is a subset of eager.
func TestMaterializationDeferralNotDeallocation(t *testing.T) {
	f := loadMatFixture(t, "deferral_not_deallocation.json")
	factory := func(k string) int { return f.Spec.Val[k] }
	keys := matSortedKeys(f.Spec.Val)

	ctx := NewContext()

	// This fixture also carries expected.default_mode. It was decoded and never
	// asserted here at all, which is the same #lzconsumednotasserted hole as the
	// literal comparison, just quieter.
	assertDefaultModeBuild(t, "deferral_not_deallocation.json", f.Expected.DefaultMode,
		func() int {
			m := NewComputedMap[string, int](ctx)
			m.MaterializeAll(ctx, keys, factory)
			return m.PresentCount()
		},
		func() int { return NewComputedMap[string, int](ctx).PresentCount() },
		len(keys))

	lazy := NewComputedMap[string, int](ctx)

	var sizes []int
	for _, k := range f.Reads {
		lazy.GetOrInsertWith(ctx, k, factory)
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

// parseFixtureEntryKind maps a materialization fixture's `kind` string onto an
// EntryKind. The v2 kernel renamed the node kinds to Source / Computed, so the
// shared fixtures may carry either the pre-v2 spelling ("cell" / "slot") or the
// v2 spelling ("source" / "computed"); both are accepted so this runner keeps
// passing across the fixture flip. Any other value is an error — never a silent
// default or skip.
func parseFixtureEntryKind(kind string) (EntryKind, error) {
	switch kind {
	case "cell", "source":
		return EntryKindSource, nil
	case "slot", "computed":
		return EntryKindComputed, nil
	default:
		return 0, fmt.Errorf("unknown entry kind %q (want %q/%q or %q/%q)",
			kind, "cell", "source", "slot", "computed")
	}
}

// TestFixtureEntryKindSpellingsAreForwardCompatible pins the parse contract
// above: both the pre-v2 and v2 spellings resolve to the same EntryKind values,
// and an unrecognized kind is a hard error rather than a default.
func TestFixtureEntryKindSpellingsAreForwardCompatible(t *testing.T) {
	for _, tc := range []struct {
		kind string
		want EntryKind
	}{
		{"cell", EntryKindSource},
		{"source", EntryKindSource},
		{"slot", EntryKindComputed},
		{"computed", EntryKindComputed},
	} {
		got, err := parseFixtureEntryKind(tc.kind)
		if err != nil {
			t.Fatalf("parseFixtureEntryKind(%q) errored: %v", tc.kind, err)
		}
		if got != tc.want {
			t.Fatalf("parseFixtureEntryKind(%q)=%v want %v", tc.kind, got, tc.want)
		}
	}
	for _, bad := range []string{"", "Cell", "signal", "effect", "unknown"} {
		if _, err := parseFixtureEntryKind(bad); err == nil {
			t.Fatalf("parseFixtureEntryKind(%q) must be a hard error, got nil", bad)
		}
	}
}

// cell_entries_materialized_in_every_mode / slot_entries_deferred_under_lazy:
// entry kind is orthogonal to strategy. A mixed-kind key space is modelled by a
// SourceMap over the cell entries and a ComputedMap over the slot entries.
func TestMaterializationEntryKindOrthogonalToStrategy(t *testing.T) {
	f := loadMatFixture(t, "entry_kind_orthogonal_to_mode.json")

	var cellKeys, slotKeys []string
	vals := make(map[string]int)
	for key, e := range f.Spec.Entries {
		vals[key] = e.Val
		kind, err := parseFixtureEntryKind(e.Kind)
		if err != nil {
			t.Fatalf("%v", err)
		}
		switch kind {
		case EntryKindSource:
			cellKeys = append(cellKeys, key)
		case EntryKindComputed:
			slotKeys = append(slotKeys, key)
		}
	}
	factory := func(k string) int { return vals[k] }

	ctx := NewContext()
	// Map iteration above filled these in random order; sort so every build below
	// pre-mints in the same sequence run to run.
	sort.Strings(cellKeys)
	sort.Strings(slotKeys)

	// default_mode_eager on the MIXED-kind fixture: the same dispatch with the
	// entry-kind split. Source entries are present at build under EVERY strategy,
	// computed entries only under eager — so only the eager build holds all four
	// declared entries, and the required count stays the full four either way.
	// Asserting "under lazy, only the two sources are present" instead would be a
	// tautology: the library does behave that way, so a corpus flipped to "lazy"
	// would stay GREEN and the probe would prove nothing.
	buildMixed := func(materializeComputed bool) int {
		cells := NewSourceMap[string, int](ctx)
		for _, k := range cellKeys {
			cells.Entry(k, vals[k])
		}
		slots := NewComputedMap[string, int](ctx)
		if materializeComputed {
			slots.MaterializeAll(ctx, slotKeys, factory)
		}
		return cells.PresentCount() + slots.PresentCount()
	}
	assertDefaultModeBuild(t, "entry_kind_orthogonal_to_mode.json", f.Expected.DefaultMode,
		func() int { return buildMixed(true) },
		func() int { return buildMixed(false) },
		len(f.Spec.Entries))

	// Eager build: cells pre-minted (always materialized) + slots pre-minted.
	eagerCells := NewSourceMap[string, int](ctx)
	for _, k := range cellKeys {
		eagerCells.Entry(k, vals[k])
	}
	eagerSlots := NewComputedMap[string, int](ctx)
	eagerSlots.MaterializeAll(ctx, slotKeys, factory)
	if eagerCells.EntryKind() != EntryKindSource || eagerSlots.EntryKind() != EntryKindComputed {
		t.Fatalf("entry kinds mis-tagged")
	}
	eagerPresent := append(eagerCells.PresentKeys(), eagerSlots.PresentKeys()...)
	assertSameSet(t, eagerPresent, f.Expected.EagerPresent, "eager_present")

	// Lazy build: cells present at build (always materialized), slots deferred.
	lazyCells := NewSourceMap[string, int](ctx)
	for _, k := range cellKeys {
		lazyCells.Entry(k, vals[k])
	}
	lazySlots := NewComputedMap[string, int](ctx)
	if lazySlots.PresentCount() != 0 {
		t.Fatalf("slots must defer at build, present=%d", lazySlots.PresentCount())
	}
	assertSameSet(t, lazyCells.PresentKeys(), f.Expected.LazyPresentAtBuild, "lazy_present_at_build")

	// Reads (slot pulls) grow only the slot present set.
	slotSet := strSet(slotKeys)
	for _, k := range f.Reads {
		if _, ok := slotSet[k]; ok {
			lazySlots.GetOrInsertWith(ctx, k, factory)
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
			if got, _ := eagerSlots.Observe(ctx, k); got != want {
				t.Fatalf("eagerSlots.Observe(ctx, %q)=%d want %d", k, got, want)
			}
			if got := lazySlots.GetOrInsertWith(ctx, k, factory); got != want {
				t.Fatalf("lazySlots.GetOrInsertWith(ctx, %q)=%d want %d", k, got, want)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Unit tests mirroring lazily-rs/src/cell_family.rs
// ---------------------------------------------------------------------------

func TestComputedMapEagerMaterializesAll(t *testing.T) {
	ctx := NewContext()
	m := NewComputedMap[int, int](ctx)
	m.MaterializeAll(ctx, []int{0, 1, 2, 5, 9}, func(k int) int { return k * 3 })
	if m.PresentCount() != 5 {
		t.Fatalf("eager present count %d != 5", m.PresentCount())
	}
	for _, k := range []int{0, 1, 2, 5, 9} {
		if !m.IsPresent(k) {
			t.Fatalf("key %d not present after MaterializeAll", k)
		}
	}
}

func TestComputedMapLazyDefersThenMintsOnAccess(t *testing.T) {
	ctx := NewContext()
	m := NewComputedMap[int, int](ctx)
	if m.PresentCount() != 0 || m.IsPresent(5) {
		t.Fatalf("lazy must defer slots; present=%d", m.PresentCount())
	}
	if got := m.GetOrInsertWith(ctx, 5, func(k int) int { return k * 3 }); got != 15 {
		t.Fatalf("GetOrInsertWith(5)=%d want 15", got)
	}
	if !m.IsPresent(5) {
		t.Fatalf("key 5 must be present after mint-on-access")
	}
	// Same key -> same slot; factory not re-run.
	if got := m.GetOrInsertWith(ctx, 5, func(k int) int { return k * 999 }); got != 15 {
		t.Fatalf("GetOrInsertWith(5) re-run=%d want cached 15", got)
	}
	assertSameSet(t, keysToStr(m.PresentKeys()), []string{"5"}, "present after single read")
}

func TestComputedMapPresentSetMonotone(t *testing.T) {
	ctx := NewContext()
	m := NewComputedMap[int, int](ctx)
	factory := func(k int) int { return k * 2 }
	var sizes []int
	for _, k := range []int{2, 4, 2, 5} {
		m.GetOrInsertWith(ctx, k, factory)
		sizes = append(sizes, m.PresentCount())
	}
	want := []int{1, 2, 2, 3}
	for i := range want {
		if sizes[i] != want[i] {
			t.Fatalf("sizes[%d]=%d want %d", i, sizes[i], want[i])
		}
	}
}

func TestSourceMapEntryCachesOneCellPerKey(t *testing.T) {
	ctx := NewContext()
	m := NewSourceMap[string, int](ctx)
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

func TestSourceMapSetIsCellOnly(t *testing.T) {
	ctx := NewContext()
	m := NewSourceMap[int, int](ctx)
	m.Set(7, 42)
	if got, ok := m.Read(7); !ok || got != 42 {
		t.Fatalf("Read(7)=(%d,%v) want (42,true)", got, ok)
	}
	m.Set(7, 100)
	if got, _ := m.Read(7); got != 100 {
		t.Fatalf("Read(7)=%d want 100 after Set", got)
	}
	if m.EntryKind() != EntryKindSource {
		t.Fatalf("SourceMap kind = %v, want cell", m.EntryKind())
	}
}

// TestDeprecatedMapNameAliases pins the source compatibility promise of the
// v2-kernel rename: the pre-rename names still resolve, and because they are
// type *aliases* (not new defined types) a value built through the deprecated
// constructor is assignable to the new type, and vice versa.
func TestDeprecatedMapNameAliases(t *testing.T) {
	actx := NewAsyncContext()
	defer actx.Close()
	ts := NewThreadSafeContext()
	ctx := NewContext()

	var src *SourceMap[string, int] = NewCellMap[string, int](ctx)
	var cell *CellMap[string, int] = NewSourceMap[string, int](ctx)
	src.Set("a", 1)
	cell.Set("a", 1)
	if got, _ := src.Get("a"); got != 1 {
		t.Fatalf("CellMap alias: Get(a)=%d want 1", got)
	}
	if src.EntryKind() != EntryKindSource || cell.EntryKind() != EntryKindSource {
		t.Fatalf("CellMap alias: entry kind drifted")
	}

	var cmp *ComputedMap[string, int] = NewSlotMap[string, int](ctx)
	var slot *SlotMap[string, int] = NewComputedMap[string, int](ctx)
	cmp.MaterializeAll(ctx, []string{"a"}, func(k string) int { return 7 })
	slot.MaterializeAll(ctx, []string{"a"}, func(k string) int { return 7 })
	if got, _ := cmp.Observe(ctx, "a"); got != 7 {
		t.Fatalf("SlotMap alias: Observe(a)=%d want 7", got)
	}
	if cmp.EntryKind() != EntryKindComputed || slot.EntryKind() != EntryKindComputed {
		t.Fatalf("SlotMap alias: entry kind drifted")
	}

	var tsSrc *ThreadSafeSourceMap[string, int] = NewThreadSafeCellMap[string, int](ts)
	var tsCell *ThreadSafeCellMap[string, int] = NewThreadSafeSourceMap[string, int](ts)
	tsSrc.Set("a", 1)
	tsCell.Set("a", 1)
	var tsCmp *ThreadSafeComputedMap[string, int] = NewThreadSafeSlotMap[string, int](ts)
	var tsSlot *ThreadSafeSlotMap[string, int] = NewThreadSafeComputedMap[string, int](ts)
	tsCmp.MaterializeAll([]string{"a"}, func(k string) int { return 7 })
	tsSlot.MaterializeAll([]string{"a"}, func(k string) int { return 7 })
	if got, ok := tsCmp.Observe(ctx, "a"); !ok || got != 7 {
		t.Fatalf("ThreadSafeSlotMap alias: Observe(a)=(%d,%v) want (7,true)", got, ok)
	}
	if tsSrc.EntryKind() != EntryKindSource || tsSlot.EntryKind() != EntryKindComputed {
		t.Fatalf("ThreadSafe alias: entry kind drifted")
	}

	var asSrc *AsyncSourceMap[string, int] = NewAsyncSourceMap[string, int](actx)
	var asCell *AsyncCellMap[string, int] = NewAsyncCellMap[string, int](actx)
	asSrc.Set("a", 1)
	asCell.Set("a", 1)
	var asCmp *AsyncComputedMap[string, int] = NewAsyncComputedMap[string, int](actx)
	var asSlot *AsyncSlotMap[string, int] = NewAsyncSlotMap[string, int](actx)
	asCmp.MaterializeAll([]string{"a"}, func(k string) int { return 7 })
	asSlot.MaterializeAll([]string{"a"}, func(k string) int { return 7 })
	if got := asCmp.Drive("a", func(k string) int { return 7 }); got != 7 {
		t.Fatalf("AsyncSlotMap alias: Drive(a)=%d want 7", got)
	}
}

// TestDeprecatedCellTreeNameAlias pins the same promise for the ordered keyed
// tree: CellTree is an alias of SourceTree, so nodes cross the old/new spellings
// in both directions and the deprecated constructor still builds a live tree.
func TestDeprecatedCellTreeNameAlias(t *testing.T) {
	ctx := NewContext()

	var srcTree *SourceTree[string, int] = NewCellTree[string, int](ctx, "root", 1)
	var cellTree *CellTree[string, int] = NewSourceTree[string, int](ctx, "root", 1)

	var child *CellTree[string, int] = srcTree.InsertChild("a", 10)
	srcTree.InsertChild("b", 20)
	if got := child.Get(); got != 10 {
		t.Fatalf("CellTree alias: child Get()=%d want 10", got)
	}
	if got := srcTree.ChildCount(ctx); got != 2 {
		t.Fatalf("CellTree alias: ChildCount()=%d want 2", got)
	}
	if !srcTree.MoveChildBefore("b", "a") {
		t.Fatalf("CellTree alias: MoveChildBefore failed")
	}
	if ids := srcTree.ChildIDs(ctx); len(ids) != 2 || ids[0] != "b" || ids[1] != "a" {
		t.Fatalf("CellTree alias: ChildIDs()=%v want [b a]", ids)
	}

	var back *SourceTree[string, int] = cellTree
	back.Set(2)
	if got := cellTree.Get(); got != 2 {
		t.Fatalf("CellTree alias: Get()=%d want 2", got)
	}
	if cellTree.NodeID() != "root" {
		t.Fatalf("CellTree alias: NodeID()=%q want root", cellTree.NodeID())
	}
}

// TestDeprecatedEntryKindNameAliases pins the entry-kind rename: EntryKindCell /
// EntryKindSlot stay usable as deprecated aliases, and — critically — the rename
// is identifier-only. The ordinal values and the String() wire spellings are
// shared with the conformance fixtures and the other bindings, so they must not
// move.
func TestDeprecatedEntryKindNameAliases(t *testing.T) {
	if EntryKindCell != EntryKindSource {
		t.Fatalf("EntryKindCell must alias EntryKindSource")
	}
	if EntryKindSlot != EntryKindComputed {
		t.Fatalf("EntryKindSlot must alias EntryKindComputed")
	}
	// Wire representation is frozen: ordinals and strings are unchanged.
	if int(EntryKindSource) != 0 || int(EntryKindComputed) != 1 {
		t.Fatalf("entry kind ordinals moved: source=%d computed=%d want 0/1",
			int(EntryKindSource), int(EntryKindComputed))
	}
	if EntryKindSource.String() != "cell" {
		t.Fatalf("EntryKindSource.String()=%q want %q", EntryKindSource.String(), "cell")
	}
	if EntryKindComputed.String() != "slot" {
		t.Fatalf("EntryKindComputed.String()=%q want %q", EntryKindComputed.String(), "slot")
	}

	ctx := NewContext()
	if NewSourceMap[string, int](ctx).EntryKind() != EntryKindCell {
		t.Fatalf("SourceMap entry kind must still match the deprecated EntryKindCell")
	}
	if NewComputedMap[string, int](ctx).EntryKind() != EntryKindSlot {
		t.Fatalf("ComputedMap entry kind must still match the deprecated EntryKindSlot")
	}
}

func keysToStr(ks []int) []string {
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = strconv.Itoa(k)
	}
	return out
}
