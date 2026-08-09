package lazily

import (
	"reflect"
	"sort"
	"testing"
)

// crdtTreeExpect is the scenario's assertion block. Every one of these keys used
// to be absent from the runner's structs: the three tests hardcoded the same
// claims as literals, so the fixture stated its expectations and the replay
// checked its own.
type crdtTreeExpect struct {
	TextsEqual           *bool    `json:"texts_equal"`
	VersionVectorsEqual  *bool    `json:"version_vectors_equal"`
	RestoredTextEqual    *bool    `json:"restored_text_equal"`
	OpIdsEqual           *bool    `json:"op_ids_equal"`
	LaterMergeDuplicates *int     `json:"later_merge_duplicates"`
	Delta                []TextOp `json:"delta"`
	ApplyChanged         *bool    `json:"apply_changed"`
}

// crdtTreeScenarioT is named rather than inline so the fixture struct and the
// lookup helper share ONE definition. They used to carry byte-identical
// anonymous copies, where a field added to one and not the other silently
// stopped being decoded at the other site.
type crdtTreeScenarioT struct {
	conformanceDoc
	Id   string `json:"id"`
	Name string `json:"name"`
	Seed struct {
		Peer PeerId `json:"peer"`
		Text string `json:"text"`
	} `json:"seed"`
	Replicas []struct {
		Name   string `json:"name"`
		Peer   PeerId `json:"peer"`
		Insert string `json:"insert"`
	} `json:"replicas"`
	MergeOrders        [][]string     `json:"merge_orders"`
	RestorePeer        PeerId         `json:"restore_peer"`
	Snapshot           string         `json:"snapshot"`
	Frontier           string         `json:"frontier"`
	ThenConcurrentEdit bool           `json:"then_concurrent_edit"`
	Expect             crdtTreeExpect `json:"expect"`
}

type crdtTreeFixture struct {
	conformanceMeta
	Kind      string              `json:"kind"`
	Scenarios []crdtTreeScenarioT `json:"scenarios"`
}

func loadCrdtTreeFixture(t *testing.T) crdtTreeFixture {
	t.Helper()
	raw, err := specReadFile(specPath("crdt-tree", "algebra.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture crdtTreeFixture
	mustStrictJSON(t, "crdt-tree/algebra.json", raw, &fixture)
	return fixture
}

func crdtTreeScenario(t *testing.T, fixture crdtTreeFixture, name string) (out crdtTreeScenarioT) {
	t.Helper()
	// Selecting is not replaying (#lzscenariobodyskip): the lookup walks past
	// every scenario ahead of the match, and a caller could select one and then
	// return. Booking rides on Value() below, which is the payload handoff.
	for _, sv := range typedScenarioViews("crdt-tree/algebra.json", fixture.Scenarios,
		func(s crdtTreeScenarioT) (string, string) { return s.Id, s.Name }) {
		if sv.ID() != name {
			continue
		}
		return sv.Value()
	}
	t.Fatalf("missing scenario %q", name)
	return out
}

func sortedTextOps(ops []TextOp) []TextOp {
	out := append([]TextOp(nil), ops...)
	sort.Slice(out, func(i, j int) bool { return out[i].Id.Compare(out[j].Id) < 0 })
	return out
}

func TestCrdtTreeMergeAlgebra(t *testing.T) {
	fixture := loadCrdtTreeFixture(t)
	scenario := crdtTreeScenario(t, fixture, "merge_is_order_and_duplication_independent")
	base := TextCrdtFromStr(scenario.Seed.Peer, scenario.Seed.Text)
	replicas := map[string]*TextCrdt{}
	for _, edit := range scenario.Replicas {
		replica := base.Fork(edit.Peer)
		replica.Insert(replica.Len(), edit.Insert)
		replicas[edit.Name] = replica
	}

	var text string
	var frontier map[PeerId]int64
	textsEqual, vectorsEqual := true, true
	for index, order := range scenario.MergeOrders {
		merged := base.Fork(PeerId(100 + index))
		for _, name := range order {
			merged.MergeFrom(replicas[name])
		}
		if index == 0 {
			text, frontier = merged.Text(), merged.VersionVector()
		} else {
			if merged.Text() != text {
				textsEqual = false
			}
			if !reflect.DeepEqual(merged.VersionVector(), frontier) {
				vectorsEqual = false
			}
		}
		if merged.Value() != merged.Text() {
			t.Fatal("value projection differs from text")
		}
	}
	if want := scenario.Expect.TextsEqual; want == nil {
		t.Fatal("expect.texts_equal is missing")
	} else if textsEqual != *want {
		t.Fatalf("texts_equal = %v, want %v", textsEqual, *want)
	}
	if want := scenario.Expect.VersionVectorsEqual; want == nil {
		t.Fatal("expect.version_vectors_equal is missing")
	} else if vectorsEqual != *want {
		t.Fatalf("version_vectors_equal = %v, want %v", vectorsEqual, *want)
	}
}

func TestCrdtTreeSnapshotPreservesLineage(t *testing.T) {
	fixture := loadCrdtTreeFixture(t)
	scenario := crdtTreeScenario(t, fixture, "empty_frontier_snapshot_preserves_lineage")
	if scenario.Snapshot != "delta_since({})" {
		t.Fatalf("unsupported snapshot form %q", scenario.Snapshot)
	}
	source := TextCrdtFromStr(scenario.Seed.Peer, scenario.Seed.Text)
	restored := NewTextCrdt(scenario.RestorePeer)
	if !restored.ApplyDelta(source.DeltaSince(nil)) {
		t.Fatal("snapshot did not change empty replica")
	}
	restoredTextEqual := source.Text() == restored.Text()
	opIdsEqual := reflect.DeepEqual(
		sortedTextOps(source.DeltaSince(nil)), sortedTextOps(restored.DeltaSince(nil)))
	if want := scenario.Expect.RestoredTextEqual; want == nil {
		t.Fatal("expect.restored_text_equal is missing")
	} else if restoredTextEqual != *want {
		t.Fatalf("restored_text_equal = %v, want %v", restoredTextEqual, *want)
	}
	if want := scenario.Expect.OpIdsEqual; want == nil {
		t.Fatal("expect.op_ids_equal is missing")
	} else if opIdsEqual != *want {
		t.Fatalf("op_ids_equal = %v, want %v", opIdsEqual, *want)
	}

	if !scenario.ThenConcurrentEdit {
		return
	}
	source.Insert(source.Len(), "a")
	restored.Insert(restored.Len(), "b")
	source.MergeFrom(restored)
	restored.MergeFrom(source)
	if source.Text() != restored.Text() {
		t.Fatal("concurrent descendants did not converge")
	}
	ops := source.DeltaSince(nil)
	ids := map[OpId]struct{}{}
	for _, op := range ops {
		ids[op.Id] = struct{}{}
	}
	duplicates := len(ops) - len(ids)
	if want := scenario.Expect.LaterMergeDuplicates; want == nil {
		t.Fatal("expect.later_merge_duplicates is missing")
	} else if duplicates != *want {
		t.Fatalf("later_merge_duplicates = %d, want %d", duplicates, *want)
	}
}

func TestCrdtTreeOwnFrontierIsEmpty(t *testing.T) {
	fixture := loadCrdtTreeFixture(t)
	scenario := crdtTreeScenario(t, fixture, "own_frontier_emits_empty_delta")
	if scenario.Frontier != "version_vector()" {
		t.Fatalf("unsupported frontier form %q", scenario.Frontier)
	}
	tree := TextCrdtFromStr(scenario.Seed.Peer, scenario.Seed.Text)
	delta := tree.DeltaSince(tree.VersionVector())
	if len(delta) != len(scenario.Expect.Delta) {
		t.Fatalf("delta = %v, want %v", delta, scenario.Expect.Delta)
	}
	changed := tree.ApplyDelta(delta)
	if want := scenario.Expect.ApplyChanged; want == nil {
		t.Fatal("expect.apply_changed is missing")
	} else if changed != *want {
		t.Fatalf("apply_changed = %v, want %v", changed, *want)
	}
}
