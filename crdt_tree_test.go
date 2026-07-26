package lazily

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

type crdtTreeFixture struct {
	Kind      string `json:"kind"`
	Scenarios []struct {
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
		MergeOrders [][]string `json:"merge_orders"`
		RestorePeer PeerId     `json:"restore_peer"`
	} `json:"scenarios"`
}

func loadCrdtTreeFixture(t *testing.T) crdtTreeFixture {
	t.Helper()
	raw, err := specReadFile("../lazily-spec/conformance/crdt-tree/algebra.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture crdtTreeFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func crdtTreeScenario(t *testing.T, fixture crdtTreeFixture, name string) (out struct {
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
	MergeOrders [][]string `json:"merge_orders"`
	RestorePeer PeerId     `json:"restore_peer"`
}) {
	t.Helper()
	for _, scenario := range fixture.Scenarios {
		if scenario.Name == name {
			return scenario
		}
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
	scenario := crdtTreeScenario(t, fixture, "merge algebra is order and duplication independent")
	base := TextCrdtFromStr(scenario.Seed.Peer, scenario.Seed.Text)
	replicas := map[string]*TextCrdt{}
	for _, edit := range scenario.Replicas {
		replica := base.Fork(edit.Peer)
		replica.Insert(replica.Len(), edit.Insert)
		replicas[edit.Name] = replica
	}

	var text string
	var frontier map[PeerId]int64
	for index, order := range scenario.MergeOrders {
		merged := base.Fork(PeerId(100 + index))
		for _, name := range order {
			merged.MergeFrom(replicas[name])
		}
		if index == 0 {
			text, frontier = merged.Text(), merged.VersionVector()
		} else {
			if merged.Text() != text || !reflect.DeepEqual(merged.VersionVector(), frontier) {
				t.Fatalf("merge order %d diverged", index)
			}
		}
		if merged.Value() != merged.Text() {
			t.Fatal("value projection differs from text")
		}
	}
}

func TestCrdtTreeSnapshotPreservesLineage(t *testing.T) {
	fixture := loadCrdtTreeFixture(t)
	scenario := crdtTreeScenario(t, fixture, "empty frontier snapshot preserves lineage")
	source := TextCrdtFromStr(scenario.Seed.Peer, scenario.Seed.Text)
	restored := NewTextCrdt(scenario.RestorePeer)
	if !restored.ApplyDelta(source.DeltaSince(nil)) {
		t.Fatal("snapshot did not change empty replica")
	}
	if source.Text() != restored.Text() || !reflect.DeepEqual(
		sortedTextOps(source.DeltaSince(nil)), sortedTextOps(restored.DeltaSince(nil)),
	) {
		t.Fatal("snapshot did not preserve text and operation identities")
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
	if len(ids) != len(ops) {
		t.Fatal("merge duplicated operation identity")
	}
}

func TestCrdtTreeOwnFrontierIsEmpty(t *testing.T) {
	fixture := loadCrdtTreeFixture(t)
	scenario := crdtTreeScenario(t, fixture, "own frontier emits an empty delta")
	tree := TextCrdtFromStr(scenario.Seed.Peer, scenario.Seed.Text)
	delta := tree.DeltaSince(tree.VersionVector())
	if len(delta) != 0 || tree.ApplyDelta(delta) {
		t.Fatal("own frontier must produce an idempotent empty delta")
	}
}
