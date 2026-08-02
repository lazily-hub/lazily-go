package lazily

// Conformance replay of the shared lazily-spec lossless-tree CRDT compute
// fixtures (conformance/lossless-tree/*.json).
//
// Mirrors the JS replay harness lazily-js/test/lossless-tree-crdt.test.js and
// the Kotlin lazily-kt/src/test/.../LosslessTreeCrdtConformanceTest.kt:
//   - each fixture's `scenarios` build an initial tree on replica "a", run a
//     schedule of fork/sync/deliver/op steps, then assert render text (on "a"
//     or per-replica), live-node count, and convergence across named replicas.
//
// These fixtures exercise the M1 op vocabulary (create / tombstone / reorder /
// leaf-edit / split / merge), the dotted non-contiguous version frontier
// (anti-entropy with a delivery hole), and concurrent merge convergence
// (same-parent inserts, concurrent move + edit, incompatible-shape survival).
//
// Fixtures are resolved via a relative-path helper; a test skips (never fails)
// when the spec checkout is absent.

import (
	"encoding/json"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixture resolution (mirrors receipts_distributed_conformance_test.go)
// ---------------------------------------------------------------------------

func loadLosslessTreeFixture(t *testing.T, name string) []byte {
	t.Helper()
	rel := filepath.Join("lossless-tree", name)
	candidates := []string{
		filepath.Join("..", "lazily-spec", "conformance", rel),
		filepath.Join("test", "conformance", rel),
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		dir := filepath.Dir(file)
		candidates = append(candidates,
			filepath.Join(dir, "..", "lazily-spec", "conformance", rel),
		)
	}
	for _, path := range candidates {
		if b, err := specReadFile(path); err == nil {
			return b
		}
	}
	t.Skipf("conformance fixture not found: %s", rel)
	return nil
}

// ---------------------------------------------------------------------------
// Fixture JSON shapes
// ---------------------------------------------------------------------------

type ltSeedNode struct {
	Label    string       `json:"label"`
	Element  string       `json:"element,omitempty"`
	Leaf     *ltLeafSpec  `json:"leaf,omitempty"`
	Children []ltSeedNode `json:"children,omitempty"`
}

type ltLeafSpec struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

type ltStep struct {
	Fork        string      `json:"fork,omitempty"`
	Peer        int64       `json:"peer,omitempty"`
	Sync        *ltSync     `json:"sync,omitempty"`
	Deliver     *ltDeliver  `json:"deliver,omitempty"`
	On          string      `json:"on,omitempty"`
	Op          string      `json:"op,omitempty"`
	Parent      string      `json:"parent,omitempty"`
	After       *string     `json:"after,omitempty"`
	Label       string      `json:"label,omitempty"`
	NewLabel    string      `json:"new_label,omitempty"`
	Node        string      `json:"node,omitempty"`
	Left        string      `json:"left,omitempty"`
	Right       string      `json:"right,omitempty"`
	AtByte      int         `json:"at_byte,omitempty"`
	DeleteBytes int         `json:"delete_bytes,omitempty"`
	Insert      string      `json:"insert,omitempty"`
	Element     string      `json:"element,omitempty"`
	Leaf        *ltLeafSpec `json:"leaf,omitempty"`
}

type ltSync struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type ltDeliver struct {
	From string `json:"from"`
	To   string `json:"to"`
	Only []int  `json:"only"`
}

type ltExpect struct {
	Render    string            `json:"render,omitempty"`
	RenderOn  map[string]string `json:"render_on,omitempty"`
	LiveNodes int               `json:"live_nodes,omitempty"`
	Converged []string          `json:"converged,omitempty"`
}

type ltScenario struct {
	Id     string   `json:"id"`
	Name   string   `json:"name"`
	Seed   ltSeed   `json:"seed"`
	Steps  []ltStep `json:"steps"`
	Expect ltExpect `json:"expect"`
}

type ltSeed struct {
	Peer int64      `json:"peer"`
	Tree ltSeedNode `json:"tree"`
}

type ltFixture struct {
	Description string       `json:"description"`
	Kind        string       `json:"kind"`
	Model       string       `json:"model"`
	Scenarios   []ltScenario `json:"scenarios"`
}

// ---------------------------------------------------------------------------
// Replay world
// ---------------------------------------------------------------------------

type ltWorld struct {
	replicas map[string]*LosslessTreeCrdt
	ids      map[string]OpId
}

func newLtWorld() *ltWorld {
	return &ltWorld{replicas: map[string]*LosslessTreeCrdt{}, ids: map[string]OpId{}}
}

func (w *ltWorld) id(label string) OpId {
	v, ok := w.ids[label]
	if !ok {
		panic("unknown node label: " + label)
	}
	return v
}

func (w *ltWorld) afterOf(step ltStep) *OpId {
	if step.After == nil {
		return nil
	}
	v := w.id(*step.After)
	return &v
}

func ltNodeSeed(spec ltSeedNode) TreeNodeSeed {
	return ltSeedOf(spec.Element, spec.Leaf)
}

// ltSeedOf builds a NodeSeed from the element/leaf fields shared by seed-tree
// nodes and create-op steps.
func ltSeedOf(element string, leaf *ltLeafSpec) TreeNodeSeed {
	if element != "" {
		return TreeNodeSeedElement{Kind: element}
	}
	if leaf != nil {
		kind, err := leafKindFromWireLower(leaf.Kind)
		if err != nil {
			panic(err.Error())
		}
		return TreeNodeSeedLeaf{Kind: kind, Text: leaf.Text}
	}
	panic("node spec has neither element nor leaf")
}

func (w *ltWorld) buildChildren(spec ltSeedNode, parent OpId, replica *LosslessTreeCrdt) {
	if spec.Children == nil {
		return
	}
	var prev *OpId
	for _, child := range spec.Children {
		id := replica.CreateNode(parent, prev, ltNodeSeed(child))
		w.ids[child.Label] = id
		w.buildChildren(child, id, replica)
		prev = &id
	}
}

func (w *ltWorld) applyStep(step ltStep) {
	if step.Fork != "" {
		w.replicas[step.Fork] = w.replicas["a"].Fork(step.Peer)
		return
	}
	if step.Sync != nil {
		update := w.replicas[step.Sync.From].Diff(w.replicas[step.Sync.To].Frontier())
		w.replicas[step.Sync.To].ApplyUpdate(update)
		return
	}
	if step.Deliver != nil {
		full := w.replicas[step.Deliver.From].Diff(w.replicas[step.Deliver.To].Frontier())
		selected := make([]TreeOp, len(step.Deliver.Only))
		for i, idx := range step.Deliver.Only {
			selected[i] = full.Ops[idx]
		}
		w.replicas[step.Deliver.To].ApplyUpdate(TreeUpdate{Ops: selected})
		return
	}
	if step.On != "" {
		w.applyOp(step.On, step)
		return
	}
	panic("unrecognized step")
}

func (w *ltWorld) applyOp(on string, step ltStep) {
	replica := w.replicas[on]
	switch step.Op {
	case "create":
		id := replica.CreateNode(w.id(step.Parent), w.afterOf(step), ltSeedOf(step.Element, step.Leaf))
		w.ids[step.Label] = id
	case "edit_leaf":
		replica.EditLeaf(w.id(step.Node), step.AtByte, step.DeleteBytes, step.Insert)
	case "split":
		w.ids[step.NewLabel] = replica.SplitLeaf(w.id(step.Node), step.AtByte)
	case "merge_leaves":
		replica.MergeAdjacentLeaves(w.id(step.Left), w.id(step.Right))
	case "reorder":
		replica.ReorderChild(w.id(step.Node), w.afterOf(step))
	case "tombstone":
		replica.TombstoneNode(w.id(step.Node))
	default:
		panic("unknown op: " + step.Op)
	}
}

func (w *ltWorld) assertExpect(t *testing.T, expect ltExpect, label string) {
	t.Helper()
	if expect.Render != "" {
		if got := w.replicas["a"].Render(); got != expect.Render {
			t.Errorf("%s: render on a = %q, want %q", label, got, expect.Render)
		}
	}
	for name, text := range expect.RenderOn {
		if got := w.replicas[name].Render(); got != text {
			t.Errorf("%s: render on %s = %q, want %q", label, name, got, text)
		}
	}
	if expect.LiveNodes != 0 {
		if got := w.replicas["a"].LiveNodeCount(); got != expect.LiveNodes {
			t.Errorf("%s: live_nodes = %d, want %d", label, got, expect.LiveNodes)
		}
	}
	if len(expect.Converged) > 0 {
		first := w.replicas[expect.Converged[0]].Render()
		for _, name := range expect.Converged[1:] {
			if got := w.replicas[name].Render(); got != first {
				t.Errorf("%s: %s/%s do not converge: %q vs %q",
					label, expect.Converged[0], name, first, got)
			}
		}
	}
}

func runLosslessTreeFixture(t *testing.T, name string) {
	raw := loadLosslessTreeFixture(t, name)
	var fixture ltFixture
	mustStrictJSON(t, name, raw, &fixture)
	for i, scenario := range fixture.Scenarios {
		// Rung 4 (#lzscenariocoverage): record at the point of replay.
		id := recordScenarioAt(filepath.Join("lossless-tree", name), i, scenario.Id, scenario.Name)
		label := name + "[" + id + "]"
		t.Run(label, func(t *testing.T) {
			world := newLtWorld()
			world.replicas["a"] = NewLosslessTreeCrdt(scenario.Seed.Peer)
			world.buildChildren(scenario.Seed.Tree, TreeRoot, world.replicas["a"])
			for _, step := range scenario.Steps {
				world.applyStep(step)
			}
			world.assertExpect(t, scenario.Expect, label)
		})
	}
}

func TestLosslessTreeConformance(t *testing.T) {
	fixtures := []string{
		"exact_roundtrip.json",
		"one_leaf_edit_delta.json",
		"split_merge.json",
		"concurrent_insert_same_parent.json",
		"concurrent_reorder_and_leaf_edit.json",
		"non_contiguous_anti_entropy.json",
		"token_trivia_preservation.json",
		"invalid_source_roundtrip.json",
		"concurrent_conflict_preserves_text.json",
	}
	for _, name := range fixtures {
		name := name
		t.Run(name, func(t *testing.T) {
			runLosslessTreeFixture(t, name)
		})
	}
}

// ---------------------------------------------------------------------------
// Wire schema compliance: emitted TreeUpdate round-trips through encode/decode
// and carries one of each op variant in the externally-tagged form pinned by
// schemas/lossless-tree.json + lossless-tree-delta.json.
// ---------------------------------------------------------------------------

func TestLosslessTreeWireRoundTrip(t *testing.T) {
	tree := NewLosslessTreeCrdt(1)
	para := tree.CreateNode(TreeRoot, nil, TreeNodeSeedElement{Kind: "para"})
	a := tree.CreateNode(para, nil, TreeNodeSeedLeaf{Kind: LeafKindRaw, Text: "hello world"})
	b := tree.CreateNode(para, &a, TreeNodeSeedLeaf{Kind: LeafKindToken, Text: "!"})
	tree.EditLeaf(a, 5, 0, "X")       // LeafEdit
	tail := tree.SplitLeaf(a, 6)      // SplitLeaf
	tree.MergeAdjacentLeaves(a, tail) // MergeLeaves
	tree.ReorderChild(b, nil)         // Reorder
	tree.TombstoneNode(b)             // Tombstone

	update := tree.Diff(NewTreeVersionFrontier())

	// Encode → decode → re-encode must be stable (canonical wire form).
	encoded, err := json.Marshal(update)
	if err != nil {
		t.Fatalf("marshal TreeUpdate: %v", err)
	}
	decoded, err := TreeUpdateFromWire(encoded)
	if err != nil {
		t.Fatalf("decode TreeUpdate: %v", err)
	}
	reencoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-marshal TreeUpdate: %v", err)
	}
	if string(encoded) != string(reencoded) {
		t.Errorf("wire round-trip unstable:\n first: %s\n second: %s", encoded, reencoded)
	}

	// Every op variant must be present in the emitted delta (one of each).
	kinds := map[string]bool{}
	for _, op := range decoded.Ops {
		tag := treeOpKindTag(op.Kind)
		kinds[tag] = true
	}
	for _, want := range []string{"CreateNode", "Tombstone", "Reorder", "LeafEdit", "SplitLeaf", "MergeLeaves"} {
		if !kinds[want] {
			t.Errorf("emitted delta missing op variant %q; got %v", want, sortedKeys(kinds))
		}
	}

	// The delta decodes into the conformance TreeUpdate shape {ops: [...]}.
	var generic map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &generic); err != nil {
		t.Fatalf("unmarshal generic: %v", err)
	}
	if _, ok := generic["ops"]; !ok {
		t.Fatalf("emitted delta missing top-level `ops` field")
	}

	// Applying the delta to a fresh replica converges to the same render.
	fresh := NewLosslessTreeCrdt(2)
	fresh.ApplyUpdate(decoded)
	if got, want := fresh.Render(), tree.Render(); got != want {
		t.Errorf("post-sync render = %q, want %q", got, want)
	}
}

func treeOpKindTag(k TreeOpKind) string {
	switch k.(type) {
	case TreeOpCreateNode:
		return "CreateNode"
	case TreeOpTombstone:
		return "Tombstone"
	case TreeOpReorder:
		return "Reorder"
	case TreeOpLeafEdit:
		return "LeafEdit"
	case TreeOpSplitLeaf:
		return "SplitLeaf"
	case TreeOpMergeLeaves:
		return "MergeLeaves"
	}
	return "Unknown"
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
