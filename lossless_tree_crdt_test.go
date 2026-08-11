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
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixture resolution (mirrors receipts_distributed_conformance_test.go)
// ---------------------------------------------------------------------------

func loadLosslessTreeFixture(t *testing.T, name string) []byte {
	t.Helper()
	rel := filepath.Join("lossless-tree", name)
	for _, path := range specCandidatePaths(rel) {
		if b, err := specReadFile(path); err == nil {
			return b
		}
	}
	specFixtureMissing(t, "conformance fixture not found: %s", rel)
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

// ltDeliver is a partial/reordered delivery of `from`'s diff into `to`.
//
// The op list both selectors index into is the SAME one `sync` uses:
// `from.Diff(to.Frontier())`, which lazily-go returns in canonical dotted
// `(counter, peer)` order (pinned by TestLosslessTreeDiffReturnsOpsInCanonical-
// CounterPeerOrder). Indexes are 0-based into that list.
//
//   - `only` selects a SUBSET and delivers it in canonical order. A hole in the
//     subset is the point (non_contiguous_anti_entropy.json).
//   - `order` selects a SEQUENCE and delivers it in exactly the listed order, as
//     ONE ApplyUpdate call. Re-sorting it, or splitting it across calls, would
//     destroy the very thing out_of_order_delivery_buffers.json measures: whether
//     ApplyUpdate buffers an op whose dependency has not arrived YET IN THE SAME
//     BATCH and retries it as the batch drains.
//
// Exactly one selector must be present; see ltSelectDelivered.
type ltDeliver struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Only  []int  `json:"only,omitempty"`
	Order []int  `json:"order,omitempty"`
}

// ltSelectDelivered resolves a `deliver` step's selector against the canonical
// diff, returning the ops to hand to a single ApplyUpdate call.
//
// It fails closed on three things rather than papering over them:
//
//   - both selectors present, or neither. Defaulting either way would let a
//     fixture that meant "in this order" be replayed as "in canonical order"
//     (or vice versa) with no diagnostic.
//   - an index outside the diff. CLAMPING an out-of-range index is the silent
//     failure this guards: it would deliver a plausible-looking batch and go
//     green while replaying a schedule the fixture never described.
//   - a diff shorter than the fixture assumes, which is the same bug seen from
//     the other side and is reported by the same bounds check.
func ltSelectDelivered(diff []TreeOp, d *ltDeliver) ([]TreeOp, error) {
	switch {
	case d.Only != nil && d.Order != nil:
		return nil, fmt.Errorf("deliver{from:%s,to:%s} carries BOTH `only` and `order`; exactly one selector is allowed", d.From, d.To)
	case d.Only == nil && d.Order == nil:
		return nil, fmt.Errorf("deliver{from:%s,to:%s} carries NEITHER `only` nor `order`; exactly one selector is required", d.From, d.To)
	}

	indexes := d.Order
	selector := "order"
	if d.Only != nil {
		// `only` names a subset; canonical order is the diff's own order, so
		// sorting the indexes is what "in canonical order" means here.
		indexes = append([]int(nil), d.Only...)
		sort.Ints(indexes)
		selector = "only"
	}

	selected := make([]TreeOp, 0, len(indexes))
	for _, idx := range indexes {
		if idx < 0 || idx >= len(diff) {
			return nil, fmt.Errorf("deliver{from:%s,to:%s} `%s` index %d is out of range for a %d-op diff "+
				"(indexes are 0-based into from.Diff(to.Frontier()); an out-of-range index is a fixture/runner "+
				"disagreement, never something to clamp)", d.From, d.To, selector, idx, len(diff))
		}
		selected = append(selected, diff[idx])
	}
	return selected, nil
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
	t        *testing.T
	replicas map[string]*LosslessTreeCrdt
	ids      map[string]OpId
}

func newLtWorld(t *testing.T) *ltWorld {
	return &ltWorld{t: t, replicas: map[string]*LosslessTreeCrdt{}, ids: map[string]OpId{}}
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

// applyStep runs one schedule step.
//
// The loop over steps is deliberately FLAT: there is no fork/edit/sync phase
// ordering, and neither a replica's diff nor its clock is cached between steps —
// every `sync` and `deliver` recomputes `from.Diff(to.Frontier())` at the moment
// it runs. That is what lets a schedule MUTATE a replica after a sync INTO it
// (apply_update_advances_counter.json), where the post-sync write must be minted
// against the counter the ingest advanced. A runner that hoisted the diff, or
// that assumed all local edits precede all syncs, would replay that fixture as a
// different schedule and could not see the bug it exists for.
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
		selected, err := ltSelectDelivered(full.Ops, step.Deliver)
		if err != nil {
			w.t.Fatalf("%v", err)
		}
		// ONE ApplyUpdate for the whole selection: the batch is the unit the
		// dependency buffer drains over.
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
	for _, sv := range typedScenarioViews(filepath.Join("lossless-tree", name), fixture.Scenarios,
		func(s ltScenario) (string, string) { return s.Id, s.Name }) {
		label := name + "[" + sv.Label() + "]"
		t.Run(label, func(t *testing.T) {
			// Rung 4 books HERE (#lzscenariobodyskip), on the payload handoff
			// inside the subtest — never at the loop header, which cannot tell a
			// body that replayed from one that returned early.
			scenario := sv.Value()
			world := newLtWorld(t)
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
		// #lzspecoutoforderfixtures (lazily-spec 39df4b3). These two are the
		// first fixtures that discriminate ApplyUpdate's two ingest obligations:
		// advancing the Lamport counter past every observed op unconditionally
		// and BEFORE the idempotence skip, and BUFFERING an op whose dependency
		// has not arrived instead of dropping it while recording its dot.
		"apply_update_advances_counter.json",
		"out_of_order_delivery_buffers.json",
	}
	for _, name := range fixtures {
		name := name
		t.Run(name, func(t *testing.T) {
			runLosslessTreeFixture(t, name)
		})
	}
}

// ---------------------------------------------------------------------------
// `deliver` selector contract (#lzspecoutoforderfixtures)
// ---------------------------------------------------------------------------
//
// The fixtures cannot police these: a clamped index or a defaulted selector
// still produces a plausible batch, and the two fixtures that use `deliver`
// would go green over both. Only a direct assertion catches it.

func ltDiffFixtureOps(t *testing.T) []TreeOp {
	t.Helper()
	tree := NewLosslessTreeCrdt(1)
	para := tree.CreateNode(TreeRoot, nil, TreeNodeSeedElement{Kind: "para"})
	one := tree.CreateNode(para, nil, TreeNodeSeedLeaf{Kind: LeafKindTrivia, Text: "1"})
	tree.CreateNode(para, &one, TreeNodeSeedLeaf{Kind: LeafKindTrivia, Text: "2"})
	ops := tree.Diff(NewTreeVersionFrontier()).Ops
	if len(ops) != 3 {
		t.Fatalf("setup: want a 3-op diff, got %d", len(ops))
	}
	return ops
}

func TestLosslessTreeDeliverOrderIsHonouredExactly(t *testing.T) {
	ops := ltDiffFixtureOps(t)

	selected, err := ltSelectDelivered(ops, &ltDeliver{From: "a", To: "b", Order: []int{2, 1, 0}})
	if err != nil {
		t.Fatalf("order [2,1,0]: unexpected error: %v", err)
	}
	if len(selected) != 3 {
		t.Fatalf("order [2,1,0]: want 3 ops, got %d", len(selected))
	}
	// Exactly the listed SEQUENCE — not re-sorted back into canonical order,
	// which would silently defeat out_of_order_delivery_buffers.json.
	for i, want := range []int{2, 1, 0} {
		if selected[i].Id != ops[want].Id {
			t.Fatalf("order [2,1,0]: position %d = %s, want %s (a re-sorted delivery is not an out-of-order delivery)",
				i, selected[i].Id, ops[want].Id)
		}
	}

	// `only` keeps its meaning: that SUBSET, in canonical order, whatever order
	// the indexes are listed in.
	subset, err := ltSelectDelivered(ops, &ltDeliver{From: "a", To: "b", Only: []int{2, 0}})
	if err != nil {
		t.Fatalf("only [2,0]: unexpected error: %v", err)
	}
	if len(subset) != 2 || subset[0].Id != ops[0].Id || subset[1].Id != ops[2].Id {
		t.Fatalf("only [2,0] must deliver ops 0 and 2 in canonical order; got %v", treeOpIds(subset))
	}
}

func TestLosslessTreeDeliverRejectsOutOfRangeIndexRatherThanClamping(t *testing.T) {
	ops := ltDiffFixtureOps(t)

	for _, tc := range []struct {
		name    string
		deliver ltDeliver
	}{
		{"order past the end", ltDeliver{From: "a", To: "b", Order: []int{0, 3}}},
		{"only past the end", ltDeliver{From: "a", To: "b", Only: []int{0, 7}}},
		{"negative index", ltDeliver{From: "a", To: "b", Order: []int{-1}}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			selected, err := ltSelectDelivered(ops, &tc.deliver)
			if err == nil {
				t.Fatalf("out-of-range index must FAIL the fixture, not be clamped; got %d ops back", len(selected))
			}
			if !strings.Contains(err.Error(), "out of range") {
				t.Fatalf("error must name the out-of-range index; got %v", err)
			}
		})
	}
}

func TestLosslessTreeDeliverRequiresExactlyOneSelector(t *testing.T) {
	ops := ltDiffFixtureOps(t)

	if _, err := ltSelectDelivered(ops, &ltDeliver{From: "a", To: "b", Only: []int{0}, Order: []int{0}}); err == nil {
		t.Fatalf("a deliver step with BOTH `only` and `order` must be rejected, not resolved by precedence")
	} else if !strings.Contains(err.Error(), "BOTH") {
		t.Fatalf("error must say both selectors are present; got %v", err)
	}

	if _, err := ltSelectDelivered(ops, &ltDeliver{From: "a", To: "b"}); err == nil {
		t.Fatalf("a deliver step with NEITHER selector must be rejected, not defaulted to the whole diff")
	} else if !strings.Contains(err.Error(), "NEITHER") {
		t.Fatalf("error must say no selector is present; got %v", err)
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
