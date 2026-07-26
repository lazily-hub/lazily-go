package lazily

// Shared keyed-collection conformance suite.
//
// Replays the canonical lazily-spec collection fixtures identically to every
// other binding (lazily-rs, lazily-dart, lazily-js, ...). Each fixture is
// COMPUTE: load the initial state, replay each step's op, and assert the
// observable effects — reader-class invalidation (value/membership/order), the
// atomic-move stable-handle invariant, LIS move-minimized reconciliation, CRDT
// convergence (order/values/tombstones) with idempotent/commutative merge,
// delta sync, incremental memoized fold, and manufactured-identity alignment.
//
// Mirrors lazily-dart test/collections_conformance_test.dart and
// collections_extra_conformance_test.dart. Fixtures resolve via a relative-path
// helper; absent fixtures cause a t.Skip rather than a failure.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// --- fixture loading -------------------------------------------------------

func loadCollectionFixture(t *testing.T, name string) (map[string]any, bool) {
	t.Helper()
	candidates := []string{
		filepath.Join("..", "lazily-spec", "conformance", "collections", name),
		filepath.Join("conformance", "collections", name),
		filepath.Join("testdata", "conformance", "collections", name),
	}
	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var fixture map[string]any
		if err := json.Unmarshal(data, &fixture); err != nil {
			t.Fatalf("%s: parse: %v", name, err)
		}
		return fixture, true
	}
	t.Skipf("collections fixture not found: %s", name)
	return nil, false
}

// --- small typed accessors over decoded JSON -------------------------------

func jsInt(v any) int    { return int(v.(float64)) }
func jsStr(v any) string { return v.(string) }
func jsMap(v any) map[string]any {
	if v == nil {
		return nil
	}
	return v.(map[string]any)
}
func jsList(v any) []any {
	if v == nil {
		return nil
	}
	return v.([]any)
}
func jsStrList(v any) []string {
	raw := jsList(v)
	out := make([]string, len(raw))
	for i, e := range raw {
		out[i] = jsStr(e)
	}
	return out
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
	}
	for _, n := range m {
		if n != 0 {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// SourceMap steps: value / membership / order reactivity independence + moves
// ---------------------------------------------------------------------------

func applySourceMapOp(m *SourceMap[string, int], op map[string]any) {
	switch op["type"].(string) {
	case "set_value":
		m.Set(jsStr(op["key"]), jsInt(op["value"]))
	case "insert":
		key := jsStr(op["key"])
		value := jsInt(op["value"])
		switch at := op["at"].(type) {
		case string:
			switch at {
			case "front":
				m.Set(key, value)
				m.MoveTo(key, 0)
			default: // "end"
				m.Set(key, value)
			}
		case float64:
			m.Set(key, value)
			m.MoveTo(key, int(at))
		default:
			m.Set(key, value)
		}
	case "remove":
		m.Remove(jsStr(op["key"]))
	case "move_to":
		m.MoveTo(jsStr(op["key"]), jsInt(op["index"]))
	case "move_before":
		m.MoveBefore(jsStr(op["key"]), jsStr(op["before"]))
	case "move_after":
		m.MoveAfter(jsStr(op["key"]), jsStr(op["after"]))
	default:
		panic("unknown cellmap op: " + op["type"].(string))
	}
}

func runSourceMapStepsFixture(t *testing.T, name string) {
	fixture, ok := loadCollectionFixture(t, name)
	if !ok {
		return
	}
	// The fixture must actually be a source-map fixture. Accepts both the
	// current "SourceMap" spelling and the pre-v2-kernel "CellMap" spelling.
	if model := jsStr(fixture["model"]); !isSourceMapModel(model) {
		t.Fatalf("%s: fixture model = %q, want SourceMap (or the deprecated CellMap spelling)", name, model)
	}

	ctx := NewContext()
	m := NewSourceMap[string, int](ctx)

	initial := jsMap(fixture["initial"])
	values := jsMap(initial["values"])
	for _, k := range jsStrList(initial["order"]) {
		m.Set(k, jsInt(values[k]))
	}

	for i, rawStep := range jsList(fixture["steps"]) {
		step := jsMap(rawStep)
		op := jsMap(step["op"])
		expected := jsMap(step["expected"])
		invalidates := jsMap(expected["invalidates"])

		// Prime readers against the CURRENT key set so each step's invalidation
		// is measured in isolation (matches lazily-rs / lazily-dart).
		valueReaders := map[string]*Computed[int]{}
		for _, k := range m.Keys(ctx) {
			key := k
			slot := NewSlot(ctx, func(c *Compute) int { return Get(c, m.Cell(key)) })
			slot.Get() // prime
			valueReaders[key] = slot
		}
		membershipReader := NewSlot(ctx, func(c *Compute) int { Get(c, m.membership); return 0 })
		membershipReader.Get()
		orderReader := NewSlot(ctx, func(c *Compute) int { Get(c, m.orderSignal); return 0 })
		orderReader.Get()

		// Snapshot handles for stability assertions.
		handleStable := jsMap(expected["handle_stable"])
		handlesBefore := map[string]*Source[int]{}
		for k, v := range handleStable {
			if v == true {
				handlesBefore[k] = m.Cell(k)
			}
		}

		applySourceMapOp(m, op)

		// Value readers: only survivor keys are checked (removed keys are not).
		expectedValueInval := map[string]bool{}
		for _, k := range jsStrList(invalidates["value"]) {
			expectedValueInval[k] = true
		}
		survivors := map[string]bool{}
		for _, k := range m.Keys(ctx) {
			survivors[k] = true
		}
		for k, slot := range valueReaders {
			if !survivors[k] {
				continue
			}
			_, warm := slot.Peek()
			wantInval := expectedValueInval[k]
			if warm == wantInval {
				t.Errorf("%s step %d %s: value reader %q warm=%v, wantInvalidated=%v",
					name, i, op["type"], k, warm, wantInval)
			}
		}

		_, memWarm := membershipReader.Peek()
		memInval := invalidates["membership"] == true
		if memWarm == memInval {
			t.Errorf("%s step %d %s: membership warm=%v, wantInvalidated=%v",
				name, i, op["type"], memWarm, memInval)
		}
		_, ordWarm := orderReader.Peek()
		ordInval := invalidates["order"] == true
		if ordWarm == ordInval {
			t.Errorf("%s step %d %s: order warm=%v, wantInvalidated=%v",
				name, i, op["type"], ordWarm, ordInval)
		}

		// Resulting state.
		if got := m.Keys(ctx); !reflect.DeepEqual(got, jsStrList(expected["order"])) {
			t.Errorf("%s step %d %s: order = %v, want %v", name, i, op["type"], got, jsStrList(expected["order"]))
		}
		if !sameStringSet(m.Keys(ctx), jsStrList(expected["membership"])) {
			t.Errorf("%s step %d %s: membership = %v, want set %v", name, i, op["type"], m.Keys(ctx), jsStrList(expected["membership"]))
		}
		if ev := jsMap(expected["values"]); ev != nil {
			for k, want := range ev {
				if got, _ := m.Get(k); got != jsInt(want) {
					t.Errorf("%s step %d %s: value[%s] = %d, want %d", name, i, op["type"], k, got, jsInt(want))
				}
			}
		}

		// Handle stability: same *Cell identity before and after.
		for k, before := range handlesBefore {
			after := m.Cell(k)
			if after == nil {
				t.Errorf("%s step %d: handle %q missing after op", name, i, k)
			} else if before != after {
				t.Errorf("%s step %d %s: handle %q not stable", name, i, op["type"], k)
			}
		}
	}
}

func TestCollectionsSourceMapIndependence(t *testing.T) {
	runSourceMapStepsFixture(t, "cellmap_independence.json")
}

func TestCollectionsSourceMapAtomicMove(t *testing.T) {
	runSourceMapStepsFixture(t, "cellmap_atomic_move.json")
}

// ---------------------------------------------------------------------------
// Keyed reconciliation (LIS move-minimized)
// ---------------------------------------------------------------------------

func TestCollectionsKeyedReconciliationLIS(t *testing.T) {
	name := "keyed_reconciliation_lis.json"
	fixture, ok := loadCollectionFixture(t, name)
	if !ok {
		return
	}
	reconcile := jsMap(fixture["reconcile"])
	expected := jsMap(fixture["expected"])

	pairs := func(state map[string]any) []KeyValue[string, int] {
		values := jsMap(state["values"])
		out := []KeyValue[string, int]{}
		for _, k := range jsStrList(state["order"]) {
			out = append(out, KeyValue[string, int]{Key: k, Value: jsInt(values[k])})
		}
		return out
	}
	prior := pairs(jsMap(reconcile["prior"]))
	target := pairs(jsMap(reconcile["target"]))
	resultOrder := jsStrList(expected["result_order"])

	ops := ReconcileDiff(prior, target)
	expectedOps := jsList(expected["ops"])
	if len(ops) != len(expectedOps) {
		t.Fatalf("%s: op count = %d, want %d (%v)", name, len(ops), len(expectedOps), ops)
	}
	for i, rawWant := range expectedOps {
		want := jsMap(rawWant)
		switch want["type"].(string) {
		case "remove":
			rem, isRem := ops[i].(DiffOpRemove[string, int])
			if !isRem {
				t.Errorf("%s op[%d]: want Remove, got %T", name, i, ops[i])
			} else if rem.Key != jsStr(want["key"]) {
				t.Errorf("%s op[%d]: remove key = %q, want %q", name, i, rem.Key, jsStr(want["key"]))
			}
		case "move":
			mv, isMv := ops[i].(DiffOpMove[string, int])
			if !isMv {
				t.Errorf("%s op[%d]: want Move, got %T", name, i, ops[i])
				continue
			}
			if mv.Key != jsStr(want["key"]) {
				t.Errorf("%s op[%d]: move key = %q, want %q", name, i, mv.Key, jsStr(want["key"]))
			}
			// Resolve the fixture's relative anchor to a final index.
			anchorIdx := func(anchor string) int {
				for j, k := range resultOrder {
					if k == anchor {
						return j
					}
				}
				return -1
			}
			if after, ok := want["after"]; ok {
				if got, exp := mv.To, anchorIdx(jsStr(after))+1; got != exp {
					t.Errorf("%s op[%d]: move to = %d, want %d", name, i, got, exp)
				}
			} else if before, ok := want["before"]; ok {
				if got, exp := mv.To, anchorIdx(jsStr(before)); got != exp {
					t.Errorf("%s op[%d]: move to = %d, want %d", name, i, got, exp)
				}
			}
		case "insert":
			ins, isIns := ops[i].(DiffOpInsert[string, int])
			if !isIns {
				t.Errorf("%s op[%d]: want Insert, got %T", name, i, ops[i])
			} else if ins.Key != jsStr(want["key"]) {
				t.Errorf("%s op[%d]: insert key = %q, want %q", name, i, ins.Key, jsStr(want["key"]))
			}
		case "update":
			upd, isUpd := ops[i].(DiffOpUpdate[string, int])
			if !isUpd {
				t.Errorf("%s op[%d]: want Update, got %T", name, i, ops[i])
			} else if upd.Key != jsStr(want["key"]) {
				t.Errorf("%s op[%d]: update key = %q, want %q", name, i, upd.Key, jsStr(want["key"]))
			}
		}
	}

	// Convergence: applying the minimal op set reproduces result_order.
	ctx := NewContext()
	m := NewSourceMap[string, int](ctx)
	for _, e := range prior {
		m.Set(e.Key, e.Value)
	}
	targetOrder := make([]string, len(target))
	targetValues := map[string]int{}
	for i, e := range target {
		targetOrder[i] = e.Key
		targetValues[e.Key] = e.Value
	}
	m.Reconcile(targetOrder, targetValues)
	if got := m.Keys(ctx); !reflect.DeepEqual(got, resultOrder) {
		t.Errorf("%s: convergence order = %v, want %v", name, got, resultOrder)
	}

	// stable_keys_not_invalidated: prime readers, run a no-op reconcile, assert warm.
	stableKeys := jsStrList(expected["stable_keys_not_invalidated"])
	readers := map[string]*Computed[int]{}
	for _, k := range stableKeys {
		key := k
		slot := NewSlot(ctx, func(c *Compute) int { v, _ := m.Read(key); return v })
		slot.Get()
		readers[key] = slot
	}
	m.Reconcile(targetOrder, targetValues) // no-op
	for _, k := range stableKeys {
		if _, warm := readers[k].Peek(); !warm {
			t.Errorf("%s: stable key %q invalidated by sibling reorder", name, k)
		}
	}
}

// ---------------------------------------------------------------------------
// SemTree incremental memoized fold
// ---------------------------------------------------------------------------

func parseTreeNode(m map[string]any) TreeNodeSpec[int] {
	spec := TreeNodeSpec[int]{ID: jsStr(m["id"]), Value: jsInt(m["value"])}
	if cd := jsMap(m["children"]); cd != nil {
		children := &TreeNodeChildren[int]{Values: map[string]TreeNodeSpec[int]{}}
		children.Order = jsStrList(cd["order"])
		for k, v := range jsMap(cd["values"]) {
			children.Values[k] = parseTreeNode(jsMap(v))
		}
		spec.Children = children
	}
	return spec
}

func semFold(name string) FoldFn[int, int] {
	switch name {
	case "sum":
		return func(value int, childDerived []int) int {
			s := value
			for _, d := range childDerived {
				s += d
			}
			return s
		}
	case "count_positive":
		return func(value int, childDerived []int) int {
			c := 0
			if value > 0 {
				c++
			}
			for _, d := range childDerived {
				if d > 0 {
					c++
				}
			}
			return c
		}
	default:
		panic("unknown fold: " + name)
	}
}

func TestCollectionsSemTreeIncremental(t *testing.T) {
	name := "semtree_incremental.json"
	fixture, ok := loadCollectionFixture(t, name)
	if !ok {
		return
	}
	for _, rawScenario := range jsList(fixture["scenarios"]) {
		scenario := jsMap(rawScenario)
		t.Run(jsStr(scenario["name"]), func(t *testing.T) {
			ctx := NewContext()
			fold := semFold(jsStr(scenario["fold"]))
			tree := BuildSemTree(ctx, parseTreeNode(jsMap(scenario["tree"])), fold)

			// Prime + assert initial derived values.
			if init := jsMap(scenario["expect_initial"]); init != nil {
				for id, want := range init {
					if id == "sibling_a_cached" || id == "downstream_consumer_reran" {
						continue
					}
					if got, _ := tree.NodeValue(id); got != jsInt(want) {
						t.Errorf("initial %s = %d, want %d", id, got, jsInt(want))
					}
				}
			}

			// The memo-guard contract is "downstream did not RE-RUN", so it is
			// asserted with a real downstream consumer and a run counter, the
			// way lazily-rs (tests/collections_conformance.rs) and lazily-js
			// (test/sem-tree.test.js) assert it. It used to be approximated
			// here by IsCached("root"), which is only equivalent under eager
			// invalidation: with the pull-time memo guard the root is marked
			// stale by the write and becomes clean again when it is read, so
			// the cached flag measures whether a read has happened yet rather
			// than whether the fold re-ran. That proxy also made the outcome
			// depend on Go's randomized map iteration order over expect_after,
			// since the root's value is read from the same loop.
			after := jsMap(scenario["expect_after"])
			downstreamRuns := 0
			var downstream *Computed[int]
			if _, checked := after["downstream_consumer_reran"]; checked {
				root := tree.RootHandle()
				downstream = NewSlot(ctx, func(c *Compute) int {
					downstreamRuns++
					return Get(c, root)
				})
				downstream.Get() // prime
			}
			runsBefore := downstreamRuns

			if edit := jsMap(scenario["edit"]); edit != nil {
				if err := tree.SetValue(jsStr(edit["id"]), jsInt(edit["value"])); err != nil {
					t.Fatalf("edit: %v", err)
				}
				if downstream != nil {
					downstream.Get() // pull the consumer, as rs and js do
				}
				checkSemTreeAfter(t, tree, after, downstreamRuns > runsBefore)
			}

			if rc := jsMap(scenario["remove_child"]); rc != nil {
				if err := tree.RemoveChild(jsStr(rc["parent"]), jsStr(rc["child"])); err != nil {
					t.Fatalf("remove_child: %v", err)
				}
				if downstream != nil {
					downstream.Get() // pull the consumer, as rs and js do
				}
				checkSemTreeAfter(t, tree, after, downstreamRuns > runsBefore)
			}
		})
	}
}

func checkSemTreeAfter(t *testing.T, tree *SemTree[int, int], after map[string]any, didRerun bool) {
	t.Helper()
	if after == nil {
		return
	}
	for id, want := range after {
		switch id {
		case "sibling_a_cached":
			if got := tree.IsCached("a"); got != (want == true) {
				t.Errorf("sibling_a_cached = %v, want %v", got, want)
			}
		case "downstream_consumer_reran":
			if didRerun != (want == true) {
				t.Errorf("downstream_consumer_reran = %v, want %v (memo guard)",
					didRerun, want)
			}
		default:
			if got, _ := tree.NodeValue(id); got != jsInt(want) {
				t.Errorf("after %s = %d, want %d", id, got, jsInt(want))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// SeqCrdt convergence
// ---------------------------------------------------------------------------

func TestCollectionsSeqCrdtConvergence(t *testing.T) {
	name := "seqcrdt_convergence.json"
	fixture, ok := loadCollectionFixture(t, name)
	if !ok {
		return
	}
	for _, rawScenario := range jsList(fixture["scenarios"]) {
		scenario := jsMap(rawScenario)
		t.Run(jsStr(scenario["name"]), func(t *testing.T) {
			replicas := map[string]*SeqCrdt[string, any]{}

			seedPeer := int64(1)
			if r := jsMap(scenario["replica"]); r != nil {
				seedPeer = int64(jsInt(r["peer"]))
			}
			if seed := jsMap(scenario["seed"]); seed != nil {
				seedPeer = int64(jsInt(seed["peer"]))
				replicas["a"] = NewSeqCrdt[string, any](seedPeer)
				for _, rawIns := range jsList(seed["inserts"]) {
					ins := jsMap(rawIns)
					replicas["a"].InsertBack(jsStr(ins["id"]), ins["value"], int64(jsInt(ins["now"])))
				}
			} else {
				replicas["a"] = NewSeqCrdt[string, any](seedPeer)
			}

			for _, rawStep := range jsList(scenario["steps"]) {
				applySeqCrdtStep(replicas, jsMap(rawStep))
			}
			checkSeqCrdtExpect(t, replicas, jsMap(scenario["expect"]))
		})
	}
}

func applySeqCrdtStep(replicas map[string]*SeqCrdt[string, any], step map[string]any) {
	on := "a"
	if v, ok := step["on"]; ok {
		on = jsStr(v)
	}
	if fork, ok := step["fork"]; ok {
		replicas[jsStr(fork)] = replicas[on].Fork(int64(jsInt(step["peer"])))
		return
	}
	if clone, ok := step["clone"]; ok {
		replicas[jsStr(clone)] = replicas[jsStr(step["from"])].Clone()
		return
	}
	if merge := jsMap(step["merge"]); merge != nil {
		replicas[jsStr(merge["into"])].Merge(replicas[jsStr(merge["from"])], int64(jsInt(step["now"])))
		return
	}
	op := jsStr(step["op"])
	crdt := replicas[on]
	now := int64(jsInt(step["now"]))
	id := jsStr(step["id"])
	switch op {
	case "insert_back":
		crdt.InsertBack(id, step["value"], now)
	case "insert_front":
		crdt.InsertFront(id, step["value"], now)
	case "set_value":
		crdt.SetValue(id, step["value"], now)
	case "move_after":
		crdt.MoveAfter(id, jsStr(step["anchor"]), now)
	case "move_before":
		crdt.MoveBefore(id, jsStr(step["anchor"]), now)
	case "remove":
		crdt.Remove(id, now)
	default:
		panic("unknown seqcrdt op: " + op)
	}
}

func checkSeqCrdtExpect(t *testing.T, replicas map[string]*SeqCrdt[string, any], expect map[string]any) {
	if expect == nil {
		return
	}
	primary := "a"
	if oe := jsList(expect["orders_equal"]); len(oe) > 0 {
		primary = jsStr(jsList(oe[0])[0])
	}
	if oo := jsMap(expect["order_on"]); len(oo) > 0 {
		for k := range oo {
			primary = k
			break
		}
	}

	if order, ok := expect["order"]; ok {
		if got, want := replicas[primary].Order(), jsStrList(order); !reflect.DeepEqual(got, want) {
			t.Errorf("order = %v, want %v", got, want)
		}
	}
	if l, ok := expect["len"]; ok {
		if got := replicas[primary].Len(); got != jsInt(l) {
			t.Errorf("len = %d, want %d", got, jsInt(l))
		}
	}
	if get := jsMap(expect["get"]); get != nil {
		for id, want := range get {
			got, _ := replicas[primary].Get(id)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("get %s = %v, want %v", id, got, want)
			}
		}
	}
	for _, rawPair := range jsList(expect["orders_equal"]) {
		pair := jsList(rawPair)
		a, b := jsStr(pair[0]), jsStr(pair[1])
		if !reflect.DeepEqual(replicas[a].Order(), replicas[b].Order()) {
			t.Errorf("orders_equal %s/%s: %v != %v", a, b, replicas[a].Order(), replicas[b].Order())
		}
	}
	for _, id := range jsList(expect["contains_all"]) {
		if !replicas[primary].Contains(jsStr(id)) {
			t.Errorf("contains_all: missing %v", id)
		}
	}
	if oo := jsMap(expect["order_on"]); oo != nil {
		for rep, want := range oo {
			if got := replicas[rep].Order(); !reflect.DeepEqual(got, jsStrList(want)) {
				t.Errorf("order_on %s = %v, want %v", rep, got, jsStrList(want))
			}
		}
	}
	if go_ := jsMap(expect["get_on"]); go_ != nil {
		for rep, kvs := range go_ {
			for id, want := range jsMap(kvs) {
				got, _ := replicas[rep].Get(id)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("get_on %s/%s = %v, want %v", rep, id, got, want)
				}
			}
		}
	}
	if nc := jsMap(expect["not_contains_on"]); nc != nil {
		for rep, ids := range nc {
			for _, id := range jsList(ids) {
				if replicas[rep].Contains(jsStr(id)) {
					t.Errorf("not_contains_on %s: still contains %v", rep, id)
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// TextCrdt convergence
// ---------------------------------------------------------------------------

func TestCollectionsTextCrdtConvergence(t *testing.T) {
	name := "textcrdt_convergence.json"
	fixture, ok := loadCollectionFixture(t, name)
	if !ok {
		return
	}
	for _, rawScenario := range jsList(fixture["scenarios"]) {
		scenario := jsMap(rawScenario)
		t.Run(jsStr(scenario["name"]), func(t *testing.T) {
			replicas := map[string]*TextCrdt{}
			replicas["a"] = seedTextCrdt(scenario)
			for _, rawStep := range jsList(scenario["steps"]) {
				applyTextCrdtStep(t, replicas, jsMap(rawStep))
			}
			checkTextCrdtExpect(t, replicas, jsMap(scenario["expect"]))
		})
	}
}

func seedTextCrdt(scenario map[string]any) *TextCrdt {
	seed := scenario["seed"]
	replicaSpec := jsMap(scenario["replica"])
	switch s := seed.(type) {
	case string:
		peer := int64(1)
		if replicaSpec != nil {
			peer = int64(jsInt(replicaSpec["peer"]))
		}
		return TextCrdtFromStr(peer, s)
	case map[string]any:
		return TextCrdtFromStr(int64(jsInt(s["peer"])), jsStr(s["text"]))
	default:
		if replicaSpec != nil {
			return NewTextCrdt(int64(jsInt(replicaSpec["peer"])))
		}
		return NewTextCrdt(1)
	}
}

func applyTextCrdtStep(t *testing.T, replicas map[string]*TextCrdt, step map[string]any) {
	on := "a"
	if v, ok := step["on"]; ok {
		on = jsStr(v)
	}
	if fork, ok := step["fork"]; ok {
		replicas[jsStr(fork)] = replicas[on].Fork(int64(jsInt(step["peer"])))
		return
	}
	if clone, ok := step["clone"]; ok {
		replicas[jsStr(clone)] = replicas[jsStr(step["from"])].Clone()
		return
	}
	if merge := jsMap(step["merge"]); merge != nil {
		replicas[jsStr(merge["into"])].Merge(replicas[jsStr(merge["from"])])
		return
	}
	op := jsStr(step["op"])
	crdt := replicas[on]
	switch op {
	case "insert":
		crdt.Insert(jsInt(step["index"]), jsStr(step["ch"]))
	case "insert_str":
		crdt.InsertStr(jsInt(step["index"]), jsStr(step["str"]))
	case "delete":
		crdt.Delete(jsInt(step["index"]))
	case "gc":
		stable := step["stable"] == true
		collected := crdt.GcWith(func(OpId) bool { return stable })
		if ec, ok := step["expect_collected"]; ok {
			if collected != jsInt(ec) {
				t.Errorf("gc collected = %d, want %d", collected, jsInt(ec))
			}
		}
	default:
		panic("unknown textcrdt op: " + op)
	}
}

func checkTextCrdtExpect(t *testing.T, replicas map[string]*TextCrdt, expect map[string]any) {
	if expect == nil {
		return
	}
	if text, ok := expect["text"]; ok {
		if got := replicas["a"].Text(); got != jsStr(text) {
			t.Errorf("text = %q, want %q", got, jsStr(text))
		}
	}
	if l, ok := expect["len"]; ok {
		if got := replicas["a"].Len(); got != jsInt(l) {
			t.Errorf("len = %d, want %d", got, jsInt(l))
		}
	}
	for _, rawPair := range jsList(expect["texts_equal"]) {
		pair := jsList(rawPair)
		a, b := jsStr(pair[0]), jsStr(pair[1])
		if replicas[a].Text() != replicas[b].Text() {
			t.Errorf("texts_equal %s/%s: %q != %q", a, b, replicas[a].Text(), replicas[b].Text())
		}
	}
	if p, ok := expect["a_starts_with"]; ok {
		if txt := replicas["a"].Text(); len(txt) < len(jsStr(p)) || txt[:len(jsStr(p))] != jsStr(p) {
			t.Errorf("a_starts_with %q: text = %q", jsStr(p), txt)
		}
	}
	if s, ok := expect["a_ends_with"]; ok {
		txt := replicas["a"].Text()
		suf := jsStr(s)
		if len(txt) < len(suf) || txt[len(txt)-len(suf):] != suf {
			t.Errorf("a_ends_with %q: text = %q", suf, txt)
		}
	}
	if tc, ok := expect["tombstone_count"]; ok {
		if got := replicas["a"].TombstoneCount(); got != jsInt(tc) {
			t.Errorf("tombstone_count = %d, want %d", got, jsInt(tc))
		}
	}
}

// ---------------------------------------------------------------------------
// TextCrdt delta sync (#lztextsync)
// ---------------------------------------------------------------------------

func TestCollectionsTextCrdtDeltaSync(t *testing.T) {
	name := "textcrdt_delta_sync.json"
	fixture, ok := loadCollectionFixture(t, name)
	if !ok {
		return
	}
	for _, rawScenario := range jsList(fixture["scenarios"]) {
		scenario := jsMap(rawScenario)
		t.Run(jsStr(scenario["name"]), func(t *testing.T) {
			replicas := map[string]*TextCrdt{}
			seed := jsMap(scenario["seed"])
			replicas["a"] = TextCrdtFromStr(int64(jsInt(seed["peer"])), jsStr(seed["text"]))
			for _, rawStep := range jsList(scenario["steps"]) {
				applyTextCrdtDeltaStep(t, replicas, jsMap(rawStep))
			}
			checkTextCrdtDeltaExpect(t, replicas, jsMap(scenario["expect"]))
		})
	}
}

func applyTextCrdtDeltaStep(t *testing.T, replicas map[string]*TextCrdt, step map[string]any) {
	on := "a"
	if v, ok := step["on"]; ok {
		on = jsStr(v)
	}
	if fork, ok := step["fork"]; ok {
		replicas[jsStr(fork)] = replicas[on].Fork(int64(jsInt(step["peer"])))
		return
	}
	if nw, ok := step["new"]; ok {
		replicas[jsStr(nw)] = NewTextCrdt(int64(jsInt(step["peer"])))
		return
	}
	if snap := jsMap(step["snapshot"]); snap != nil {
		ops := replicas[jsStr(snap["from"])].DeltaSince(nil)
		into := NewTextCrdt(int64(jsInt(snap["peer"])))
		replicas[jsStr(snap["into"])] = into
		changed := into.ApplyDelta(ops)
		if ec, ok := step["expect_changed"]; ok && changed != (ec == true) {
			t.Errorf("snapshot apply_delta changed = %v, want %v", changed, ec)
		}
		return
	}
	if ex := jsList(step["exchange"]); ex != nil {
		a, b := jsStr(ex[0]), jsStr(ex[1])
		opsAB := replicas[a].DeltaSince(replicas[b].VersionVector())
		opsBA := replicas[b].DeltaSince(replicas[a].VersionVector())
		replicas[a].ApplyDelta(opsBA)
		replicas[b].ApplyDelta(opsAB)
		return
	}
	if delta := jsMap(step["delta"]); delta != nil {
		into := jsStr(delta["into"])
		from := jsStr(delta["from"])
		ops := replicas[from].DeltaSince(replicas[into].VersionVector())
		changed := replicas[into].ApplyDelta(ops)
		if ec, ok := step["expect_changed"]; ok && changed != (ec == true) {
			t.Errorf("delta apply changed = %v, want %v", changed, ec)
		}
		return
	}
	op := jsStr(step["op"])
	crdt := replicas[on]
	switch op {
	case "insert":
		crdt.Insert(jsInt(step["index"]), jsStr(step["ch"]))
	case "insert_str":
		crdt.InsertStr(jsInt(step["index"]), jsStr(step["str"]))
	case "delete":
		crdt.Delete(jsInt(step["index"]))
	default:
		panic("unknown textcrdt delta op: " + op)
	}
}

func checkTextCrdtDeltaExpect(t *testing.T, replicas map[string]*TextCrdt, expect map[string]any) {
	if expect == nil {
		return
	}
	for _, rawPair := range jsList(expect["texts_equal"]) {
		pair := jsList(rawPair)
		a, b := jsStr(pair[0]), jsStr(pair[1])
		if replicas[a].Text() != replicas[b].Text() {
			t.Errorf("texts_equal %s/%s: %q != %q", a, b, replicas[a].Text(), replicas[b].Text())
		}
	}
	if to := jsMap(expect["text_on"]); to != nil {
		for rep, want := range to {
			if got := replicas[rep].Text(); got != jsStr(want) {
				t.Errorf("text_on %s = %q, want %q", rep, got, jsStr(want))
			}
		}
	}
	if vv := jsMap(expect["version_vector_on"]); vv != nil {
		for rep, want := range vv {
			got := replicas[rep].VersionVector()
			wantMap := map[PeerId]int64{}
			for k, v := range jsMap(want) {
				peer := int64(0)
				// keys are decimal strings
				for _, r := range k {
					peer = peer*10 + int64(r-'0')
				}
				wantMap[peer] = int64(jsInt(v))
			}
			if !reflect.DeepEqual(got, wantMap) {
				t.Errorf("version_vector_on %s = %v, want %v", rep, got, wantMap)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Stable-id alignment
// ---------------------------------------------------------------------------

func parseBlock(m map[string]any) Block {
	if a, ok := m["anchor"]; ok {
		return NewAnchoredBlock(jsStr(a), jsStr(m["text"]))
	}
	return NewBlock(jsStr(m["text"]))
}

func TestCollectionsStableIdAlignment(t *testing.T) {
	name := "stableid_alignment.json"
	fixture, ok := loadCollectionFixture(t, name)
	if !ok {
		return
	}
	for _, rawScenario := range jsList(fixture["scenarios"]) {
		scenario := jsMap(rawScenario)
		t.Run(jsStr(scenario["name"]), func(t *testing.T) {
			expect := jsMap(scenario["expect"])

			if blocksField := jsList(scenario["blocks"]); blocksField != nil {
				keys := make([]BlockKey, len(blocksField))
				for i, b := range blocksField {
					keys[i] = BlockKeyOf(parseBlock(jsMap(b)))
				}
				for _, rawPair := range jsList(expect["key_equal"]) {
					pair := jsList(rawPair)
					i, j := jsInt(pair[0]), jsInt(pair[1])
					if !keys[i].Equals(keys[j]) {
						t.Errorf("key_equal [%d,%d]: %s != %s", i, j, keys[i], keys[j])
					}
				}
				for _, rawPair := range jsList(expect["key_not_equal"]) {
					pair := jsList(rawPair)
					i, j := jsInt(pair[0]), jsInt(pair[1])
					if keys[i].Equals(keys[j]) {
						t.Errorf("key_not_equal [%d,%d]: %s == %s", i, j, keys[i], keys[j])
					}
				}
				return
			}

			oldBlocks := make([]Block, 0)
			for _, b := range jsList(scenario["old"]) {
				oldBlocks = append(oldBlocks, parseBlock(jsMap(b)))
			}
			newBlocks := make([]Block, 0)
			for _, b := range jsList(scenario["new"]) {
				newBlocks = append(newBlocks, parseBlock(jsMap(b)))
			}

			if matches := jsList(expect["matches"]); matches != nil {
				alignment := Align(oldBlocks, newBlocks)
				for i, want := range matches {
					if got := alignment.NewMatches[i].String(); got != jsStr(want) {
						t.Errorf("match[%d] = %q, want %q", i, got, jsStr(want))
					}
				}
			}
			if removed, ok := expect["removed"]; ok {
				alignment := Align(oldBlocks, newBlocks)
				wantRemoved := []int{}
				for _, r := range jsList(removed) {
					wantRemoved = append(wantRemoved, jsInt(r))
				}
				if !reflect.DeepEqual(alignment.Removed, wantRemoved) {
					t.Errorf("removed = %v, want %v", alignment.Removed, wantRemoved)
				}
			}
			if min, ok := expect["similarity_min"]; ok {
				alignment := Align(oldBlocks, newBlocks)
				for _, m := range alignment.NewMatches {
					if m.Kind == "edited" && m.Similarity < min.(float64) {
						t.Errorf("similarity_min: edited similarity %v < %v", m.Similarity, min)
					}
				}
			}
			if nkeo := jsList(expect["new_key_equals_old_key"]); nkeo != nil {
				keys := AssignStableKeys(oldBlocks, newBlocks)
				oldKeys := make([]string, len(oldBlocks))
				for i, b := range oldBlocks {
					oldKeys[i] = BlockKeyOf(b).AsString()
				}
				for _, rawPair := range nkeo {
					pair := jsList(rawPair)
					ni, oi := jsInt(pair[0]), jsInt(pair[1])
					if keys[ni] != oldKeys[oi] {
						t.Errorf("new_key_equals_old_key [%d,%d]: %q != %q", ni, oi, keys[ni], oldKeys[oi])
					}
				}
			}
		})
	}
}
