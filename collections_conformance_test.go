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
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// --- fixture loading -------------------------------------------------------

func loadCollectionFixture(t *testing.T, name string) (map[string]any, bool) {
	t.Helper()
	for _, path := range specCandidatePaths("collections", name) {
		data, err := specReadFile(path)
		if err != nil {
			continue
		}
		var fixture map[string]any
		mustStrictJSON(t, name, data, &fixture)
		return fixture, true
	}
	specFixtureMissing(t, "collections fixture not found: %s", name)
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
		case nil:
			// No placement stated: minting appends, so this is already "end".
			m.Set(key, value)
		case string:
			switch at {
			case "front":
				m.Set(key, value)
				m.MoveTo(key, 0)
			case "end":
				m.Set(key, value)
			default:
				// Fail closed (#lzscenariobodyskip). This arm used to be
				// `default: // "end"`, so a placement this runner does not
				// implement ("middle", "before_x", ...) silently appended and
				// the scenario still reported as covered.
				panic("unknown cellmap insert placement: " + at)
			}
		case float64:
			m.Set(key, value)
			m.MoveTo(key, int(at))
		default:
			// Fail closed (#lzscenariobodyskip). The old default appended for
			// ANY shape of `at` — a bool, a list, an object — so a fixture
			// stating a placement in a form this runner cannot read replayed a
			// plain append and reported the scenario as covered.
			panic(fmt.Sprintf("unsupported cellmap insert placement %T (%v)", at, at))
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

	consumeFixtureKeys(t, name, fixture, "initial", "steps")
	excuseKey(t, fixture, "steps", "replay input: the step list drives the loop below, and each step's own `expected` block is asserted there")
	// Seeding is also an assertion: the map must actually hold the state the
	// fixture says it starts from, or every step below measures a different graph
	// than the corpus describes. Descended into rather than read field-by-field
	// (#lzsubblockkeyset), so a third key inside `initial` fails here.
	initial := assertKeySub(t, fixture, "initial", "order", "values")
	initialValues := jsMap(initial["values"])
	for _, k := range jsStrList(initial["order"]) {
		m.Set(k, jsInt(initialValues[k]))
	}
	assertKeyWith(t, initial, "order", func(wantValue fixtureValue) {
		want := wantValue.Value()
		if got := m.Keys(ctx); !reflect.DeepEqual(got, jsStrList(want)) {
			t.Fatalf("%s: seeded order = %v, want %v", name, got, jsStrList(want))
		}
	})
	assertKeyEach(t, initial, "values", func(k string, want any) {
		if got, _ := m.Get(k); got != jsInt(want) {
			t.Fatalf("%s: seeded value[%s] = %d, want %d", name, k, got, jsInt(want))
		}
	})

	for i, rawStep := range jsList(fixture["steps"]) {
		step := jsMap(rawStep)
		op := jsMap(step["op"])
		expected := consumeKeys(t, fmt.Sprintf("%s step %d expected", name, i), jsMap(step["expected"]),
			"invalidates", "handle_stable", "order", "membership", "values")
		invalidates := consumeKeys(t, fmt.Sprintf("%s step %d expected.invalidates", name, i),
			jsMap(expected["invalidates"]), "value", "membership", "order")
		excuseKey(t, expected, "invalidates", "container: its three reader classes are asserted key-by-key against the expected.invalidates block below")

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

		// Snapshot handles for stability assertions. Every key the fixture names
		// is snapshotted, not only the ones it expects to survive: a `false`
		// entry is the claim that the identity CHANGED, and without a before
		// there is nothing to compare it to.
		handlesBefore := map[string]*Source[int]{}
		for k := range jsMap(expected["handle_stable"]) {
			if _, present := m.Get(k); present {
				handlesBefore[k] = m.Cell(k)
			}
		}

		applySourceMapOp(m, op)

		// Value readers: only survivor keys are checked (removed keys are not).
		survivors := map[string]bool{}
		for _, k := range m.Keys(ctx) {
			survivors[k] = true
		}
		assertKeyWith(t, invalidates, "value", func(wantValue fixtureValue) {
			want := wantValue.Value()
			expectedValueInval := map[string]bool{}
			for _, k := range jsStrList(want) {
				expectedValueInval[k] = true
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
		})

		assertKeyWith(t, invalidates, "membership", func(wantValue fixtureValue) {
			want := wantValue.Value()
			_, memWarm := membershipReader.Peek()
			if memWarm == (want == true) {
				t.Errorf("%s step %d %s: membership warm=%v, wantInvalidated=%v",
					name, i, op["type"], memWarm, want)
			}
		})
		assertKeyWith(t, invalidates, "order", func(wantValue fixtureValue) {
			want := wantValue.Value()
			_, ordWarm := orderReader.Peek()
			if ordWarm == (want == true) {
				t.Errorf("%s step %d %s: order warm=%v, wantInvalidated=%v",
					name, i, op["type"], ordWarm, want)
			}
		})

		// Resulting state.
		assertKey(t, expected, "order", m.Keys(ctx))
		assertKeyWith(t, expected, "membership", func(wantValue fixtureValue) {
			want := wantValue.Value()
			if !sameStringSet(m.Keys(ctx), jsStrList(want)) {
				t.Errorf("%s step %d %s: membership = %v, want set %v", name, i, op["type"], m.Keys(ctx), jsStrList(want))
			}
		})
		if _, stated := expected["values"]; stated {
			assertKeyEach(t, expected, "values", func(k string, v any) {
				if got, _ := m.Get(k); got != jsInt(v) {
					t.Errorf("%s step %d %s: value[%s] = %d, want %d", name, i, op["type"], k, got, jsInt(v))
				}
			})
		}

		// Handle stability: same *Cell identity before and after.
		if _, stated := expected["handle_stable"]; stated {
			assertKeyEach(t, expected, "handle_stable", func(k string, raw any) {
				before, snapped := handlesBefore[k]
				after := m.Cell(k)
				if raw != true {
					// The fixture claims this handle is NOT stable, so a
					// surviving identity is the failure.
					if !snapped {
						t.Errorf("%s step %d: handle_stable names %q as re-minted, but it did not exist before the op", name, i, k)
					} else if after != nil && before == after {
						t.Errorf("%s step %d %s: handle %q stayed stable, want re-minted", name, i, op["type"], k)
					}
					return
				}
				if after == nil {
					t.Errorf("%s step %d: handle %q missing after op", name, i, k)
				} else if !snapped || before != after {
					t.Errorf("%s step %d %s: handle %q not stable", name, i, op["type"], k)
				}
			})
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
	consumeFixtureKeys(t, name, fixture, "reconcile", "expected")
	excuseKey(t, fixture, "reconcile", "replay input: the prior/target pair the diff is computed FROM; what it produces is asserted through expected.ops and expected.result_order")
	excuseKey(t, fixture, "expected", "container: its three claims are asserted key-by-key against the expected block below")
	reconcile := jsMap(fixture["reconcile"])
	expected := consumeKeys(t, name+" expected", jsMap(fixture["expected"]),
		"ops", "result_order", "stable_keys_not_invalidated")

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
	assertKeyWith(t, expected, "ops", func(wantValue fixtureValue) {
		wantOps := wantValue.Value()
		assertReconcileOps(t, name, ops, jsList(wantOps), resultOrder)
	})

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
	// Prime readers for the declared stable keys BEFORE the reorder
	// (#lzgoassertkeygaps). This used to prime AFTER the prior->target reconcile
	// and then run a SECOND, no-op reconcile to the same target — which
	// invalidates nothing by construction, so every reader was trivially warm.
	// Measured: with the no-op, `["b","c","a"]` passed, `["b"]` passed, and
	// `["zzz"]` — a key not in the map at all — passed. Only `[]` failed, via the
	// emptiness guard. The fixture's whole claim (b and c survive the sibling
	// reorder that moves a and removes d) was untested.
	stableKeys := jsStrList(expected["stable_keys_not_invalidated"])
	if len(stableKeys) == 0 {
		t.Fatalf("%s: stable_keys_not_invalidated is empty; there is no claim to test", name)
	}
	readers := map[string]*Computed[int]{}
	for _, k := range stableKeys {
		key := k
		if _, ok := m.Read(key); !ok {
			t.Fatalf("%s: stable_keys_not_invalidated names %q, which the PRIOR map does not carry — "+
				"a key that is not there cannot survive a reorder, so this entry asserts nothing", name, key)
		}
		slot := NewSlot(ctx, func(c *Compute) int { v, _ := m.Read(key); return v })
		slot.Get()
		readers[key] = slot
	}

	m.Reconcile(targetOrder, targetValues)
	assertKey(t, expected, "result_order", m.Keys(ctx))

	// The reorder has now actually happened; the stable keys must have survived it.
	//
	// The warmth check alone is a LOWER bound — it cannot see a list that has
	// been shortened, because a dropped key simply stops being checked. So the
	// declared set is also compared against the set the fixture's own data
	// defines as stable: present in both prior and target, and not named by a
	// `move` op. That is the spec's definition of an LIS member, derived from
	// keys the fixture already declares rather than restated here, and it is what
	// makes `["b"]` a failure instead of a smaller obligation.
	assertKeyWith(t, expected, "stable_keys_not_invalidated", func(wantValue fixtureValue) {
		want := wantValue.Value()
		declared := jsStrList(want)
		for _, k := range declared {
			if _, warm := readers[k].Peek(); !warm {
				t.Errorf("%s: stable key %q invalidated by sibling reorder", name, k)
			}
		}

		moved := map[string]bool{}
		for _, raw := range jsList(expected["ops"]) {
			op, _ := raw.(map[string]any)
			if op["type"] == "move" {
				if key, ok := op["key"].(string); ok {
					moved[key] = true
				}
			}
		}
		inTarget := map[string]bool{}
		for _, k := range targetOrder {
			inTarget[k] = true
		}
		var stable []string
		for _, e := range prior {
			if inTarget[e.Key] && !moved[e.Key] {
				stable = append(stable, e.Key)
			}
		}
		sort.Strings(stable)
		got := append([]string(nil), declared...)
		sort.Strings(got)
		if !reflect.DeepEqual(stable, got) {
			t.Errorf("%s: stable_keys_not_invalidated is %v, but prior∩target minus moved keys is %v — "+
				"the list must account for every key the reconcile left in place, or it can shrink "+
				"an obligation without failing", name, got, stable)
		}
	})
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
	consumeFixtureKeys(t, name, fixture, "scenarios")
	excuseKey(t, fixture, "scenarios", "replay input: each scenario's own expect_initial / expect_after blocks are asserted in the subtest below")
	for _, sv := range scenarioViews("collections/"+name, jsList(fixture["scenarios"])) {
		t.Run(sv.Label(), func(t *testing.T) {
			// Rung 4 books HERE (#lzscenariobodyskip), on the first read of the
			// PAYLOAD inside the subtest — never at the loop header, which
			// cannot tell a body that replayed from one that returned early.
			scenario := sv.Map()
			consumeKeys(t, name+" scenario", scenario,
				"id", "name", "fold", "tree", "expect_initial", "expect_after", "edit", "remove_child")
			excuseKeys(t, scenario, "identity: `id` is the canonical scenario key this run records in the replay ledger and `name` is its prose label; neither states anything the replay must observe (#recommendedconformanceco)", "id", "name")
			excuseKey(t, scenario, "fold", "discriminator: selects which fold is built; what the fold computes is asserted through expect_initial and expect_after")
			excuseKey(t, scenario, "tree", "replay input: the tree the fold is built over; the derived values it produces are asserted through expect_initial")
			excuseKeys(t, scenario, "replay input: the mutation whose effect expect_after asserts", "edit", "remove_child")
			ctx := NewContext()
			fold := semFold(jsStr(scenario["fold"]))
			tree := BuildSemTree(ctx, parseTreeNode(jsMap(scenario["tree"])), fold)

			// Prime + assert initial derived values.
			//
			// The two reserved keys used to be `continue`d past here — read out
			// of the block, which marked them consumed, and then dropped. They
			// are now handled: `sibling_a_cached` is an observable fact about
			// the tree before any edit, and `downstream_consumer_reran` cannot
			// be stated before there is an edit to re-run from, so a fixture
			// that states it here is wrong rather than ignorable.
			if _, stated := scenario["expect_initial"]; stated {
				assertKeyEach(t, scenario, "expect_initial", func(id string, value any) {
					switch id {
					case "sibling_a_cached":
						if got := tree.IsCached("a"); got != (value == true) {
							t.Errorf("initial sibling_a_cached = %v, want %v", got, value)
						}
					case "downstream_consumer_reran":
						t.Errorf("expect_initial states downstream_consumer_reran; there has been no edit to re-run from")
					default:
						if got, _ := tree.NodeValue(id); got != jsInt(value) {
							t.Errorf("initial %s = %d, want %d", id, got, jsInt(value))
						}
					}
				})
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

			mutated := false
			if edit := jsMap(scenario["edit"]); edit != nil {
				if err := tree.SetValue(jsStr(edit["id"]), jsInt(edit["value"])); err != nil {
					t.Fatalf("edit: %v", err)
				}
				if downstream != nil {
					downstream.Get() // pull the consumer, as rs and js do
				}
				mutated = true
				assertKeyEach(t, scenario, "expect_after", func(id string, want any) {
					checkSemTreeAfterEntry(t, tree, id, want, downstreamRuns > runsBefore)
				})
			}

			if rc := jsMap(scenario["remove_child"]); rc != nil {
				if err := tree.RemoveChild(jsStr(rc["parent"]), jsStr(rc["child"])); err != nil {
					t.Fatalf("remove_child: %v", err)
				}
				if downstream != nil {
					downstream.Get() // pull the consumer, as rs and js do
				}
				mutated = true
				assertKeyEach(t, scenario, "expect_after", func(id string, want any) {
					checkSemTreeAfterEntry(t, tree, id, want, downstreamRuns > runsBefore)
				})
			}
			if !mutated {
				t.Fatalf("scenario states expect_after but neither edit nor remove_child: nothing changed, so expect_after asserts nothing")
			}
		})
	}
}

// checkSemTreeAfterEntry asserts ONE entry of an `expect_after` block. The
// iteration lives in assertKeyEach rather than here (#lzsubblockkeyset), so a
// key added to the block upstream reaches this switch instead of being walked
// past by a loop that only knows the names it was written with.
func checkSemTreeAfterEntry(t *testing.T, tree *SemTree[int, int], id string, want any, didRerun bool) {
	t.Helper()
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

// ---------------------------------------------------------------------------
// SeqCrdt convergence
// ---------------------------------------------------------------------------

func TestCollectionsSeqCrdtConvergence(t *testing.T) {
	name := "seqcrdt_convergence.json"
	fixture, ok := loadCollectionFixture(t, name)
	if !ok {
		return
	}
	consumeFixtureKeys(t, name, fixture, "scenarios")
	excuseKey(t, fixture, "scenarios", "replay input: each scenario's own expect block is asserted in the subtest below")
	for _, sv := range scenarioViews("collections/"+name, jsList(fixture["scenarios"])) {
		t.Run(sv.Label(), func(t *testing.T) {
			// Rung 4 books HERE (#lzscenariobodyskip), on the first read of the
			// PAYLOAD inside the subtest — never at the loop header, which
			// cannot tell a body that replayed from one that returned early.
			scenario := sv.Map()
			consumeKeys(t, name+" scenario", scenario, "id", "name", "replica", "seed", "steps", "expect")
			consumeKeys(t, name+" scenario expect", jsMap(scenario["expect"]),
				"orders_equal", "order_on", "order", "len", "get", "get_on",
				"contains_all", "not_contains_on")
			excuseKeys(t, scenario, "identity: `id` is the canonical scenario key this run records in the replay ledger and `name` is its prose label; neither states anything the replay must observe (#recommendedconformanceco)", "id", "name")
			excuseKeys(t, scenario, "replay input: seeds the replicas and drives the ops; what they produce is asserted through the expect block",
				"replica", "seed", "steps")
			excuseKey(t, scenario, "expect", "container: asserted key-by-key in checkSeqCrdtExpect")
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

	if _, stated := expect["order"]; stated {
		assertKey(t, expect, "order", replicas[primary].Order())
	}
	if _, stated := expect["len"]; stated {
		assertKey(t, expect, "len", replicas[primary].Len())
	}
	if _, stated := expect["get"]; stated {
		assertKeyEach(t, expect, "get", func(id string, value any) {
			got, _ := replicas[primary].Get(id)
			if !reflect.DeepEqual(got, value) {
				t.Errorf("get %s = %v, want %v", id, got, value)
			}
		})
	}
	if _, stated := expect["orders_equal"]; stated {
		assertKeyWith(t, expect, "orders_equal", func(wantValue fixtureValue) {
			want := wantValue.Value()
			for _, rawPair := range jsList(want) {
				pair := jsList(rawPair)
				a, b := jsStr(pair[0]), jsStr(pair[1])
				if !reflect.DeepEqual(replicas[a].Order(), replicas[b].Order()) {
					t.Errorf("orders_equal %s/%s: %v != %v", a, b, replicas[a].Order(), replicas[b].Order())
				}
			}
		})
	}
	if _, stated := expect["contains_all"]; stated {
		assertKeyWith(t, expect, "contains_all", func(wantValue fixtureValue) {
			want := wantValue.Value()
			for _, id := range jsList(want) {
				if !replicas[primary].Contains(jsStr(id)) {
					t.Errorf("contains_all: missing %v", id)
				}
			}
		})
	}
	if _, stated := expect["order_on"]; stated {
		assertKeyEach(t, expect, "order_on", func(replica string, order any) {
			if replicas[replica] == nil {
				t.Fatalf("order_on names replica %q, which this scenario never created", replica)
			}
			if got := replicas[replica].Order(); !reflect.DeepEqual(got, jsStrList(order)) {
				t.Errorf("order_on %s = %v, want %v", replica, got, jsStrList(order))
			}
		})
	}
	if _, stated := expect["get_on"]; stated {
		assertKeyEach(t, expect, "get_on", func(replica string, kvs any) {
			if replicas[replica] == nil {
				t.Fatalf("get_on names replica %q, which this scenario never created", replica)
			}
			for id, value := range jsMap(kvs) {
				got, _ := replicas[replica].Get(id)
				if !reflect.DeepEqual(got, value) {
					t.Errorf("get_on %s/%s = %v, want %v", replica, id, got, value)
				}
			}
		})
	}
	if _, stated := expect["not_contains_on"]; stated {
		assertKeyEach(t, expect, "not_contains_on", func(replica string, ids any) {
			if replicas[replica] == nil {
				t.Fatalf("not_contains_on names replica %q, which this scenario never created", replica)
			}
			for _, id := range jsList(ids) {
				if replicas[replica].Contains(jsStr(id)) {
					t.Errorf("not_contains_on %s: still contains %v", replica, id)
				}
			}
		})
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
	consumeFixtureKeys(t, name, fixture, "scenarios")
	excuseKey(t, fixture, "scenarios", "replay input: each scenario's own expect block is asserted in the subtest below")
	for _, sv := range scenarioViews("collections/"+name, jsList(fixture["scenarios"])) {
		t.Run(sv.Label(), func(t *testing.T) {
			// Rung 4 books HERE (#lzscenariobodyskip), on the first read of the
			// PAYLOAD inside the subtest — never at the loop header, which
			// cannot tell a body that replayed from one that returned early.
			scenario := sv.Map()
			consumeKeys(t, name+" scenario", scenario, "id", "name", "replica", "seed", "steps", "expect")
			consumeKeys(t, name+" scenario expect", jsMap(scenario["expect"]),
				"text", "len", "texts_equal", "a_starts_with", "a_ends_with", "tombstone_count")
			excuseKeys(t, scenario, "identity: `id` is the canonical scenario key this run records in the replay ledger and `name` is its prose label; neither states anything the replay must observe (#recommendedconformanceco)", "id", "name")
			excuseKeys(t, scenario, "replay input: seeds the replicas and drives the ops; what they produce is asserted through the expect block",
				"replica", "seed", "steps")
			excuseKey(t, scenario, "expect", "container: asserted key-by-key in checkTextCrdtExpect")
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
	case nil:
		// No seed stated: start empty.
		if replicaSpec != nil {
			return NewTextCrdt(int64(jsInt(replicaSpec["peer"])))
		}
		return NewTextCrdt(1)
	default:
		// Fail closed (#lzscenariobodyskip). The absent-seed arm used to be the
		// `default`, so a seed in any other shape (a number, a list) was read as
		// "no seed", replayed against an EMPTY document, and still reported the
		// scenario as covered.
		panic(fmt.Sprintf("unsupported textcrdt seed %T (%v)", s, s))
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
	if _, stated := expect["text"]; stated {
		assertKey(t, expect, "text", replicas["a"].Text())
	}
	if _, stated := expect["len"]; stated {
		assertKey(t, expect, "len", replicas["a"].Len())
	}
	if _, stated := expect["texts_equal"]; stated {
		assertKeyWith(t, expect, "texts_equal", func(wantValue fixtureValue) {
			want := wantValue.Value()
			for _, rawPair := range jsList(want) {
				pair := jsList(rawPair)
				a, b := jsStr(pair[0]), jsStr(pair[1])
				if replicas[a].Text() != replicas[b].Text() {
					t.Errorf("texts_equal %s/%s: %q != %q", a, b, replicas[a].Text(), replicas[b].Text())
				}
			}
		})
	}
	if _, stated := expect["a_starts_with"]; stated {
		assertKeyWith(t, expect, "a_starts_with", func(wantValue fixtureValue) {
			want := wantValue.Value()
			prefix := jsStr(want)
			if txt := replicas["a"].Text(); !strings.HasPrefix(txt, prefix) {
				t.Errorf("a_starts_with %q: text = %q", prefix, txt)
			}
		})
	}
	if _, stated := expect["a_ends_with"]; stated {
		assertKeyWith(t, expect, "a_ends_with", func(wantValue fixtureValue) {
			want := wantValue.Value()
			suffix := jsStr(want)
			if txt := replicas["a"].Text(); !strings.HasSuffix(txt, suffix) {
				t.Errorf("a_ends_with %q: text = %q", suffix, txt)
			}
		})
	}
	if _, stated := expect["tombstone_count"]; stated {
		assertKey(t, expect, "tombstone_count", replicas["a"].TombstoneCount())
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
	consumeFixtureKeys(t, name, fixture, "scenarios")
	excuseKey(t, fixture, "scenarios", "replay input: each scenario's own expect block is asserted in the subtest below")
	for _, sv := range scenarioViews("collections/"+name, jsList(fixture["scenarios"])) {
		t.Run(sv.Label(), func(t *testing.T) {
			// Rung 4 books HERE (#lzscenariobodyskip), on the first read of the
			// PAYLOAD inside the subtest — never at the loop header, which
			// cannot tell a body that replayed from one that returned early.
			scenario := sv.Map()
			consumeKeys(t, name+" scenario", scenario, "id", "name", "seed", "steps", "expect")
			consumeKeys(t, name+" scenario expect", jsMap(scenario["expect"]),
				"texts_equal", "text_on", "version_vector_on")
			excuseKeys(t, scenario, "identity: `id` is the canonical scenario key this run records in the replay ledger and `name` is its prose label; neither states anything the replay must observe (#recommendedconformanceco)", "id", "name")
			excuseKeys(t, scenario, "replay input: seeds the replicas and drives the delta exchange; what it produces is asserted through the expect block",
				"seed", "steps")
			excuseKey(t, scenario, "expect", "container: asserted key-by-key in checkTextCrdtDeltaExpect")
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
	if _, stated := expect["texts_equal"]; stated {
		assertKeyWith(t, expect, "texts_equal", func(wantValue fixtureValue) {
			want := wantValue.Value()
			for _, rawPair := range jsList(want) {
				pair := jsList(rawPair)
				a, b := jsStr(pair[0]), jsStr(pair[1])
				if replicas[a].Text() != replicas[b].Text() {
					t.Errorf("texts_equal %s/%s: %q != %q", a, b, replicas[a].Text(), replicas[b].Text())
				}
			}
		})
	}
	if _, stated := expect["text_on"]; stated {
		assertKeyEach(t, expect, "text_on", func(replica string, text any) {
			if replicas[replica] == nil {
				t.Fatalf("text_on names replica %q, which this scenario never created", replica)
			}
			if got := replicas[replica].Text(); got != jsStr(text) {
				t.Errorf("text_on %s = %q, want %q", replica, got, jsStr(text))
			}
		})
	}
	if _, stated := expect["version_vector_on"]; stated {
		assertKeyEach(t, expect, "version_vector_on", func(replica string, vector any) {
			if replicas[replica] == nil {
				t.Fatalf("version_vector_on names replica %q, which this scenario never created", replica)
			}
			got := replicas[replica].VersionVector()
			wantMap := map[PeerId]int64{}
			for k, v := range jsMap(vector) {
				peer := int64(0)
				// keys are decimal strings
				for _, r := range k {
					peer = peer*10 + int64(r-'0')
				}
				wantMap[peer] = int64(jsInt(v))
			}
			if !reflect.DeepEqual(got, wantMap) {
				t.Errorf("version_vector_on %s = %v, want %v", replica, got, wantMap)
			}
		})
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
	consumeFixtureKeys(t, name, fixture, "scenarios")
	excuseKey(t, fixture, "scenarios", "replay input: each scenario's own expect block is asserted in the subtest below")
	for _, sv := range scenarioViews("collections/"+name, jsList(fixture["scenarios"])) {
		t.Run(sv.Label(), func(t *testing.T) {
			// Rung 4 books HERE (#lzscenariobodyskip), on the first read of the
			// PAYLOAD inside the subtest — never at the loop header, which
			// cannot tell a body that replayed from one that returned early.
			scenario := sv.Map()
			consumeKeys(t, name+" scenario", scenario, "id", "name", "blocks", "old", "new", "expect")
			expect := consumeKeys(t, name+" scenario expect", jsMap(scenario["expect"]),
				"key_equal", "key_not_equal", "matches", "removed", "similarity_min",
				"new_key_equals_old_key")
			excuseKeys(t, scenario, "identity: `id` is the canonical scenario key this run records in the replay ledger and `name` is its prose label; neither states anything the replay must observe (#recommendedconformanceco)", "id", "name")
			excuseKeys(t, scenario, "replay input: the block lists the keys and the alignment are computed FROM; what they produce is asserted through the expect block",
				"blocks", "old", "new")
			excuseKey(t, scenario, "expect", "container: asserted key-by-key against the expect block below")

			if blocksField := jsList(scenario["blocks"]); blocksField != nil {
				keys := make([]BlockKey, len(blocksField))
				for i, b := range blocksField {
					keys[i] = BlockKeyOf(parseBlock(jsMap(b)))
				}
				if _, stated := expect["key_equal"]; stated {
					assertKeyWith(t, expect, "key_equal", func(wantValue fixtureValue) {
						want := wantValue.Value()
						for _, rawPair := range jsList(want) {
							pair := jsList(rawPair)
							i, j := jsInt(pair[0]), jsInt(pair[1])
							if !keys[i].Equals(keys[j]) {
								t.Errorf("key_equal [%d,%d]: %s != %s", i, j, keys[i], keys[j])
							}
						}
					})
				}
				if _, stated := expect["key_not_equal"]; stated {
					assertKeyWith(t, expect, "key_not_equal", func(wantValue fixtureValue) {
						want := wantValue.Value()
						for _, rawPair := range jsList(want) {
							pair := jsList(rawPair)
							i, j := jsInt(pair[0]), jsInt(pair[1])
							if keys[i].Equals(keys[j]) {
								t.Errorf("key_not_equal [%d,%d]: %s == %s", i, j, keys[i], keys[j])
							}
						}
					})
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

			if _, stated := expect["matches"]; stated {
				assertKeyWith(t, expect, "matches", func(wantValue fixtureValue) {
					want := wantValue.Value()
					alignment := Align(oldBlocks, newBlocks)
					for i, m := range jsList(want) {
						if got := alignment.NewMatches[i].String(); got != jsStr(m) {
							t.Errorf("match[%d] = %q, want %q", i, got, jsStr(m))
						}
					}
				})
			}
			if _, stated := expect["removed"]; stated {
				assertKeyWith(t, expect, "removed", func(wantValue fixtureValue) {
					want := wantValue.Value()
					alignment := Align(oldBlocks, newBlocks)
					wantRemoved := []int{}
					for _, r := range jsList(want) {
						wantRemoved = append(wantRemoved, jsInt(r))
					}
					if !reflect.DeepEqual(alignment.Removed, wantRemoved) {
						t.Errorf("removed = %v, want %v", alignment.Removed, wantRemoved)
					}
				})
			}
			if _, stated := expect["similarity_min"]; stated {
				assertKeyWith(t, expect, "similarity_min", func(wantValue fixtureValue) {
					want := wantValue.Value()
					alignment := Align(oldBlocks, newBlocks)
					edited := 0
					for _, m := range alignment.NewMatches {
						if m.Kind != "edited" {
							continue
						}
						edited++
						if m.Similarity < want.(float64) {
							t.Errorf("similarity_min: edited similarity %v < %v", m.Similarity, want)
						}
					}
					if edited == 0 {
						t.Errorf("similarity_min: the alignment produced no edited match, so the floor bounded nothing")
					}
				})
			}
			if _, stated := expect["new_key_equals_old_key"]; stated {
				assertKeyWith(t, expect, "new_key_equals_old_key", func(wantValue fixtureValue) {
					want := wantValue.Value()
					keys := AssignStableKeys(oldBlocks, newBlocks)
					oldKeys := make([]string, len(oldBlocks))
					for i, b := range oldBlocks {
						oldKeys[i] = BlockKeyOf(b).AsString()
					}
					for _, rawPair := range jsList(want) {
						pair := jsList(rawPair)
						ni, oi := jsInt(pair[0]), jsInt(pair[1])
						if keys[ni] != oldKeys[oi] {
							t.Errorf("new_key_equals_old_key [%d,%d]: %q != %q", ni, oi, keys[ni], oldKeys[oi])
						}
					}
				})
			}
		})
	}
}

// assertReconcileOps checks the diff the library produced against the op list
// the fixture states, resolving the fixture's relative move anchors against the
// fixture's own result order.
func assertReconcileOps(t *testing.T, name string, ops []DiffOp[string, int], expectedOps []any, resultOrder []string) {
	t.Helper()
	if len(ops) != len(expectedOps) {
		t.Fatalf("%s: op count = %d, want %d (%v)", name, len(ops), len(expectedOps), ops)
	}
	anchorIdx := func(anchor string) int {
		for j, k := range resultOrder {
			if k == anchor {
				return j
			}
		}
		return -1
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
		default:
			// Fail closed (#lzscenariobodyskip). Without this the switch fell
			// through, so a fixture stating an op type this runner does not
			// implement asserted NOTHING about that op — only the op COUNT was
			// checked — and the scenario still reported as covered.
			t.Fatalf("%s op[%d]: unknown expected op type %v", name, i, want["type"])
		}
	}
}
