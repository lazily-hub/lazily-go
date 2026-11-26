package lazily

import (
	"encoding/json"
	"strings"
	"testing"
)

const counterJSON = `{
  "kind": "graph", "ir": "1", "id": "counter",
  "nodes": [
    {"id": "count", "kind": "source", "init": 1},
    {"id": "doubled", "kind": "computed", "eager": true,
      "expr": {"op": "mul", "args": [{"ref": "count"}, {"lit": 2}]}},
    {"id": "log", "kind": "effect", "body": {"do": "eval", "expr": {"ref": "doubled"}}}
  ]
}`

const counterEditJSON = `{
  "kind": "edit", "ir": "1",
  "ops": [
    {"op": "replace", "node": {"id": "doubled", "kind": "computed",
      "expr": {"op": "add", "args": [{"ref": "count"}, {"lit": 10}]}}},
    {"op": "add", "node": {"id": "label", "kind": "source", "init": "n"}},
    {"op": "add", "node": {"id": "shown", "kind": "computed",
      "expr": {"op": "concat", "args": [{"ref": "label"}, {"lit": ":"}]}}},
    {"op": "remove", "id": "log"}
  ]
}`

const counterSource = "graph counter ir 1\n" +
	"source count = 1\n" +
	"computed doubled eager = mul(ref(count), lit(2))\n" +
	"effect log = eval(ref(doubled))\n"

func mustGraph(t *testing.T, text string) *GraphIr {
	t.Helper()
	doc, err := GraphIrFromJSON(text)
	if err != nil {
		t.Fatalf("parse graph: %v", err)
	}
	return doc
}

func mustEdit(t *testing.T, text string) *GraphEdit {
	t.Helper()
	edit, err := GraphEditFromJSON(text)
	if err != nil {
		t.Fatalf("parse edit: %v", err)
	}
	return edit
}

func mustRuntime(t *testing.T, text string) *GraphRuntime {
	t.Helper()
	rt, err := LoadGraphRuntime(text)
	if err != nil {
		t.Fatalf("lower: %v", err)
	}
	return rt
}

func mustGet(t *testing.T, rt *GraphRuntime, id string) any {
	t.Helper()
	value, err := rt.Get(id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return value
}

func wantErr(t *testing.T, err error, fragment string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q", fragment)
	}
	if _, ok := err.(*GraphIrError); !ok {
		t.Fatalf("expected *GraphIrError, got %T %v", err, err)
	}
	if !strings.Contains(err.Error(), fragment) {
		t.Fatalf("error %q does not contain %q", err.Error(), fragment)
	}
}

func reads(edges []Edge) []Edge {
	out := make([]Edge, 0, len(edges))
	for _, edge := range edges {
		out = append(out, Edge{From: edge.From, Kind: EdgeRead, To: edge.To})
	}
	return out
}

func TestCounterLowersThenEditReplacesTheGraph(t *testing.T) {
	doc := mustGraph(t, counterJSON)
	id, ok := doc.ID()
	if !ok || id != "counter" {
		t.Fatalf("id = %q %v", id, ok)
	}
	doubled := doc.Node("doubled")
	if doubled == nil || !doubled.Eager || doubled.Expr.Op != "mul" {
		t.Fatalf("doubled = %#v", doubled)
	}
	want := []Edge{{From: "doubled", Kind: EdgeRead, To: "count"}, {From: "log", Kind: EdgeRead, To: "doubled"}}
	if got := reads(doc.StaticEdges()); !sameEdges(got, want) {
		t.Fatalf("edges = %#v", got)
	}
	if doc.EmitSource() != counterSource {
		t.Fatalf("emit:\n%s", doc.EmitSource())
	}
	raw, err := json.Marshal(doc.ToJSON())
	if err != nil {
		t.Fatal(err)
	}
	if mustGraph(t, string(raw)).EmitSource() != counterSource {
		t.Fatal(mustGraph(t, string(raw)).EmitSource())
	}

	rt, err := LowerGraph(doc, 0)
	if err != nil {
		t.Fatal(err)
	}
	if mustGet(t, rt, "doubled") != int64(2) {
		t.Fatalf("doubled = %#v", mustGet(t, rt, "doubled"))
	}
	trace := rt.EffectTrace()
	if len(trace) != 1 || trace[0].ID != "log" || trace[0].Result != int64(2) {
		t.Fatalf("trace = %#v", trace)
	}
	if !sameSet(rt.ObservedReads("doubled"), "count") || !sameSet(rt.ObservedReads("log"), "doubled") {
		t.Fatalf("observed doubled=%v log=%v", rt.ObservedReads("doubled"), rt.ObservedReads("log"))
	}
	eager, err := rt.IsEager("doubled")
	if err != nil || !eager {
		t.Fatalf("eager %v %v", eager, err)
	}
	policy, err := rt.SourcePolicy("count")
	if err != nil || policy != PolicyKeepLatest {
		t.Fatalf("policy %q %v", policy, err)
	}

	before := rt.EmitSource()
	if err := rt.WriteSource("count", int64(4)); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, rt, "doubled") != int64(8) {
		t.Fatalf("doubled after write = %#v", mustGet(t, rt, "doubled"))
	}
	trace = rt.EffectTrace()
	if len(trace) != 2 || trace[1].Result != int64(8) {
		t.Fatalf("trace after write = %#v", trace)
	}
	if rt.EmitSource() != before {
		t.Fatalf("emit changed on write:\n%s", rt.EmitSource())
	}

	if err := rt.Apply(mustEdit(t, counterEditJSON)); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, rt, "doubled") != int64(14) || mustGet(t, rt, "shown") != "n:" || rt.Contains("log") {
		t.Fatalf("after edit doubled=%#v shown=%#v log=%v", mustGet(t, rt, "doubled"), mustGet(t, rt, "shown"), rt.Contains("log"))
	}
	eager, err = rt.IsEager("doubled")
	if err != nil || eager {
		t.Fatalf("eager after replace %v %v", eager, err)
	}
	traceLen := len(rt.EffectTrace())
	if err := rt.WriteSource("count", int64(5)); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, rt, "doubled") != int64(15) || len(rt.EffectTrace()) != traceLen {
		t.Fatalf("doubled=%#v trace=%d", mustGet(t, rt, "doubled"), len(rt.EffectTrace()))
	}
	emitted := rt.EmitSource()
	wantEmit := "graph counter ir 1\n" +
		"source count = 1\n" +
		"computed doubled = add(ref(count), lit(10))\n" +
		"source label = \"n\"\n" +
		"computed shown = concat(ref(label), lit(\":\"))\n"
	if emitted != wantEmit {
		t.Fatalf("emit:\n%s", emitted)
	}
}

func TestDynamicIfObservesOnlyTheTakenBranch(t *testing.T) {
	rt := mustRuntime(t, `{
    "kind":"graph","ir":"1","nodes":[
      {"id":"flag","kind":"source","init":false},
      {"id":"left","kind":"source","init":"L"},
      {"id":"right","kind":"source","init":"R"},
      {"id":"picked","kind":"computed","expr":{"op":"if","args":[{"ref":"flag"},{"ref":"left"},{"ref":"right"}]}}
    ]}`)
	want := []Edge{
		{From: "picked", Kind: EdgeRead, To: "flag"},
		{From: "picked", Kind: EdgeRead, To: "left"},
		{From: "picked", Kind: EdgeRead, To: "right"},
	}
	if got := reads(rt.StaticEdges()); !sameEdges(got, want) {
		t.Fatalf("edges %#v", got)
	}
	if mustGet(t, rt, "picked") != "R" || !sameSet(rt.ObservedReads("picked"), "flag", "right") {
		t.Fatalf("picked=%#v observed=%v", mustGet(t, rt, "picked"), rt.ObservedReads("picked"))
	}
	if err := rt.WriteSource("flag", true); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, rt, "picked") != "L" || !sameSet(rt.ObservedReads("picked"), "flag", "left") {
		t.Fatalf("picked=%#v observed=%v", mustGet(t, rt, "picked"), rt.ObservedReads("picked"))
	}
}

func TestSumPolicyFoldsAndSkipsEqualWrite(t *testing.T) {
	rt := mustRuntime(t, `{
    "kind":"graph","ir":"1","nodes":[
      {"id":"tick","kind":"source","init":0},
      {"id":"total","kind":"source","init":0,"policy":"sum"},
      {"id":"bump","kind":"effect","body":{"do":"seq","steps":[
        {"do":"eval","expr":{"ref":"tick"}},
        {"do":"merge","target":"total","expr":{"lit":1}}
      ]}}
    ]}`)
	emitted := rt.EmitSource()
	if !strings.Contains(emitted, "policy sum") || !strings.Contains(emitted, "merge(total, lit(1))") {
		t.Fatalf("emit:\n%s", emitted)
	}
	if mustGet(t, rt, "total") != int64(1) {
		t.Fatalf("total %#v", mustGet(t, rt, "total"))
	}
	trace := rt.EffectTrace()
	if len(trace) != 1 || trace[0].ID != "bump" {
		t.Fatalf("trace %#v", trace)
	}
	got, ok := trace[0].Result.([]any)
	if !ok || len(got) != 2 || got[0] != int64(0) || got[1] != int64(1) {
		t.Fatalf("result %#v", trace[0].Result)
	}
	policy, err := rt.SourcePolicy("total")
	if err != nil || policy != PolicySum {
		t.Fatalf("policy %q %v", policy, err)
	}
	if err := rt.WriteSource("tick", int64(1)); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, rt, "total") != int64(2) || len(rt.EffectTrace()) != 2 {
		t.Fatalf("total %#v trace %d", mustGet(t, rt, "total"), len(rt.EffectTrace()))
	}
	if err := rt.WriteSource("tick", int64(1)); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, rt, "total") != int64(2) || len(rt.EffectTrace()) != 2 {
		t.Fatalf("equal write total %#v trace %d", mustGet(t, rt, "total"), len(rt.EffectTrace()))
	}
}

func TestStructuresReadFieldsLengthAndObjects(t *testing.T) {
	rt := mustRuntime(t, `{
    "kind":"graph","ir":"1","nodes":[
      {"id":"row","kind":"source","init":{"name":"ada","n":2}},
      {"id":"name","kind":"computed","expr":{"op":"get","args":[{"ref":"row"},{"lit":"name"}]}},
      {"id":"n","kind":"computed","expr":{"op":"get","args":[{"ref":"row"},{"lit":"n"}]}},
      {"id":"tag","kind":"computed","expr":{"op":"concat","args":[{"ref":"name"},{"lit":"#"}]}},
      {"id":"width","kind":"computed","expr":{"op":"len","args":[{"ref":"name"}]}},
      {"id":"pair","kind":"computed","expr":{"op":"array","args":[{"ref":"name"},{"ref":"n"}]}},
      {"id":"wrapped","kind":"computed","expr":{"object":[{"key":"name","value":{"ref":"name"}}]}}
    ]}`)
	if mustGet(t, rt, "name") != "ada" || mustGet(t, rt, "n") != int64(2) || mustGet(t, rt, "tag") != "ada#" || mustGet(t, rt, "width") != int64(3) {
		t.Fatalf("name=%#v n=%#v tag=%#v width=%#v", mustGet(t, rt, "name"), mustGet(t, rt, "n"), mustGet(t, rt, "tag"), mustGet(t, rt, "width"))
	}
	pair, ok := mustGet(t, rt, "pair").([]any)
	if !ok || len(pair) != 2 || pair[0] != "ada" || pair[1] != int64(2) {
		t.Fatalf("pair %#v", mustGet(t, rt, "pair"))
	}
	wrapped, ok := mustGet(t, rt, "wrapped").(map[string]any)
	if !ok || wrapped["name"] != "ada" || len(wrapped) != 1 {
		t.Fatalf("wrapped %#v", mustGet(t, rt, "wrapped"))
	}
}

func TestCycleAndSnapshotAreRejected(t *testing.T) {
	_, err := GraphIrFromJSON(`{"kind":"graph","ir":"1","nodes":[{"id":"a","kind":"computed","expr":{"ref":"b"}},{"id":"b","kind":"computed","expr":{"ref":"a"}}]}`)
	wantErr(t, err, "cycle in read edges")
	_, err = GraphIrFromJSON(`{"Snapshot":{"epoch":0,"nodes":[],"edges":[],"roots":[]}}`)
	wantErr(t, err, "graph")
}

func TestNullInitAndOrderedBooleanPolicies(t *testing.T) {
	rt := mustRuntime(t, `{
    "kind":"graph","ir":"1","nodes":[
      {"id":"empty","kind":"source","init":null},
      {"id":"hi","kind":"source","init":3,"policy":"max"},
      {"id":"lo","kind":"source","init":3,"policy":"min"},
      {"id":"both","kind":"source","init":true,"policy":"and"},
      {"id":"either","kind":"source","init":false,"policy":"or"},
      {"id":"fold","kind":"effect","body":{"do":"seq","steps":[
        {"do":"merge","target":"hi","expr":{"lit":1}},
        {"do":"merge","target":"hi","expr":{"lit":5}},
        {"do":"merge","target":"hi","expr":{"lit":5}},
        {"do":"merge","target":"lo","expr":{"lit":1}},
        {"do":"merge","target":"both","expr":{"lit":false}},
        {"do":"merge","target":"either","expr":{"lit":true}}
      ]}}
    ]}`)
	if mustGet(t, rt, "empty") != nil || mustGet(t, rt, "hi") != int64(5) || mustGet(t, rt, "lo") != int64(1) || mustGet(t, rt, "both") != false || mustGet(t, rt, "either") != true {
		t.Fatalf("empty=%#v hi=%#v lo=%#v both=%#v either=%#v", mustGet(t, rt, "empty"), mustGet(t, rt, "hi"), mustGet(t, rt, "lo"), mustGet(t, rt, "both"), mustGet(t, rt, "either"))
	}
	for _, item := range []struct{ id, policy string }{{"hi", PolicyMax}, {"lo", PolicyMin}, {"both", PolicyAnd}, {"either", PolicyOr}} {
		got, err := rt.SourcePolicy(item.id)
		if err != nil || got != item.policy {
			t.Fatalf("%s policy %q %v", item.id, got, err)
		}
	}
}

func TestAndShortCircuitAndDivByZero(t *testing.T) {
	rt := mustRuntime(t, `{
    "kind":"graph","ir":"1","nodes":[
      {"id":"flag","kind":"source","init":false},
      {"id":"n","kind":"source","init":1},
      {"id":"bad","kind":"computed","expr":{"op":"div","args":[{"ref":"n"},{"lit":0}]}},
      {"id":"gate","kind":"computed","expr":{"op":"and","args":[{"ref":"flag"},{"ref":"bad"}]}}
    ]}`)
	if mustGet(t, rt, "gate") != false || !sameSet(rt.ObservedReads("gate"), "flag") {
		t.Fatalf("gate=%#v observed=%v", mustGet(t, rt, "gate"), rt.ObservedReads("gate"))
	}
	_, err := rt.Get("bad")
	wantErr(t, err, "division by zero")
}

func TestFailedEditsLeaveThePreviousGraph(t *testing.T) {
	rt := mustRuntime(t, `{"kind":"graph","ir":"1","nodes":[{"id":"a","kind":"source","init":1},{"id":"b","kind":"computed","expr":{"ref":"a"}}]}`)
	err := rt.Apply(mustEdit(t, `{"kind":"edit","ir":"1","ops":[{"op":"remove","id":"a"}]}`))
	wantErr(t, err, "edit op 0")
	if !rt.Contains("a") || !rt.Contains("b") || mustGet(t, rt, "b") != int64(1) {
		t.Fatal("remove of a live dependency changed the graph")
	}

	empty := mustRuntime(t, `{"kind":"graph","ir":"1","nodes":[]}`)
	err = empty.Apply(mustEdit(t, `{"kind":"edit","ir":"1","ops":[{"op":"add","node":{"id":"a","kind":"computed","expr":{"ref":"b"}}},{"op":"add","node":{"id":"b","kind":"source","init":1}}]}`))
	wantErr(t, err, "edit op 0")
	if empty.Contains("a") || empty.Contains("b") || empty.EmitSource() != "graph ir 1\n" {
		t.Fatalf("forward ref applied: %s contains a=%v b=%v", empty.EmitSource(), empty.Contains("a"), empty.Contains("b"))
	}
}

func TestReplaceSourceResetsLiveValueAndWhenRecordsNull(t *testing.T) {
	rt := mustRuntime(t, `{"kind":"graph","ir":"1","id":"g","nodes":[{"id":"count","kind":"source","init":1}]}`)
	if err := rt.WriteSource("count", int64(9)); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, rt, "count") != int64(9) || rt.EmitSource() != "graph g ir 1\nsource count = 1\n" {
		t.Fatalf("count=%#v emit=%s", mustGet(t, rt, "count"), rt.EmitSource())
	}
	if err := rt.Apply(mustEdit(t, `{"kind":"edit","ir":"1","ops":[{"op":"replace","node":{"id":"count","kind":"source","init":0}}]}`)); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, rt, "count") != int64(0) || rt.EmitSource() != "graph g ir 1\nsource count = 0\n" {
		t.Fatalf("count=%#v emit=%s", mustGet(t, rt, "count"), rt.EmitSource())
	}

	on := mustRuntime(t, `{"kind":"graph","ir":"1","nodes":[{"id":"on","kind":"source","init":true},{"id":"value","kind":"source","init":0},{"id":"apply","kind":"effect","body":{"do":"when","cond":{"ref":"on"},"then":{"do":"set","target":"value","expr":{"lit":7}}}}]}`)
	if mustGet(t, on, "value") != int64(7) || on.EffectTrace()[0].Result != int64(7) {
		t.Fatalf("on value=%#v trace=%#v", mustGet(t, on, "value"), on.EffectTrace())
	}
	off := mustRuntime(t, `{"kind":"graph","ir":"1","nodes":[{"id":"on","kind":"source","init":false},{"id":"value","kind":"source","init":0},{"id":"apply","kind":"effect","body":{"do":"when","cond":{"ref":"on"},"then":{"do":"set","target":"value","expr":{"lit":7}}}}]}`)
	if mustGet(t, off, "value") != int64(0) || off.EffectTrace()[0].Result != nil {
		t.Fatalf("off value=%#v trace=%#v", mustGet(t, off, "value"), off.EffectTrace())
	}
}

func TestEmptyGraphAcceptsAddAndRejectionsFailClosed(t *testing.T) {
	rt := mustRuntime(t, `{"kind":"graph","ir":"1","nodes":[]}`)
	if err := rt.Apply(mustEdit(t, `{"kind":"edit","ir":"1","ops":[{"op":"add","node":{"id":"x","kind":"source","init":1}}]}`)); err != nil {
		t.Fatal(err)
	}
	if mustGet(t, rt, "x") != int64(1) || rt.EmitSource() != "graph ir 1\nsource x = 1\n" {
		t.Fatalf("x=%#v emit=%s", mustGet(t, rt, "x"), rt.EmitSource())
	}
	for _, text := range []string{
		`{"kind":"graph","ir":"2","nodes":[]}`,
		`{"kind":"graph","ir":"1","nodes":[{"id":"1count","kind":"source","init":0}]}`,
		`{"kind":"graph","ir":"1","nodes":[{"id":"c","kind":"computed","expr":{"op":"pow","args":[{"lit":2},{"lit":3}]}}]}`,
		`{"kind":"graph","ir":"1","nodes":[{"id":"e","kind":"effect","body":"log"}]}`,
	} {
		_, err := GraphIrFromJSON(text)
		if err == nil {
			t.Fatalf("accepted %s", text)
		}
		if _, ok := err.(*GraphIrError); !ok {
			t.Fatalf("%T %v", err, err)
		}
	}
	_, err := GraphEditFromJSON(`{"kind":"edit","ir":"1","ops":[]}`)
	if err == nil {
		t.Fatal("empty edit accepted")
	}
}

func TestFeedbackStopsAtTheDrainBudget(t *testing.T) {
	doc := mustGraph(t, `{"kind":"graph","ir":"1","nodes":[{"id":"n","kind":"source","init":0},{"id":"loop","kind":"effect","body":{"do":"set","target":"n","expr":{"op":"add","args":[{"ref":"n"},{"lit":1}]}}}]}`)
	rt, err := LowerGraph(doc, 4)
	if err != nil {
		t.Fatal(err)
	}
	if rt.LastDrainExhaustion == nil || *rt.LastDrainExhaustion != 4 || mustGet(t, rt, "n") != int64(4) {
		t.Fatalf("exhaustion=%v n=%#v", rt.LastDrainExhaustion, mustGet(t, rt, "n"))
	}
}

func sameEdges(got, want []Edge) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func sameSet(got map[string]bool, keys ...string) bool {
	if len(got) != len(keys) {
		return false
	}
	for _, key := range keys {
		if !got[key] {
			return false
		}
	}
	return true
}
