// Graph IR interpreter.
//
// A lazily-spec graph document parses into an expression tree, lowers onto
// sources, computeds, and effects, and emits canonical-source. Edits add,
// replace, and remove nodes while the runtime holds the document. This is
// compute, not a Snapshot or Delta. Behavior matches the lazily-rs graph-ir
// reference.
package lazily

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	graphIRVersion = "1"
	drainBudget    = 100_000
)

var graphIDPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

var graphUnary = map[string]bool{"neg": true, "not": true, "len": true}
var graphBinary = map[string]bool{
	"add": true, "sub": true, "mul": true, "div": true, "mod": true,
	"min": true, "max": true, "eq": true, "ne": true, "lt": true, "le": true,
	"gt": true, "ge": true, "concat": true, "get": true, "and": true, "or": true,
}

// GraphIrError is a parse, check, edit, or value fault.
type GraphIrError struct{ Msg string }

func (e *GraphIrError) Error() string { return e.Msg }

func graphErr(format string, args ...any) error {
	return &GraphIrError{Msg: fmt.Sprintf(format, args...)}
}

// Policy is a source merge policy. The zero value is not used; callers compare
// the wire strings keep-latest, sum, max, min, and, or.
const (
	PolicyKeepLatest = "keep-latest"
	PolicySum        = "sum"
	PolicyMax        = "max"
	PolicyMin        = "min"
	PolicyAnd        = "and"
	PolicyOr         = "or"
)

// EdgeKind is read or write. Read sorts before write.
const (
	EdgeRead  = "read"
	EdgeWrite = "write"
)

// Edge is a derived read or write.
type Edge struct {
	From string
	Kind string
	To   string
}

// GraphEffectRun is one successful effect evaluation. Removal does not erase it.
type GraphEffectRun struct {
	ID     string
	Result any
}

type graphExpr struct {
	Type   string
	Lit    any
	Ref    string
	Op     string
	Args   []*graphExpr
	Fields []graphField
}

type graphField struct {
	Key   string
	Value *graphExpr
}

type graphStmt struct {
	Type   string
	Expr   *graphExpr
	Target string
	Steps  []*graphStmt
	Cond   *graphExpr
	Then   *graphStmt
	Else   *graphStmt
}

type graphNode struct {
	Kind   string
	ID     string
	Init   any
	Policy string
	Eager  bool
	Expr   *graphExpr
	Body   *graphStmt
}

type graphEditOp struct {
	Op   string
	Node *graphNode
	ID   string
}

// GraphIr is a parsed graph definition.
type GraphIr struct {
	id    *string
	nodes []*graphNode
	index map[string]int
}

// GraphEdit is an ordered list of add, replace, and remove ops.
type GraphEdit struct {
	ops []graphEditOp
}

// GraphIrFromJSON parses a graph document.
func GraphIrFromJSON(text string) (*GraphIr, error) {
	value, err := parseGraphJSON(text)
	if err != nil {
		return nil, err
	}
	return graphIrFromValue(value)
}

// GraphEditFromJSON parses an edit document.
func GraphEditFromJSON(text string) (*GraphEdit, error) {
	value, err := parseGraphJSON(text)
	if err != nil {
		return nil, err
	}
	return graphEditFromValue(value)
}

func (g *GraphIr) ID() (string, bool) {
	if g.id == nil {
		return "", false
	}
	return *g.id, true
}

func (g *GraphIr) Node(id string) *graphNode {
	slot, ok := g.index[id]
	if !ok {
		return nil
	}
	return g.nodes[slot]
}

func (g *GraphIr) ToJSON() map[string]any {
	obj := map[string]any{"kind": "graph", "ir": graphIRVersion}
	if g.id != nil {
		obj["id"] = *g.id
	}
	nodes := make([]any, len(g.nodes))
	for i, node := range g.nodes {
		nodes[i] = nodeJSON(node)
	}
	obj["nodes"] = nodes
	return obj
}

func (g *GraphIr) StaticEdges() []Edge {
	edges := []Edge{}
	for _, node := range g.nodes {
		reads, writes := edgeSets(node)
		for _, to := range reads {
			edges = append(edges, Edge{From: node.ID, Kind: EdgeRead, To: to})
		}
		for _, to := range writes {
			edges = append(edges, Edge{From: node.ID, Kind: EdgeWrite, To: to})
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From != edges[j].From {
			return edges[i].From < edges[j].From
		}
		if edges[i].Kind != edges[j].Kind {
			return edges[i].Kind < edges[j].Kind
		}
		return edges[i].To < edges[j].To
	})
	return edges
}

func (g *GraphIr) EmitSource() string {
	var b strings.Builder
	if g.id == nil {
		fmt.Fprintf(&b, "graph ir %s\n", graphIRVersion)
	} else {
		fmt.Fprintf(&b, "graph %s ir %s\n", *g.id, graphIRVersion)
	}
	for _, node := range g.nodes {
		b.WriteString(emitNode(node))
		b.WriteByte('\n')
	}
	return b.String()
}

func (g *GraphIr) check() error {
	if err := g.checkRefs(); err != nil {
		return err
	}
	if err := g.checkInits(); err != nil {
		return err
	}
	return g.checkCycles()
}

func (g *GraphIr) clone() *GraphIr {
	copied := &GraphIr{id: g.id, nodes: append([]*graphNode(nil), g.nodes...)}
	copied.reindex()
	return copied
}

func (g *GraphIr) applyEditOp(op graphEditOp) error {
	switch op.Op {
	case "add":
		if _, ok := g.index[op.Node.ID]; ok {
			return graphErr("add requires a new id, `%s` already exists", op.Node.ID)
		}
		g.nodes = append(g.nodes, op.Node)
	case "replace":
		slot, ok := g.index[op.Node.ID]
		if !ok {
			return graphErr("replace requires an existing node `%s`", op.Node.ID)
		}
		g.nodes[slot] = op.Node
	case "remove":
		slot, ok := g.index[op.ID]
		if !ok {
			return graphErr("remove requires an existing node `%s`", op.ID)
		}
		g.nodes = append(g.nodes[:slot], g.nodes[slot+1:]...)
	}
	if err := g.reindex(); err != nil {
		return err
	}
	return g.check()
}

func (g *GraphIr) reindex() error {
	g.index = map[string]int{}
	for slot, node := range g.nodes {
		if _, ok := g.index[node.ID]; ok {
			return graphErr("duplicate node id `%s`", node.ID)
		}
		g.index[node.ID] = slot
	}
	return nil
}

func (g *GraphIr) checkRefs() error {
	for _, node := range g.nodes {
		reads, writes := edgeSets(node)
		for _, to := range reads {
			target := g.Node(to)
			if target == nil {
				return graphErr("node `%s` reads unknown node `%s`", node.ID, to)
			}
			if target.Kind == "effect" {
				return graphErr("node `%s` reads effect `%s`, which has no value", node.ID, to)
			}
		}
		for _, to := range writes {
			target := g.Node(to)
			if target == nil {
				return graphErr("node `%s` writes unknown node `%s`", node.ID, to)
			}
			if target.Kind != "source" {
				return graphErr("node `%s` writes non-source `%s`", node.ID, to)
			}
		}
	}
	return nil
}

func (g *GraphIr) checkInits() error {
	for _, node := range g.nodes {
		if node.Kind != "source" {
			continue
		}
		if err := checkPolicyInit(node.Policy, node.Init); err != nil {
			return graphErr("source `%s`: %s", node.ID, err.Error())
		}
	}
	return nil
}

func (g *GraphIr) checkCycles() error {
	color := map[string]int{}
	var stack []string
	var visit func(string) error
	visit = func(id string) error {
		switch color[id] {
		case 1:
			start := 0
			for i, item := range stack {
				if item == id {
					start = i
					break
				}
			}
			cycle := append(append([]string{}, stack[start:]...), id)
			return graphErr("cycle in read edges: %s", strings.Join(cycle, " -> "))
		case 2:
			return nil
		}
		color[id] = 1
		stack = append(stack, id)
		if target := g.Node(id); target != nil {
			reads, _ := edgeSets(target)
			sort.Strings(reads)
			for _, dep := range reads {
				if g.Node(dep) != nil {
					if err := visit(dep); err != nil {
						return err
					}
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = 2
		return nil
	}
	for _, node := range g.nodes {
		if err := visit(node.ID); err != nil {
			return err
		}
	}
	return nil
}

func (e *GraphEdit) Ops() []graphEditOp { return append([]graphEditOp(nil), e.ops...) }

type sourceState struct {
	value  any
	policy string
}

type computedState struct {
	expr     *graphExpr
	eager    bool
	cache    any
	hasValue bool
	dirty    bool
}

type effectState struct{ body *graphStmt }

type bindingSnap struct {
	kind     string
	source   *sourceState
	computed *computedState
	effect   *effectState
	deps     map[string]bool
}

// GraphRuntime is a document plus the cells it was lowered onto.
type GraphRuntime struct {
	doc                 *GraphIr
	sources             map[string]*sourceState
	computeds           map[string]*computedState
	effects             map[string]*effectState
	deps                map[string]map[string]bool
	subs                map[string]map[string]bool
	observed            map[string]map[string]bool
	trace               []GraphEffectRun
	frames              []map[string]bool
	stack               []string
	current             string
	currentOK           bool
	computing           map[string]bool
	scheduled           []string
	scheduledSet        map[string]bool
	draining            bool
	fault               string
	DrainBudget         int
	LastDrainExhaustion *int
}

// LoadGraphRuntime parses and lowers a document.
func LoadGraphRuntime(text string) (*GraphRuntime, error) {
	doc, err := GraphIrFromJSON(text)
	if err != nil {
		return nil, err
	}
	return LowerGraph(doc, drainBudget)
}

// LowerGraph installs doc. drainBudget bounds a feedback loop; zero selects the default.
func LowerGraph(doc *GraphIr, budget int) (*GraphRuntime, error) {
	if err := doc.check(); err != nil {
		return nil, err
	}
	if budget <= 0 {
		budget = drainBudget
	}
	rt := &GraphRuntime{
		doc:          doc.clone(),
		sources:      map[string]*sourceState{},
		computeds:    map[string]*computedState{},
		effects:      map[string]*effectState{},
		deps:         map[string]map[string]bool{},
		subs:         map[string]map[string]bool{},
		observed:     map[string]map[string]bool{},
		computing:    map[string]bool{},
		scheduledSet: map[string]bool{},
		DrainBudget:  budget,
	}
	if err := rt.installDocument(); err != nil {
		return nil, err
	}
	return rt, nil
}

func (rt *GraphRuntime) Document() *GraphIr { return rt.doc }

func (rt *GraphRuntime) Contains(id string) bool { return rt.doc.Node(id) != nil }

func (rt *GraphRuntime) IsEager(id string) (bool, error) {
	node := rt.doc.Node(id)
	if node == nil {
		return false, graphErr("unknown node `%s`", id)
	}
	if node.Kind != "computed" {
		return false, graphErr("`%s` is not a computed", id)
	}
	return node.Eager, nil
}

func (rt *GraphRuntime) SourcePolicy(id string) (string, error) {
	node := rt.doc.Node(id)
	if node == nil {
		return "", graphErr("unknown node `%s`", id)
	}
	if node.Kind != "source" {
		return "", graphErr("`%s` is not a source", id)
	}
	return node.Policy, nil
}

func (rt *GraphRuntime) StaticEdges() []Edge { return rt.doc.StaticEdges() }

func (rt *GraphRuntime) EmitSource() string { return rt.doc.EmitSource() }

func (rt *GraphRuntime) ObservedReads(id string) map[string]bool {
	out := map[string]bool{}
	for key := range rt.observed[id] {
		out[key] = true
	}
	return out
}

func (rt *GraphRuntime) EffectTrace() []GraphEffectRun {
	return append([]GraphEffectRun(nil), rt.trace...)
}

func (rt *GraphRuntime) Get(id string) (any, error) {
	if _, ok := rt.effects[id]; ok {
		if _, src := rt.sources[id]; !src {
			if _, comp := rt.computeds[id]; !comp {
				return nil, graphErr("`%s` is an effect and has no value", id)
			}
		}
	}
	if src, ok := rt.sources[id]; ok {
		return freezeGraph(src.value), nil
	}
	if comp, ok := rt.computeds[id]; ok {
		if comp.dirty || !comp.hasValue {
			if _, err := rt.recompute(id); err != nil {
				return nil, err
			}
			comp = rt.computeds[id]
		}
		return freezeGraph(comp.cache), nil
	}
	return nil, graphErr("unknown node `%s`", id)
}

func (rt *GraphRuntime) WriteSource(id string, value any) error {
	if _, err := rt.setSource(id, freezeGraph(value)); err != nil {
		return err
	}
	rt.drain()
	return rt.raiseFault()
}

func (rt *GraphRuntime) Apply(edit *GraphEdit) error {
	for index, op := range edit.ops {
		next := rt.doc.clone()
		if err := next.applyEditOp(op); err != nil {
			return graphErr("edit op %d: %s", index, err.Error())
		}
		if err := rt.installOp(op); err != nil {
			return graphErr("edit op %d: %s", index, err.Error())
		}
		rt.doc = next
	}
	return nil
}

func (rt *GraphRuntime) installDocument() error {
	for _, node := range rt.doc.nodes {
		if node.Kind == "source" {
			if err := rt.insertSource(node); err != nil {
				return err
			}
		}
	}
	for _, node := range rt.doc.nodes {
		if node.Kind == "computed" {
			rt.insertComputed(node)
		}
	}
	for _, node := range rt.doc.nodes {
		if node.Kind == "computed" && node.Eager {
			if _, err := rt.recompute(node.ID); err != nil {
				return err
			}
		}
	}
	for _, node := range rt.doc.nodes {
		if node.Kind == "effect" {
			if err := rt.insertEffect(node); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rt *GraphRuntime) installOp(op graphEditOp) error {
	var err error
	switch op.Op {
	case "add":
		err = rt.addNode(op.Node)
	case "replace":
		err = rt.replaceNode(op.Node)
	default:
		err = rt.removeNode(op.ID)
	}
	if err != nil {
		return err
	}
	rt.drain()
	return rt.raiseFault()
}

func (rt *GraphRuntime) addNode(node *graphNode) error {
	switch node.Kind {
	case "source":
		return rt.insertSource(node)
	case "computed":
		rt.insertComputed(node)
		if node.Eager {
			if _, err := rt.recompute(node.ID); err != nil {
				rt.dropBinding(node.ID)
				return err
			}
		}
		return nil
	default:
		return rt.insertEffect(node)
	}
}

func (rt *GraphRuntime) replaceNode(node *graphNode) error {
	snap := rt.snapshot(node.ID)
	rt.dropBinding(node.ID)
	if err := rt.addNode(node); err != nil {
		rt.dropBinding(node.ID)
		rt.restore(node.ID, snap)
		return err
	}
	return rt.notify(node.ID)
}

func (rt *GraphRuntime) removeNode(id string) error {
	if _, ok := rt.sources[id]; !ok {
		if _, ok := rt.computeds[id]; !ok {
			if _, ok := rt.effects[id]; !ok {
				return graphErr("unknown node `%s`", id)
			}
		}
	}
	rt.dropBinding(id)
	delete(rt.subs, id)
	return nil
}

func (rt *GraphRuntime) insertSource(node *graphNode) error {
	if err := checkPolicyInit(node.Policy, node.Init); err != nil {
		return err
	}
	rt.sources[node.ID] = &sourceState{value: freezeGraph(node.Init), policy: node.Policy}
	return nil
}

func (rt *GraphRuntime) insertComputed(node *graphNode) {
	rt.computeds[node.ID] = &computedState{expr: node.Expr, eager: node.Eager, dirty: true}
	rt.deps[node.ID] = map[string]bool{}
}

func (rt *GraphRuntime) insertEffect(node *graphNode) error {
	rt.effects[node.ID] = &effectState{body: node.Body}
	rt.deps[node.ID] = map[string]bool{}
	if err := rt.runEffect(node.ID); err != nil {
		rt.dropBinding(node.ID)
		return err
	}
	return nil
}

func (rt *GraphRuntime) snapshot(id string) *bindingSnap {
	if src, ok := rt.sources[id]; ok {
		return &bindingSnap{kind: "source", source: &sourceState{value: freezeGraph(src.value), policy: src.policy}}
	}
	if comp, ok := rt.computeds[id]; ok {
		var cache any
		if comp.hasValue {
			cache = freezeGraph(comp.cache)
		}
		return &bindingSnap{
			kind: "computed",
			computed: &computedState{
				expr: comp.expr, eager: comp.eager, cache: cache, hasValue: comp.hasValue, dirty: comp.dirty,
			},
			deps: copySet(rt.deps[id]),
		}
	}
	if effect, ok := rt.effects[id]; ok {
		return &bindingSnap{kind: "effect", effect: &effectState{body: effect.body}, deps: copySet(rt.deps[id])}
	}
	return nil
}

func (rt *GraphRuntime) restore(id string, snap *bindingSnap) {
	if snap == nil {
		return
	}
	switch snap.kind {
	case "source":
		rt.sources[id] = snap.source
	case "computed":
		rt.computeds[id] = snap.computed
		rt.deps[id] = map[string]bool{}
		for dep := range snap.deps {
			rt.subAdd(dep, id)
			rt.deps[id][dep] = true
		}
	case "effect":
		rt.effects[id] = snap.effect
		rt.deps[id] = map[string]bool{}
		for dep := range snap.deps {
			rt.subAdd(dep, id)
			rt.deps[id][dep] = true
		}
	}
}

func (rt *GraphRuntime) dropBinding(id string) {
	delete(rt.sources, id)
	delete(rt.computeds, id)
	delete(rt.effects, id)
	for dep := range rt.deps[id] {
		delete(rt.subs[dep], id)
	}
	delete(rt.deps, id)
	delete(rt.observed, id)
	delete(rt.scheduledSet, id)
}

func (rt *GraphRuntime) setSource(id string, value any) (any, error) {
	src, ok := rt.sources[id]
	if !ok {
		if _, comp := rt.computeds[id]; comp {
			return nil, graphErr("`%s` is not a source", id)
		}
		if _, effect := rt.effects[id]; effect {
			return nil, graphErr("`%s` is not a source", id)
		}
		return nil, graphErr("unknown node `%s`", id)
	}
	changed := !jsonEq(src.value, value)
	src.value = freezeGraph(value)
	if changed {
		if err := rt.notify(id); err != nil {
			return nil, err
		}
	}
	return freezeGraph(src.value), nil
}

func (rt *GraphRuntime) mergeSource(id string, value any) (any, error) {
	src, ok := rt.sources[id]
	if !ok {
		if _, comp := rt.computeds[id]; comp {
			return nil, graphErr("`%s` is not a source", id)
		}
		if _, effect := rt.effects[id]; effect {
			return nil, graphErr("`%s` is not a source", id)
		}
		return nil, graphErr("unknown node `%s`", id)
	}
	folded, err := foldPolicy(src.policy, src.value, value)
	if err != nil {
		return nil, err
	}
	return rt.setSource(id, folded)
}

func (rt *GraphRuntime) notify(id string) error {
	for _, dep := range setKeys(rt.subs[id]) {
		if comp, ok := rt.computeds[dep]; ok {
			if comp.eager || len(rt.subs[dep]) > 0 {
				if rt.computing[dep] {
					comp.dirty = true
					continue
				}
				if _, err := rt.recompute(dep); err != nil {
					return err
				}
			} else {
				comp.dirty = true
			}
		} else if _, ok := rt.effects[dep]; ok {
			rt.schedule(dep)
		}
	}
	return nil
}

func (rt *GraphRuntime) schedule(id string) {
	if _, ok := rt.effects[id]; ok && !rt.scheduledSet[id] {
		rt.scheduledSet[id] = true
		rt.scheduled = append(rt.scheduled, id)
	}
}

func (rt *GraphRuntime) drain() {
	if rt.draining {
		return
	}
	rt.draining = true
	defer func() { rt.draining = false }()
	iterations := 0
	for len(rt.scheduled) > 0 {
		id := rt.scheduled[0]
		rt.scheduled = rt.scheduled[1:]
		delete(rt.scheduledSet, id)
		iterations++
		if iterations >= rt.DrainBudget {
			rt.LastDrainExhaustion = &iterations
			return
		}
		if _, ok := rt.effects[id]; !ok {
			continue
		}
		if err := rt.runEffect(id); err != nil {
			rt.fault = err.Error()
			return
		}
	}
}

func (rt *GraphRuntime) raiseFault() error {
	if rt.fault == "" {
		return nil
	}
	msg := rt.fault
	rt.fault = ""
	return &GraphIrError{Msg: msg}
}

func (rt *GraphRuntime) runEffect(id string) error {
	state := rt.effects[id]
	rt.begin(id)
	result, err := rt.evalStmt(state.body)
	rt.end(id)
	if err != nil {
		return err
	}
	rt.trace = append(rt.trace, GraphEffectRun{ID: id, Result: freezeGraph(result)})
	rt.drain()
	return nil
}

func (rt *GraphRuntime) recompute(id string) (any, error) {
	state := rt.computeds[id]
	if rt.computing[id] {
		return nil, graphErr("cycle in read edges: %s", id)
	}
	rt.computing[id] = true
	defer delete(rt.computing, id)
	rt.begin(id)
	value, err := rt.evalExpr(state.expr)
	rt.end(id)
	if err != nil {
		state.dirty = true
		state.hasValue = false
		state.cache = nil
		return nil, err
	}
	changed := !state.hasValue || !jsonEq(state.cache, value)
	state.cache = freezeGraph(value)
	state.hasValue = true
	state.dirty = false
	if changed {
		if err := rt.notify(id); err != nil {
			return nil, err
		}
	}
	return state.cache, nil
}

func (rt *GraphRuntime) begin(id string) {
	for dep := range rt.deps[id] {
		delete(rt.subs[dep], id)
	}
	rt.deps[id] = map[string]bool{}
	rt.frames = append(rt.frames, map[string]bool{})
	if rt.currentOK {
		rt.stack = append(rt.stack, rt.current)
	} else {
		rt.stack = append(rt.stack, "")
	}
	rt.current = id
	rt.currentOK = true
}

func (rt *GraphRuntime) end(id string) {
	frame := rt.frames[len(rt.frames)-1]
	rt.frames = rt.frames[:len(rt.frames)-1]
	rt.observed[id] = frame
	prev := rt.stack[len(rt.stack)-1]
	rt.stack = rt.stack[:len(rt.stack)-1]
	if len(rt.stack) == 0 && prev == "" && !rt.currentOK {
		rt.currentOK = false
		return
	}
	// An empty previous marker means the outer frame had no current node.
	if prev == "" && len(rt.frames) == 0 {
		rt.current = ""
		rt.currentOK = false
		return
	}
	rt.current = prev
	rt.currentOK = prev != "" || len(rt.frames) > 0
}

func (rt *GraphRuntime) note(id string) error {
	if len(rt.frames) == 0 || !rt.currentOK {
		return graphErr("read outside a node evaluation")
	}
	rt.frames[len(rt.frames)-1][id] = true
	rt.subAdd(id, rt.current)
	rt.deps[rt.current][id] = true
	return nil
}

func (rt *GraphRuntime) subAdd(src, reader string) {
	set, ok := rt.subs[src]
	if !ok {
		set = map[string]bool{}
		rt.subs[src] = set
	}
	set[reader] = true
}

func (rt *GraphRuntime) readRef(id string) (any, error) {
	if err := rt.note(id); err != nil {
		return nil, err
	}
	if src, ok := rt.sources[id]; ok {
		return freezeGraph(src.value), nil
	}
	if comp, ok := rt.computeds[id]; ok {
		if comp.dirty || !comp.hasValue {
			value, err := rt.recompute(id)
			if err != nil {
				return nil, err
			}
			return freezeGraph(value), nil
		}
		return freezeGraph(comp.cache), nil
	}
	if _, ok := rt.effects[id]; ok {
		return nil, graphErr("`%s` is an effect and has no value", id)
	}
	return nil, graphErr("unknown node `%s`", id)
}

func (rt *GraphRuntime) evalExpr(expr *graphExpr) (any, error) {
	switch expr.Type {
	case "lit":
		return freezeGraph(expr.Lit), nil
	case "ref":
		return rt.readRef(expr.Ref)
	case "object":
		obj := map[string]any{}
		for _, field := range expr.Fields {
			if _, ok := obj[field.Key]; ok {
				return nil, graphErr("duplicate object key `%s`", field.Key)
			}
			value, err := rt.evalExpr(field.Value)
			if err != nil {
				return nil, err
			}
			obj[field.Key] = value
		}
		return obj, nil
	}
	switch expr.Op {
	case "and":
		return rt.evalAnd(expr.Args)
	case "or":
		return rt.evalOr(expr.Args)
	case "if":
		return rt.evalIf(expr.Args)
	case "array":
		out := make([]any, len(expr.Args))
		for i, arg := range expr.Args {
			value, err := rt.evalExpr(arg)
			if err != nil {
				return nil, err
			}
			out[i] = value
		}
		return out, nil
	default:
		values := make([]any, len(expr.Args))
		for i, arg := range expr.Args {
			value, err := rt.evalExpr(arg)
			if err != nil {
				return nil, err
			}
			values[i] = value
		}
		return evalValues(expr.Op, values)
	}
}

func (rt *GraphRuntime) evalAnd(args []*graphExpr) (any, error) {
	if len(args) != 2 {
		return nil, graphErr("op `and` has the wrong number of arguments")
	}
	left, err := rt.evalExpr(args[0])
	if err != nil {
		return nil, err
	}
	bit, err := expectBool(left)
	if err != nil {
		return nil, err
	}
	if !bit {
		return false, nil
	}
	right, err := rt.evalExpr(args[1])
	if err != nil {
		return nil, err
	}
	return expectBool(right)
}

func (rt *GraphRuntime) evalOr(args []*graphExpr) (any, error) {
	if len(args) != 2 {
		return nil, graphErr("op `or` has the wrong number of arguments")
	}
	left, err := rt.evalExpr(args[0])
	if err != nil {
		return nil, err
	}
	bit, err := expectBool(left)
	if err != nil {
		return nil, err
	}
	if bit {
		return true, nil
	}
	right, err := rt.evalExpr(args[1])
	if err != nil {
		return nil, err
	}
	return expectBool(right)
}

func (rt *GraphRuntime) evalIf(args []*graphExpr) (any, error) {
	if len(args) != 3 {
		return nil, graphErr("op `if` has the wrong number of arguments")
	}
	cond, err := rt.evalExpr(args[0])
	if err != nil {
		return nil, err
	}
	bit, err := expectBool(cond)
	if err != nil {
		return nil, err
	}
	if bit {
		return rt.evalExpr(args[1])
	}
	return rt.evalExpr(args[2])
}

func (rt *GraphRuntime) evalStmt(stmt *graphStmt) (any, error) {
	switch stmt.Type {
	case "eval":
		return rt.evalExpr(stmt.Expr)
	case "set":
		value, err := rt.evalExpr(stmt.Expr)
		if err != nil {
			return nil, err
		}
		return rt.setSource(stmt.Target, value)
	case "merge":
		value, err := rt.evalExpr(stmt.Expr)
		if err != nil {
			return nil, err
		}
		return rt.mergeSource(stmt.Target, value)
	case "seq":
		out := make([]any, len(stmt.Steps))
		for i, step := range stmt.Steps {
			value, err := rt.evalStmt(step)
			if err != nil {
				return nil, err
			}
			out[i] = value
		}
		return out, nil
	case "when":
		cond, err := rt.evalExpr(stmt.Cond)
		if err != nil {
			return nil, err
		}
		bit, err := expectBool(cond)
		if err != nil {
			return nil, err
		}
		if bit {
			return rt.evalStmt(stmt.Then)
		}
		if stmt.Else != nil {
			return rt.evalStmt(stmt.Else)
		}
		return nil, nil
	default:
		return nil, graphErr("unknown statement `%s`", stmt.Type)
	}
}

func parseGraphJSON(text string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, graphErr("graph document is not JSON: %s", err.Error())
	}
	if err := dec.Decode(&struct{}{}); err != nil && err.Error() != "EOF" {
		return nil, graphErr("graph document is not JSON: %s", err.Error())
	}
	return normalizeGraph(value)
}

func normalizeGraph(value any) (any, error) {
	switch typed := value.(type) {
	case json.Number:
		text := typed.String()
		if strings.ContainsAny(text, ".eE") {
			number, err := typed.Float64()
			if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
				return nil, graphErr("number %s is outside the graph IR numeric range", text)
			}
			return number, nil
		}
		integer, err := typed.Int64()
		if err != nil {
			number, ferr := typed.Float64()
			if ferr != nil || math.IsNaN(number) || math.IsInf(number, 0) {
				return nil, graphErr("number %s is outside the graph IR numeric range", text)
			}
			return number, nil
		}
		return integer, nil
	case map[string]any:
		out := map[string]any{}
		for key, item := range typed {
			norm, err := normalizeGraph(item)
			if err != nil {
				return nil, err
			}
			out[key] = norm
		}
		return out, nil
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			norm, err := normalizeGraph(item)
			if err != nil {
				return nil, err
			}
			out[i] = norm
		}
		return out, nil
	default:
		return value, nil
	}
}

func graphIrFromValue(value any) (*GraphIr, error) {
	obj, err := asObject(value, "graph document")
	if err != nil {
		return nil, err
	}
	if err := allowKeys(obj, "graph document", "kind", "ir", "id", "nodes"); err != nil {
		return nil, err
	}
	kind, err := requireString(obj, "kind", "graph document")
	if err != nil {
		return nil, err
	}
	if kind != "graph" {
		return nil, graphErr("graph document kind must be \"graph\", got `%s`", kind)
	}
	if err := requireVersion(obj, "graph document"); err != nil {
		return nil, err
	}
	id, err := optionalID(obj, "id", "graph id")
	if err != nil {
		return nil, err
	}
	raw, err := requireField(obj, "nodes", "graph document")
	if err != nil {
		return nil, err
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, graphErr("graph nodes must be an array")
	}
	nodes := make([]*graphNode, 0, len(list))
	seen := map[string]bool{}
	for _, item := range list {
		node, err := parseNode(item)
		if err != nil {
			return nil, err
		}
		if seen[node.ID] {
			return nil, graphErr("duplicate node id `%s`", node.ID)
		}
		seen[node.ID] = true
		nodes = append(nodes, node)
	}
	doc := &GraphIr{id: id, nodes: nodes}
	if err := doc.reindex(); err != nil {
		return nil, err
	}
	if err := doc.check(); err != nil {
		return nil, err
	}
	return doc, nil
}

func graphEditFromValue(value any) (*GraphEdit, error) {
	obj, err := asObject(value, "graph edit")
	if err != nil {
		return nil, err
	}
	if err := allowKeys(obj, "graph edit", "kind", "ir", "ops"); err != nil {
		return nil, err
	}
	kind, err := requireString(obj, "kind", "graph edit")
	if err != nil {
		return nil, err
	}
	if kind != "edit" {
		return nil, graphErr("graph edit kind must be \"edit\", got `%s`", kind)
	}
	if err := requireVersion(obj, "graph edit"); err != nil {
		return nil, err
	}
	raw, err := requireField(obj, "ops", "graph edit")
	if err != nil {
		return nil, err
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, graphErr("graph edit ops must be an array")
	}
	if len(list) == 0 {
		return nil, graphErr("graph edit ops must not be empty")
	}
	ops := make([]graphEditOp, len(list))
	for i, item := range list {
		op, err := parseEditOp(item)
		if err != nil {
			return nil, err
		}
		ops[i] = op
	}
	return &GraphEdit{ops: ops}, nil
}

func asObject(value any, what string) (map[string]any, error) {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, graphErr("%s must be an object", what)
	}
	return obj, nil
}

func allowKeys(obj map[string]any, what string, keys ...string) error {
	allowed := map[string]bool{}
	for _, key := range keys {
		allowed[key] = true
	}
	for key := range obj {
		if !allowed[key] {
			return graphErr("%s: unknown field `%s`", what, key)
		}
	}
	return nil
}

func requireField(obj map[string]any, field, what string) (any, error) {
	value, ok := obj[field]
	if !ok {
		return nil, graphErr("%s requires `%s`", what, field)
	}
	return value, nil
}

func requireString(obj map[string]any, field, what string) (string, error) {
	value, err := requireField(obj, field, what)
	if err != nil {
		return "", err
	}
	text, ok := value.(string)
	if !ok {
		return "", graphErr("%s `%s` must be a string, got %s", what, field, jsonText(value))
	}
	return text, nil
}

func parseID(text, what string) (string, error) {
	if !graphIDPattern.MatchString(text) || len(text) > 128 {
		return "", graphErr("%s `%s` is not a graph IR id", what, text)
	}
	return text, nil
}

func requireVersion(obj map[string]any, what string) error {
	version, err := requireString(obj, "ir", what)
	if err != nil {
		return err
	}
	if version != graphIRVersion {
		return graphErr("unsupported graph IR version `%s`", version)
	}
	return nil
}

func optionalID(obj map[string]any, field, what string) (*string, error) {
	value, ok := obj[field]
	if !ok {
		return nil, nil
	}
	text, ok := value.(string)
	if !ok {
		return nil, graphErr("%s must be a string, got %s", what, jsonText(value))
	}
	id, err := parseID(text, what)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func parseNode(value any) (*graphNode, error) {
	obj, err := asObject(value, "node")
	if err != nil {
		return nil, err
	}
	kind, err := requireString(obj, "kind", "node")
	if err != nil {
		return nil, err
	}
	switch kind {
	case "source":
		if err := allowKeys(obj, "source", "id", "kind", "init", "policy"); err != nil {
			return nil, err
		}
		id, err := requireString(obj, "id", "source")
		if err != nil {
			return nil, err
		}
		id, err = parseID(id, "source id")
		if err != nil {
			return nil, err
		}
		if _, ok := obj["init"]; !ok {
			return nil, graphErr("source requires `init`")
		}
		policy, err := parsePolicy(obj, id)
		if err != nil {
			return nil, err
		}
		return &graphNode{Kind: kind, ID: id, Init: freezeGraph(obj["init"]), Policy: policy}, nil
	case "computed":
		if err := allowKeys(obj, "computed", "id", "kind", "expr", "eager"); err != nil {
			return nil, err
		}
		id, err := requireString(obj, "id", "computed")
		if err != nil {
			return nil, err
		}
		id, err = parseID(id, "computed id")
		if err != nil {
			return nil, err
		}
		eager, err := parseEager(obj, id)
		if err != nil {
			return nil, err
		}
		raw, err := requireField(obj, "expr", "computed")
		if err != nil {
			return nil, err
		}
		expr, err := parseExpr(raw)
		if err != nil {
			return nil, err
		}
		return &graphNode{Kind: kind, ID: id, Eager: eager, Expr: expr}, nil
	case "effect":
		if err := allowKeys(obj, "effect", "id", "kind", "body"); err != nil {
			return nil, err
		}
		id, err := requireString(obj, "id", "effect")
		if err != nil {
			return nil, err
		}
		id, err = parseID(id, "effect id")
		if err != nil {
			return nil, err
		}
		raw, err := requireField(obj, "body", "effect")
		if err != nil {
			return nil, err
		}
		body, err := parseStmt(raw)
		if err != nil {
			return nil, err
		}
		return &graphNode{Kind: kind, ID: id, Body: body}, nil
	default:
		return nil, graphErr("unknown node kind `%s`", kind)
	}
}

func parsePolicy(obj map[string]any, id string) (string, error) {
	value, ok := obj["policy"]
	if !ok {
		return PolicyKeepLatest, nil
	}
	text, ok := value.(string)
	if !ok {
		return "", graphErr("source `%s` policy must be a string, got %s", id, jsonText(value))
	}
	switch text {
	case PolicyKeepLatest, PolicySum, PolicyMax, PolicyMin, PolicyAnd, PolicyOr:
		return text, nil
	default:
		return "", graphErr("unknown source policy `%s`", text)
	}
}

func parseEager(obj map[string]any, id string) (bool, error) {
	value, ok := obj["eager"]
	if !ok {
		return false, nil
	}
	bit, ok := value.(bool)
	if !ok {
		return false, graphErr("computed `%s` eager must be a boolean, got %s", id, jsonText(value))
	}
	return bit, nil
}

func parseExpr(value any) (*graphExpr, error) {
	obj, err := asObject(value, "expression")
	if err != nil {
		return nil, err
	}
	if err := allowKeys(obj, "expression", "lit", "ref", "op", "args", "object"); err != nil {
		return nil, err
	}
	forms := 0
	for _, key := range []string{"lit", "ref", "op", "object"} {
		if _, ok := obj[key]; ok {
			forms++
		}
	}
	if forms != 1 {
		return nil, graphErr("expression must be exactly one of lit, ref, op, or object")
	}
	if _, ok := obj["args"]; ok {
		if _, hasOp := obj["op"]; !hasOp {
			return nil, graphErr("args is only valid on an op expression")
		}
	}
	if _, ok := obj["lit"]; ok {
		return &graphExpr{Type: "lit", Lit: freezeGraph(obj["lit"])}, nil
	}
	if _, ok := obj["ref"]; ok {
		text, err := requireString(obj, "ref", "expression")
		if err != nil {
			return nil, err
		}
		id, err := parseID(text, "ref")
		if err != nil {
			return nil, err
		}
		return &graphExpr{Type: "ref", Ref: id}, nil
	}
	if raw, ok := obj["object"]; ok {
		return parseObjectExpr(raw)
	}
	op, err := requireString(obj, "op", "expression")
	if err != nil {
		return nil, err
	}
	if !graphUnary[op] && !graphBinary[op] && op != "if" && op != "array" {
		return nil, graphErr("unknown op `%s`", op)
	}
	raw, err := requireField(obj, "args", "op expression")
	if err != nil {
		return nil, err
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, graphErr("op args must be an array")
	}
	if err := checkArity(op, len(list)); err != nil {
		return nil, err
	}
	args := make([]*graphExpr, len(list))
	for i, item := range list {
		arg, err := parseExpr(item)
		if err != nil {
			return nil, err
		}
		args[i] = arg
	}
	return &graphExpr{Type: "call", Op: op, Args: args}, nil
}

func checkArity(op string, length int) error {
	ok := false
	switch {
	case graphUnary[op]:
		ok = length == 1
	case op == "if":
		ok = length == 3
	case op == "array":
		ok = true
	default:
		ok = length == 2
	}
	if !ok {
		return graphErr("op `%s` has the wrong number of arguments", op)
	}
	return nil
}

func parseObjectExpr(value any) (*graphExpr, error) {
	list, ok := value.([]any)
	if !ok {
		return nil, graphErr("object expression must be an array")
	}
	fields := make([]graphField, 0, len(list))
	seen := map[string]bool{}
	for _, item := range list {
		obj, err := asObject(item, "object field")
		if err != nil {
			return nil, err
		}
		if err := allowKeys(obj, "object field", "key", "value"); err != nil {
			return nil, err
		}
		key, err := requireString(obj, "key", "object field")
		if err != nil {
			return nil, err
		}
		if seen[key] {
			return nil, graphErr("duplicate object key `%s`", key)
		}
		seen[key] = true
		raw, err := requireField(obj, "value", "object field")
		if err != nil {
			return nil, err
		}
		expr, err := parseExpr(raw)
		if err != nil {
			return nil, err
		}
		fields = append(fields, graphField{Key: key, Value: expr})
	}
	return &graphExpr{Type: "object", Fields: fields}, nil
}

func parseStmt(value any) (*graphStmt, error) {
	obj, err := asObject(value, "statement")
	if err != nil {
		return nil, err
	}
	verb, err := requireString(obj, "do", "statement")
	if err != nil {
		return nil, err
	}
	switch verb {
	case "eval":
		if err := allowKeys(obj, "eval", "do", "expr"); err != nil {
			return nil, err
		}
		raw, err := requireField(obj, "expr", "eval")
		if err != nil {
			return nil, err
		}
		expr, err := parseExpr(raw)
		if err != nil {
			return nil, err
		}
		return &graphStmt{Type: "eval", Expr: expr}, nil
	case "set", "merge":
		if err := allowKeys(obj, verb, "do", "target", "expr"); err != nil {
			return nil, err
		}
		target, err := requireString(obj, "target", verb)
		if err != nil {
			return nil, err
		}
		target, err = parseID(target, "write target")
		if err != nil {
			return nil, err
		}
		raw, err := requireField(obj, "expr", verb)
		if err != nil {
			return nil, err
		}
		expr, err := parseExpr(raw)
		if err != nil {
			return nil, err
		}
		return &graphStmt{Type: verb, Target: target, Expr: expr}, nil
	case "seq":
		if err := allowKeys(obj, "seq", "do", "steps"); err != nil {
			return nil, err
		}
		raw, err := requireField(obj, "steps", "seq")
		if err != nil {
			return nil, err
		}
		list, ok := raw.([]any)
		if !ok {
			return nil, graphErr("seq steps must be an array")
		}
		if len(list) == 0 {
			return nil, graphErr("seq steps must not be empty")
		}
		steps := make([]*graphStmt, len(list))
		for i, item := range list {
			step, err := parseStmt(item)
			if err != nil {
				return nil, err
			}
			steps[i] = step
		}
		return &graphStmt{Type: "seq", Steps: steps}, nil
	case "when":
		if err := allowKeys(obj, "when", "do", "cond", "then", "else"); err != nil {
			return nil, err
		}
		rawCond, err := requireField(obj, "cond", "when")
		if err != nil {
			return nil, err
		}
		cond, err := parseExpr(rawCond)
		if err != nil {
			return nil, err
		}
		rawThen, err := requireField(obj, "then", "when")
		if err != nil {
			return nil, err
		}
		then, err := parseStmt(rawThen)
		if err != nil {
			return nil, err
		}
		var elseStmt *graphStmt
		if rawElse, ok := obj["else"]; ok {
			elseStmt, err = parseStmt(rawElse)
			if err != nil {
				return nil, err
			}
		}
		return &graphStmt{Type: "when", Cond: cond, Then: then, Else: elseStmt}, nil
	default:
		return nil, graphErr("unknown statement `%s`", verb)
	}
}

func parseEditOp(value any) (graphEditOp, error) {
	obj, err := asObject(value, "edit op")
	if err != nil {
		return graphEditOp{}, err
	}
	op, err := requireString(obj, "op", "edit op")
	if err != nil {
		return graphEditOp{}, err
	}
	switch op {
	case "add", "replace":
		if err := allowKeys(obj, op, "op", "node"); err != nil {
			return graphEditOp{}, err
		}
		raw, err := requireField(obj, "node", op)
		if err != nil {
			return graphEditOp{}, err
		}
		node, err := parseNode(raw)
		if err != nil {
			return graphEditOp{}, err
		}
		return graphEditOp{Op: op, Node: node}, nil
	case "remove":
		if err := allowKeys(obj, "remove", "op", "id"); err != nil {
			return graphEditOp{}, err
		}
		id, err := requireString(obj, "id", "remove")
		if err != nil {
			return graphEditOp{}, err
		}
		id, err = parseID(id, "remove id")
		if err != nil {
			return graphEditOp{}, err
		}
		return graphEditOp{Op: op, ID: id}, nil
	default:
		return graphEditOp{}, graphErr("unknown edit op `%s`", op)
	}
}

func edgeSets(node *graphNode) ([]string, []string) {
	reads := map[string]bool{}
	writes := map[string]bool{}
	if node.Kind == "computed" {
		collectExpr(node.Expr, reads)
	} else if node.Kind == "effect" {
		collectStmt(node.Body, reads, writes)
	}
	return setKeys(reads), setKeys(writes)
}

func collectExpr(expr *graphExpr, reads map[string]bool) {
	switch expr.Type {
	case "ref":
		reads[expr.Ref] = true
	case "call":
		for _, arg := range expr.Args {
			collectExpr(arg, reads)
		}
	case "object":
		for _, field := range expr.Fields {
			collectExpr(field.Value, reads)
		}
	}
}

func collectStmt(stmt *graphStmt, reads, writes map[string]bool) {
	switch stmt.Type {
	case "eval":
		collectExpr(stmt.Expr, reads)
	case "set", "merge":
		writes[stmt.Target] = true
		collectExpr(stmt.Expr, reads)
	case "seq":
		for _, step := range stmt.Steps {
			collectStmt(step, reads, writes)
		}
	case "when":
		collectExpr(stmt.Cond, reads)
		collectStmt(stmt.Then, reads, writes)
		if stmt.Else != nil {
			collectStmt(stmt.Else, reads, writes)
		}
	}
}

func emitNode(node *graphNode) string {
	switch node.Kind {
	case "source":
		line := fmt.Sprintf("source %s = %s", node.ID, jsonText(node.Init))
		if node.Policy != PolicyKeepLatest {
			line += " policy " + node.Policy
		}
		return line
	case "computed":
		if node.Eager {
			return fmt.Sprintf("computed %s eager = %s", node.ID, emitExpr(node.Expr))
		}
		return fmt.Sprintf("computed %s = %s", node.ID, emitExpr(node.Expr))
	default:
		return fmt.Sprintf("effect %s = %s", node.ID, emitStmt(node.Body))
	}
}

func emitExpr(expr *graphExpr) string {
	switch expr.Type {
	case "lit":
		return "lit(" + jsonText(expr.Lit) + ")"
	case "ref":
		return "ref(" + expr.Ref + ")"
	case "call":
		parts := make([]string, len(expr.Args))
		for i, arg := range expr.Args {
			parts[i] = emitExpr(arg)
		}
		return expr.Op + "(" + strings.Join(parts, ", ") + ")"
	default:
		parts := make([]string, len(expr.Fields))
		for i, field := range expr.Fields {
			parts[i] = jsonText(field.Key) + ": " + emitExpr(field.Value)
		}
		return "object(" + strings.Join(parts, ", ") + ")"
	}
}

func emitStmt(stmt *graphStmt) string {
	switch stmt.Type {
	case "eval":
		return "eval(" + emitExpr(stmt.Expr) + ")"
	case "set":
		return fmt.Sprintf("set(%s, %s)", stmt.Target, emitExpr(stmt.Expr))
	case "merge":
		return fmt.Sprintf("merge(%s, %s)", stmt.Target, emitExpr(stmt.Expr))
	case "seq":
		parts := make([]string, len(stmt.Steps))
		for i, step := range stmt.Steps {
			parts[i] = emitStmt(step)
		}
		return "seq(" + strings.Join(parts, ", ") + ")"
	default:
		if stmt.Else == nil {
			return fmt.Sprintf("when(%s, %s)", emitExpr(stmt.Cond), emitStmt(stmt.Then))
		}
		return fmt.Sprintf("when(%s, %s, %s)", emitExpr(stmt.Cond), emitStmt(stmt.Then), emitStmt(stmt.Else))
	}
}

func nodeJSON(node *graphNode) map[string]any {
	switch node.Kind {
	case "source":
		obj := map[string]any{"id": node.ID, "kind": "source", "init": freezeGraph(node.Init)}
		if node.Policy != PolicyKeepLatest {
			obj["policy"] = node.Policy
		}
		return obj
	case "computed":
		obj := map[string]any{"id": node.ID, "kind": "computed"}
		if node.Eager {
			obj["eager"] = true
		}
		obj["expr"] = exprJSON(node.Expr)
		return obj
	default:
		return map[string]any{"id": node.ID, "kind": "effect", "body": stmtJSON(node.Body)}
	}
}

func exprJSON(expr *graphExpr) map[string]any {
	switch expr.Type {
	case "lit":
		return map[string]any{"lit": freezeGraph(expr.Lit)}
	case "ref":
		return map[string]any{"ref": expr.Ref}
	case "call":
		args := make([]any, len(expr.Args))
		for i, arg := range expr.Args {
			args[i] = exprJSON(arg)
		}
		return map[string]any{"op": expr.Op, "args": args}
	default:
		fields := make([]any, len(expr.Fields))
		for i, field := range expr.Fields {
			fields[i] = map[string]any{"key": field.Key, "value": exprJSON(field.Value)}
		}
		return map[string]any{"object": fields}
	}
}

func stmtJSON(stmt *graphStmt) map[string]any {
	switch stmt.Type {
	case "eval":
		return map[string]any{"do": "eval", "expr": exprJSON(stmt.Expr)}
	case "set":
		return map[string]any{"do": "set", "target": stmt.Target, "expr": exprJSON(stmt.Expr)}
	case "merge":
		return map[string]any{"do": "merge", "target": stmt.Target, "expr": exprJSON(stmt.Expr)}
	case "seq":
		steps := make([]any, len(stmt.Steps))
		for i, step := range stmt.Steps {
			steps[i] = stmtJSON(step)
		}
		return map[string]any{"do": "seq", "steps": steps}
	default:
		obj := map[string]any{"do": "when", "cond": exprJSON(stmt.Cond), "then": stmtJSON(stmt.Then)}
		if stmt.Else != nil {
			obj["else"] = stmtJSON(stmt.Else)
		}
		return obj
	}
}

func jsonText(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "null"
	}
	return string(encoded)
}

func freezeGraph(value any) any {
	switch typed := value.(type) {
	case nil, bool, string, int64, float64:
		return typed
	case int:
		return int64(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = freezeGraph(item)
		}
		return out
	case map[string]any:
		out := map[string]any{}
		for key, item := range typed {
			out[key] = freezeGraph(item)
		}
		return out
	default:
		return typed
	}
}

func jsonEq(left, right any) bool {
	switch a := left.(type) {
	case nil:
		return right == nil
	case bool:
		b, ok := right.(bool)
		return ok && a == b
	case int64:
		b, ok := right.(int64)
		return ok && a == b
	case float64:
		b, ok := right.(float64)
		return ok && a == b
	case string:
		b, ok := right.(string)
		return ok && a == b
	case []any:
		b, ok := right.([]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for i := range a {
			if !jsonEq(a[i], b[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		b, ok := right.(map[string]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for key, value := range a {
			other, exists := b[key]
			if !exists || !jsonEq(value, other) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func expectBool(value any) (bool, error) {
	bit, ok := value.(bool)
	if !ok {
		return false, graphErr("expected a boolean, got %s", jsonText(value))
	}
	return bit, nil
}

func expectString(value any) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", graphErr("expected a string, got %s", jsonText(value))
	}
	return text, nil
}

func asNum(value any) (string, int64, float64, error) {
	switch typed := value.(type) {
	case int64:
		return "int", typed, 0, nil
	case int:
		return "int", int64(typed), 0, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return "", 0, 0, graphErr("non-finite number")
		}
		return "float", 0, typed, nil
	default:
		return "", 0, 0, graphErr("expected a number, got %s", jsonText(value))
	}
}

func checkPolicyInit(policy string, init any) error {
	switch policy {
	case PolicySum, PolicyMax, PolicyMin:
		_, _, _, err := asNum(init)
		return err
	case PolicyAnd, PolicyOr:
		_, err := expectBool(init)
		return err
	default:
		return nil
	}
}

func canonicalF64(number float64) (any, error) {
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return nil, graphErr("non-finite number")
	}
	if number == math.Trunc(number) && number >= float64(math.MinInt64) && number < 9223372036854775808.0 {
		integer := int64(number)
		if float64(integer) == number {
			return integer, nil
		}
	}
	return number, nil
}

func checkedInt(value int64, ok bool) (int64, error) {
	if !ok {
		return 0, graphErr("integer overflow")
	}
	return value, nil
}

func addInt(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, graphErr("integer overflow")
	}
	return a + b, nil
}

func subInt(a, b int64) (int64, error) {
	if (b < 0 && a > math.MaxInt64+b) || (b > 0 && a < math.MinInt64+b) {
		return 0, graphErr("integer overflow")
	}
	return a - b, nil
}

func mulInt(a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	result := a * b
	if result/b != a {
		return 0, graphErr("integer overflow")
	}
	return result, nil
}

func ordering(left, right any) (int, error) {
	ak, ai, af, err := asNum(left)
	if err != nil {
		return 0, err
	}
	bk, bi, bf, err := asNum(right)
	if err != nil {
		return 0, err
	}
	if ak == "int" && bk == "int" {
		switch {
		case ai < bi:
			return -1, nil
		case ai > bi:
			return 1, nil
		default:
			return 0, nil
		}
	}
	lf, rf := af, bf
	if ak == "int" {
		lf = float64(ai)
	}
	if bk == "int" {
		rf = float64(bi)
	}
	switch {
	case lf < rf:
		return -1, nil
	case lf > rf:
		return 1, nil
	default:
		return 0, nil
	}
}

func numBin(op string, left, right any) (any, error) {
	ak, ai, af, err := asNum(left)
	if err != nil {
		return nil, err
	}
	bk, bi, bf, err := asNum(right)
	if err != nil {
		return nil, err
	}
	if op != "div" && ak == "int" && bk == "int" {
		switch op {
		case "add":
			return addInt(ai, bi)
		case "sub":
			return subInt(ai, bi)
		case "mul":
			return mulInt(ai, bi)
		default:
			return nil, graphErr("op `%s` is not an integer operator", op)
		}
	}
	lf, rf := af, bf
	if ak == "int" {
		lf = float64(ai)
	}
	if bk == "int" {
		rf = float64(bi)
	}
	if op == "div" && rf == 0 {
		return nil, graphErr("division by zero")
	}
	var value float64
	switch op {
	case "add":
		value = lf + rf
	case "sub":
		value = lf - rf
	case "mul":
		value = lf * rf
	case "div":
		value = lf / rf
	default:
		return nil, graphErr("op `%s` is not a numeric operator", op)
	}
	return canonicalF64(value)
}

func evalValues(op string, values []any) (any, error) {
	one := func(name string) (any, error) {
		if len(values) != 1 {
			return nil, graphErr("op `%s` has the wrong number of arguments", name)
		}
		return values[0], nil
	}
	pair := func(name string) (any, any, error) {
		if len(values) != 2 {
			return nil, nil, graphErr("op `%s` has the wrong number of arguments", name)
		}
		return values[0], values[1], nil
	}
	switch op {
	case "neg":
		value, err := one("neg")
		if err != nil {
			return nil, err
		}
		kind, integer, float, err := asNum(value)
		if err != nil {
			return nil, err
		}
		if kind == "int" {
			if integer == math.MinInt64 {
				return nil, graphErr("integer overflow")
			}
			return -integer, nil
		}
		return canonicalF64(-float)
	case "not":
		value, err := one("not")
		if err != nil {
			return nil, err
		}
		bit, err := expectBool(value)
		if err != nil {
			return nil, err
		}
		return !bit, nil
	case "len":
		value, err := one("len")
		if err != nil {
			return nil, err
		}
		return lenValue(value)
	case "add", "sub", "mul", "div":
		left, right, err := pair(op)
		if err != nil {
			return nil, err
		}
		return numBin(op, left, right)
	case "mod":
		left, right, err := pair("mod")
		if err != nil {
			return nil, err
		}
		ak, ai, _, err := asNum(left)
		if err != nil {
			return nil, err
		}
		bk, bi, _, err := asNum(right)
		if err != nil {
			return nil, err
		}
		if ak != "int" || bk != "int" {
			return nil, graphErr("mod requires integers")
		}
		if bi == 0 {
			return nil, graphErr("division by zero")
		}
		return ai % bi, nil
	case "min", "max":
		left, right, err := pair(op)
		if err != nil {
			return nil, err
		}
		order, err := ordering(left, right)
		if err != nil {
			return nil, err
		}
		if op == "max" {
			if order >= 0 {
				return left, nil
			}
			return right, nil
		}
		if order <= 0 {
			return left, nil
		}
		return right, nil
	case "eq", "ne":
		left, right, err := pair(op)
		if err != nil {
			return nil, err
		}
		equal := jsonEq(left, right)
		if op == "ne" {
			return !equal, nil
		}
		return equal, nil
	case "lt", "le", "gt", "ge":
		left, right, err := pair(op)
		if err != nil {
			return nil, err
		}
		order, err := ordering(left, right)
		if err != nil {
			return nil, err
		}
		switch op {
		case "lt":
			return order < 0, nil
		case "le":
			return order <= 0, nil
		case "gt":
			return order > 0, nil
		default:
			return order >= 0, nil
		}
	case "concat":
		left, right, err := pair("concat")
		if err != nil {
			return nil, err
		}
		a, err := expectString(left)
		if err != nil {
			return nil, err
		}
		b, err := expectString(right)
		if err != nil {
			return nil, err
		}
		return a + b, nil
	case "get":
		target, key, err := pair("get")
		if err != nil {
			return nil, err
		}
		return evalGet(target, key)
	default:
		return nil, graphErr("op `%s` is not an eager operator", op)
	}
}

func lenValue(value any) (any, error) {
	switch typed := value.(type) {
	case string:
		return int64(utf8.RuneCountInString(typed)), nil
	case []any:
		return int64(len(typed)), nil
	case map[string]any:
		return int64(len(typed)), nil
	default:
		return nil, graphErr("len requires a string, array, or object, got %s", jsonText(value))
	}
}

func evalGet(target, key any) (any, error) {
	switch typed := target.(type) {
	case []any:
		kind, integer, _, err := asNum(key)
		if err != nil {
			return nil, err
		}
		if kind != "int" {
			return nil, graphErr("array index must be an integer")
		}
		if integer < 0 {
			return nil, graphErr("array index must be >= 0")
		}
		if integer >= int64(len(typed)) {
			return nil, graphErr("index %d out of range", integer)
		}
		return freezeGraph(typed[integer]), nil
	case map[string]any:
		name, err := expectString(key)
		if err != nil {
			return nil, err
		}
		value, ok := typed[name]
		if !ok {
			return nil, graphErr("missing key `%s`", name)
		}
		return freezeGraph(value), nil
	default:
		return nil, graphErr("get requires an array or object, got %s", jsonText(target))
	}
}

func foldPolicy(policy string, old, op any) (any, error) {
	switch policy {
	case PolicyKeepLatest:
		return freezeGraph(op), nil
	case PolicySum:
		return numBin("add", old, op)
	case PolicyMax, PolicyMin:
		order, err := ordering(old, op)
		if err != nil {
			return nil, err
		}
		if policy == PolicyMax {
			if order >= 0 {
				return freezeGraph(old), nil
			}
			return freezeGraph(op), nil
		}
		if order <= 0 {
			return freezeGraph(old), nil
		}
		return freezeGraph(op), nil
	case PolicyAnd:
		left, err := expectBool(old)
		if err != nil {
			return nil, err
		}
		right, err := expectBool(op)
		if err != nil {
			return nil, err
		}
		return left && right, nil
	case PolicyOr:
		left, err := expectBool(old)
		if err != nil {
			return nil, err
		}
		right, err := expectBool(op)
		if err != nil {
			return nil, err
		}
		return left || right, nil
	default:
		return nil, graphErr("unknown source policy `%s`", policy)
	}
}

func setKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func copySet(set map[string]bool) map[string]bool {
	out := map[string]bool{}
	for key := range set {
		out[key] = true
	}
	return out
}

// checkedInt is retained so overflow helpers stay next to the numeric core.
var _ = checkedInt
