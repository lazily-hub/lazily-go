// Harel/SCXML hierarchical state chart — compound + parallel (orthogonal)
// regions, shallow + deep history, entry/exit/transition action ordering,
// named guards (fail-closed), and external + internal transitions.
//
// Ported from lazily-dart lib/src/state_chart.dart (feature row "Harel state
// charts"). The native counterpart of lazily-formal's LazilyFormal.StateChart
// and lazily-rs/lazily-kt state charts. It is COMPUTE, not protocol: a chart is
// never serialized as a distinct wire kind — only its converged active
// configuration crosses the wire as an ordinary cell payload.
//
// The active configuration is backed by a Cell so any Slot/Signal/Memo/observer
// reading Configuration, ActiveLeaves, or Matches is invalidated on a real
// transition; a no-op (configuration unchanged) is suppressed by the cell's
// structural-equality guard. Because Cell requires a comparable value type and
// a set of active states is not comparable, the cell holds a canonical
// sorted-and-joined configuration key (structural equality on that string ==
// structural equality on the set); the authoritative set is kept alongside.
//
// Send is deterministic by construction — a total function of
// (chart, configuration, history, guards, event), mirroring the Lean
// StateChart.send. The declarative chart form is parsed from JSON conforming to
// lazily-spec/schemas/statechart.json via ChartDefFromJSON. `run` actions and
// {"expr": …} context guards are rejected explicitly; `final` states are
// accepted as leaves without raising completion (done) events, matching
// lazily-py and lazily-kt.
package lazily

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// configKeySep separates state ids in a canonical configuration key. It is the
// ASCII unit separator, which never appears in a state id.
const configKeySep = "\x1f"

// -- State / transition definitions -------------------------------------------

// kindTag is the structural kind of a state node — mirrors
// LazilyFormal.StateChart.Kind. History states carry deep-vs-shallow in the tag.
type kindTag int

const (
	kindAtomic kindTag = iota
	kindCompound
	kindParallel
	kindFinal
	kindHistoryShallow
	kindHistoryDeep
)

// isLeaf reports whether a kind is a leaf (atomic or final). History states are
// pseudo-states and are never active leaves.
func isLeaf(k kindTag) bool { return k == kindAtomic || k == kindFinal }

// isHistory reports whether a kind is a history pseudo-state.
func isHistory(k kindTag) bool { return k == kindHistoryShallow || k == kindHistoryDeep }

// transition is a parsed transition. guard == nil means "no guard".
type transition struct {
	target   string
	guard    *string
	action   []string
	internal bool
}

// stateDef is a parsed state node. parent/initial/defaultChild are "" when
// absent (state ids are never empty).
type stateDef struct {
	kind         kindTag
	parent       string
	initial      string
	defaultChild string
	transitions  map[string]transition
	entry        []string
	exit         []string
}

// recording is a history recording for a region exited at least once.
type recording interface{ isRecording() }

// shallowRecording is the direct child of the region that was active.
type shallowRecording struct{ child string }

// deepRecording is the full active sub-configuration (sorted) below the region.
type deepRecording struct{ set []string }

func (shallowRecording) isRecording() {}
func (deepRecording) isRecording()    {}

// -- Chart definition ---------------------------------------------------------

// ChartDef is a parsed, immutable chart definition — the node-labeled functions
// of the declarative JSON form materialized as maps for deterministic descent.
// Build it via ChartDefFromJSON and hand it to NewStateChart.
type ChartDef struct {
	states   map[string]stateDef
	children map[string][]string
	order    map[string]int
	depths   map[string]int
	root     string
}

type rawChart struct {
	Initial json.RawMessage `json:"initial"`
	States  json.RawMessage `json:"states"`
}

// ChartDefFromJSON parses a chart definition from the declarative JSON form
// (lazily-spec/schemas/statechart.json). `run` actions and {"expr": …} context
// guards are rejected explicitly.
func ChartDefFromJSON(data []byte) (*ChartDef, error) {
	var rc rawChart
	if err := json.Unmarshal(data, &rc); err != nil {
		return nil, err
	}

	// chart.initial is required and must be a string. Descent uses each
	// compound's own `initial` from the root, so the value itself is not stored.
	var initial string
	if len(rc.Initial) == 0 || json.Unmarshal(rc.Initial, &initial) != nil {
		return nil, errors.New("chart.initial is required")
	}

	if len(rc.States) == 0 {
		return nil, errors.New("chart.states is required")
	}
	var statesMap map[string]any
	if err := json.Unmarshal(rc.States, &statesMap); err != nil {
		return nil, errors.New("chart.states is required")
	}
	keys, err := orderedObjectKeys(rc.States)
	if err != nil {
		return nil, errors.New("chart.states is required")
	}

	stateDefs := make(map[string]stateDef, len(keys))
	docOrder := make(map[string]int, len(keys))
	for idx, key := range keys {
		docOrder[key] = idx
		obj, _ := statesMap[key].(map[string]any)
		sd, err := parseState(key, obj)
		if err != nil {
			return nil, err
		}
		stateDefs[key] = sd
	}

	kids := map[string][]string{}
	root := ""
	rootSet := false
	for _, key := range keys {
		p := stateDefs[key].parent
		if p != "" {
			kids[p] = append(kids[p], key)
		} else {
			if rootSet {
				return nil, errors.New("chart has more than one root (parent-less state)")
			}
			root = key
			rootSet = true
		}
	}
	if !rootSet {
		return nil, errors.New("chart has no root (parent-less state)")
	}

	// Sort children by document order for deterministic parallel descent.
	for _, list := range kids {
		l := list
		sort.SliceStable(l, func(i, j int) bool {
			return orderOf(docOrder, l[i]) < orderOf(docOrder, l[j])
		})
	}

	depths := map[string]int{}
	computeDepth(stateDefs, root, 0, depths)

	return &ChartDef{
		states:   stateDefs,
		children: kids,
		order:    docOrder,
		depths:   depths,
		root:     root,
	}, nil
}

// orderedObjectKeys returns the keys of a JSON object in document order.
func orderedObjectKeys(raw json.RawMessage) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, errors.New("expected object")
	}
	var keys []string
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, errors.New("expected object key")
		}
		keys = append(keys, key)
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func parseState(id string, obj map[string]any) (stateDef, error) {
	if obj == nil {
		return stateDef{}, fmt.Errorf("state %s must be an object", id)
	}
	parent := asStr(obj["parent"])
	initial := asStr(obj["initial"])
	defaultChild := asStr(obj["default"])

	if obj["run"] != nil {
		return stateDef{}, fmt.Errorf("state %s uses `run` actions, which are not supported", id)
	}

	var kind kindTag
	switch h := obj["history"].(type) {
	case string:
		switch h {
		case "shallow":
			kind = kindHistoryShallow
		case "deep":
			kind = kindHistoryDeep
		default:
			return stateDef{}, fmt.Errorf("state %s: unknown history kind %s", id, h)
		}
	default:
		if p, ok := obj["parallel"].(bool); ok && p {
			kind = kindParallel
		} else if asStr(obj["kind"]) == "final" {
			kind = kindFinal
		} else if _, ok := obj["initial"].(string); ok {
			kind = kindCompound
		} else {
			kind = kindAtomic
		}
	}

	entry, err := actionList(obj["entry"])
	if err != nil {
		return stateDef{}, err
	}
	exit, err := actionList(obj["exit"])
	if err != nil {
		return stateDef{}, err
	}

	transitions := map[string]transition{}
	if on, ok := obj["on"].(map[string]any); ok {
		for event, raw := range on {
			t, err := parseTransition(raw)
			if err != nil {
				return stateDef{}, err
			}
			transitions[event] = t
		}
	}

	return stateDef{
		kind:         kind,
		parent:       parent,
		initial:      initial,
		defaultChild: defaultChild,
		transitions:  transitions,
		entry:        entry,
		exit:         exit,
	}, nil
}

func asStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func actionList(raw any) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, errors.New("action must be a string (object-form actions are rejected explicitly per spec)")
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		s, ok := e.(string)
		if !ok {
			return nil, errors.New("action must be a string (object-form actions are rejected explicitly per spec)")
		}
		out = append(out, s)
	}
	return out, nil
}

func parseTransition(raw any) (transition, error) {
	if s, ok := raw.(string); ok {
		return transition{target: s}, nil
	}
	if m, ok := raw.(map[string]any); ok {
		target, ok := m["target"].(string)
		if !ok {
			return transition{}, errors.New("transition target must be a string")
		}
		var guard *string
		switch g := m["guard"].(type) {
		case nil:
			// no guard
		case string:
			gg := g
			guard = &gg
		case map[string]any:
			return transition{}, errors.New("context-expression `{expr: …}` guards are not supported (rejected explicitly per spec)")
		default:
			return transition{}, errors.New("guard must be a string")
		}
		action, err := actionList(m["action"])
		if err != nil {
			return transition{}, err
		}
		internal := false
		if b, ok := m["internal"].(bool); ok {
			internal = b
		}
		return transition{target: target, guard: guard, action: action, internal: internal}, nil
	}
	return transition{}, errors.New("transition must be a string or object")
}

func computeDepth(states map[string]stateDef, id string, current int, out map[string]int) {
	out[id] = current
	for childID, sd := range states {
		if sd.parent == id {
			computeDepth(states, childID, current+1, out)
		}
	}
}

func orderOf(order map[string]int, id string) int {
	if v, ok := order[id]; ok {
		return v
	}
	return 1 << 30
}

func (d *ChartDef) kind(id string) kindTag {
	if sd, ok := d.states[id]; ok {
		return sd.kind
	}
	return kindAtomic
}

// ancestorsInclusive returns the ancestors of id inclusive, [id, …, root].
func (d *ChartDef) ancestorsInclusive(id string) []string {
	var out []string
	cur := id
	for {
		out = append(out, cur)
		sd, ok := d.states[cur]
		if !ok || sd.parent == "" {
			break
		}
		cur = sd.parent
	}
	return out
}

// lca returns the lowest common ancestor (inclusive) of a and b; falls back to
// the root.
func (d *ChartDef) lca(a, b string) string {
	ancA := map[string]struct{}{}
	for _, x := range d.ancestorsInclusive(a) {
		ancA[x] = struct{}{}
	}
	for _, cid := range d.ancestorsInclusive(b) {
		if _, ok := ancA[cid]; ok {
			return cid
		}
	}
	return d.root
}

// isProperDescendant reports whether desc is a proper descendant of anc.
func (d *ChartDef) isProperDescendant(desc, anc string) bool {
	if desc == anc {
		return false
	}
	for _, a := range d.ancestorsInclusive(desc) {
		if a == anc {
			return true
		}
	}
	return false
}

func (d *ChartDef) depth(id string) int {
	if v, ok := d.depths[id]; ok {
		return v
	}
	return 0
}

func (d *ChartDef) parentOf(id string) string {
	if sd, ok := d.states[id]; ok {
		return sd.parent
	}
	return ""
}

// -- Active configuration -----------------------------------------------------

// Configuration is the active configuration: the set of active states (atomic
// leaves plus all active ancestors). It holds a sorted, unique snapshot.
type Configuration struct {
	states []string
}

func newConfiguration(set map[string]struct{}) Configuration {
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return Configuration{states: out}
}

// Contains reports whether id is in the active configuration.
func (c Configuration) Contains(id string) bool {
	i := sort.SearchStrings(c.states, id)
	return i < len(c.states) && c.states[i] == id
}

// IsEmpty reports whether the configuration is empty.
func (c Configuration) IsEmpty() bool { return len(c.states) == 0 }

// ToSet returns a sorted snapshot of the active states.
func (c Configuration) ToSet() []string {
	out := make([]string, len(c.states))
	copy(out, c.states)
	return out
}

// configKey builds the canonical (sorted, joined) key for a configuration set.
func configKey(set map[string]struct{}) string {
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return strings.Join(out, configKeySep)
}

// -- Reactive chart -----------------------------------------------------------

// StateChart is a reactive full-Harel state chart backed by a configuration
// Cell.
//
// Construct via NewStateChart (descending the root's initial configuration,
// recording initial entry actions). Drive with Send; query with Configuration,
// ActiveLeaves, Matches; inspect the last step's action trace with LastActions.
type StateChart struct {
	ctx          *Context
	def          *ChartDef
	history      map[string]recording
	lastActions  []string
	config       *Cell[string]
	configStates map[string]struct{}
}

// NewStateChart creates a chart bound to ctx that enters the initial
// configuration by descending from def's root.
func NewStateChart(ctx *Context, def *ChartDef) *StateChart {
	sc := &StateChart{
		ctx:     ctx,
		def:     def,
		history: map[string]recording{},
	}
	enter := map[string]struct{}{}
	var actions []string
	sc.enterSubtree(def.root, enter, &actions)
	sc.configStates = enter
	sc.config = NewCell[string](ctx, configKey(enter))
	sc.lastActions = actions
	return sc
}

// Ctx returns the reactive context this chart belongs to.
func (sc *StateChart) Ctx() *Context { return sc.ctx }

// Def returns the parsed chart definition.
func (sc *StateChart) Def() *ChartDef { return sc.def }

// LastActions returns the ordered action names fired by the initial entry or the
// most recent Send (exit innermost-first -> transition -> entry outermost-first).
func (sc *StateChart) LastActions() []string {
	out := make([]string, len(sc.lastActions))
	copy(out, sc.lastActions)
	return out
}

// Configuration returns the full active configuration (active leaves plus all
// active ancestors). Reading inside a computation subscribes the reader.
func (sc *StateChart) Configuration() Configuration {
	sc.config.Get() // track: subscribe the reader to real transitions
	return newConfiguration(sc.configStates)
}

// ActiveLeaves returns the active atomic leaves, sorted (one per parallel
// region; one for a single-region chart).
func (sc *StateChart) ActiveLeaves() []string {
	cfg := sc.Configuration()
	var out []string
	for _, s := range cfg.states {
		if isLeaf(sc.def.kind(s)) {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// Matches is the hierarchical "state-in" predicate: true iff id is in the active
// configuration. Reading inside a computation subscribes the reader.
func (sc *StateChart) Matches(id string) bool {
	return sc.Configuration().Contains(id)
}

// candidate is an enabled transition matched from an active leaf's ancestor
// chain.
type candidate struct {
	source string
	trans  transition
	leaf   string
}

// Send delivers an event (run-to-completion). It returns true if any transition
// was taken, false if rejected (configuration unchanged, no actions fired).
//
// guards resolves named guards for this send (absent/unknown name -> fail-closed
// false). Pass nil for no guards.
func (sc *StateChart) Send(event string, guards map[string]bool) bool {
	def := sc.def
	config := sc.configStates

	// 1. Enabled transitions: per active leaf, innermost passing match.
	var candidates []candidate
	for _, leaf := range sc.sortedLeaves(config) {
		for _, anc := range def.ancestorsInclusive(leaf) {
			sd, ok := def.states[anc]
			if !ok {
				continue
			}
			t, ok := sd.transitions[event]
			if ok && guardPasses(t, guards) {
				candidates = append(candidates, candidate{source: anc, trans: t, leaf: leaf})
				break // innermost wins for this leaf's chain
			}
		}
	}
	if len(candidates) == 0 {
		sc.lastActions = nil
		return false
	}

	// 2. Conflict resolution: order by source depth desc, then document order;
	//    take greedily, skipping any whose exit set intersects the taken union.
	sort.SliceStable(candidates, func(i, j int) bool {
		di, dj := def.depth(candidates[i].source), def.depth(candidates[j].source)
		if di != dj {
			return di > dj // depth descending
		}
		return orderOf(def.order, candidates[i].source) < orderOf(def.order, candidates[j].source)
	})

	exitUnion := map[string]struct{}{}
	enterUnion := map[string]struct{}{}
	var takenTransitions []transition
	for _, cand := range candidates {
		exitSet, enterSet := sc.computeExitEnter(cand.source, cand.trans, cand.leaf, config)
		conflict := false
		for s := range exitSet {
			if _, ok := exitUnion[s]; ok {
				conflict = true
				break
			}
		}
		if conflict {
			continue // conflicts with a taken transition
		}
		for s := range exitSet {
			exitUnion[s] = struct{}{}
		}
		for s := range enterSet {
			enterUnion[s] = struct{}{}
		}
		takenTransitions = append(takenTransitions, cand.trans)
	}

	if len(takenTransitions) == 0 {
		sc.lastActions = nil
		return false
	}

	// 3. Record history for regions being exited that own a history child.
	for s := range exitUnion {
		if hChild := sc.historyChildOf(s); hChild != "" {
			sc.recordRegion(s, hChild, config)
		}
	}

	// 4. Action trace: exit (innermost-first) -> transition -> entry
	//    (outermost-first).
	var actions []string
	exitSorted := keysOf(exitUnion)
	sort.SliceStable(exitSorted, func(i, j int) bool {
		return sc.exitLess(exitSorted[i], exitSorted[j])
	})
	for _, s := range exitSorted {
		actions = append(actions, def.states[s].exit...)
	}
	for _, t := range takenTransitions {
		actions = append(actions, t.action...)
	}
	enterSorted := keysOf(enterUnion)
	sort.SliceStable(enterSorted, func(i, j int) bool {
		return sc.enterLess(enterSorted[i], enterSorted[j])
	})
	for _, s := range enterSorted {
		actions = append(actions, def.states[s].entry...)
	}

	// 5. Apply new configuration (structural-equality guard suppresses no-ops).
	newSet := make(map[string]struct{}, len(config))
	for s := range config {
		newSet[s] = struct{}{}
	}
	for s := range exitUnion {
		delete(newSet, s)
	}
	for s := range enterUnion {
		newSet[s] = struct{}{}
	}

	sc.lastActions = actions
	newKey := configKey(newSet)
	if newKey != sc.config.Peek() {
		sc.configStates = newSet
		sc.config.Set(newKey)
	}
	return true
}

// exitLess orders exit states innermost-first (depth desc), tiebreaking by
// document order then id for determinism.
func (sc *StateChart) exitLess(a, b string) bool {
	da, db := sc.def.depth(a), sc.def.depth(b)
	if da != db {
		return da > db
	}
	oa, ob := orderOf(sc.def.order, a), orderOf(sc.def.order, b)
	if oa != ob {
		return oa < ob
	}
	return a < b
}

// enterLess orders entry states outermost-first (depth asc), tiebreaking by
// document order then id for determinism.
func (sc *StateChart) enterLess(a, b string) bool {
	da, db := sc.def.depth(a), sc.def.depth(b)
	if da != db {
		return da < db
	}
	oa, ob := orderOf(sc.def.order, a), orderOf(sc.def.order, b)
	if oa != ob {
		return oa < ob
	}
	return a < b
}

func (sc *StateChart) computeExitEnter(source string, t transition, leaf string, config map[string]struct{}) (exit, enter map[string]struct{}) {
	def := sc.def
	target := t.target
	internal := t.internal && (target == source || def.isProperDescendant(target, source))
	lca := def.lca(leaf, target)
	if internal {
		lca = source
	}

	// Exit set: active proper-descendants of the lca.
	exit = map[string]struct{}{}
	for s := range config {
		if def.isProperDescendant(s, lca) {
			exit[s] = struct{}{}
		}
	}

	enter = map[string]struct{}{}
	if isHistory(def.kind(target)) {
		region := def.parentOf(target)
		if region == "" {
			region = def.root
		}
		for _, p := range sc.pathBelow(lca, region) {
			enter[p] = struct{}{}
		}
		sc.restoreViaHistory(target, region, enter)
	} else {
		for _, p := range sc.pathBelow(lca, target) {
			enter[p] = struct{}{}
		}
		var tmp []string
		sc.enterSubtree(target, enter, &tmp)
	}
	return exit, enter
}

func (sc *StateChart) restoreViaHistory(hist, region string, enter map[string]struct{}) {
	def := sc.def
	switch r := sc.history[hist].(type) {
	case shallowRecording:
		enter[r.child] = struct{}{}
		var tmp []string
		sc.enterSubtree(r.child, enter, &tmp)
	case deepRecording:
		for _, s := range r.set {
			enter[s] = struct{}{}
		}
	default:
		// First entry: descend via `default`, else the region's `initial`.
		start := ""
		if sd, ok := def.states[hist]; ok {
			start = sd.defaultChild
		}
		if start == "" {
			if sd, ok := def.states[region]; ok {
				start = sd.initial
			}
		}
		if start != "" {
			for _, p := range sc.pathBelow(region, start) {
				enter[p] = struct{}{}
			}
			var tmp []string
			sc.enterSubtree(start, enter, &tmp)
		}
	}
}

// enterSubtree enters state and its default descendants, recording entry actions
// top-down.
func (sc *StateChart) enterSubtree(state string, enter map[string]struct{}, actions *[]string) {
	def := sc.def
	enter[state] = struct{}{}
	if sd, ok := def.states[state]; ok {
		*actions = append(*actions, sd.entry...)
	}
	switch def.kind(state) {
	case kindAtomic, kindFinal, kindHistoryShallow, kindHistoryDeep:
		return
	case kindCompound:
		if sd, ok := def.states[state]; ok && sd.initial != "" {
			sc.enterSubtree(sd.initial, enter, actions)
		}
	case kindParallel:
		for _, region := range def.children[state] {
			sc.enterSubtree(region, enter, actions)
		}
	}
}

// pathBelow returns the path from just-below lca down to target (exclusive lca,
// inclusive target): [child-of-lca, …, target].
func (sc *StateChart) pathBelow(lca, target string) []string {
	chain := sc.def.ancestorsInclusive(target) // [target, …, root]
	idx := indexOfString(chain, lca)
	if idx < 0 {
		idx = len(chain)
	}
	sub := chain[:idx]
	out := make([]string, len(sub))
	for i, s := range sub {
		out[len(sub)-1-i] = s
	}
	return out
}

func (sc *StateChart) historyChildOf(region string) string {
	for _, child := range sc.def.children[region] {
		if isHistory(sc.def.kind(child)) {
			return child
		}
	}
	return ""
}

func (sc *StateChart) recordRegion(region, histChild string, config map[string]struct{}) {
	def := sc.def
	k := def.kind(histChild)
	if !isHistory(k) {
		return
	}
	if k == kindHistoryShallow {
		// Shallow: record the direct child of `region` that was active.
		for _, child := range def.children[region] {
			if _, ok := config[child]; ok && !isHistory(def.kind(child)) {
				sc.history[histChild] = shallowRecording{child: child}
				return
			}
		}
	} else {
		// Deep: record every active state strictly below `region`.
		var set []string
		for s := range config {
			if def.isProperDescendant(s, region) {
				set = append(set, s)
			}
		}
		sort.Strings(set)
		sc.history[histChild] = deepRecording{set: set}
	}
}

func (sc *StateChart) sortedLeaves(config map[string]struct{}) []string {
	var out []string
	for s := range config {
		if isLeaf(sc.def.kind(s)) {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// String renders the chart as its active leaves.
func (sc *StateChart) String() string {
	return "StateChart(" + strings.Join(sc.ActiveLeaves(), ", ") + ")"
}

func guardPasses(t transition, guards map[string]bool) bool {
	if t.guard == nil {
		return true
	}
	return guards[*t.guard] // fail-closed: absent/unknown -> false
}

func keysOf(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	return out
}

func indexOfString(xs []string, target string) int {
	for i, x := range xs {
		if x == target {
			return i
		}
	}
	return -1
}
