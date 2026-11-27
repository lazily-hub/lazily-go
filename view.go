package lazily

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// View is a host-neutral view compute document installed on one Context.
// Each signal is a Source, each text/hidden/disabled expression is a guarded
// Computed, and the optional chart is one StateChart cell.
type View struct {
	ctx     *Context
	root    string
	nodes   map[string]*viewNode
	order   []string
	signals map[string]*Source[viewValue]
	exprs   map[string]*Computed[viewValue]
	effects map[string]map[string]any
	chart   *StateChart
	payload *viewValue
	host    *Effect
}

type viewNode struct {
	role     string
	children []string
	flow     any
}

type viewKind int

const (
	viewNull viewKind = iota
	viewBool
	viewInt
	viewStr
)

type viewValue struct {
	k viewKind
	b bool
	n int64
	s string
}

func (v viewValue) any() any {
	switch v.k {
	case viewBool:
		return v.b
	case viewInt:
		return v.n
	case viewStr:
		return v.s
	default:
		return nil
	}
}

var (
	viewRoles   = setOf("text", "button", "field", "list", "region")
	viewFlows   = setOf("row", "column", "stack")
	viewAligns  = setOf("start", "center", "end", "stretch")
	viewEvents  = setOf("activate", "change", "submit")
	viewExprOps = setOf("lit", "ref", "call", "if", "eq", "concat", "trim")
	viewEffOps  = setOf("set", "batch", "send")
	viewCalls   = setOf("event", "not", "add", "len")
)

func setOf(items ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(items))
	for _, item := range items {
		out[item] = struct{}{}
	}
	return out
}

// InstallView installs doc, a schemas/view.json object decoded with
// json.Decoder.UseNumber so integers stay exact.
func InstallView(ctx *Context, doc any) (*View, error) {
	obj, err := viewMap(doc, "view")
	if err != nil {
		return nil, err
	}
	if err := viewAllow(obj, "view", "id", "root", "signals", "nodes", "chart"); err != nil {
		return nil, err
	}
	id, err := viewString(obj["id"], "view id")
	if err != nil || id == "" {
		return nil, fmt.Errorf("view id must be a non-empty string")
	}
	root, err := viewString(obj["root"], "view root")
	if err != nil || root == "" {
		return nil, fmt.Errorf("view root must be a non-empty string")
	}
	signalsDoc, err := viewMap(obj["signals"], "view signals")
	if err != nil {
		return nil, err
	}
	signalIDs := map[string]struct{}{}
	initials := map[string]viewValue{}
	for sid, raw := range signalsDoc {
		if sid == "" {
			return nil, fmt.Errorf("signal id must be a non-empty string")
		}
		sig, err := viewMap(raw, "signal "+sid)
		if err != nil {
			return nil, err
		}
		if err := viewAllow(sig, "signal "+sid, "initial"); err != nil {
			return nil, err
		}
		initial, err := viewLiteral(sig["initial"])
		if err != nil {
			return nil, fmt.Errorf("signal %s: %w", sid, err)
		}
		signalIDs[sid] = struct{}{}
		initials[sid] = initial
	}
	_, hasChart := obj["chart"]
	var chartRaw []byte
	if hasChart {
		if _, ok := obj["chart"].(map[string]any); !ok {
			return nil, fmt.Errorf("chart must be a statechart document")
		}
		chartRaw, err = json.Marshal(obj["chart"])
		if err != nil {
			return nil, err
		}
		if _, err := ChartDefFromJSON(chartRaw); err != nil {
			return nil, err
		}
	}
	nodesDoc, err := viewMap(obj["nodes"], "view nodes")
	if err != nil {
		return nil, err
	}
	if len(nodesDoc) == 0 {
		return nil, fmt.Errorf("view nodes must contain at least one node")
	}
	nodes := map[string]*viewNode{}
	effects := map[string]map[string]any{}
	var exprDocs []viewExprDoc
	for nid, raw := range nodesDoc {
		if nid == "" {
			return nil, fmt.Errorf("node id must be a non-empty string")
		}
		node, err := viewMap(raw, "node "+nid)
		if err != nil {
			return nil, err
		}
		if err := viewAllow(node, "node "+nid, "role", "children", "layout", "text", "bind", "hidden", "disabled", "effects"); err != nil {
			return nil, err
		}
		role, err := viewString(node["role"], "node "+nid+" role")
		if err != nil {
			return nil, err
		}
		if _, ok := viewRoles[role]; !ok {
			return nil, fmt.Errorf("unknown role `%s`", role)
		}
		children := []string{}
		if rawChildren, ok := node["children"]; ok {
			children, err = viewChildren(nid, rawChildren)
			if err != nil {
				return nil, err
			}
		}
		var flow any
		if rawLayout, ok := node["layout"]; ok {
			flow, err = viewLayout(nid, rawLayout)
			if err != nil {
				return nil, err
			}
		}
		if rawBind, ok := node["bind"]; ok {
			bind, err := viewString(rawBind, "bind")
			if err != nil || bind == "" {
				return nil, fmt.Errorf("node `%s` bind must be a non-empty string", nid)
			}
			if _, ok := signalIDs[bind]; !ok {
				return nil, fmt.Errorf("bind names unknown signal `%s`", bind)
			}
		}
		if rawText, ok := node["text"]; ok {
			expr, err := viewStandingText(rawText, signalIDs)
			if err != nil {
				return nil, err
			}
			exprDocs = append(exprDocs, viewExprDoc{key: nid + ".text", expr: expr})
		}
		if rawHidden, ok := node["hidden"]; ok {
			expr, err := viewStandingFlag("hidden", rawHidden, signalIDs)
			if err != nil {
				return nil, err
			}
			exprDocs = append(exprDocs, viewExprDoc{key: nid + ".hidden", expr: expr})
		}
		if rawDisabled, ok := node["disabled"]; ok {
			expr, err := viewStandingFlag("disabled", rawDisabled, signalIDs)
			if err != nil {
				return nil, err
			}
			exprDocs = append(exprDocs, viewExprDoc{key: nid + ".disabled", expr: expr})
		}
		if rawEffects, ok := node["effects"]; ok {
			effectMap, err := viewMap(rawEffects, "node "+nid+" effects")
			if err != nil {
				return nil, err
			}
			if err := viewAllow(effectMap, "node "+nid+" effects", "activate", "change", "submit"); err != nil {
				return nil, err
			}
			stored := map[string]any{}
			for event, effect := range effectMap {
				if err := viewValidateEffect(effect, signalIDs, hasChart); err != nil {
					return nil, err
				}
				stored[event] = effect
			}
			if len(stored) > 0 {
				effects[nid] = stored
			}
		}
		nodes[nid] = &viewNode{role: role, children: children, flow: flow}
	}
	if _, ok := nodes[root]; !ok {
		return nil, fmt.Errorf("root `%s` is not a node", root)
	}
	if err := viewCheckTree(root, nodes); err != nil {
		return nil, err
	}

	signals := map[string]*Source[viewValue]{}
	for sid, initial := range initials {
		signals[sid] = NewSource(ctx, initial)
	}
	readBare := func(sid string) (viewValue, error) {
		src := signals[sid]
		if src == nil {
			return viewValue{}, fmt.Errorf("unknown signal `%s`", sid)
		}
		return Get(ctx, src), nil
	}
	for _, doc := range exprDocs {
		if _, err := viewEvalExpr(doc.expr, readBare, nil); err != nil {
			return nil, fmt.Errorf("%s: %w", doc.key, err)
		}
	}
	exprs := map[string]*Computed[viewValue]{}
	for _, doc := range exprDocs {
		expr := doc.expr
		key := doc.key
		exprs[key] = NewComputed(ctx, func(c *Compute) viewValue {
			value, err := viewEvalExpr(expr, func(sid string) (viewValue, error) {
				src := signals[sid]
				if src == nil {
					return viewValue{}, fmt.Errorf("unknown signal `%s`", sid)
				}
				return Get(c, src), nil
			}, nil)
			if err != nil {
				panic(key + ": " + err.Error())
			}
			return value
		})
	}
	var chart *StateChart
	if hasChart {
		def, err := ChartDefFromJSON(chartRaw)
		if err != nil {
			return nil, err
		}
		chart = NewStateChart(ctx, def)
	}
	watchedExprs := make([]*Computed[viewValue], 0, len(exprs))
	for _, expr := range exprs {
		watchedExprs = append(watchedExprs, expr)
	}
	watchedSignals := make([]*Source[viewValue], 0, len(signals))
	for _, src := range signals {
		watchedSignals = append(watchedSignals, src)
	}
	host := NewEffect(ctx, func(c *Compute) func() {
		if len(watchedExprs) > 0 {
			for _, expr := range watchedExprs {
				Get(c, expr)
			}
		} else {
			for _, src := range watchedSignals {
				Get(c, src)
			}
		}
		return nil
	})
	return &View{
		ctx: ctx, root: root, nodes: nodes, signals: signals, exprs: exprs,
		effects: effects, chart: chart, host: host,
	}, nil
}

// Subscription is the host effect. Dropping the handle does not dispose it.
func (v *View) Subscription() *Effect { return v.host }

func (v *View) SignalsJSON() map[string]any {
	out := map[string]any{}
	for id, src := range v.signals {
		out[id] = Get(v.ctx, src).any()
	}
	return out
}

func (v *View) ExpressionsJSON() map[string]any {
	out := map[string]any{}
	for id, expr := range v.exprs {
		out[id] = Get(v.ctx, expr).any()
	}
	return out
}

func (v *View) LayoutJSON() []any {
	var out []any
	var walk func(string)
	walk = func(id string) {
		node := v.nodes[id]
		children := make([]any, len(node.children))
		for i, child := range node.children {
			children[i] = child
		}
		out = append(out, map[string]any{
			"id": id, "role": node.role, "children": children, "flow": node.flow,
		})
		for _, child := range node.children {
			walk(child)
		}
	}
	walk(v.root)
	return out
}

func (v *View) ChartJSON() any {
	if v.chart == nil {
		return nil
	}
	leaves := v.chart.ActiveLeaves()
	out := make([]any, len(leaves))
	for i, leaf := range leaves {
		out[i] = leaf
	}
	return out
}

func (v *View) DispatchJSON(node, event string, value any) ([]any, error) {
	if _, ok := viewEvents[event]; !ok {
		return nil, fmt.Errorf("unknown event `%s`", event)
	}
	if _, ok := v.nodes[node]; !ok {
		return nil, fmt.Errorf("unknown node `%s`", node)
	}
	effect := v.effects[node][event]
	if effect == nil {
		return []any{}, nil
	}
	payload, err := viewLiteral(value)
	if err != nil {
		return nil, err
	}
	v.payload = &payload
	var trace []any
	err = v.exec(effect, &trace)
	v.payload = nil
	if err != nil {
		return nil, err
	}
	if trace == nil {
		trace = []any{}
	}
	return trace, nil
}

func (v *View) exec(effect any, trace *[]any) error {
	obj, err := viewMap(effect, "effect")
	if err != nil {
		return err
	}
	op, _ := obj["op"].(string)
	args, _ := obj["args"].([]any)
	switch op {
	case "set":
		if len(args) != 2 {
			return fmt.Errorf("set takes a signal id and a value")
		}
		sid, _ := args[0].(string)
		src := v.signals[sid]
		if src == nil {
			return fmt.Errorf("unknown signal `%s`", sid)
		}
		value, err := v.evalArg(args[1])
		if err != nil {
			return err
		}
		src.Set(value)
		*trace = append(*trace, map[string]any{"op": "set", "signal": sid, "value": value.any()})
		return nil
	case "send":
		if v.chart == nil {
			return fmt.Errorf("send requires a chart")
		}
		if len(args) != 1 && len(args) != 2 {
			return fmt.Errorf("send takes an event name and an optional payload")
		}
		name, _ := args[0].(string)
		record := map[string]any{"op": "send", "event": name}
		if len(args) == 2 {
			payload, err := v.evalArg(args[1])
			if err != nil {
				return err
			}
			record["payload"] = payload.any()
		}
		v.chart.Send(name, map[string]bool{})
		*trace = append(*trace, record)
		return nil
	case "batch":
		if len(args) == 0 {
			return fmt.Errorf("batch takes one or more effects")
		}
		for _, nested := range args {
			if err := v.exec(nested, trace); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown effect op `%s`", op)
	}
}

func (v *View) evalArg(value any) (viewValue, error) {
	if _, ok := value.(map[string]any); ok {
		return viewEvalExpr(value, v.read, v.payload)
	}
	return viewLiteral(value)
}

func (v *View) read(sid string) (viewValue, error) {
	src := v.signals[sid]
	if src == nil {
		return viewValue{}, fmt.Errorf("unknown signal `%s`", sid)
	}
	return Get(v.ctx, src), nil
}

type viewExprDoc struct {
	key  string
	expr any
}

type viewRead func(string) (viewValue, error)

func viewEvalExpr(expr any, read viewRead, payload *viewValue) (viewValue, error) {
	obj, err := viewMap(expr, "expression")
	if err != nil {
		return viewValue{}, err
	}
	op, _ := obj["op"].(string)
	args, _ := obj["args"].([]any)
	switch op {
	case "lit":
		if len(args) != 1 {
			return viewValue{}, fmt.Errorf("lit takes one literal")
		}
		return viewLiteral(args[0])
	case "ref":
		if len(args) != 1 {
			return viewValue{}, fmt.Errorf("ref takes one signal id")
		}
		sid, _ := args[0].(string)
		return read(sid)
	case "call":
		return viewEvalCall(args, read, payload)
	case "if":
		if len(args) != 3 {
			return viewValue{}, fmt.Errorf("if takes three arguments")
		}
		cond, err := viewEvalArg(args[0], read, payload)
		if err != nil {
			return viewValue{}, err
		}
		if cond.k != viewBool {
			return viewValue{}, fmt.Errorf("if condition must be a boolean")
		}
		if cond.b {
			return viewEvalArg(args[1], read, payload)
		}
		return viewEvalArg(args[2], read, payload)
	case "eq":
		if len(args) != 2 {
			return viewValue{}, fmt.Errorf("eq takes two arguments")
		}
		left, err := viewEvalArg(args[0], read, payload)
		if err != nil {
			return viewValue{}, err
		}
		right, err := viewEvalArg(args[1], read, payload)
		if err != nil {
			return viewValue{}, err
		}
		return viewValue{k: viewBool, b: left == right}, nil
	case "concat":
		var b strings.Builder
		for _, arg := range args {
			value, err := viewEvalArg(arg, read, payload)
			if err != nil {
				return viewValue{}, err
			}
			b.WriteString(viewStringify(value))
		}
		return viewValue{k: viewStr, s: b.String()}, nil
	case "trim":
		if len(args) != 1 {
			return viewValue{}, fmt.Errorf("trim takes one value")
		}
		value, err := viewEvalArg(args[0], read, payload)
		if err != nil {
			return viewValue{}, err
		}
		return viewValue{k: viewStr, s: strings.TrimFunc(viewStringify(value), unicode.IsSpace)}, nil
	default:
		return viewValue{}, fmt.Errorf("unknown expression op `%s`", op)
	}
}

func viewEvalCall(args []any, read viewRead, payload *viewValue) (viewValue, error) {
	if len(args) == 0 {
		return viewValue{}, fmt.Errorf("call takes a name")
	}
	name, _ := args[0].(string)
	rest := args[1:]
	switch name {
	case "event":
		if len(rest) != 0 {
			return viewValue{}, fmt.Errorf("event takes no arguments")
		}
		if payload == nil {
			return viewValue{}, fmt.Errorf("event is illegal outside an effect")
		}
		return *payload, nil
	case "not":
		if len(rest) != 1 {
			return viewValue{}, fmt.Errorf("not takes one boolean")
		}
		value, err := viewEvalArg(rest[0], read, payload)
		if err != nil {
			return viewValue{}, err
		}
		if value.k != viewBool {
			return viewValue{}, fmt.Errorf("not expects a boolean")
		}
		return viewValue{k: viewBool, b: !value.b}, nil
	case "add":
		if len(rest) < 2 {
			return viewValue{}, fmt.Errorf("add takes two or more integers")
		}
		var sum int64
		for _, arg := range rest {
			value, err := viewEvalArg(arg, read, payload)
			if err != nil {
				return viewValue{}, err
			}
			if value.k != viewInt {
				return viewValue{}, fmt.Errorf("add expects integers")
			}
			next := sum + value.n
			if (value.n > 0 && next < sum) || (value.n < 0 && next > sum) {
				return viewValue{}, fmt.Errorf("add overflow")
			}
			sum = next
		}
		return viewValue{k: viewInt, n: sum}, nil
	case "len":
		if len(rest) != 1 {
			return viewValue{}, fmt.Errorf("len takes one string")
		}
		value, err := viewEvalArg(rest[0], read, payload)
		if err != nil {
			return viewValue{}, err
		}
		if value.k != viewStr {
			return viewValue{}, fmt.Errorf("len expects a string")
		}
		return viewValue{k: viewInt, n: int64(len([]rune(value.s)))}, nil
	default:
		return viewValue{}, fmt.Errorf("unknown call `%s`", name)
	}
}

func viewEvalArg(value any, read viewRead, payload *viewValue) (viewValue, error) {
	if _, ok := value.(map[string]any); ok {
		return viewEvalExpr(value, read, payload)
	}
	return viewLiteral(value)
}

func viewStringify(value viewValue) string {
	switch value.k {
	case viewBool:
		if value.b {
			return "true"
		}
		return "false"
	case viewInt:
		return strconv.FormatInt(value.n, 10)
	case viewStr:
		return value.s
	default:
		return ""
	}
}

func viewLiteral(value any) (viewValue, error) {
	switch typed := value.(type) {
	case nil:
		return viewValue{}, nil
	case bool:
		return viewValue{k: viewBool, b: typed}, nil
	case string:
		return viewValue{k: viewStr, s: typed}, nil
	case json.Number:
		n, err := typed.Int64()
		if err != nil {
			return viewValue{}, fmt.Errorf("non-integer number %s", typed)
		}
		return viewValue{k: viewInt, n: n}, nil
	case float64:
		if typed != float64(int64(typed)) {
			return viewValue{}, fmt.Errorf("non-integer number %v", typed)
		}
		return viewValue{k: viewInt, n: int64(typed)}, nil
	case int:
		return viewValue{k: viewInt, n: int64(typed)}, nil
	case int64:
		return viewValue{k: viewInt, n: typed}, nil
	default:
		return viewValue{}, fmt.Errorf("value must be null, boolean, integer, or string")
	}
}

func viewValidateEffect(effect any, signals map[string]struct{}, hasChart bool) error {
	obj, err := viewMap(effect, "effect")
	if err != nil {
		return err
	}
	if err := viewAllow(obj, "effect", "op", "args"); err != nil {
		return err
	}
	op, _ := obj["op"].(string)
	if _, ok := viewEffOps[op]; !ok {
		return fmt.Errorf("unknown effect op `%s`", op)
	}
	args, ok := obj["args"].([]any)
	if !ok {
		return fmt.Errorf("effect args must be an array")
	}
	switch op {
	case "set":
		if len(args) != 2 {
			return fmt.Errorf("set takes a signal id and a value")
		}
		sid, ok := args[0].(string)
		if !ok {
			return fmt.Errorf("set signal id must be a string")
		}
		if _, ok := signals[sid]; !ok {
			return fmt.Errorf("unknown signal `%s`", sid)
		}
		return viewValidateArg(args[1], signals, true)
	case "send":
		if !hasChart {
			return fmt.Errorf("send requires a chart")
		}
		if (len(args) != 1 && len(args) != 2) || !isString(args[0]) {
			return fmt.Errorf("send takes an event name and an optional payload")
		}
		if len(args) == 2 {
			return viewValidateArg(args[1], signals, true)
		}
		return nil
	case "batch":
		if len(args) == 0 {
			return fmt.Errorf("batch takes one or more effects")
		}
		for _, nested := range args {
			if err := viewValidateEffect(nested, signals, hasChart); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown effect op `%s`", op)
	}
}

func viewValidateArg(value any, signals map[string]struct{}, inEffect bool) error {
	if _, ok := value.(map[string]any); ok {
		return viewValidateExpr(value, signals, inEffect)
	}
	_, err := viewLiteral(value)
	return err
}

func viewValidateExpr(expr any, signals map[string]struct{}, inEffect bool) error {
	obj, err := viewMap(expr, "expression")
	if err != nil {
		return err
	}
	if err := viewAllow(obj, "expression", "op", "args"); err != nil {
		return err
	}
	op, _ := obj["op"].(string)
	if _, ok := viewExprOps[op]; !ok {
		return fmt.Errorf("unknown expression op `%s`", op)
	}
	args, ok := obj["args"].([]any)
	if !ok {
		return fmt.Errorf("expression args must be an array")
	}
	switch op {
	case "lit":
		if len(args) != 1 {
			return fmt.Errorf("lit takes one literal")
		}
		_, err := viewLiteral(args[0])
		return err
	case "ref":
		if len(args) != 1 || !isString(args[0]) {
			return fmt.Errorf("ref takes one signal id")
		}
		if _, ok := signals[args[0].(string)]; !ok {
			return fmt.Errorf("unknown signal `%s`", args[0])
		}
		return nil
	case "call":
		return viewValidateCall(args, signals, inEffect)
	case "if":
		if len(args) != 3 {
			return fmt.Errorf("if takes three arguments")
		}
	case "eq":
		if len(args) != 2 {
			return fmt.Errorf("eq takes two arguments")
		}
	case "concat":
	case "trim":
		if len(args) != 1 {
			return fmt.Errorf("trim takes one value")
		}
	}
	if op == "call" {
		return nil
	}
	for _, arg := range args {
		if err := viewValidateArg(arg, signals, inEffect); err != nil {
			return err
		}
	}
	return nil
}

func viewValidateCall(args []any, signals map[string]struct{}, inEffect bool) error {
	if len(args) == 0 || !isString(args[0]) {
		return fmt.Errorf("call takes a name")
	}
	name := args[0].(string)
	if _, ok := viewCalls[name]; !ok {
		return fmt.Errorf("unknown call `%s`", name)
	}
	rest := args[1:]
	switch name {
	case "event":
		if len(rest) != 0 {
			return fmt.Errorf("event takes no arguments")
		}
		if !inEffect {
			return fmt.Errorf("event is illegal outside an effect")
		}
		return nil
	case "not":
		if len(rest) != 1 {
			return fmt.Errorf("not takes one boolean")
		}
	case "add":
		if len(rest) < 2 {
			return fmt.Errorf("add takes two or more integers")
		}
	case "len":
		if len(rest) != 1 {
			return fmt.Errorf("len takes one string")
		}
	}
	for _, arg := range rest {
		if err := viewValidateArg(arg, signals, inEffect); err != nil {
			return err
		}
	}
	return nil
}

func viewStandingText(value any, signals map[string]struct{}) (any, error) {
	if text, ok := value.(string); ok {
		return map[string]any{"op": "lit", "args": []any{text}}, nil
	}
	if _, ok := value.(map[string]any); ok {
		if err := viewValidateExpr(value, signals, false); err != nil {
			return nil, err
		}
		return value, nil
	}
	return nil, fmt.Errorf("text must be a string or an expression")
}

func viewStandingFlag(field string, value any, signals map[string]struct{}) (any, error) {
	if _, ok := value.(bool); ok {
		return map[string]any{"op": "lit", "args": []any{value}}, nil
	}
	if _, ok := value.(map[string]any); ok {
		if err := viewValidateExpr(value, signals, false); err != nil {
			return nil, err
		}
		return value, nil
	}
	return nil, fmt.Errorf("%s must be a boolean or an expression", field)
}

func viewChildren(nid string, value any) ([]string, error) {
	arr, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("node `%s` children must be an array", nid)
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(arr))
	for _, child := range arr {
		text, ok := child.(string)
		if !ok || text == "" {
			return nil, fmt.Errorf("node `%s` children must be non-empty strings", nid)
		}
		if _, ok := seen[text]; ok {
			return nil, fmt.Errorf("node `%s` repeats child `%s`", nid, text)
		}
		seen[text] = struct{}{}
		out = append(out, text)
	}
	return out, nil
}

func viewLayout(nid string, value any) (any, error) {
	obj, err := viewMap(value, "node "+nid+" layout")
	if err != nil {
		return nil, err
	}
	if err := viewAllow(obj, "node "+nid+" layout", "flow", "align", "grow"); err != nil {
		return nil, err
	}
	var flow any
	if raw, ok := obj["flow"]; ok {
		text, err := viewString(raw, "flow")
		if err != nil {
			return nil, err
		}
		if _, ok := viewFlows[text]; !ok {
			return nil, fmt.Errorf("unknown flow `%s`", text)
		}
		flow = text
	}
	if raw, ok := obj["align"]; ok {
		text, err := viewString(raw, "align")
		if err != nil {
			return nil, err
		}
		if _, ok := viewAligns[text]; !ok {
			return nil, fmt.Errorf("unknown align `%s`", text)
		}
	}
	if raw, ok := obj["grow"]; ok {
		grow, err := viewLiteral(raw)
		if err != nil || grow.k != viewInt || grow.n < 0 {
			return nil, fmt.Errorf("node `%s` layout.grow must be an integer >= 0", nid)
		}
	}
	return flow, nil
}

func viewCheckTree(root string, nodes map[string]*viewNode) error {
	stack := map[string]struct{}{}
	done := map[string]struct{}{}
	var walk func(string) error
	walk = func(id string) error {
		if _, ok := stack[id]; ok {
			return fmt.Errorf("children cycle at `%s`", id)
		}
		if _, ok := done[id]; ok {
			return fmt.Errorf("node `%s` has more than one parent", id)
		}
		stack[id] = struct{}{}
		for _, child := range nodes[id].children {
			if _, ok := nodes[child]; !ok {
				return fmt.Errorf("children name unknown node `%s`", child)
			}
			if err := walk(child); err != nil {
				return err
			}
		}
		delete(stack, id)
		done[id] = struct{}{}
		return nil
	}
	if err := walk(root); err != nil {
		return err
	}
	if len(done) != len(nodes) {
		return fmt.Errorf("a node is not reachable from root")
	}
	return nil
}

func viewMap(value any, what string) (map[string]any, error) {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", what)
	}
	return obj, nil
}

func viewAllow(obj map[string]any, what string, keys ...string) error {
	allowed := map[string]struct{}{}
	for _, key := range keys {
		allowed[key] = struct{}{}
	}
	for key := range obj {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("%s has unknown field `%s`", what, key)
		}
	}
	return nil
}

func viewString(value any, what string) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", what)
	}
	return text, nil
}

func isString(value any) bool {
	_, ok := value.(string)
	return ok
}
