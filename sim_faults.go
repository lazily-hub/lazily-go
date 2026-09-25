package lazily

import (
	"fmt"
	"math"
	"sort"
	"strconv"
)

// Modeled faults exercise declared deterministic boundaries. They do not
// replace conformance testing against real databases, brokers, operating
// systems, clocks, or networks.
type SimFaultKind string

const (
	SimFaultDelay              SimFaultKind = "delay"
	SimFaultDrop               SimFaultKind = "drop"
	SimFaultDuplicate          SimFaultKind = "duplicate"
	SimFaultReorder            SimFaultKind = "reorder"
	SimFaultPartition          SimFaultKind = "partition"
	SimFaultHeal               SimFaultKind = "heal"
	SimFaultCrash              SimFaultKind = "crash"
	SimFaultRestart            SimFaultKind = "restart"
	SimFaultTimeout            SimFaultKind = "timeout"
	SimFaultCancel             SimFaultKind = "cancel"
	SimFaultStaleResponse      SimFaultKind = "stale_response"
	SimFaultOptimisticConflict SimFaultKind = "optimistic_conflict"
	SimFaultCommit             SimFaultKind = "commit"
	SimFaultRollback           SimFaultKind = "rollback"
	SimFaultClockAdvance       SimFaultKind = "clock_advance"
)

type SimFaultRule struct {
	ID         string
	CauseID    string
	Kind       SimFaultKind
	Port       string
	CallID     string
	Occurrence uint64
	DelayTicks uint64
	Copies     uint64
	ActorID    string
	Payload    any
}

type SimFaultPlan struct {
	Name      string
	Version   string
	SeedHex   string
	PlanIndex uint64
	Rules     []SimFaultRule
}

func NewSimFaultPlan(name, version, seedHex string, index uint64, rules []SimFaultRule) (SimFaultPlan, error) {
	if !validSimID(name) || version == "" {
		return SimFaultPlan{}, simErrorf("fault plan needs a stable name and version")
	}
	if _, err := ParseSimSeed(seedHex); err != nil {
		return SimFaultPlan{}, fmt.Errorf("%w: invalid fault-plan seed: %v", ErrSimulation, err)
	}
	seen := map[string]struct{}{}
	out := append([]SimFaultRule(nil), rules...)
	for i := range out {
		rule := &out[i]
		if !validSimID(rule.ID) || !validSimID(rule.Port) || !validSimFaultKind(rule.Kind) {
			return SimFaultPlan{}, simErrorf("fault rule needs stable id, port, and supported kind")
		}
		if _, duplicate := seen[rule.ID]; duplicate {
			return SimFaultPlan{}, simErrorf("duplicate fault rule id %q", rule.ID)
		}
		if rule.CauseID != "" {
			if _, resolved := seen[rule.CauseID]; !resolved {
				return SimFaultPlan{}, simErrorf("fault rule %q has unresolved cause %q", rule.ID, rule.CauseID)
			}
		}
		if rule.CallID != "" && !validSimID(rule.CallID) {
			return SimFaultPlan{}, simErrorf("fault rule %q has invalid call id", rule.ID)
		}
		if (rule.Kind == SimFaultCrash || rule.Kind == SimFaultRestart) && !validSimID(rule.ActorID) {
			return SimFaultPlan{}, simErrorf("fault rule %q needs an actor id", rule.ID)
		}
		payload, err := freezeSimValue(rule.Payload)
		if err != nil {
			return SimFaultPlan{}, err
		}
		rule.Payload = payload
		seen[rule.ID] = struct{}{}
	}
	return SimFaultPlan{Name: name, Version: version, SeedHex: seedHex, PlanIndex: index, Rules: out}, nil
}

func (p SimFaultPlan) CanonicalBytes() ([]byte, error) { return ReplayCanonicalBytes(p) }
func (p SimFaultPlan) Digest() (string, error)         { return ReplayCanonicalDigest(p) }

func validSimFaultKind(kind SimFaultKind) bool {
	switch kind {
	case SimFaultDelay, SimFaultDrop, SimFaultDuplicate, SimFaultReorder,
		SimFaultPartition, SimFaultHeal, SimFaultCrash, SimFaultRestart,
		SimFaultTimeout, SimFaultCancel, SimFaultStaleResponse,
		SimFaultOptimisticConflict, SimFaultCommit, SimFaultRollback,
		SimFaultClockAdvance:
		return true
	default:
		return false
	}
}

type SimFaultTemplate struct {
	Name   string
	Weight uint64
	Build  func(random *SimRandomStream, ordinal uint64) (SimFaultRule, error)
}

// MaterializeSimFaultPlan fixes every random choice before execution. Selection
// and rule payload randomness use separate streams, so callback draw counts do
// not change later fault selection.
func MaterializeSimFaultPlan(world *SimWorld, name, version string, index, count uint64, templates []SimFaultTemplate) (SimFaultPlan, error) {
	if world == nil || count > uint64(math.MaxInt) {
		return SimFaultPlan{}, simErrorf("fault materialization needs a world and bounded count")
	}
	templates = append([]SimFaultTemplate(nil), templates...)
	sort.Slice(templates, func(i, j int) bool { return templates[i].Name < templates[j].Name })
	var total uint64
	seenTemplates := map[string]struct{}{}
	for _, template := range templates {
		if !validSimID(template.Name) || template.Build == nil {
			return SimFaultPlan{}, simErrorf("fault template needs a stable name and Build")
		}
		if _, duplicate := seenTemplates[template.Name]; duplicate {
			return SimFaultPlan{}, simErrorf("duplicate fault template %q", template.Name)
		}
		seenTemplates[template.Name] = struct{}{}
		if math.MaxUint64-total < template.Weight {
			return SimFaultPlan{}, simErrorf("fault template weights overflow")
		}
		total += template.Weight
	}
	if count > 0 && total == 0 {
		return SimFaultPlan{}, simErrorf("fault materialization has no positive-weight template")
	}
	rules := make([]SimFaultRule, 0, count)
	for ordinal := uint64(0); ordinal < count; ordinal++ {
		base := []string{"fault-plan", name, version, strconv.FormatUint(index, 10), strconv.FormatUint(ordinal, 10)}
		selectStream, err := newSimRandomStream(world.seed, append(append([]string(nil), base...), "select"))
		if err != nil {
			return SimFaultPlan{}, err
		}
		draw, err := selectStream.Uint64N(total)
		if err != nil {
			return SimFaultPlan{}, err
		}
		selected := templates[len(templates)-1]
		for _, template := range templates {
			if template.Weight == 0 {
				continue
			}
			if draw < template.Weight {
				selected = template
				break
			}
			draw -= template.Weight
		}
		buildStream, err := newSimRandomStream(world.seed, append(append([]string(nil), base...), "payload", selected.Name))
		if err != nil {
			return SimFaultPlan{}, err
		}
		rule, err := selected.Build(buildStream, ordinal)
		if err != nil {
			return SimFaultPlan{}, fmt.Errorf("%w: build fault template %q: %v", ErrSimulation, selected.Name, err)
		}
		rules = append(rules, rule)
	}
	return NewSimFaultPlan(name, version, world.seed.String(), index, rules)
}

type SimFaultController struct {
	world       *SimWorld
	plan        SimFaultPlan
	occurrences map[string]uint64
	partitioned map[string]bool
	consumed    map[string]bool
}

func NewSimFaultController(world *SimWorld, plan SimFaultPlan) (*SimFaultController, error) {
	if world == nil {
		return nil, simErrorf("fault controller needs a world")
	}
	validated, err := NewSimFaultPlan(plan.Name, plan.Version, plan.SeedHex, plan.PlanIndex, plan.Rules)
	if err != nil {
		return nil, err
	}
	return &SimFaultController{world: world, plan: validated, occurrences: map[string]uint64{}, partitioned: map[string]bool{}, consumed: map[string]bool{}}, nil
}

type SimFaultDisposition struct {
	At       uint64
	Copies   uint64
	Invoke   bool
	UseStale bool
	Outcome  string
	RuleIDs  []string
	Payload  any
}

func (c *SimFaultController) Apply(port, callID string, at uint64) (SimFaultDisposition, error) {
	if c.world.finished {
		return SimFaultDisposition{}, simErrorf("cannot inject fault after terminal status")
	}
	if !validSimID(port) || !validSimID(callID) || at < c.world.Now() {
		return SimFaultDisposition{}, simErrorf("fault call needs stable ids and non-past time")
	}
	c.occurrences[port]++
	occurrence := c.occurrences[port]
	d := SimFaultDisposition{At: at, Copies: 1, Invoke: true, Outcome: "success"}
	for _, rule := range c.plan.Rules {
		if c.consumed[rule.ID] || rule.Port != port || (rule.CallID != "" && rule.CallID != callID) || (rule.Occurrence != 0 && rule.Occurrence != occurrence) {
			continue
		}
		if err := c.record(rule, callID); err != nil {
			return SimFaultDisposition{}, err
		}
		c.consumed[rule.ID] = true
		d.RuleIDs = append(d.RuleIDs, rule.ID)
		d.Payload = cloneSimAction(SimAction{Payload: rule.Payload}).Payload
		switch rule.Kind {
		case SimFaultDelay, SimFaultReorder, SimFaultClockAdvance:
			if math.MaxUint64-d.At < rule.DelayTicks {
				return SimFaultDisposition{}, simErrorf("fault %q overflows logical time", rule.ID)
			}
			d.At += rule.DelayTicks
		case SimFaultDrop:
			d.Copies, d.Invoke, d.Outcome = 0, false, "dropped"
		case SimFaultDuplicate:
			if d.Copies == 0 {
				continue
			}
			extra := rule.Copies
			if extra == 0 {
				extra = 1
			}
			if math.MaxUint64-d.Copies < extra {
				return SimFaultDisposition{}, simErrorf("fault %q overflows duplicate count", rule.ID)
			}
			d.Copies += extra
		case SimFaultPartition:
			c.partitioned[port] = true
			d.Copies, d.Invoke, d.Outcome = 0, false, "partitioned"
		case SimFaultHeal:
			delete(c.partitioned, port)
			d.Copies, d.Invoke, d.Outcome = 1, true, "healed"
		case SimFaultCrash:
			if _, err := c.world.ScheduleLifecycle(d.At, rule.ActorID, SimActorCrashed); err != nil {
				return SimFaultDisposition{}, err
			}
		case SimFaultRestart:
			if _, err := c.world.ScheduleLifecycle(d.At, rule.ActorID, SimActorRunning); err != nil {
				return SimFaultDisposition{}, err
			}
		case SimFaultTimeout:
			d.Invoke, d.Outcome = false, "timeout"
		case SimFaultCancel:
			d.Invoke, d.Outcome = false, "canceled"
		case SimFaultStaleResponse:
			d.Invoke, d.UseStale, d.Outcome = false, true, "stale"
		case SimFaultOptimisticConflict:
			d.Invoke, d.Outcome = false, "optimistic_conflict"
		case SimFaultCommit:
			d.Outcome = "committed"
		case SimFaultRollback:
			d.Invoke, d.Outcome = false, "rolled_back"
		}
	}
	if c.partitioned[port] {
		d.Copies, d.Invoke, d.Outcome = 0, false, "partitioned"
		if len(d.RuleIDs) == 0 {
			if err := c.world.appendEntry(SimTraceEntry{Kind: "fault_effect", Version: SimTraceEntryVersion, ActionID: callID, ActionKind: string(SimFaultPartition), Outcome: "partitioned", Detail: port}); err != nil {
				return SimFaultDisposition{}, err
			}
		}
	}
	return d, nil
}

func (c *SimFaultController) record(rule SimFaultRule, callID string) error {
	kind := "fault_injected"
	if rule.Kind == SimFaultHeal || rule.Kind == SimFaultRestart {
		kind = "fault_recovered"
	}
	return c.world.appendEntry(SimTraceEntry{Kind: kind, Version: SimTraceEntryVersion, ActionID: callID, ActionKind: string(rule.Kind), CauseID: rule.CauseID, Outcome: rule.ID, Detail: rule.Port})
}

type SimPortOutcome[T any] struct {
	Status       string
	Value        T
	Error        string
	FaultPayload any
}

// SimFaultPort is a deterministic generic boundary. Invoke must be a simulated,
// deterministic adapter. Real database, broker, OS, clock, and network clients
// are forbidden inside SimWorld; exercise them in separate conformance tests.
// ResponseAction turns the simulated result into ordinary scheduled domain work.
type SimFaultPort[Request, Response any] struct {
	Name           string
	Controller     *SimFaultController
	Invoke         func(Request) (Response, error)
	ResponseAction func(callID string, copyIndex uint64, outcome SimPortOutcome[Response]) SimAction
	stale          *SimPortOutcome[Response]
}

func (p *SimFaultPort[Request, Response]) Call(at uint64, callID string, request Request) ([]SimScheduledItem, error) {
	if p.Controller == nil || p.Invoke == nil || p.ResponseAction == nil || !validSimID(p.Name) {
		return nil, simErrorf("fault port is not fully configured")
	}
	if err := p.Controller.world.appendEntry(SimTraceEntry{Kind: "port_call", Version: SimTraceEntryVersion, At: at, ActionID: callID, ActionKind: p.Name, Outcome: "pending"}); err != nil {
		return nil, err
	}
	d, err := p.Controller.Apply(p.Name, callID, at)
	if err != nil {
		return nil, err
	}
	if d.Copies == 0 {
		if err := p.Controller.world.appendEntry(SimTraceEntry{Kind: "port_result", Version: SimTraceEntryVersion, At: d.At, ActionID: callID, ActionKind: p.Name, Outcome: d.Outcome}); err != nil {
			return nil, err
		}
		return nil, nil
	}
	var outcome SimPortOutcome[Response]
	if d.UseStale {
		if p.stale == nil {
			return nil, simErrorf("port %q has no stale response for call %q", p.Name, callID)
		}
		outcome = cloneSimPortOutcome(*p.stale)
		outcome.Status = d.Outcome
	} else if d.Invoke {
		value, invokeErr := p.Invoke(request)
		outcome = SimPortOutcome[Response]{Status: d.Outcome, Value: value}
		if invokeErr != nil {
			outcome.Error = invokeErr.Error()
		}
		copyForStale := cloneSimPortOutcome(outcome)
		p.stale = &copyForStale
	} else {
		outcome.Status = d.Outcome
	}
	outcome.FaultPayload = d.Payload
	if d.Copies >= uint64(math.MaxInt) {
		return nil, simBoundError{limit: "max_queue"}
	}
	world := p.Controller.world
	if world.maxQueue > 0 && uint64(world.queue.Len())+d.Copies > world.maxQueue {
		return nil, simBoundError{limit: "max_queue"}
	}
	remainingOrdinals := uint64(math.MaxUint64 - world.nextOrdinal)
	if world.ordinalSpent || (d.Copies > 0 && d.Copies-1 > remainingOrdinals) {
		return nil, simBoundError{limit: "enqueue_ordinal"}
	}
	if err := world.ensureTraceCapacity(int(d.Copies) + 1); err != nil {
		return nil, err
	}
	scheduled := make([]SimScheduledAction, 0, d.Copies)
	for copyIndex := uint64(0); copyIndex < d.Copies; copyIndex++ {
		action := p.ResponseAction(callID, copyIndex, cloneSimPortOutcome(outcome))
		scheduled = append(scheduled, SimScheduledAction{At: d.At, Action: action})
	}
	if err := world.validateScheduleBatch(scheduled); err != nil {
		return nil, err
	}
	if err := world.appendEntry(SimTraceEntry{Kind: "port_result", Version: SimTraceEntryVersion, At: d.At, ActionID: callID, ActionKind: p.Name, Outcome: outcome.Status}); err != nil {
		return nil, err
	}
	items := make([]SimScheduledItem, 0, d.Copies)
	for _, candidate := range scheduled {
		item, err := p.Controller.world.Schedule(candidate.At, candidate.Action)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func cloneSimPortOutcome[T any](outcome SimPortOutcome[T]) SimPortOutcome[T] {
	if value, err := freezeSimValue(outcome.Value); err == nil {
		if typed, ok := value.(T); ok {
			outcome.Value = typed
		}
	}
	if payload, err := freezeSimValue(outcome.FaultPayload); err == nil {
		outcome.FaultPayload = payload
	}
	return outcome
}

// ShrinkSimFaultPlan removes dependency-closed rules while preserving the
// caller-defined target failure. Materialized rule IDs and ordering are stable.
func ShrinkSimFaultPlan(plan SimFaultPlan, fails func(SimFaultPlan) (bool, error)) (SimFaultPlan, error) {
	if fails == nil {
		return SimFaultPlan{}, simErrorf("fault shrinker needs a failure predicate")
	}
	current := plan
	failed, err := fails(current)
	if err != nil {
		return SimFaultPlan{}, err
	}
	if !failed {
		return SimFaultPlan{}, simErrorf("initial fault plan does not fail")
	}
	for i := 0; i < len(current.Rules); {
		removed := map[string]struct{}{current.Rules[i].ID: {}}
		changed := true
		for changed {
			changed = false
			for _, rule := range current.Rules {
				if _, gone := removed[rule.ID]; gone {
					continue
				}
				if _, causeGone := removed[rule.CauseID]; rule.CauseID != "" && causeGone {
					removed[rule.ID] = struct{}{}
					changed = true
				}
			}
		}
		rules := make([]SimFaultRule, 0, len(current.Rules)-len(removed))
		for _, rule := range current.Rules {
			if _, gone := removed[rule.ID]; !gone {
				rules = append(rules, rule)
			}
		}
		candidate, err := NewSimFaultPlan(current.Name, current.Version, current.SeedHex, current.PlanIndex, rules)
		if err != nil {
			return SimFaultPlan{}, err
		}
		failed, err := fails(candidate)
		if err != nil {
			return SimFaultPlan{}, err
		}
		if failed {
			current = candidate
			continue
		}
		i++
	}
	return current, nil
}
