package lazily

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"
)

var ErrSimConsumerConformance = errors.New("lazily: consumer simulation conformance failed")

func simConsumerErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrSimConsumerConformance}, args...)...)
}

// SimConsumerAdapterKind identifies the execution boundary behind a consumer
// conformance adapter. Postgres and NATS adapters are real-service adapters, not
// alternate in-memory implementations.
type SimConsumerAdapterKind string

const (
	SimConsumerAdapterInMemory SimConsumerAdapterKind = "in_memory"
	SimConsumerAdapterPostgres SimConsumerAdapterKind = "postgres"
	SimConsumerAdapterNATS     SimConsumerAdapterKind = "nats"
)

func (kind SimConsumerAdapterKind) real() bool {
	return kind == SimConsumerAdapterPostgres || kind == SimConsumerAdapterNATS
}

// SimConsumerPort declares one narrow external boundary used by every adapter.
// Stubbed ports are permitted only at nondeterministic boundaries, and never on
// a real-service adapter.
type SimConsumerPort struct {
	ID                       string
	Kind                     string
	NondeterministicBoundary bool
	Stubbed                  bool
}

// SimConsumerAdapter makes an in-memory implementation and selected real
// services interchangeable under one materialized generated history.
//
// ProductionReducerID is evidence that every adapter invokes the same production
// decision/reducer path. Real-service adapters must provide a stable ServiceID
// and Probe that verifies the service is reachable before a scenario starts.
type SimConsumerAdapter struct {
	ID                  string
	Kind                SimConsumerAdapterKind
	ProductionReducerID string
	ServiceID           string
	Ports               []SimConsumerPort
	Probe               func() error
	SimulationWorld     func() *SimWorld
	Reset               func() error
	Apply               func(SimAction) error
	Observe             func() (map[string]any, error)
}

type SimConsumerTestkitSpec struct {
	SimulationAdapterID  string
	RequiredRealAdapters []SimConsumerAdapterKind
	Adapters             []SimConsumerAdapter
}

type SimConsumerTestkit struct {
	spec          SimConsumerTestkitSpec
	baselineIndex int
}

type SimConsumerCheckpoint struct {
	Step               uint64
	ActionID           string
	ObservationDigests map[string]string
}

type SimConsumerRunResult struct {
	ScenarioDigest string
	AdapterIDs     []string
	Checkpoints    []SimConsumerCheckpoint
}

// SimConsumerDivergenceError localizes the first adapter observation that
// differs from the in-memory execution after an action.
type SimConsumerDivergenceError struct {
	Step              uint64
	ActionID          string
	BaselineAdapterID string
	AdapterID         string
	ObservationID     string
	Kind              string
}

func (e *SimConsumerDivergenceError) Error() string {
	observation := ""
	if e.ObservationID != "" {
		observation = fmt.Sprintf(" observation %q", e.ObservationID)
	}
	return fmt.Sprintf("%v: step %d action %q adapter %q differs from %q: %s%s",
		ErrSimConsumerConformance, e.Step, e.ActionID, e.AdapterID,
		e.BaselineAdapterID, e.Kind, observation)
}

func (e *SimConsumerDivergenceError) Unwrap() error { return ErrSimConsumerConformance }

// NewSimConsumerTestkit validates the conformance topology before a service is
// touched. At least one real Postgres or NATS kind must be selected explicitly;
// every selected kind must have a matching adapter.
func NewSimConsumerTestkit(spec SimConsumerTestkitSpec) (*SimConsumerTestkit, error) {
	if !validSimID(spec.SimulationAdapterID) {
		return nil, simConsumerErrorf("simulation adapter needs a stable id")
	}
	if len(spec.RequiredRealAdapters) == 0 {
		return nil, simConsumerErrorf("select at least one real Postgres or NATS adapter")
	}
	if len(spec.Adapters) < 2 {
		return nil, simConsumerErrorf("testkit needs an in-memory adapter and at least one real adapter")
	}

	required := map[SimConsumerAdapterKind]struct{}{}
	for _, kind := range spec.RequiredRealAdapters {
		if !kind.real() {
			return nil, simConsumerErrorf("required adapter kind %q is not a real Postgres or NATS service", kind)
		}
		if _, duplicate := required[kind]; duplicate {
			return nil, simConsumerErrorf("duplicate required real adapter kind %q", kind)
		}
		required[kind] = struct{}{}
	}

	adapters := append([]SimConsumerAdapter(nil), spec.Adapters...)
	seenIDs := map[string]struct{}{}
	presentKinds := map[SimConsumerAdapterKind]struct{}{}
	productionReducerID := ""
	portContract := []string(nil)
	for i := range adapters {
		adapter := &adapters[i]
		adapter.Ports = append([]SimConsumerPort(nil), adapter.Ports...)
		if err := validateSimConsumerAdapter(*adapter); err != nil {
			return nil, err
		}
		if _, duplicate := seenIDs[adapter.ID]; duplicate {
			return nil, simConsumerErrorf("duplicate adapter id %q", adapter.ID)
		}
		seenIDs[adapter.ID] = struct{}{}
		presentKinds[adapter.Kind] = struct{}{}
		if adapter.Kind.real() {
			if _, selected := required[adapter.Kind]; !selected {
				return nil, simConsumerErrorf("real adapter %q kind %q was not explicitly selected", adapter.ID, adapter.Kind)
			}
		}
		if productionReducerID == "" {
			productionReducerID = adapter.ProductionReducerID
		} else if adapter.ProductionReducerID != productionReducerID {
			return nil, simConsumerErrorf("adapter %q uses reducer %q, want shared production reducer %q", adapter.ID, adapter.ProductionReducerID, productionReducerID)
		}
		contract := simConsumerPortContract(adapter.Ports)
		if portContract == nil {
			portContract = contract
		} else if !equalStrings(portContract, contract) {
			return nil, simConsumerErrorf("adapter %q does not expose the shared narrow-port contract", adapter.ID)
		}
	}
	if len(portContract) == 0 {
		return nil, simConsumerErrorf("consumer adapters must declare at least one narrow port")
	}
	for kind := range required {
		if _, present := presentKinds[kind]; !present {
			return nil, simConsumerErrorf("required real adapter kind %q is missing", kind)
		}
	}

	sort.Slice(adapters, func(i, j int) bool { return adapters[i].ID < adapters[j].ID })
	baselineIndex := -1
	for i, adapter := range adapters {
		if adapter.ID == spec.SimulationAdapterID {
			if adapter.Kind != SimConsumerAdapterInMemory {
				return nil, simConsumerErrorf("simulation adapter %q must have kind %q", adapter.ID, SimConsumerAdapterInMemory)
			}
			baselineIndex = i
		}
	}
	if baselineIndex < 0 {
		return nil, simConsumerErrorf("simulation adapter %q is missing", spec.SimulationAdapterID)
	}
	spec.Adapters = adapters
	spec.RequiredRealAdapters = append([]SimConsumerAdapterKind(nil), spec.RequiredRealAdapters...)
	return &SimConsumerTestkit{spec: spec, baselineIndex: baselineIndex}, nil
}

func validateSimConsumerAdapter(adapter SimConsumerAdapter) error {
	if !validSimID(adapter.ID) || !validSimID(adapter.ProductionReducerID) {
		return simConsumerErrorf("adapter needs stable adapter and production-reducer ids")
	}
	if adapter.Reset == nil || adapter.Apply == nil || adapter.Observe == nil {
		return simConsumerErrorf("adapter %q needs Reset, Apply, and Observe callbacks", adapter.ID)
	}
	switch adapter.Kind {
	case SimConsumerAdapterInMemory:
		if adapter.ServiceID != "" || adapter.Probe != nil {
			return simConsumerErrorf("in-memory adapter %q cannot claim a real service", adapter.ID)
		}
		if adapter.SimulationWorld == nil {
			return simConsumerErrorf("in-memory adapter %q must expose its SimWorld", adapter.ID)
		}
	case SimConsumerAdapterPostgres, SimConsumerAdapterNATS:
		if !validSimID(adapter.ServiceID) || adapter.Probe == nil {
			return simConsumerErrorf("real adapter %q needs a stable service id and Probe", adapter.ID)
		}
		if adapter.SimulationWorld != nil {
			return simConsumerErrorf("real adapter %q cannot expose a simulation world", adapter.ID)
		}
	default:
		return simConsumerErrorf("adapter %q has unknown kind %q", adapter.ID, adapter.Kind)
	}

	seenPorts := map[string]struct{}{}
	for _, port := range adapter.Ports {
		if !validSimID(port.ID) || !validSimID(port.Kind) {
			return simConsumerErrorf("adapter %q has a port without stable id and kind", adapter.ID)
		}
		if _, duplicate := seenPorts[port.ID]; duplicate {
			return simConsumerErrorf("adapter %q has duplicate port %q", adapter.ID, port.ID)
		}
		seenPorts[port.ID] = struct{}{}
		if port.Stubbed && !port.NondeterministicBoundary {
			return simConsumerErrorf("adapter %q stubs deterministic port %q", adapter.ID, port.ID)
		}
		if adapter.Kind.real() && port.Stubbed {
			return simConsumerErrorf("real adapter %q cannot stub port %q", adapter.ID, port.ID)
		}
	}
	return nil
}

func simConsumerPortContract(ports []SimConsumerPort) []string {
	contract := make([]string, len(ports))
	for i, port := range ports {
		contract[i] = fmt.Sprintf("%s\x00%s\x00%t", port.ID, port.Kind, port.NondeterministicBoundary)
	}
	sort.Strings(contract)
	return contract
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// Run executes one materialized generated scenario against the in-memory and
// selected real adapters, comparing canonical observations after every action.
func (kit *SimConsumerTestkit) Run(scenario SimGeneratedScenario) (SimConsumerRunResult, error) {
	if kit == nil {
		return SimConsumerRunResult{}, simConsumerErrorf("nil consumer simulation testkit")
	}
	if err := validateSimConsumerScenario(scenario); err != nil {
		return SimConsumerRunResult{}, err
	}
	scenarioDigest, err := ReplayCanonicalDigest(scenario)
	if err != nil {
		return SimConsumerRunResult{}, simConsumerErrorf("encode scenario: %v", err)
	}
	for _, adapter := range kit.spec.Adapters {
		if adapter.Kind.real() {
			if err := adapter.Probe(); err != nil {
				return SimConsumerRunResult{}, simConsumerErrorf("probe %s adapter %q service %q: %v", adapter.Kind, adapter.ID, adapter.ServiceID, err)
			}
		}
		if err := adapter.Reset(); err != nil {
			return SimConsumerRunResult{}, simConsumerErrorf("reset adapter %q: %v", adapter.ID, err)
		}
		if adapter.Kind == SimConsumerAdapterInMemory && adapter.SimulationWorld() == nil {
			return SimConsumerRunResult{}, simConsumerErrorf("reset adapter %q did not create its SimWorld", adapter.ID)
		}
	}

	result := SimConsumerRunResult{ScenarioDigest: scenarioDigest}
	result.AdapterIDs = make([]string, len(kit.spec.Adapters))
	for i, adapter := range kit.spec.Adapters {
		result.AdapterIDs[i] = adapter.ID
	}
	result.Checkpoints = make([]SimConsumerCheckpoint, 0, len(scenario.Actions))
	for actionIndex, generated := range scenario.Actions {
		observed := make([]map[string]any, len(kit.spec.Adapters))
		checkpoint := SimConsumerCheckpoint{
			Step:               uint64(actionIndex + 1),
			ActionID:           generated.Action.ID,
			ObservationDigests: map[string]string{},
		}
		for adapterIndex, adapter := range kit.spec.Adapters {
			var simulationWorld *SimWorld
			var stepsBefore uint64
			var traceEntriesBefore int
			if adapter.Kind == SimConsumerAdapterInMemory {
				simulationWorld = adapter.SimulationWorld()
				stepsBefore = simulationWorld.Steps()
				traceEntriesBefore = len(simulationWorld.Trace().Entries)
			}
			if err := adapter.Apply(cloneSimAction(generated.Action)); err != nil {
				return SimConsumerRunResult{}, simConsumerErrorf("step %d action %q apply adapter %q: %v", checkpoint.Step, checkpoint.ActionID, adapter.ID, err)
			}
			if adapter.Kind == SimConsumerAdapterInMemory {
				if adapter.SimulationWorld() != simulationWorld || simulationWorld.Steps() <= stepsBefore {
					return SimConsumerRunResult{}, simConsumerErrorf("step %d action %q adapter %q did not execute through its SimWorld", checkpoint.Step, checkpoint.ActionID, adapter.ID)
				}
				executedAction := false
				for _, entry := range simulationWorld.Trace().Entries[traceEntriesBefore:] {
					if entry.ActionID == generated.Action.ID && strings.HasPrefix(entry.Kind, "action_") {
						executedAction = true
						break
					}
				}
				if !executedAction {
					return SimConsumerRunResult{}, simConsumerErrorf("step %d action %q adapter %q advanced SimWorld without executing that action", checkpoint.Step, checkpoint.ActionID, adapter.ID)
				}
			}
			values, err := adapter.Observe()
			if err != nil {
				return SimConsumerRunResult{}, simConsumerErrorf("step %d action %q observe adapter %q: %v", checkpoint.Step, checkpoint.ActionID, adapter.ID, err)
			}
			if len(values) == 0 {
				return SimConsumerRunResult{}, simConsumerErrorf("step %d action %q adapter %q returned no observations", checkpoint.Step, checkpoint.ActionID, adapter.ID)
			}
			observed[adapterIndex] = cloneOracleValues(values)
			digest, err := ReplayCanonicalDigest(observed[adapterIndex])
			if err != nil {
				return SimConsumerRunResult{}, simConsumerErrorf("step %d action %q encode adapter %q observations: %v", checkpoint.Step, checkpoint.ActionID, adapter.ID, err)
			}
			checkpoint.ObservationDigests[adapter.ID] = digest
		}
		baseline := observed[kit.baselineIndex]
		baselineID := kit.spec.Adapters[kit.baselineIndex].ID
		for adapterIndex, adapter := range kit.spec.Adapters {
			if adapterIndex == kit.baselineIndex {
				continue
			}
			if err := compareSimConsumerObservations(checkpoint.Step, checkpoint.ActionID, baselineID, adapter.ID, baseline, observed[adapterIndex]); err != nil {
				return SimConsumerRunResult{}, err
			}
		}
		result.Checkpoints = append(result.Checkpoints, checkpoint)
	}
	return result, nil
}

func validateSimConsumerScenario(scenario SimGeneratedScenario) error {
	if !validSimID(scenario.GeneratorName) || scenario.GeneratorVersion == "" {
		return simConsumerErrorf("scenario needs a stable generator name and version")
	}
	if _, err := ParseSimSeed(scenario.SeedHex); err != nil {
		return simConsumerErrorf("scenario seed: %v", err)
	}
	if len(scenario.Actions) == 0 {
		return simConsumerErrorf("scenario must contain at least one generated action")
	}
	seen := map[string]struct{}{}
	for _, generated := range scenario.Actions {
		if !validSimID(generated.Command) {
			return simConsumerErrorf("scenario action %q has no stable generator command", generated.Action.ID)
		}
		if err := validateSimAction(generated.Action); err != nil {
			return simConsumerErrorf("scenario action: %v", err)
		}
		if _, duplicate := seen[generated.Action.ID]; duplicate {
			return simConsumerErrorf("scenario has duplicate action id %q", generated.Action.ID)
		}
		if generated.Action.CauseID != "" {
			if _, resolved := seen[generated.Action.CauseID]; !resolved {
				return simConsumerErrorf("scenario action %q has unresolved cause %q", generated.Action.ID, generated.Action.CauseID)
			}
		}
		seen[generated.Action.ID] = struct{}{}
	}
	return nil
}

func compareSimConsumerObservations(step uint64, actionID, baselineID, adapterID string, baseline, actual map[string]any) error {
	if len(baseline) != len(actual) {
		return &SimConsumerDivergenceError{Step: step, ActionID: actionID, BaselineAdapterID: baselineID, AdapterID: adapterID, Kind: "observation count"}
	}
	keys := make([]string, 0, len(baseline))
	for key := range baseline {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		actualValue, ok := actual[key]
		if !ok {
			return &SimConsumerDivergenceError{Step: step, ActionID: actionID, BaselineAdapterID: baselineID, AdapterID: adapterID, ObservationID: key, Kind: "missing observation"}
		}
		baselineBytes, err := ReplayCanonicalBytes(baseline[key])
		if err != nil {
			return simConsumerErrorf("encode baseline observation %q: %v", key, err)
		}
		actualBytes, err := ReplayCanonicalBytes(actualValue)
		if err != nil {
			return simConsumerErrorf("encode adapter %q observation %q: %v", adapterID, key, err)
		}
		if !bytes.Equal(baselineBytes, actualBytes) {
			return &SimConsumerDivergenceError{Step: step, ActionID: actionID, BaselineAdapterID: baselineID, AdapterID: adapterID, ObservationID: key, Kind: "value mismatch"}
		}
	}
	return nil
}
