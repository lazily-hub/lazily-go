package lazily

import (
	"context"
	"maps"
	"slices"
)

// MaterializedProjectionEventKind is one complete-history projection mutation.
type MaterializedProjectionEventKind string

const (
	MaterializedProjectionAppend  MaterializedProjectionEventKind = "append"
	MaterializedProjectionAmend   MaterializedProjectionEventKind = "amend"
	MaterializedProjectionRetract MaterializedProjectionEventKind = "retract"
)

// MaterializedProjectionEvent uses synthetic string identities and values so
// the reducer can be shared by in-memory and database-backed conformance hosts.
type MaterializedProjectionEvent struct {
	ID    string
	Kind  MaterializedProjectionEventKind
	Key   string
	Value string
}

// ReduceMaterializedProjectionHistory is the production reducer used for both
// incremental writes and complete rebuilds. It rejects invalid histories rather
// than silently inventing append/amend/retract semantics.
func ReduceMaterializedProjectionHistory(history []MaterializedProjectionEvent) (map[string]string, error) {
	projection := map[string]string{}
	seen := map[string]struct{}{}
	for index, event := range history {
		if !validSimID(event.ID) || !validSimID(event.Key) {
			return nil, simConsumerErrorf("projection event %d needs stable event and key identities", index)
		}
		if _, duplicate := seen[event.ID]; duplicate {
			return nil, simConsumerErrorf("projection event id %q is duplicated", event.ID)
		}
		seen[event.ID] = struct{}{}
		_, present := projection[event.Key]
		switch event.Kind {
		case MaterializedProjectionAppend:
			if present {
				return nil, simConsumerErrorf("append event %q targets existing key %q", event.ID, event.Key)
			}
			projection[event.Key] = event.Value
		case MaterializedProjectionAmend:
			if !present {
				return nil, simConsumerErrorf("amend event %q targets missing key %q", event.ID, event.Key)
			}
			projection[event.Key] = event.Value
		case MaterializedProjectionRetract:
			if !present {
				return nil, simConsumerErrorf("retract event %q targets missing key %q", event.ID, event.Key)
			}
			delete(projection, event.Key)
		default:
			return nil, simConsumerErrorf("projection event %q has unsupported kind %q", event.ID, event.Kind)
		}
	}
	return projection, nil
}

type SimProjectionMaintenanceKind string

const (
	SimProjectionMaintenanceEvent             SimProjectionMaintenanceKind = "event"
	SimProjectionMaintenanceFullRebuild       SimProjectionMaintenanceKind = "full_rebuild"
	SimProjectionMaintenanceConcurrentRebuild SimProjectionMaintenanceKind = "concurrent_rebuild"
	SimProjectionMaintenanceLockTimeout       SimProjectionMaintenanceKind = "lock_timeout"
	SimProjectionMaintenanceRetry             SimProjectionMaintenanceKind = "serialization_retry"
	SimProjectionMaintenanceCancellation      SimProjectionMaintenanceKind = "cancellation"
	SimProjectionMaintenanceCrashBeforeCommit SimProjectionMaintenanceKind = "crash_before_commit"
	SimProjectionMaintenanceCrashAfterCommit  SimProjectionMaintenanceKind = "crash_after_commit"
)

// SimProjectionMaintenanceAction is carried as SimAction.Payload. Every kind
// except FullRebuild contains an attempted materialized event.
type SimProjectionMaintenanceAction struct {
	Kind  SimProjectionMaintenanceKind
	Event MaterializedProjectionEvent
}

type SimProjectionMaintenanceOutcome struct {
	Kind     SimProjectionMaintenanceKind
	Accepted bool
}

type SimProjectionMaintenanceObservation struct {
	History    []MaterializedProjectionEvent
	Projection map[string]string
}

// SimProjectionMaintenanceAdapter extends one SimConsumerAdapter with the
// history/rebuild operations needed by projection-maintenance conformance.
type SimProjectionMaintenanceAdapter struct {
	Reset   func(context.Context) error
	Apply   func(context.Context, SimProjectionMaintenanceAction) (SimProjectionMaintenanceOutcome, error)
	Observe func(context.Context) (SimProjectionMaintenanceObservation, error)
}

type SimProjectionMaintenanceCheckpoint struct {
	Step            uint64
	ActionID        string
	Outcome         SimProjectionMaintenanceOutcome
	AcceptedHistory int
}

type SimProjectionMaintenanceRunResult struct {
	AdapterIDs      []string
	Checkpoints     []SimProjectionMaintenanceCheckpoint
	FinalHistory    []MaterializedProjectionEvent
	FinalProjection map[string]string
}

// RunProjectionMaintenance replays one materialized history through the
// in-memory reference adapter and an explicitly selected real PostgreSQL
// adapter. After every step it proves exact accepted-history equality and full
// replay equivalence, so a source update cannot disappear behind a rebuild.
func (kit *SimConsumerTestkit) RunProjectionMaintenance(
	ctx context.Context,
	scenario SimGeneratedScenario,
) (SimProjectionMaintenanceRunResult, error) {
	if kit == nil {
		return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("nil consumer simulation testkit")
	}
	if err := validateSimConsumerScenario(scenario); err != nil {
		return SimProjectionMaintenanceRunResult{}, err
	}
	type selectedAdapter struct {
		consumer SimConsumerAdapter
		port     *SimProjectionMaintenanceAdapter
	}
	selected := make([]selectedAdapter, 0, len(kit.spec.Adapters))
	baselineIndex := -1
	postgresSelected := false
	for _, adapter := range kit.spec.Adapters {
		if adapter.ProjectionMaintenance == nil {
			continue
		}
		port := adapter.ProjectionMaintenance
		if port.Reset == nil || port.Apply == nil || port.Observe == nil {
			return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("projection adapter %q needs reset, apply, and observe", adapter.ID)
		}
		if adapter.ID == kit.spec.SimulationAdapterID {
			baselineIndex = len(selected)
		}
		if adapter.Kind == SimConsumerAdapterPostgres {
			postgresSelected = true
		}
		selected = append(selected, selectedAdapter{consumer: adapter, port: port})
	}
	if baselineIndex < 0 || !postgresSelected {
		return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("projection history requires its in-memory reference and an explicitly selected Postgres adapter")
	}
	for _, adapter := range selected {
		if adapter.consumer.Kind.real() {
			if err := adapter.consumer.Probe(); err != nil {
				return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("probe projection adapter %q: %v", adapter.consumer.ID, err)
			}
		}
		if err := adapter.port.Reset(ctx); err != nil {
			return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("reset projection adapter %q: %v", adapter.consumer.ID, err)
		}
	}

	result := SimProjectionMaintenanceRunResult{AdapterIDs: make([]string, len(selected))}
	for index := range selected {
		result.AdapterIDs[index] = selected[index].consumer.ID
	}
	expectedHistory := []MaterializedProjectionEvent(nil)
	for actionIndex, generated := range scenario.Actions {
		action, ok := generated.Action.Payload.(SimProjectionMaintenanceAction)
		if !ok {
			return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("step %d action %q has payload %T, want SimProjectionMaintenanceAction", actionIndex+1, generated.Action.ID, generated.Action.Payload)
		}
		if generated.Command != string(action.Kind) {
			return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("step %d action %q command %q does not match projection-maintenance kind %q", actionIndex+1, generated.Action.ID, generated.Command, action.Kind)
		}
		expectedOutcome, err := expectedSimProjectionOutcome(action)
		if err != nil {
			return SimProjectionMaintenanceRunResult{}, err
		}
		if action.Kind != SimProjectionMaintenanceFullRebuild {
			candidate := append(slices.Clone(expectedHistory), action.Event)
			if _, err := ReduceMaterializedProjectionHistory(candidate); err != nil {
				return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("step %d action %q is not valid against its accepted-history prefix: %v", actionIndex+1, generated.Action.ID, err)
			}
		}
		outcomes := make([]SimProjectionMaintenanceOutcome, len(selected))
		for adapterIndex, adapter := range selected {
			outcome, err := adapter.port.Apply(ctx, action)
			if err != nil {
				return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("step %d action %q apply projection adapter %q: %v", actionIndex+1, generated.Action.ID, adapter.consumer.ID, err)
			}
			if outcome != expectedOutcome {
				return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("step %d action %q adapter %q outcome = %+v, want %+v", actionIndex+1, generated.Action.ID, adapter.consumer.ID, outcome, expectedOutcome)
			}
			outcomes[adapterIndex] = outcome
		}
		if outcomes[baselineIndex].Accepted {
			expectedHistory = append(expectedHistory, action.Event)
		}
		expectedProjection, err := ReduceMaterializedProjectionHistory(expectedHistory)
		if err != nil {
			return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("step %d action %q expected history: %v", actionIndex+1, generated.Action.ID, err)
		}
		for _, adapter := range selected {
			observation, err := adapter.port.Observe(ctx)
			if err != nil {
				return SimProjectionMaintenanceRunResult{}, simConsumerErrorf("step %d action %q observe projection adapter %q: %v", actionIndex+1, generated.Action.ID, adapter.consumer.ID, err)
			}
			if !slices.Equal(observation.History, expectedHistory) {
				return SimProjectionMaintenanceRunResult{}, &SimConsumerDivergenceError{Step: uint64(actionIndex + 1), ActionID: generated.Action.ID, BaselineAdapterID: selected[baselineIndex].consumer.ID, AdapterID: adapter.consumer.ID, Kind: "accepted projection history"}
			}
			if !maps.Equal(observation.Projection, expectedProjection) {
				return SimProjectionMaintenanceRunResult{}, &SimConsumerDivergenceError{Step: uint64(actionIndex + 1), ActionID: generated.Action.ID, BaselineAdapterID: selected[baselineIndex].consumer.ID, AdapterID: adapter.consumer.ID, Kind: "complete-history projection"}
			}
		}
		result.Checkpoints = append(result.Checkpoints, SimProjectionMaintenanceCheckpoint{
			Step: uint64(actionIndex + 1), ActionID: generated.Action.ID,
			Outcome: expectedOutcome, AcceptedHistory: len(expectedHistory),
		})
	}
	result.FinalHistory = append([]MaterializedProjectionEvent(nil), expectedHistory...)
	finalProjection, err := ReduceMaterializedProjectionHistory(expectedHistory)
	if err != nil {
		return SimProjectionMaintenanceRunResult{}, err
	}
	result.FinalProjection = finalProjection
	return result, nil
}

func expectedSimProjectionOutcome(action SimProjectionMaintenanceAction) (SimProjectionMaintenanceOutcome, error) {
	outcome := SimProjectionMaintenanceOutcome{Kind: action.Kind}
	switch action.Kind {
	case SimProjectionMaintenanceFullRebuild:
		if action.Event != (MaterializedProjectionEvent{}) {
			return outcome, simConsumerErrorf("full rebuild cannot carry an event")
		}
	case SimProjectionMaintenanceEvent, SimProjectionMaintenanceConcurrentRebuild,
		SimProjectionMaintenanceRetry, SimProjectionMaintenanceCrashAfterCommit:
		outcome.Accepted = true
	case SimProjectionMaintenanceLockTimeout, SimProjectionMaintenanceCancellation,
		SimProjectionMaintenanceCrashBeforeCommit:
	default:
		return outcome, simConsumerErrorf("unsupported projection-maintenance action %q", action.Kind)
	}
	if action.Kind != SimProjectionMaintenanceFullRebuild {
		if !validSimID(action.Event.ID) || !validSimID(action.Event.Key) {
			return outcome, simConsumerErrorf("projection-maintenance action %q needs an event", action.Kind)
		}
		if !action.Event.Kind.valid() {
			return outcome, simConsumerErrorf("projection-maintenance action %q has unsupported event kind %q", action.Kind, action.Event.Kind)
		}
	}
	return outcome, nil
}

func (kind MaterializedProjectionEventKind) valid() bool {
	switch kind {
	case MaterializedProjectionAppend, MaterializedProjectionAmend, MaterializedProjectionRetract:
		return true
	default:
		return false
	}
}

type simProjectionShrinkModel struct {
	projection  map[string]string
	acceptedIDs map[string]struct{}
}

// ShrinkSimProjectionMaintenanceFailure preserves CauseID dependencies and
// command validity while minimizing a reproducible projection-history failure.
func ShrinkSimProjectionMaintenanceFailure(
	scenario SimGeneratedScenario,
	fails func(SimGeneratedScenario) (bool, error),
) (SimGeneratedScenario, error) {
	if fails == nil {
		return SimGeneratedScenario{}, simGenerationErrorf("projection shrinker needs a failure predicate")
	}
	commands := map[string]SimShrinkCommand[simProjectionShrinkModel]{}
	for _, kind := range []SimProjectionMaintenanceKind{
		SimProjectionMaintenanceEvent, SimProjectionMaintenanceFullRebuild,
		SimProjectionMaintenanceConcurrentRebuild, SimProjectionMaintenanceLockTimeout,
		SimProjectionMaintenanceRetry, SimProjectionMaintenanceCancellation,
		SimProjectionMaintenanceCrashBeforeCommit, SimProjectionMaintenanceCrashAfterCommit,
	} {
		commands[string(kind)] = SimShrinkCommand[simProjectionShrinkModel]{
			Precondition: simProjectionShrinkPrecondition,
			Apply:        simProjectionShrinkApply,
		}
	}
	shrinker := SimCausalShrinker[simProjectionShrinkModel]{
		InitialModel: simProjectionShrinkModel{
			projection:  map[string]string{},
			acceptedIDs: map[string]struct{}{},
		},
		CloneModel: func(model simProjectionShrinkModel) simProjectionShrinkModel {
			return simProjectionShrinkModel{
				projection:  maps.Clone(model.projection),
				acceptedIDs: maps.Clone(model.acceptedIDs),
			}
		},
		Commands: commands,
	}
	actions, err := shrinker.Shrink(scenario.Actions, func(actions []SimGeneratedAction) (bool, error) {
		candidate := scenario
		candidate.Actions = actions
		return fails(candidate)
	})
	if err != nil {
		return SimGeneratedScenario{}, err
	}
	result := scenario
	result.Actions = actions
	return result, nil
}

func simProjectionShrinkPrecondition(model simProjectionShrinkModel, _ []SimGeneratedAction, action SimAction) bool {
	step, ok := action.Payload.(SimProjectionMaintenanceAction)
	if !ok {
		return false
	}
	outcome, err := expectedSimProjectionOutcome(step)
	if err != nil || step.Kind == SimProjectionMaintenanceFullRebuild {
		return err == nil
	}
	if outcome.Accepted {
		if _, duplicate := model.acceptedIDs[step.Event.ID]; duplicate {
			return false
		}
	}
	_, present := model.projection[step.Event.Key]
	switch step.Event.Kind {
	case MaterializedProjectionAppend:
		return !present
	case MaterializedProjectionAmend, MaterializedProjectionRetract:
		return present
	default:
		return false
	}
}

func simProjectionShrinkApply(model simProjectionShrinkModel, action SimAction) (simProjectionShrinkModel, error) {
	next := simProjectionShrinkModel{
		projection:  maps.Clone(model.projection),
		acceptedIDs: maps.Clone(model.acceptedIDs),
	}
	step := action.Payload.(SimProjectionMaintenanceAction)
	outcome, err := expectedSimProjectionOutcome(step)
	if err != nil || !outcome.Accepted {
		return next, err
	}
	switch step.Event.Kind {
	case MaterializedProjectionAppend, MaterializedProjectionAmend:
		next.projection[step.Event.Key] = step.Event.Value
	case MaterializedProjectionRetract:
		delete(next.projection, step.Event.Key)
	}
	next.acceptedIDs[step.Event.ID] = struct{}{}
	return next, nil
}
