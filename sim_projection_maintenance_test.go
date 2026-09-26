package lazily

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
)

const simProjectionSeed = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

func projectionGeneratedAction(id string, kind SimProjectionMaintenanceKind, event MaterializedProjectionEvent, causeID string) SimGeneratedAction {
	return SimGeneratedAction{
		Command: string(kind),
		Action: SimAction{
			ID: id, ActorID: "projection.worker", Kind: "projection.maintenance", Version: "1",
			CauseID: causeID,
			Payload: SimProjectionMaintenanceAction{Kind: kind, Event: event},
		},
	}
}

func projectionScenario(actions ...SimGeneratedAction) SimGeneratedScenario {
	return SimGeneratedScenario{
		GeneratorName: "projection.history", GeneratorVersion: "1", SeedHex: simProjectionSeed,
		Actions: actions,
	}
}

type memoryProjectionState struct {
	history    []MaterializedProjectionEvent
	projection map[string]string
}

func newMemoryProjectionPort() (*SimProjectionMaintenanceAdapter, *memoryProjectionState) {
	state := &memoryProjectionState{}
	return &SimProjectionMaintenanceAdapter{
		Reset: func(context.Context) error {
			state.history = nil
			state.projection = map[string]string{}
			return nil
		},
		Apply: func(_ context.Context, action SimProjectionMaintenanceAction) (SimProjectionMaintenanceOutcome, error) {
			outcome, err := expectedSimProjectionOutcome(action)
			if err != nil {
				return outcome, err
			}
			if outcome.Accepted {
				candidate := append(slices.Clone(state.history), action.Event)
				projection, err := ReduceMaterializedProjectionHistory(candidate)
				if err != nil {
					return outcome, err
				}
				state.history, state.projection = candidate, projection
			} else if action.Kind == SimProjectionMaintenanceFullRebuild {
				state.projection, err = ReduceMaterializedProjectionHistory(state.history)
			}
			return outcome, err
		},
		Observe: func(context.Context) (SimProjectionMaintenanceObservation, error) {
			return SimProjectionMaintenanceObservation{
				History: slices.Clone(state.history), Projection: maps.Clone(state.projection),
			}, nil
		},
	}, state
}

func TestReduceMaterializedProjectionHistory(t *testing.T) {
	history := []MaterializedProjectionEvent{
		{ID: "event.append.alpha", Kind: MaterializedProjectionAppend, Key: "item.alpha", Value: "one"},
		{ID: "event.amend.alpha", Kind: MaterializedProjectionAmend, Key: "item.alpha", Value: "two"},
		{ID: "event.append.beta", Kind: MaterializedProjectionAppend, Key: "item.beta", Value: "three"},
		{ID: "event.retract.beta", Kind: MaterializedProjectionRetract, Key: "item.beta"},
	}
	got, err := ReduceMaterializedProjectionHistory(history)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"item.alpha": "two"}
	if !maps.Equal(got, want) {
		t.Fatalf("projection = %#v, want %#v", got, want)
	}

	for _, invalid := range [][]MaterializedProjectionEvent{
		{{ID: "event.amend.missing", Kind: MaterializedProjectionAmend, Key: "item.missing"}},
		{{ID: "event.retract.missing", Kind: MaterializedProjectionRetract, Key: "item.missing"}},
		{{ID: "event.first", Kind: MaterializedProjectionAppend, Key: "item.alpha"}, {ID: "event.second", Kind: MaterializedProjectionAppend, Key: "item.alpha"}},
		{{ID: "event.duplicate", Kind: MaterializedProjectionAppend, Key: "item.alpha"}, {ID: "event.duplicate", Kind: MaterializedProjectionAppend, Key: "item.beta"}},
	} {
		if _, err := ReduceMaterializedProjectionHistory(invalid); err == nil {
			t.Fatalf("ReduceMaterializedProjectionHistory(%#v) succeeded", invalid)
		}
	}
}

func TestSimProjectionMaintenanceRunProvesAcceptedHistoryAndReplay(t *testing.T) {
	memory, _ := newSimConsumerTestAdapter("memory", SimConsumerAdapterInMemory, 0)
	postgres, _ := newSimConsumerTestAdapter("postgres.selected", SimConsumerAdapterPostgres, 0)
	memory.ProjectionMaintenance, _ = newMemoryProjectionPort()
	postgres.ProjectionMaintenance, _ = newMemoryProjectionPort()
	kit, err := NewSimConsumerTestkit(SimConsumerTestkitSpec{
		SimulationAdapterID: "memory", RequiredRealAdapters: []SimConsumerAdapterKind{SimConsumerAdapterPostgres},
		Adapters: []SimConsumerAdapter{postgres, memory},
	})
	if err != nil {
		t.Fatal(err)
	}
	scenario := projectionScenario(
		projectionGeneratedAction("action.append", SimProjectionMaintenanceEvent, MaterializedProjectionEvent{ID: "event.append", Kind: MaterializedProjectionAppend, Key: "item.alpha", Value: "one"}, ""),
		projectionGeneratedAction("action.rebuild", SimProjectionMaintenanceFullRebuild, MaterializedProjectionEvent{}, ""),
		projectionGeneratedAction("action.timeout", SimProjectionMaintenanceLockTimeout, MaterializedProjectionEvent{ID: "event.timeout", Kind: MaterializedProjectionAmend, Key: "item.alpha", Value: "lost"}, "action.append"),
		projectionGeneratedAction("action.retry", SimProjectionMaintenanceRetry, MaterializedProjectionEvent{ID: "event.retry", Kind: MaterializedProjectionAmend, Key: "item.alpha", Value: "two"}, "action.append"),
	)
	result, err := kit.RunProjectionMaintenance(t.Context(), scenario)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.FinalHistory) != 2 || result.FinalProjection["item.alpha"] != "two" {
		t.Fatalf("result = %+v", result)
	}
}

func TestShrinkSimProjectionMaintenanceFailureKeepsCausalPrefix(t *testing.T) {
	scenario := projectionScenario(
		projectionGeneratedAction("action.append.alpha", SimProjectionMaintenanceEvent, MaterializedProjectionEvent{ID: "event.append.alpha", Kind: MaterializedProjectionAppend, Key: "item.alpha", Value: "one"}, ""),
		projectionGeneratedAction("action.append.beta", SimProjectionMaintenanceEvent, MaterializedProjectionEvent{ID: "event.append.beta", Kind: MaterializedProjectionAppend, Key: "item.beta", Value: "other"}, ""),
		projectionGeneratedAction("action.amend.alpha", SimProjectionMaintenanceEvent, MaterializedProjectionEvent{ID: "event.amend.alpha", Kind: MaterializedProjectionAmend, Key: "item.alpha", Value: "two"}, "action.append.alpha"),
		projectionGeneratedAction("action.rebuild", SimProjectionMaintenanceFullRebuild, MaterializedProjectionEvent{}, ""),
	)
	shrunk, err := ShrinkSimProjectionMaintenanceFailure(scenario, func(candidate SimGeneratedScenario) (bool, error) {
		for _, generated := range candidate.Actions {
			if generated.Action.ID == "action.amend.alpha" {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(shrunk.Actions) != 2 || shrunk.Actions[0].Action.ID != "action.append.alpha" || shrunk.Actions[1].Action.CauseID != "action.append.alpha" {
		t.Fatalf("shrunk actions = %+v", shrunk.Actions)
	}
}

func FuzzMaterializedProjectionReplay(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 64 {
			input = input[:64]
		}
		model := map[string]string{}
		history := make([]MaterializedProjectionEvent, 0, len(input))
		keys := []string{"item.alpha", "item.beta", "item.gamma"}
		for index, value := range input {
			key := keys[int(value)%len(keys)]
			event := MaterializedProjectionEvent{ID: fmt.Sprintf("event.%d", index), Key: key, Value: fmt.Sprintf("value.%d", value)}
			if _, present := model[key]; !present {
				event.Kind = MaterializedProjectionAppend
				model[key] = event.Value
			} else if value&1 == 0 {
				event.Kind = MaterializedProjectionAmend
				model[key] = event.Value
			} else {
				event.Kind = MaterializedProjectionRetract
				delete(model, key)
			}
			history = append(history, event)
		}
		first, err := ReduceMaterializedProjectionHistory(history)
		if err != nil {
			t.Fatal(err)
		}
		second, err := ReduceMaterializedProjectionHistory(slices.Clone(history))
		if err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(first, model) || !maps.Equal(first, second) {
			t.Fatalf("history replay mismatch: first=%v second=%v model=%v", first, second, model)
		}
	})
}
