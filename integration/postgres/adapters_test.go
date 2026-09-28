package postgres_test

import (
	"context"
	"fmt"
	"maps"
	"slices"

	lazily "github.com/lazily-hub/lazily-go"

	// The PostgreSQL driver registration for every test in this package.
	// Declared here rather than beside one test so the package's whole reason for
	// being a separate module is stated in one place (#lzgooptionalpgx).
	_ "github.com/jackc/pgx/v5/stdlib"
)

const simProjectionSeed = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

func projectionGeneratedAction(id string, kind lazily.SimProjectionMaintenanceKind, event lazily.MaterializedProjectionEvent, causeID string) lazily.SimGeneratedAction {
	return lazily.SimGeneratedAction{
		Command: string(kind),
		Action: lazily.SimAction{
			ID: id, ActorID: "projection.worker", Kind: "projection.maintenance", Version: "1",
			CauseID: causeID,
			Payload: lazily.SimProjectionMaintenanceAction{Kind: kind, Event: event},
		},
	}
}

func projectionScenario(actions ...lazily.SimGeneratedAction) lazily.SimGeneratedScenario {
	return lazily.SimGeneratedScenario{
		GeneratorName: "projection.history", GeneratorVersion: "1", SeedHex: simProjectionSeed,
		Actions: actions,
	}
}

type memoryProjectionState struct {
	history    []lazily.MaterializedProjectionEvent
	projection map[string]string
}

// newMemoryProjectionPort is the in-memory reference projection the testkit
// compares the real PostgreSQL adapter against.
//
// Its Apply defers to lazily.ExpectedSimProjectionMaintenanceOutcome rather than
// restating the accept/reject rules, so the reference cannot drift from the
// behaviour the production oracle defines.
func newMemoryProjectionPort() *lazily.SimProjectionMaintenanceAdapter {
	state := &memoryProjectionState{}
	return &lazily.SimProjectionMaintenanceAdapter{
		Reset: func(context.Context) error {
			state.history = nil
			state.projection = map[string]string{}
			return nil
		},
		Apply: func(_ context.Context, action lazily.SimProjectionMaintenanceAction) (lazily.SimProjectionMaintenanceOutcome, error) {
			outcome, err := lazily.ExpectedSimProjectionMaintenanceOutcome(action)
			if err != nil {
				return outcome, err
			}
			if outcome.Accepted {
				candidate := append(slices.Clone(state.history), action.Event)
				projection, err := lazily.ReduceMaterializedProjectionHistory(candidate)
				if err != nil {
					return outcome, err
				}
				state.history, state.projection = candidate, projection
			} else if action.Kind == lazily.SimProjectionMaintenanceFullRebuild {
				state.projection, err = lazily.ReduceMaterializedProjectionHistory(state.history)
			}
			return outcome, err
		},
		Observe: func(context.Context) (lazily.SimProjectionMaintenanceObservation, error) {
			return lazily.SimProjectionMaintenanceObservation{
				History: slices.Clone(state.history), Projection: maps.Clone(state.projection),
			}, nil
		},
	}
}

// newProjectionAdapter builds the consumer-adapter shell the projection corpus
// needs: identity, ports, and the callbacks the testkit requires of each kind.
//
// It is deliberately NOT the root package's counter-reducer test adapter. That
// one carries a reducer this suite immediately overwrites, and reaching it from
// here would mean exporting test scaffolding. The projection behaviour under test
// lives entirely in the ProjectionMaintenance port the caller assigns; this shell
// only has to satisfy the testkit's adapter contract.
//
// The two kinds have genuinely different obligations: an in-memory adapter must
// expose the SimWorld it steps, and a real one must name its service and be able
// to replay what it durably accepted. The testkit refuses either shape if the
// other's fields are missing, so both are built here rather than patched in by
// each caller.
func newProjectionAdapter(id string, kind lazily.SimConsumerAdapterKind) lazily.SimConsumerAdapter {
	applied := []lazily.SimAction{}
	var world *lazily.SimWorld
	inMemory := kind == lazily.SimConsumerAdapterInMemory

	record := func(action lazily.SimAction) error {
		if _, ok := action.Payload.(lazily.SimProjectionMaintenanceAction); !ok {
			return fmt.Errorf("payload is %T, want lazily.SimProjectionMaintenanceAction", action.Payload)
		}
		applied = append(applied, action)
		return nil
	}

	adapter := lazily.SimConsumerAdapter{
		ID:                  id,
		Kind:                kind,
		ProductionReducerID: "materialized.projection.reducer.v1",
		ProtocolID:          "materialized.projection.protocol.v1",
		ReducerID:           "materialized.projection.reducer.v1",
		Ports: []lazily.SimConsumerPort{
			{ID: "state.store", Kind: "storage", Determinism: lazily.SimConsumerPortDeterministic},
			{ID: "event.stream", Kind: "messaging", Determinism: lazily.SimConsumerPortNondeterministic},
			{ID: "logical.clock", Kind: "clock", Determinism: lazily.SimConsumerPortNondeterministic, Stubbed: inMemory},
		},
		Reset: func() error {
			applied = applied[:0]
			if !inMemory {
				return nil
			}
			seed, err := lazily.ParseSimSeed(simProjectionSeed)
			if err != nil {
				return err
			}
			world = lazily.NewSimWorld(seed)
			return world.RegisterActor("projection.consumer", lazily.SimActorFuncs{
				DecideFunc: func(action lazily.SimAction) (lazily.SimDecision, error) {
					return lazily.SimDecision{Accepted: true}, record(action)
				},
				ObserveFunc: func() map[string]any { return map[string]any{"applied": len(applied)} },
			})
		},
		Apply: func(action lazily.SimAction) error {
			if !inMemory {
				return record(action)
			}
			if _, err := world.Schedule(world.Now(), action); err != nil {
				return err
			}
			_, err := world.Step()
			return err
		},
		Observe: func() (map[string]any, error) {
			if inMemory {
				return world.Observe()
			}
			return map[string]any{"applied": len(applied)}, nil
		},
	}

	if inMemory {
		adapter.SimulationWorld = func() *lazily.SimWorld { return world }
	} else {
		adapter.ServiceID = id + ".service"
		adapter.MaterializedHistory = func() ([]lazily.SimAction, error) {
			return slices.Clone(applied), nil
		}
	}
	return adapter
}
