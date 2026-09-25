package lazily_test

import (
	"fmt"

	lazily "github.com/lazily-hub/lazily-go"
)

type exampleConsumerState struct{ value int }

func exampleConsumerAdapter(id string, kind lazily.SimConsumerAdapterKind) lazily.SimConsumerAdapter {
	state := &exampleConsumerState{}
	var world *lazily.SimWorld
	applyReducer := func(action lazily.SimAction) error {
		state.value += action.Payload.(int)
		return nil
	}
	adapter := lazily.SimConsumerAdapter{
		ID:                  id,
		Kind:                kind,
		ProductionReducerID: "example.reducer.v1",
		Ports: []lazily.SimConsumerPort{
			{ID: "state.store", Kind: "storage"},
			{ID: "logical.clock", Kind: "clock", NondeterministicBoundary: true, Stubbed: kind == lazily.SimConsumerAdapterInMemory},
		},
		Reset: func() error {
			state.value = 0
			if kind == lazily.SimConsumerAdapterInMemory {
				seed, _ := lazily.ParseSimSeed("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
				world = lazily.NewSimWorld(seed)
				return world.RegisterActor("consumer", lazily.SimActorFuncs{
					DecideFunc: func(action lazily.SimAction) (lazily.SimDecision, error) {
						return lazily.SimDecision{Accepted: true}, applyReducer(action)
					},
					ObserveFunc: func() map[string]any { return map[string]any{"value": state.value} },
				})
			}
			return nil
		},
		Apply: func(action lazily.SimAction) error {
			if kind != lazily.SimConsumerAdapterInMemory {
				return applyReducer(action)
			}
			if _, err := world.Schedule(world.Now(), action); err != nil {
				return err
			}
			_, err := world.Step()
			return err
		},
		Observe: func() (map[string]any, error) {
			if kind == lazily.SimConsumerAdapterInMemory {
				return world.Observe()
			}
			return map[string]any{"consumer.value": state.value}, nil
		},
	}
	if kind == lazily.SimConsumerAdapterInMemory {
		adapter.SimulationWorld = func() *lazily.SimWorld { return world }
	}
	if kind == lazily.SimConsumerAdapterPostgres {
		// A consumer repository supplies callbacks backed by its actual Postgres
		// test service here. This compact example keeps only the testkit wiring.
		adapter.ServiceID = "postgres.test.service"
		adapter.Probe = func() error { return nil }
	}
	return adapter
}

func ExampleSimConsumerTestkit_Run() {
	seed, _ := lazily.ParseSimSeed("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	scenario := lazily.SimGeneratedScenario{
		GeneratorName: "example.consumer", GeneratorVersion: "1", SeedHex: seed.String(),
		Actions: []lazily.SimGeneratedAction{
			{Command: "increment", Action: lazily.SimAction{ID: "increment.0", ActorID: "consumer", Kind: "counter.increment", Version: "1", Payload: 2}},
			{Command: "increment", Action: lazily.SimAction{ID: "increment.1", ActorID: "consumer", Kind: "counter.increment", Version: "1", Payload: 3}},
		},
	}
	kit, _ := lazily.NewSimConsumerTestkit(lazily.SimConsumerTestkitSpec{
		SimulationAdapterID:  "memory",
		RequiredRealAdapters: []lazily.SimConsumerAdapterKind{lazily.SimConsumerAdapterPostgres},
		Adapters: []lazily.SimConsumerAdapter{
			exampleConsumerAdapter("memory", lazily.SimConsumerAdapterInMemory),
			exampleConsumerAdapter("postgres.integration", lazily.SimConsumerAdapterPostgres),
		},
	})
	result, err := kit.Run(scenario)
	fmt.Println(err == nil, len(result.Checkpoints), result.AdapterIDs)
	// Output:
	// true 2 [memory postgres.integration]
}
