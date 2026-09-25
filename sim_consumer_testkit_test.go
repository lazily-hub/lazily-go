package lazily

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type simConsumerTestState struct {
	value        int
	probes       int
	resets       int
	applications int
	history      []SimAction
}

func simConsumerTestPorts(stubClock bool) []SimConsumerPort {
	return []SimConsumerPort{
		{ID: "state.store", Kind: "storage", Determinism: SimConsumerPortDeterministic},
		{ID: "event.stream", Kind: "messaging", Determinism: SimConsumerPortNondeterministic},
		{ID: "logical.clock", Kind: "clock", Determinism: SimConsumerPortNondeterministic, Stubbed: stubClock},
	}
}

func newSimConsumerTestAdapter(id string, kind SimConsumerAdapterKind, mutation int) (SimConsumerAdapter, *simConsumerTestState) {
	state := &simConsumerTestState{}
	var world *SimWorld
	applyReducer := func(action SimAction) error {
		delta, ok := action.Payload.(int)
		if !ok {
			return fmt.Errorf("payload is %T, want int", action.Payload)
		}
		state.value += delta + mutation
		state.applications++
		return nil
	}
	adapter := SimConsumerAdapter{
		ID:                  id,
		Kind:                kind,
		ProductionReducerID: "counter.reducer.v1",
		ProtocolID:          "counter.protocol.v1",
		ReducerID:           "counter.reducer.v1",
		Ports:               simConsumerTestPorts(kind == SimConsumerAdapterInMemory),
		Reset: func() error {
			state.value = 0
			state.history = nil
			state.resets++
			if kind == SimConsumerAdapterInMemory {
				world = NewSimWorld(mustParseSimConsumerSeed())
				return world.RegisterActor("consumer", SimActorFuncs{
					DecideFunc: func(action SimAction) (SimDecision, error) {
						return SimDecision{Accepted: true}, applyReducer(action)
					},
					ObserveFunc: func() map[string]any { return map[string]any{"value": state.value} },
				})
			}
			return nil
		},
		Apply: func(action SimAction) error {
			if kind != SimConsumerAdapterInMemory {
				if err := applyReducer(action); err != nil {
					return err
				}
				state.history = append(state.history, cloneSimAction(action))
				return nil
			}
			if _, err := world.Schedule(world.Now(), action); err != nil {
				return err
			}
			_, err := world.Step()
			return err
		},
		Observe: func() (map[string]any, error) {
			if kind == SimConsumerAdapterInMemory {
				return world.Observe()
			}
			return map[string]any{"consumer.value": state.value}, nil
		},
	}
	if kind == SimConsumerAdapterInMemory {
		adapter.SimulationWorld = func() *SimWorld { return world }
	}
	if kind.real() {
		adapter.ServiceID = id + ".service"
		adapter.Probe = func() error {
			state.probes++
			return nil
		}
		adapter.MaterializedHistory = func() ([]SimAction, error) {
			history := make([]SimAction, len(state.history))
			for i := range state.history {
				history[i] = cloneSimAction(state.history[i])
			}
			return history, nil
		}
	}
	return adapter, state
}

func newSimConsumerExternalProcessAdapter(id string, port SimConsumerExternalPortKind, mutation int) (SimConsumerAdapter, *simConsumerTestState) {
	adapter, state := newSimConsumerTestAdapter(id, SimConsumerAdapterExternalProcess, mutation)
	adapter.ProductionReducerID = ""
	adapter.ReducerID = id + ".reducer.v1"
	adapter.ExternalPort = port
	return adapter, state
}

func mustParseSimConsumerSeed() SimSeed {
	seed, err := ParseSimSeed("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	if err != nil {
		panic(err)
	}
	return seed
}

func simConsumerGeneratedScenario(t *testing.T) SimGeneratedScenario {
	t.Helper()
	generator, err := NewSimGenerator(NewSimWorld(mustSimTestSeed(t)), SimGeneratorSpec[int]{
		Name: "consumer.scenario", Version: "1", MaxActions: 3,
		Commands: []SimModelCommand[int]{{
			Name:   "increment",
			Weight: 1,
			Build: func(_ *SimRandomStream, _ int, ordinal uint64) (SimAction, error) {
				return SimAction{
					ID:      fmt.Sprintf("increment.%d", ordinal),
					ActorID: "consumer",
					Kind:    "counter.increment",
					Version: "1",
					Payload: int(ordinal + 1),
				}, nil
			},
			Apply: func(model int, action SimAction) (int, error) {
				return model + action.Payload.(int), nil
			},
		}},
	})
	if err != nil {
		t.Fatalf("NewSimGenerator: %v", err)
	}
	scenario, err := generator.Generate(7)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return scenario
}

func TestSimConsumerTestkitRunsGeneratedHistoryAcrossMemoryPostgresAndNATS(t *testing.T) {
	memory, memoryState := newSimConsumerTestAdapter("memory", SimConsumerAdapterInMemory, 0)
	postgres, postgresState := newSimConsumerTestAdapter("postgres.integration", SimConsumerAdapterPostgres, 0)
	nats, natsState := newSimConsumerTestAdapter("nats.integration", SimConsumerAdapterNATS, 0)
	kit, err := NewSimConsumerTestkit(SimConsumerTestkitSpec{
		SimulationAdapterID:  "memory",
		RequiredRealAdapters: []SimConsumerAdapterKind{SimConsumerAdapterPostgres, SimConsumerAdapterNATS},
		Adapters:             []SimConsumerAdapter{nats, memory, postgres},
	})
	if err != nil {
		t.Fatalf("NewSimConsumerTestkit: %v", err)
	}
	scenario := simConsumerGeneratedScenario(t)
	result, err := kit.Run(scenario)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.ScenarioDigest == "" || len(result.Checkpoints) != len(scenario.Actions) {
		t.Fatalf("result = %+v, want digest and %d checkpoints", result, len(scenario.Actions))
	}
	for _, checkpoint := range result.Checkpoints {
		baseline := checkpoint.ObservationDigests["memory"]
		if baseline == "" || checkpoint.ObservationDigests["postgres.integration"] != baseline || checkpoint.ObservationDigests["nats.integration"] != baseline {
			t.Fatalf("checkpoint digests differ: %+v", checkpoint)
		}
	}
	for id, state := range map[string]*simConsumerTestState{
		"memory": memoryState, "postgres": postgresState, "nats": natsState,
	} {
		if state.resets != 1 || state.applications != len(scenario.Actions) {
			t.Fatalf("%s state = %+v", id, state)
		}
	}
	if postgresState.probes != 1 || natsState.probes != 1 || memoryState.probes != 0 {
		t.Fatalf("probe counts memory=%d postgres=%d nats=%d", memoryState.probes, postgresState.probes, natsState.probes)
	}
}

func TestSimConsumerTestkitRunsExactHistoryAcrossExternalProcessPorts(t *testing.T) {
	ports := []SimConsumerExternalPortKind{
		SimConsumerExternalPortCLI,
		SimConsumerExternalPortFilesystem,
		SimConsumerExternalPortLocalSocket,
		SimConsumerExternalPortEditorReplica,
	}
	for _, port := range ports {
		t.Run(string(port), func(t *testing.T) {
			memory, _ := newSimConsumerTestAdapter("memory", SimConsumerAdapterInMemory, 0)
			external, externalState := newSimConsumerExternalProcessAdapter("sample."+string(port), port, 0)
			kit, err := NewSimConsumerTestkit(SimConsumerTestkitSpec{
				SimulationAdapterID: "memory",
				RequiredExternalProcesses: []SimConsumerExternalProcessSelection{{
					AdapterID: external.ID,
					Port:      port,
				}},
				Adapters: []SimConsumerAdapter{external, memory},
			})
			if err != nil {
				t.Fatalf("NewSimConsumerTestkit: %v", err)
			}
			scenario := simConsumerGeneratedScenario(t)
			result, err := kit.Run(scenario)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if len(externalState.history) != len(scenario.Actions) {
				t.Fatalf("external history length = %d, want %d", len(externalState.history), len(scenario.Actions))
			}
			if len(result.AdapterEvidence) != 2 {
				t.Fatalf("adapter evidence = %+v", result.AdapterEvidence)
			}
			var found bool
			for _, evidence := range result.AdapterEvidence {
				if evidence.AdapterID != external.ID {
					continue
				}
				found = evidence.ExternalPort == port && evidence.ProtocolID == "counter.protocol.v1" && evidence.ReducerID == external.ReducerID && evidence.ProductionReducerID == ""
			}
			if !found {
				t.Fatalf("external identity evidence = %+v", result.AdapterEvidence)
			}
		})
	}
}

func TestSimConsumerTestkitRejectsExternalMaterializedHistoryDrift(t *testing.T) {
	memory, _ := newSimConsumerTestAdapter("memory", SimConsumerAdapterInMemory, 0)
	external, _ := newSimConsumerExternalProcessAdapter("sample.cli", SimConsumerExternalPortCLI, 0)
	external.MaterializedHistory = func() ([]SimAction, error) { return nil, nil }
	kit, err := NewSimConsumerTestkit(SimConsumerTestkitSpec{
		SimulationAdapterID: "memory",
		RequiredExternalProcesses: []SimConsumerExternalProcessSelection{{
			AdapterID: external.ID,
			Port:      SimConsumerExternalPortCLI,
		}},
		Adapters: []SimConsumerAdapter{memory, external},
	})
	if err != nil {
		t.Fatalf("NewSimConsumerTestkit: %v", err)
	}
	_, err = kit.Run(simConsumerGeneratedScenario(t))
	if err == nil || !errors.Is(err, ErrSimConsumerConformance) || !strings.Contains(err.Error(), "want exact prefix") {
		t.Fatalf("Run error = %v, want exact materialized-history rejection", err)
	}
}

func TestSimConsumerTestkitCatchesStepwiseRealAdapterMutation(t *testing.T) {
	memory, _ := newSimConsumerTestAdapter("memory", SimConsumerAdapterInMemory, 0)
	postgres, _ := newSimConsumerTestAdapter("postgres.mutant", SimConsumerAdapterPostgres, 1)
	kit, err := NewSimConsumerTestkit(SimConsumerTestkitSpec{
		SimulationAdapterID:  "memory",
		RequiredRealAdapters: []SimConsumerAdapterKind{SimConsumerAdapterPostgres},
		Adapters:             []SimConsumerAdapter{memory, postgres},
	})
	if err != nil {
		t.Fatalf("NewSimConsumerTestkit: %v", err)
	}
	_, err = kit.Run(simConsumerGeneratedScenario(t))
	var divergence *SimConsumerDivergenceError
	if !errors.As(err, &divergence) || divergence.Step != 1 || divergence.AdapterID != "postgres.mutant" || divergence.ObservationID != "consumer.value" {
		t.Fatalf("Run error = %v, want first-step Postgres value divergence", err)
	}
}

func TestSimConsumerTestkitRejectsInMemoryAdapterThatBypassesSimWorld(t *testing.T) {
	memory, _ := newSimConsumerTestAdapter("memory", SimConsumerAdapterInMemory, 0)
	postgres, _ := newSimConsumerTestAdapter("postgres.integration", SimConsumerAdapterPostgres, 0)
	memory.Apply = func(SimAction) error { return nil }
	kit, err := NewSimConsumerTestkit(SimConsumerTestkitSpec{
		SimulationAdapterID:  "memory",
		RequiredRealAdapters: []SimConsumerAdapterKind{SimConsumerAdapterPostgres},
		Adapters:             []SimConsumerAdapter{memory, postgres},
	})
	if err != nil {
		t.Fatalf("NewSimConsumerTestkit: %v", err)
	}
	_, err = kit.Run(simConsumerGeneratedScenario(t))
	if err == nil || !errors.Is(err, ErrSimConsumerConformance) || !strings.Contains(err.Error(), "did not execute through its SimWorld") {
		t.Fatalf("Run error = %v, want SimWorld bypass rejection", err)
	}
}

func TestSimConsumerTestkitRejectsMissingRealServicesReducerDriftAndBroadStubs(t *testing.T) {
	memory, _ := newSimConsumerTestAdapter("memory", SimConsumerAdapterInMemory, 0)
	postgres, _ := newSimConsumerTestAdapter("postgres.integration", SimConsumerAdapterPostgres, 0)

	cases := []struct {
		name     string
		mutate   func(*SimConsumerTestkitSpec)
		contains string
	}{
		{
			name: "no selected real service",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.RequiredRealAdapters = nil
			},
			contains: "select at least one real",
		},
		{
			name: "selected service missing",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.RequiredRealAdapters = []SimConsumerAdapterKind{SimConsumerAdapterNATS}
			},
			contains: "not explicitly selected",
		},
		{
			name: "production reducer drift",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].ProductionReducerID = "test.only.reducer"
				spec.Adapters[1].ReducerID = "test.only.reducer"
			},
			contains: "shared production reducer",
		},
		{
			name: "deterministic port stub",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[0].Ports[0].Stubbed = true
			},
			contains: "stubs deterministic port",
		},
		{
			name: "real deterministic port stub",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].Ports[0].Stubbed = true
			},
			contains: "stubs deterministic port",
		},
		{
			name: "real adapter stub",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].Ports[1].Stubbed = true
			},
			contains: "real adapter",
		},
		{
			name: "real adapter exposes SimWorld",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].SimulationWorld = func() *SimWorld { return NewSimWorld(mustParseSimConsumerSeed()) }
			},
			contains: "cannot expose a simulation world",
		},
		{
			name: "port contract drift",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].Ports[0].Kind = "different.storage"
			},
			contains: "shared narrow-port contract",
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			spec := SimConsumerTestkitSpec{
				SimulationAdapterID:  "memory",
				RequiredRealAdapters: []SimConsumerAdapterKind{SimConsumerAdapterPostgres},
				Adapters:             []SimConsumerAdapter{memory, postgres},
			}
			spec.Adapters[0].Ports = append([]SimConsumerPort(nil), memory.Ports...)
			spec.Adapters[1].Ports = append([]SimConsumerPort(nil), postgres.Ports...)
			test.mutate(&spec)
			_, err := NewSimConsumerTestkit(spec)
			if err == nil || !errors.Is(err, ErrSimConsumerConformance) || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("NewSimConsumerTestkit error = %v, want ErrSimConsumerConformance containing %q", err, test.contains)
			}
		})
	}
}

func TestSimConsumerTestkitRejectsInvalidExternalProcessContracts(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*SimConsumerTestkitSpec)
		contains string
	}{
		{
			name: "not explicitly selected",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.RequiredExternalProcesses = nil
				spec.RequiredRealAdapters = []SimConsumerAdapterKind{SimConsumerAdapterPostgres}
				postgres, _ := newSimConsumerTestAdapter("postgres.integration", SimConsumerAdapterPostgres, 0)
				spec.Adapters = append(spec.Adapters, postgres)
			},
			contains: "was not explicitly selected",
		},
		{
			name: "selection port mismatch",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.RequiredExternalProcesses[0].Port = SimConsumerExternalPortFilesystem
			},
			contains: "selected \"filesystem\"",
		},
		{
			name: "selection names non-external adapter",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.RequiredExternalProcesses[0].AdapterID = "memory"
			},
			contains: "refers to adapter kind \"in_memory\"",
		},
		{
			name: "implicit port determinism",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].Ports[0].Determinism = ""
			},
			contains: "must declare determinism",
		},
		{
			name: "claims in-memory reducer",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].ProductionReducerID = "counter.reducer.v1"
			},
			contains: "without claiming the in-memory production reducer",
		},
		{
			name: "protocol drift",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].ProtocolID = "other.protocol.v1"
			},
			contains: "shared protocol",
		},
		{
			name: "missing service identity",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].ServiceID = ""
			},
			contains: "stable service id",
		},
		{
			name: "missing reducer identity",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].ReducerID = ""
			},
			contains: "stable adapter, protocol, and reducer ids",
		},
		{
			name: "missing probe callback",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].Probe = nil
			},
			contains: "Probe, and MaterializedHistory",
		},
		{
			name: "missing reset callback",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].Reset = nil
			},
			contains: "Reset, Apply, and Observe",
		},
		{
			name: "missing apply callback",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].Apply = nil
			},
			contains: "Reset, Apply, and Observe",
		},
		{
			name: "missing observe callback",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].Observe = nil
			},
			contains: "Reset, Apply, and Observe",
		},
		{
			name: "missing materialized history",
			mutate: func(spec *SimConsumerTestkitSpec) {
				spec.Adapters[1].MaterializedHistory = nil
			},
			contains: "MaterializedHistory",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			memory, _ := newSimConsumerTestAdapter("memory", SimConsumerAdapterInMemory, 0)
			external, _ := newSimConsumerExternalProcessAdapter("sample.cli", SimConsumerExternalPortCLI, 0)
			spec := SimConsumerTestkitSpec{
				SimulationAdapterID: "memory",
				RequiredExternalProcesses: []SimConsumerExternalProcessSelection{{
					AdapterID: external.ID,
					Port:      SimConsumerExternalPortCLI,
				}},
				Adapters: []SimConsumerAdapter{memory, external},
			}
			test.mutate(&spec)
			_, err := NewSimConsumerTestkit(spec)
			if err == nil || !errors.Is(err, ErrSimConsumerConformance) || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("NewSimConsumerTestkit error = %v, want ErrSimConsumerConformance containing %q", err, test.contains)
			}
		})
	}
}
