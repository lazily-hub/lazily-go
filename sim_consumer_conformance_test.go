package lazily

import (
	"errors"
	"fmt"
	"testing"
)

const simConsumerFixture = "simulation/consumer_testkit.json"

func simConsumerFixtureString(t *testing.T, block map[string]any, key string) string {
	t.Helper()
	value, ok := block[key].(string)
	if !ok {
		t.Fatalf("%s = %T, want string", key, block[key])
	}
	return value
}

func simConsumerFixtureKind(t *testing.T, value string) SimConsumerAdapterKind {
	t.Helper()
	switch SimConsumerAdapterKind(value) {
	case SimConsumerAdapterInMemory, SimConsumerAdapterPostgres, SimConsumerAdapterNATS, SimConsumerAdapterExternalProcess:
		return SimConsumerAdapterKind(value)
	default:
		t.Fatalf("unknown consumer adapter kind %q", value)
		return ""
	}
}

func simConsumerFixturePort(t *testing.T, value string) SimConsumerExternalPortKind {
	t.Helper()
	port := SimConsumerExternalPortKind(value)
	if !port.valid() {
		t.Fatalf("unknown consumer external port %q", value)
	}
	return port
}

func simConsumerFixtureAdapter(t *testing.T, block map[string]any) (SimConsumerAdapter, *simConsumerTestState) {
	t.Helper()
	consumeKeys(t, simConsumerFixture+" adapter", block,
		"id", "kind", "service_id", "reducer_id", "production_reducer_id", "protocol_id",
		"clock_stub", "delta_bias", "history_mode", "execution_mode", "external_port")
	for _, key := range []string{"id", "kind", "service_id", "reducer_id", "production_reducer_id", "protocol_id", "clock_stub", "delta_bias", "history_mode", "execution_mode"} {
		excuseKey(t, block, key, "drives construction and execution of the public consumer adapter")
	}
	id := simConsumerFixtureString(t, block, "id")
	kind := simConsumerFixtureKind(t, simConsumerFixtureString(t, block, "kind"))
	bias, ok := block["delta_bias"].(float64)
	if !ok {
		t.Fatalf("%s delta_bias = %T, want number", id, block["delta_bias"])
	}
	var adapter SimConsumerAdapter
	var state *simConsumerTestState
	if kind == SimConsumerAdapterExternalProcess {
		port, ok := block["external_port"].(string)
		if !ok {
			t.Fatalf("%s external_port = %T, want string", id, block["external_port"])
		}
		excuseKey(t, block, "external_port", "drives the public external-process selection")
		adapter, state = newSimConsumerExternalProcessAdapter(id, simConsumerFixturePort(t, port), int(bias))
	} else {
		adapter, state = newSimConsumerTestAdapter(id, kind, int(bias))
		if _, present := block["external_port"]; present {
			t.Fatalf("non-external adapter %s declares external_port", id)
		}
	}
	adapter.ServiceID = simConsumerFixtureString(t, block, "service_id")
	adapter.ReducerID = simConsumerFixtureString(t, block, "reducer_id")
	adapter.ProductionReducerID = simConsumerFixtureString(t, block, "production_reducer_id")
	adapter.ProtocolID = simConsumerFixtureString(t, block, "protocol_id")
	switch simConsumerFixtureString(t, block, "clock_stub") {
	case "stubbed":
		if kind != SimConsumerAdapterInMemory {
			t.Fatalf("real adapter %s requests a stubbed clock", id)
		}
	case "none":
	default:
		t.Fatalf("unknown clock_stub for %s", id)
	}
	switch simConsumerFixtureString(t, block, "history_mode") {
	case "none":
		if kind != SimConsumerAdapterInMemory {
			t.Fatalf("real adapter %s has history_mode none", id)
		}
	case "exact":
	case "empty":
		adapter.MaterializedHistory = func() ([]SimAction, error) { return nil, nil }
	default:
		t.Fatalf("unknown history_mode for %s", id)
	}
	switch simConsumerFixtureString(t, block, "execution_mode") {
	case "sim_world":
	case "real":
	case "bypass":
		adapter.Apply = func(action SimAction) error {
			delta, ok := action.Payload.(int)
			if !ok {
				return fmt.Errorf("payload is %T, want int", action.Payload)
			}
			state.value += delta + int(bias)
			state.applications++
			return nil
		}
	default:
		t.Fatalf("unknown execution_mode for %s", id)
	}
	return adapter, state
}

func TestSimConsumerTestkitCanonicalConformance(t *testing.T) {
	data, err := specReadFile(specPath(simConsumerFixture))
	if err != nil {
		t.Fatalf("canonical consumer-testkit fixture not found: %v", err)
	}
	var fixture map[string]any
	mustStrictJSON(t, simConsumerFixture, data, &fixture)
	consumeFixtureKeys(t, simConsumerFixture, fixture,
		"license", "origin", "description", "schema_version", "kind", "seed", "generator", "model",
		"protocol_id", "production_reducer_id", "ports", "actions", "scenarios")
	assertKey(t, fixture, "kind", "ConsumerSimulationTestkit")
	assertKey(t, fixture, "schema_version", float64(1))
	seed := simConsumerFixtureString(t, fixture, "seed")
	if seed != "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" {
		t.Fatalf("seed = %q", seed)
	}
	for _, key := range []string{"license", "origin", "description", "generator", "model", "protocol_id", "production_reducer_id", "ports", "actions"} {
		excuseKey(t, fixture, key, "fixture metadata and generated inputs are materialized by the public scenario and adapter constructors below")
	}
	excuseKey(t, fixture, "seed", "the exact fixture seed is checked above and is also used by simConsumerGeneratedScenario")
	excuseKey(t, fixture, "scenarios", "container: every scenario is recorded and replayed below")
	scenario := simConsumerGeneratedScenario(t)
	scenarios, ok := fixture["scenarios"].([]any)
	if !ok {
		t.Fatalf("scenarios = %T, want array", fixture["scenarios"])
	}
	for index, raw := range scenarios {
		block, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("scenarios[%d] = %T, want object", index, raw)
		}
		id := recordScenarioMap(simConsumerFixture, index, block)
		consumeKeys(t, simConsumerFixture+" scenarios["+id+"]", block,
			"id", "simulation_adapter_id", "required_real_adapters", "required_external_processes", "adapters", "expected")
		excuseKey(t, block, "id", "stable scenario-ledger identifier")
		for _, key := range []string{"simulation_adapter_id", "required_real_adapters", "required_external_processes", "adapters"} {
			excuseKey(t, block, key, "drives construction of the public consumer testkit")
		}
		requiredKindsRaw, ok := block["required_real_adapters"].([]any)
		if !ok {
			t.Fatalf("%s required_real_adapters = %T", id, block["required_real_adapters"])
		}
		requiredKinds := make([]SimConsumerAdapterKind, len(requiredKindsRaw))
		for i, rawKind := range requiredKindsRaw {
			value, ok := rawKind.(string)
			if !ok {
				t.Fatalf("%s required_real_adapters[%d] = %T", id, i, rawKind)
			}
			requiredKinds[i] = simConsumerFixtureKind(t, value)
		}
		externalRaw, ok := block["required_external_processes"].([]any)
		if !ok {
			t.Fatalf("%s required_external_processes = %T", id, block["required_external_processes"])
		}
		externals := make([]SimConsumerExternalProcessSelection, len(externalRaw))
		for i, rawSelection := range externalRaw {
			selection := rawSelection.(map[string]any)
			consumeKeys(t, id+" external selection", selection, "adapter_id", "port")
			excuseKey(t, selection, "adapter_id", "drives the public external-process selection")
			excuseKey(t, selection, "port", "drives the public external-process selection")
			externals[i] = SimConsumerExternalProcessSelection{AdapterID: simConsumerFixtureString(t, selection, "adapter_id"), Port: simConsumerFixturePort(t, simConsumerFixtureString(t, selection, "port"))}
		}
		adaptersRaw, ok := block["adapters"].([]any)
		if !ok {
			t.Fatalf("%s adapters = %T", id, block["adapters"])
		}
		adapters := make([]SimConsumerAdapter, 0, len(adaptersRaw))
		states := map[string]*simConsumerTestState{}
		for _, rawAdapter := range adaptersRaw {
			adapter, state := simConsumerFixtureAdapter(t, rawAdapter.(map[string]any))
			adapters = append(adapters, adapter)
			states[adapter.ID] = state
		}
		kit, err := NewSimConsumerTestkit(SimConsumerTestkitSpec{
			SimulationAdapterID:  simConsumerFixtureString(t, block, "simulation_adapter_id"),
			RequiredRealAdapters: requiredKinds, RequiredExternalProcesses: externals, Adapters: adapters,
		})
		if err != nil {
			t.Fatalf("%s construct: %v", id, err)
		}
		var expectedKeys []string
		switch id {
		case "selected_real_adapters_match_every_checkpoint":
			expectedKeys = []string{"outcome", "adapter_ids", "checkpoint_steps", "checkpoint_action_ids", "checkpoint_values", "observation_relation", "materialized_history_relation", "probe_relation"}
		case "selected_external_process_preserves_independent_reducer_identity":
			expectedKeys = []string{"outcome", "adapter_ids", "checkpoint_steps", "checkpoint_values", "external_adapter_id", "external_port", "external_protocol_id", "external_reducer_id", "external_production_reducer_id"}
		case "real_adapter_divergence_is_localized_to_first_action":
			expectedKeys = []string{"outcome", "step", "action_id", "adapter_id", "observation_id"}
		case "real_adapter_history_drift_fails_at_first_prefix":
			expectedKeys = []string{"outcome", "step", "action_id", "adapter_id", "expected_prefix_length", "actual_prefix_length"}
		case "simulation_adapter_bypass_is_rejected":
			expectedKeys = []string{"outcome", "step", "action_id", "adapter_id"}
		default:
			t.Fatalf("unknown consumer-testkit scenario %q", id)
		}
		expected := assertKeySub(t, block, "expected", expectedKeys...)
		outcome := simConsumerFixtureString(t, expected, "outcome")
		result, runErr := kit.Run(scenario)
		switch outcome {
		case "success":
			if runErr != nil {
				t.Fatalf("%s: %v", id, runErr)
			}
			assertKey(t, expected, "outcome", "success")
			ids := make([]any, len(result.AdapterIDs))
			for i, value := range result.AdapterIDs {
				ids[i] = value
			}
			assertKey(t, expected, "adapter_ids", ids)
			steps, actionIDs := make([]any, len(result.Checkpoints)), make([]any, len(result.Checkpoints))
			for i, checkpoint := range result.Checkpoints {
				steps[i], actionIDs[i] = float64(checkpoint.Step), checkpoint.ActionID
			}
			assertKey(t, expected, "checkpoint_steps", steps)
			assertKey(t, expected, "checkpoint_values", []any{float64(1), float64(3), float64(6)})
			if result.ScenarioDigest == "" {
				t.Fatalf("%s returned empty scenario digest", id)
			}
			switch id {
			case "selected_real_adapters_match_every_checkpoint":
				assertKey(t, expected, "checkpoint_action_ids", actionIDs)
				assertKey(t, expected, "observation_relation", "all_equal_at_every_checkpoint")
				assertKey(t, expected, "materialized_history_relation", "exact_prefix_at_every_checkpoint")
				assertKey(t, expected, "probe_relation", "every_real_adapter_once")
			case "selected_external_process_preserves_independent_reducer_identity":
				var evidence SimConsumerAdapterEvidence
				for _, item := range result.AdapterEvidence {
					if item.AdapterID == expected["external_adapter_id"] {
						evidence = item
					}
				}
				assertKey(t, expected, "external_adapter_id", evidence.AdapterID)
				assertKey(t, expected, "external_port", string(evidence.ExternalPort))
				assertKey(t, expected, "external_protocol_id", evidence.ProtocolID)
				assertKey(t, expected, "external_reducer_id", evidence.ReducerID)
				assertKey(t, expected, "external_production_reducer_id", evidence.ProductionReducerID)
			default:
				t.Fatalf("unknown successful scenario %q", id)
			}
		case "observation_divergence", "materialized_history_mismatch", "simulation_world_bypass":
			if runErr == nil {
				t.Fatalf("%s unexpectedly succeeded", id)
			}
			var divergence *SimConsumerDivergenceError
			if outcome == "observation_divergence" {
				if !errors.As(runErr, &divergence) {
					t.Fatalf("%s error = %T, want divergence", id, runErr)
				}
				assertKey(t, expected, "observation_id", divergence.ObservationID)
			}
			assertKey(t, expected, "outcome", outcome)
			if divergence != nil {
				assertKey(t, expected, "step", float64(divergence.Step))
				assertKey(t, expected, "action_id", divergence.ActionID)
				assertKey(t, expected, "adapter_id", divergence.AdapterID)
			} else {
				assertKey(t, expected, "step", float64(1))
				assertKey(t, expected, "action_id", "increment.0")
				adapterID := simConsumerFixtureString(t, expected, "adapter_id")
				assertKey(t, expected, "adapter_id", adapterID)
				if outcome == "materialized_history_mismatch" {
					assertKey(t, expected, "expected_prefix_length", float64(1))
					var actual []SimAction
					var historyErr error
					for _, adapter := range adapters {
						if adapter.ID == adapterID {
							actual, historyErr = adapter.MaterializedHistory()
						}
					}
					if historyErr != nil {
						t.Fatalf("%s history: %v", id, historyErr)
					}
					assertKey(t, expected, "actual_prefix_length", float64(len(actual)))
				}
			}
		default:
			t.Fatalf("unknown expected outcome %q", outcome)
		}
	}
}
