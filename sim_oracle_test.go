package lazily

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type simOracleCounterReference struct {
	value int
}

func (m *simOracleCounterReference) Apply(result SimStepResult) error {
	if !result.Decision.Accepted {
		return nil
	}
	delta, ok := result.Item.Action.Payload.(int)
	if !ok {
		return fmt.Errorf("payload is %T, want int", result.Item.Action.Payload)
	}
	m.value += delta
	return nil
}

func (m *simOracleCounterReference) Observe() map[string]any {
	return map[string]any{"counter.value": m.value}
}

type simOracleReplayCounter struct {
	value int
}

func (c *simOracleReplayCounter) Apply(event ReplayEvent) {
	action, ok := event.Payload.(SimReplayAction)
	if !ok {
		return
	}
	delta, ok := action.Action.Payload.(int)
	if ok {
		c.value += delta
	}
}

func (c *simOracleReplayCounter) Observe() map[string]any {
	return map[string]any{"value": c.value}
}

func newSimOracleCounterWorld(t *testing.T, mutation func(SimAction, int) int) (*SimWorld, *int) {
	t.Helper()
	world := mustBuildSimTestWorld(t, SimWorldSpec{
		Seed:           mustSimTestSeed(t),
		SubjectKind:    "oracle.counter",
		SubjectVersion: "1",
	})
	value := new(int)
	if err := world.RegisterActor("counter", SimActorFuncs{
		DecideFunc: func(action SimAction) (SimDecision, error) {
			delta, ok := action.Payload.(int)
			if !ok {
				return SimDecision{}, fmt.Errorf("payload is %T, want int", action.Payload)
			}
			if mutation != nil {
				delta = mutation(action, delta)
			}
			*value += delta
			return SimDecision{Accepted: true}, nil
		},
		ObserveFunc: func() map[string]any {
			return map[string]any{"value": *value}
		},
	}); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	return world, value
}

func newSimOracleForTest(t *testing.T, world *SimWorld, config SimOracleConfig) *SimOracle {
	t.Helper()
	oracle, err := NewSimOracle(world, config)
	if err != nil {
		t.Fatalf("NewSimOracle: %v", err)
	}
	return oracle
}

func scheduleSimOracleAction(t *testing.T, world *SimWorld, at uint64, id string, payload int) {
	t.Helper()
	if _, err := world.Schedule(at, simTestAction(id, "counter", payload)); err != nil {
		t.Fatalf("Schedule %s: %v", id, err)
	}
}

func requireSimOracleFailure(t *testing.T, err error, contains string) {
	t.Helper()
	if err == nil || !errors.Is(err, ErrSimOracle) || !strings.Contains(err.Error(), contains) {
		t.Fatalf("oracle error = %v, want ErrSimOracle containing %q", err, contains)
	}
}

func TestSimOracleSafetyCatchesSeededReducerMutation(t *testing.T) {
	world, _ := newSimOracleCounterWorld(t, func(action SimAction, delta int) int {
		if action.ID == "mutant.overcredit" {
			return delta + 100
		}
		return delta
	})
	oracle := newSimOracleForTest(t, world, SimOracleConfig{
		Safety: []SimSafetyAssertion{{
			ID: "balance.within.limit",
			Check: func(snapshot SimOracleSnapshot) error {
				if snapshot.Observations["counter.value"].(int) > 10 {
					return fmt.Errorf("balance exceeded 10")
				}
				return nil
			},
		}},
	})
	scheduleSimOracleAction(t, world, 1, "normal.credit", 4)
	scheduleSimOracleAction(t, world, 2, "mutant.overcredit", 1)

	if _, err := oracle.Step(); err != nil {
		t.Fatalf("safe Step: %v", err)
	}
	_, err := oracle.Step()
	requireSimOracleFailure(t, err, "safety balance.within.limit")
	if oracle.FirstFailure() != err {
		t.Fatalf("FirstFailure = %v, want exact first safety error %v", oracle.FirstFailure(), err)
	}
	_, sticky := oracle.Step()
	if sticky != err {
		t.Fatalf("Step after failure = %v, want sticky first error %v", sticky, err)
	}
}

func TestSimOracleReferenceModelCatchesSeededSubjectMutation(t *testing.T) {
	world, _ := newSimOracleCounterWorld(t, func(_ SimAction, delta int) int {
		return delta + 1
	})
	oracle := newSimOracleForTest(t, world, SimOracleConfig{
		Reference:       &simOracleCounterReference{},
		ReferenceSource: SimOracleSource{ID: "counter.reference", Kind: SimOracleSourceIndependentModel},
	})
	scheduleSimOracleAction(t, world, 1, "credit.one", 1)

	_, err := oracle.Step()
	requireSimOracleFailure(t, err, `reference observation "counter.value" differs`)
}

func TestSimOracleDifferentialCatchesSeededImplementationMutation(t *testing.T) {
	world, value := newSimOracleCounterWorld(t, nil)
	oracle := newSimOracleForTest(t, world, SimOracleConfig{
		Differential: []SimDifferentialSubject{{
			ID:     "mutant.shadow",
			Source: SimOracleSource{ID: "counter.production", Kind: SimOracleSourceProductionReducer},
			Observe: func() map[string]any {
				return map[string]any{"counter.value": *value + 1}
			},
		}},
	})
	scheduleSimOracleAction(t, world, 1, "credit.one", 1)

	_, err := oracle.Step()
	requireSimOracleFailure(t, err, `differential mutant.shadow observation "counter.value" differs`)
}

func TestSimOracleRejectsSimulationOnlySoleComparativeOracle(t *testing.T) {
	world, value := newSimOracleCounterWorld(t, nil)
	simulationOnly := SimDifferentialSubject{
		ID:     "simulator.shadow",
		Source: SimOracleSource{ID: "simulator.shadow", Kind: SimOracleSourceSimulationOnly},
		Observe: func() map[string]any {
			return map[string]any{"counter.value": *value}
		},
	}
	_, err := NewSimOracle(world, SimOracleConfig{Differential: []SimDifferentialSubject{simulationOnly}})
	requireSimOracleFailure(t, err, "simulation-only implementation cannot be the sole comparative oracle")

	if _, err := NewSimOracle(world, SimOracleConfig{
		Reference:       &simOracleCounterReference{},
		ReferenceSource: SimOracleSource{ID: "counter.reference", Kind: SimOracleSourceIndependentModel},
		Differential:    []SimDifferentialSubject{simulationOnly},
	}); err != nil {
		t.Fatalf("simulation-only supplemental differential: %v", err)
	}
}

func TestSimOracleBoundedLivenessCatchesMissingCompletion(t *testing.T) {
	t.Run("numeric bound", func(t *testing.T) {
		world, _ := newSimOracleCounterWorld(t, nil)
		oracle := newSimOracleForTest(t, world, SimOracleConfig{
			Liveness: []SimBoundedLivenessAssertion{{
				ID:    "request.completes",
				Bound: 2,
				When: func(snapshot SimOracleSnapshot) bool {
					return snapshot.Action.ID == "request.start"
				},
				Satisfied: func(snapshot SimOracleSnapshot) bool {
					return snapshot.Action.ID == "request.complete"
				},
			}},
		})
		scheduleSimOracleAction(t, world, 1, "request.start", 0)
		scheduleSimOracleAction(t, world, 2, "unrelated.one", 0)
		scheduleSimOracleAction(t, world, 3, "unrelated.two", 0)

		if _, err := oracle.Step(); err != nil {
			t.Fatalf("start Step: %v", err)
		}
		if _, err := oracle.Step(); err != nil {
			t.Fatalf("before bound Step: %v", err)
		}
		_, err := oracle.Step()
		requireSimOracleFailure(t, err, "bounded liveness request.completes exceeded 2 actions")
	})

	t.Run("pending at idle", func(t *testing.T) {
		world, _ := newSimOracleCounterWorld(t, nil)
		oracle := newSimOracleForTest(t, world, SimOracleConfig{
			Liveness: []SimBoundedLivenessAssertion{{
				ID:        "request.completes",
				Bound:     100,
				When:      func(snapshot SimOracleSnapshot) bool { return snapshot.Action.ID == "request.start" },
				Satisfied: func(snapshot SimOracleSnapshot) bool { return snapshot.Action.ID == "request.complete" },
			}},
		})
		scheduleSimOracleAction(t, world, 1, "request.start", 0)

		_, err := oracle.RunUntilIdle(1)
		requireSimOracleFailure(t, err, "bounded liveness request.completes remained pending at idle")
	})
}

func runSimOracleFingerprintWorld(t *testing.T, at uint64) (*SimOracle, *ReplayHarness) {
	t.Helper()
	world, _ := newSimOracleCounterWorld(t, nil)
	oracle := newSimOracleForTest(t, world, SimOracleConfig{})
	harness, err := NewReplayHarness(func() ReplayGraph { return &simOracleReplayCounter{} })
	if err != nil {
		t.Fatalf("NewReplayHarness: %v", err)
	}
	scheduleSimOracleAction(t, world, at, "credit.one", 1)
	scheduleSimOracleAction(t, world, at, "credit.two", 2)
	for world.Pending() > 0 {
		if _, err := oracle.Step(); err != nil {
			t.Fatalf("oracle Step: %v", err)
		}
	}
	return oracle, harness
}

func TestSimOracleFingerprintCatchesTraceReplayAndObservationMutations(t *testing.T) {
	baseline, harness := runSimOracleFingerprintWorld(t, 3)
	fingerprint, err := baseline.RecordFingerprint(harness)
	if err != nil {
		t.Fatalf("RecordFingerprint: %v", err)
	}
	identical, identicalHarness := runSimOracleFingerprintWorld(t, 3)
	if err := identical.VerifyFingerprint(fingerprint, identicalHarness); err != nil {
		t.Fatalf("VerifyFingerprint identical execution: %v", err)
	}

	t.Run("trace timing", func(t *testing.T) {
		mutant, mutantHarness := runSimOracleFingerprintWorld(t, 4)
		err := mutant.VerifyFingerprint(fingerprint, mutantHarness)
		requireSimOracleFailure(t, err, "simulation trace digest mismatch")
	})

	t.Run("replay log identity", func(t *testing.T) {
		mutant := fingerprint
		mutant.ReplayLogDigest = strings.Repeat("0", 64)
		mutant.Digest, err = ReplayCanonicalDigest(fingerprintWithoutDigest(mutant))
		if err != nil {
			t.Fatalf("digest mutated fingerprint: %v", err)
		}
		err := baseline.VerifyFingerprint(mutant, harness)
		requireSimOracleFailure(t, err, "simulation replay log digest mismatch")
	})

	t.Run("replay fingerprint identity", func(t *testing.T) {
		mutant := fingerprint
		mutant.ReplayFingerprintDigest = strings.Repeat("f", 64)
		mutant.Digest, err = ReplayCanonicalDigest(fingerprintWithoutDigest(mutant))
		if err != nil {
			t.Fatalf("digest mutated fingerprint: %v", err)
		}
		err := baseline.VerifyFingerprint(mutant, harness)
		requireSimOracleFailure(t, err, "replay fingerprint identity mismatch")
	})

	t.Run("oracle observations", func(t *testing.T) {
		mutant := fingerprint
		mutant.Observations = cloneOracleObservations(fingerprint.Observations)
		mutant.Observations[0].Values[0].Canonical = []byte("0")
		mutant.Digest, err = ReplayCanonicalDigest(fingerprintWithoutDigest(mutant))
		if err != nil {
			t.Fatalf("digest mutated fingerprint: %v", err)
		}
		err := baseline.VerifyFingerprint(mutant, harness)
		requireSimOracleFailure(t, err, "oracle observation checkpoint 0 value 0 differs")
	})
}

func TestSimOracleFingerprintRejectsExecutionOutsideOracleBoundary(t *testing.T) {
	world, _ := newSimOracleCounterWorld(t, nil)
	oracle := newSimOracleForTest(t, world, SimOracleConfig{})
	harness, err := NewReplayHarness(func() ReplayGraph { return &simOracleReplayCounter{} })
	if err != nil {
		t.Fatalf("NewReplayHarness: %v", err)
	}
	scheduleSimOracleAction(t, world, 1, "bypass", 1)
	if _, err := world.Step(); err != nil {
		t.Fatalf("world Step: %v", err)
	}

	_, err = oracle.RecordFingerprint(harness)
	requireSimOracleFailure(t, err, "execution bypassed the oracle boundary")
}

func TestSimOracleHistoryAssertionCatchesSeededDuplicateOperation(t *testing.T) {
	world, _ := newSimOracleCounterWorld(t, nil)
	oracle := newSimOracleForTest(t, world, SimOracleConfig{
		History: []SimHistoryAssertion{{
			ID: "operation.ids.unique",
			Check: func(history []SimHistoryOperation, _ SimOracleSnapshot) error {
				seen := map[string]bool{}
				for _, operation := range history {
					if seen[operation.ActionID] {
						return fmt.Errorf("duplicate action %q", operation.ActionID)
					}
					seen[operation.ActionID] = true
				}
				return nil
			},
		}},
	})
	if err := oracle.RecordHistoryOperation(SimHistoryOperation{
		ActionID: "duplicate.operation",
		ActorID:  "client",
		Kind:     "client.write",
		InvokeAt: 0,
		ReturnAt: 2,
		Accepted: true,
	}); err != nil {
		t.Fatalf("RecordHistoryOperation: %v", err)
	}
	scheduleSimOracleAction(t, world, 1, "duplicate.operation", 1)

	_, err := oracle.Step()
	requireSimOracleFailure(t, err, "history operation.ids.unique")
}

func TestSimOracleLinearizabilityRequiresClaimAndCatchesSeededHistory(t *testing.T) {
	world, _ := newSimOracleCounterWorld(t, nil)
	oracle := newSimOracleForTest(t, world, SimOracleConfig{})
	checker := func(history []SimHistoryOperation) error {
		for _, operation := range history {
			if operation.Kind == "read.stale" {
				return fmt.Errorf("stale read has no legal sequential placement")
			}
		}
		return nil
	}

	err := oracle.CheckLinearizability(SimLinearizabilityCapability{}, checker)
	requireSimOracleFailure(t, err, "requires an explicit claimed capability")
	if oracle.FirstFailure() != nil {
		t.Fatalf("a rejected capability claim poisoned the oracle: %v", oracle.FirstFailure())
	}
	if err := oracle.RecordHistoryOperation(SimHistoryOperation{
		ActionID: "invalid.interval",
		Kind:     "read.stale",
		InvokeAt: 2,
		ReturnAt: 1,
	}); err == nil || !errors.Is(err, ErrSimOracle) {
		t.Fatalf("invalid history interval error = %v, want ErrSimOracle", err)
	}
	if err := oracle.RecordHistoryOperation(SimHistoryOperation{
		ActionID: "stale.read",
		ActorID:  "client",
		Kind:     "read.stale",
		InvokeAt: 1,
		ReturnAt: 2,
		Accepted: true,
	}); err != nil {
		t.Fatalf("RecordHistoryOperation: %v", err)
	}
	capability, err := ClaimSimLinearizability("counter", "sequential-v1")
	if err != nil {
		t.Fatalf("ClaimSimLinearizability: %v", err)
	}
	err = oracle.CheckLinearizability(capability, checker)
	requireSimOracleFailure(t, err, "linearizability counter/sequential-v1")
}
