package lazily

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func simGeneratorTestAction(command string, ordinal uint64, payload any) SimAction {
	return SimAction{
		ID:      fmt.Sprintf("%s.action.%d", command, ordinal),
		ActorID: "worker",
		Kind:    "generator." + command,
		Payload: payload,
	}
}

func mustSimGeneratorScenarioBytes(t *testing.T, scenario SimGeneratedScenario) []byte {
	t.Helper()
	encoded, err := ReplayCanonicalBytes(scenario)
	if err != nil {
		t.Fatalf("ReplayCanonicalBytes: %v", err)
	}
	return encoded
}

func mustSimGeneratorWorldDigest(t *testing.T, world *SimWorld) string {
	t.Helper()
	digest, err := world.WorldDigest()
	if err != nil {
		t.Fatalf("WorldDigest: %v", err)
	}
	return digest
}

func TestSimGeneratorRepeatedIndexIsDeterministicPureAndHistoryBounded(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	spec := SimGeneratorSpec[int]{
		Name:         "stateful.generator",
		Version:      "1",
		InitialModel: 0,
		MaxActions:   5,
		MaxHistory:   2,
		Commands: []SimModelCommand[int]{
			{
				Name:   "open",
				Weight: 1,
				Precondition: func(model int, history []SimGeneratedAction) bool {
					return model == 0 && len(history) == 0
				},
				Build: func(random *SimRandomStream, _ int, ordinal uint64) (SimAction, error) {
					value, err := random.Uint64()
					return simGeneratorTestAction("open", ordinal, value), err
				},
				Apply: func(model int, _ SimAction) (int, error) { return model + 1, nil },
			},
			{
				Name:   "advance",
				Weight: 1,
				Precondition: func(model int, history []SimGeneratedAction) bool {
					return model > 0 && model < 5 && len(history) > 0 && len(history) <= 2
				},
				Build: func(random *SimRandomStream, _ int, ordinal uint64) (SimAction, error) {
					value, err := random.Uint64()
					return simGeneratorTestAction("advance", ordinal, value), err
				},
				Apply: func(model int, _ SimAction) (int, error) { return model + 1, nil },
			},
		},
	}
	generator, err := NewSimGenerator(world, spec)
	if err != nil {
		t.Fatalf("NewSimGenerator: %v", err)
	}

	before := mustSimGeneratorWorldDigest(t, world)
	first, err := generator.Generate(17)
	if err != nil {
		t.Fatalf("Generate first: %v", err)
	}
	afterFirst := mustSimGeneratorWorldDigest(t, world)
	second, err := generator.Generate(17)
	if err != nil {
		t.Fatalf("Generate repeated index: %v", err)
	}
	afterSecond := mustSimGeneratorWorldDigest(t, world)
	if !bytes.Equal(mustSimGeneratorScenarioBytes(t, first), mustSimGeneratorScenarioBytes(t, second)) {
		t.Fatalf("repeated index produced different scenarios")
	}
	if before != afterFirst || before != afterSecond {
		t.Fatalf("generation mutated runtime world digest: %q -> %q -> %q", before, afterFirst, afterSecond)
	}
	if len(first.Actions) != 5 {
		t.Fatalf("action count = %d, want MaxActions 5", len(first.Actions))
	}
	wantCommands := []string{"open", "advance", "advance", "advance", "advance"}
	gotCommands := make([]string, len(first.Actions))
	for i, action := range first.Actions {
		gotCommands[i] = action.Command
	}
	if !reflect.DeepEqual(gotCommands, wantCommands) {
		t.Fatalf("state-precondition command sequence = %v, want %v", gotCommands, wantCommands)
	}

	otherWorld := NewSimWorld(mustSimTestSeed(t))
	other, err := NewSimGenerator(otherWorld, spec)
	if err != nil {
		t.Fatalf("NewSimGenerator other: %v", err)
	}
	third, err := other.Generate(17)
	if err != nil {
		t.Fatalf("Generate other: %v", err)
	}
	if !bytes.Equal(mustSimGeneratorScenarioBytes(t, first), mustSimGeneratorScenarioBytes(t, third)) {
		t.Fatalf("same seed, spec, and index differed across generator instances")
	}
}

func TestSimGeneratorWeightsUseSeparateStreamsAndIgnoreDeclarationOrder(t *testing.T) {
	makeCommands := func(order []string, payloadDraws int) []SimModelCommand[int] {
		weights := map[string]uint64{"light": 1, "heavy": 3}
		commands := make([]SimModelCommand[int], 0, len(order))
		for _, name := range order {
			commandName := name
			commands = append(commands, SimModelCommand[int]{
				Name:   commandName,
				Weight: weights[commandName],
				Build: func(random *SimRandomStream, _ int, ordinal uint64) (SimAction, error) {
					var payload uint64
					for i := 0; i < payloadDraws; i++ {
						value, err := random.Uint64()
						if err != nil {
							return SimAction{}, err
						}
						payload = value
					}
					return simGeneratorTestAction(commandName, ordinal, payload), nil
				},
				Apply: func(model int, _ SimAction) (int, error) { return model + 1, nil },
			})
		}
		return commands
	}
	generate := func(order []string, payloadDraws int) SimGeneratedScenario {
		t.Helper()
		generator, err := NewSimGenerator(NewSimWorld(mustSimTestSeed(t)), SimGeneratorSpec[int]{
			Name: "weighted.generator", Version: "1", MaxActions: 64,
			Commands: makeCommands(order, payloadDraws),
		})
		if err != nil {
			t.Fatalf("NewSimGenerator: %v", err)
		}
		scenario, err := generator.Generate(9)
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		return scenario
	}
	commandsOf := func(scenario SimGeneratedScenario) []string {
		commands := make([]string, len(scenario.Actions))
		for i, action := range scenario.Actions {
			commands[i] = action.Command
		}
		return commands
	}

	baseline := generate([]string{"light", "heavy"}, 1)
	extraPayloadDraws := generate([]string{"light", "heavy"}, 11)
	reversed := generate([]string{"heavy", "light"}, 1)
	baselineCommands := commandsOf(baseline)
	if !reflect.DeepEqual(baselineCommands, commandsOf(extraPayloadDraws)) {
		t.Fatalf("payload draw count perturbed later command selection")
	}
	if !bytes.Equal(mustSimGeneratorScenarioBytes(t, baseline), mustSimGeneratorScenarioBytes(t, reversed)) {
		t.Fatalf("command declaration order changed generated scenario")
	}
	counts := map[string]int{}
	for _, command := range baselineCommands {
		counts[command]++
	}
	if counts["light"] == 0 || counts["heavy"] == 0 || counts["light"]+counts["heavy"] != 64 {
		t.Fatalf("weighted selections = %v, want both positive-weight commands and 64 total", counts)
	}
}

func TestSimGeneratorZeroWeightCoverageAndMissingAudit(t *testing.T) {
	zeroBuilt := false
	audit := NewSimCoverageAudit()
	generator, err := NewSimGenerator(NewSimWorld(mustSimTestSeed(t)), SimGeneratorSpec[int]{
		Name: "coverage.generator", Version: "1", MaxActions: 1, Coverage: audit,
		DeclaredScenarios: []string{"covered.scenario", "missing.scenario"},
		Commands: []SimModelCommand[int]{
			{
				Name:   "covered",
				Weight: 1,
				Build: func(_ *SimRandomStream, _ int, ordinal uint64) (SimAction, error) {
					return simGeneratorTestAction("covered", ordinal, nil), nil
				},
				Apply:    func(model int, _ SimAction) (int, error) { return model + 1, nil },
				Coverage: func(int, SimAction, int) []string { return []string{"covered.scenario"} },
			},
			{
				Name:   "disabled",
				Weight: 0,
				Build: func(_ *SimRandomStream, _ int, ordinal uint64) (SimAction, error) {
					zeroBuilt = true
					return simGeneratorTestAction("disabled", ordinal, nil), nil
				},
				Apply: func(model int, _ SimAction) (int, error) { return model, nil },
			},
		},
	})
	if err != nil {
		t.Fatalf("NewSimGenerator: %v", err)
	}
	scenario, err := generator.Generate(0)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if zeroBuilt || len(scenario.Actions) != 1 || scenario.Actions[0].Command != "covered" {
		t.Fatalf("zero-weight command participated: built=%v actions=%+v", zeroBuilt, scenario.Actions)
	}
	if !reflect.DeepEqual(scenario.CoverageLabels, []string{"covered.scenario"}) {
		t.Fatalf("coverage labels = %v", scenario.CoverageLabels)
	}
	report, err := generator.AuditCoverage()
	if err == nil || !errors.Is(err, ErrSimGeneration) {
		t.Fatalf("AuditCoverage error = %v, want incomplete coverage", err)
	}
	if !reflect.DeepEqual(report.MissingActions, []string{"disabled"}) || !reflect.DeepEqual(report.MissingScenarios, []string{"missing.scenario"}) {
		t.Fatalf("missing coverage = actions %v scenarios %v", report.MissingActions, report.MissingScenarios)
	}
	if report.ActionCounts["covered"] != 1 || report.ActionCounts["disabled"] != 0 || report.ScenarioCounts["covered.scenario"] != 1 {
		t.Fatalf("coverage counts = actions %v scenarios %v", report.ActionCounts, report.ScenarioCounts)
	}
}

func TestSimGeneratorRejectsEligibleWeightSumOverflow(t *testing.T) {
	build := func(name string) func(*SimRandomStream, int, uint64) (SimAction, error) {
		return func(_ *SimRandomStream, _ int, ordinal uint64) (SimAction, error) {
			return simGeneratorTestAction(name, ordinal, nil), nil
		}
	}
	apply := func(model int, _ SimAction) (int, error) { return model, nil }
	generator, err := NewSimGenerator(NewSimWorld(mustSimTestSeed(t)), SimGeneratorSpec[int]{
		Name: "overflow.generator", Version: "1", MaxActions: 1,
		Commands: []SimModelCommand[int]{
			{Name: "maximum", Weight: math.MaxUint64, Build: build("maximum"), Apply: apply},
			{Name: "one", Weight: 1, Build: build("one"), Apply: apply},
		},
	})
	if err != nil {
		t.Fatalf("NewSimGenerator: %v", err)
	}
	_, err = generator.Generate(0)
	if err == nil || !errors.Is(err, ErrSimGeneration) || !strings.Contains(err.Error(), "weights overflow") {
		t.Fatalf("Generate overflow error = %v", err)
	}
}

func TestSimCausalShrinkerPreservesCauseAndCustomDependenciesAndMinimizes(t *testing.T) {
	generated := func(id, command, cause string, payload any) SimGeneratedAction {
		return SimGeneratedAction{Command: command, Action: SimAction{
			ID: id, ActorID: "worker", Kind: "shrink." + command, CauseID: cause, Payload: payload,
		}}
	}
	input := []SimGeneratedAction{
		generated("noise.a", "step", "", nil),
		generated("root", "step", "", nil),
		generated("child", "step", "root", nil),
		generated("custom", "step", "", nil),
		generated("target", "requires", "child", "custom"),
		generated("noise.b", "step", "", nil),
	}
	apply := func(model int, _ SimAction) (int, error) { return model + 1, nil }
	shrinker := SimCausalShrinker[int]{
		Commands: map[string]SimShrinkCommand[int]{
			"step": {Apply: apply},
			"requires": {
				Apply: apply,
				Dependencies: func(action SimAction) []string {
					return []string{action.Payload.(string)}
				},
			},
		},
		MaxHistory: 2,
	}
	fails := func(actions []SimGeneratedAction) (bool, error) {
		for _, action := range actions {
			if action.Action.ID == "target" {
				return true, nil
			}
		}
		return false, nil
	}
	shrunk, err := shrinker.Shrink(input, fails)
	if err != nil {
		t.Fatalf("Shrink: %v", err)
	}
	gotIDs := make([]string, len(shrunk))
	for i, action := range shrunk {
		gotIDs[i] = action.Action.ID
	}
	wantIDs := []string{"root", "child", "custom", "target"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("shrunk IDs = %v, want dependency-closed %v", gotIDs, wantIDs)
	}
	if !shrinker.valid(shrunk) {
		t.Fatalf("shrunk sequence is not command/dependency valid")
	}
	failed, err := fails(shrunk)
	if err != nil || !failed {
		t.Fatalf("shrunk sequence no longer reproduces failure: failed=%v err=%v", failed, err)
	}
	for i := range shrunk {
		candidate := append([]SimGeneratedAction(nil), shrunk[:i]...)
		candidate = append(candidate, shrunk[i+1:]...)
		candidateFails, candidateErr := fails(candidate)
		if candidateErr != nil {
			t.Fatalf("failure predicate after removing %q: %v", shrunk[i].Action.ID, candidateErr)
		}
		if shrinker.valid(candidate) && candidateFails {
			t.Fatalf("sequence was not one-removal-minimal; removing %q remained valid and failing", shrunk[i].Action.ID)
		}
	}
}

func TestPersistSimCounterexampleWritesReversibleArtifactsAndRefusesConflictingOverwrite(t *testing.T) {
	counterexample := SimCounterexample{
		Scenario: SimGeneratedScenario{
			GeneratorName: "persist.generator", GeneratorVersion: "1", ScenarioIndex: 4,
			SeedHex:        mustSimTestSeed(t).String(),
			Actions:        []SimGeneratedAction{{Command: "step", Action: simGeneratorTestAction("step", 0, "payload")}},
			CoverageLabels: []string{"persisted.scenario"},
		},
		Failure: "invariant violated",
	}
	encode := func(value SimCounterexample) ([]byte, error) { return json.Marshal(value) }
	temp := t.TempDir()
	regressionDir := filepath.Join(temp, "regressions")
	fuzzDir := filepath.Join(temp, "testdata", "fuzz", "FuzzSimulation")

	first, err := PersistSimCounterexample(regressionDir, fuzzDir, counterexample, encode)
	if err != nil {
		t.Fatalf("PersistSimCounterexample: %v", err)
	}
	encoded, err := encode(counterexample)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	regressionBytes, err := os.ReadFile(first.RegressionPath)
	if err != nil {
		t.Fatalf("read regression: %v", err)
	}
	if !bytes.Equal(regressionBytes, encoded) {
		t.Fatalf("regression bytes differ from reversible encoding")
	}
	var decoded SimCounterexample
	if err := json.Unmarshal(regressionBytes, &decoded); err != nil {
		t.Fatalf("decode regression: %v", err)
	}
	if !reflect.DeepEqual(decoded, counterexample) {
		t.Fatalf("decoded regression = %+v, want %+v", decoded, counterexample)
	}
	fuzzBytes, err := os.ReadFile(first.FuzzCorpusPath)
	if err != nil {
		t.Fatalf("read fuzz corpus: %v", err)
	}
	wantFuzz := []byte("go test fuzz v1\n[]byte(" + strconv.Quote(string(encoded)) + ")\n")
	if !bytes.Equal(fuzzBytes, wantFuzz) {
		t.Fatalf("fuzz corpus = %q, want %q", fuzzBytes, wantFuzz)
	}
	if filepath.Base(first.RegressionPath) != first.Digest+".simcase" || filepath.Base(first.FuzzCorpusPath) != first.Digest || len(first.Digest) != 64 {
		t.Fatalf("content-addressed paths = %+v", first)
	}

	identical, err := PersistSimCounterexample(regressionDir, fuzzDir, counterexample, encode)
	if err != nil {
		t.Fatalf("PersistSimCounterexample identical repeat: %v", err)
	}
	if identical != first {
		t.Fatalf("identical persistence result = %+v, want %+v", identical, first)
	}

	corruptRegression := []byte("corrupt regression")
	if err := os.WriteFile(first.RegressionPath, corruptRegression, 0o644); err != nil {
		t.Fatalf("corrupt regression: %v", err)
	}
	_, err = PersistSimCounterexample(regressionDir, fuzzDir, counterexample, encode)
	if err == nil || !errors.Is(err, ErrSimGeneration) || !strings.Contains(err.Error(), "content-addressed path") {
		t.Fatalf("conflicting regression overwrite error = %v", err)
	}
	stillCorrupt, err := os.ReadFile(first.RegressionPath)
	if err != nil {
		t.Fatalf("read conflicting regression: %v", err)
	}
	if !bytes.Equal(stillCorrupt, corruptRegression) {
		t.Fatalf("conflicting regression was overwritten")
	}
	if err := os.WriteFile(first.RegressionPath, encoded, 0o644); err != nil {
		t.Fatalf("restore regression for fuzz conflict check: %v", err)
	}
	corruptFuzz := []byte("corrupt fuzz")
	if err := os.WriteFile(first.FuzzCorpusPath, corruptFuzz, 0o644); err != nil {
		t.Fatalf("corrupt fuzz: %v", err)
	}
	_, err = PersistSimCounterexample(regressionDir, fuzzDir, counterexample, encode)
	if err == nil || !errors.Is(err, ErrSimGeneration) || !strings.Contains(err.Error(), "content-addressed path") {
		t.Fatalf("conflicting fuzz overwrite error = %v", err)
	}
	stillCorrupt, err = os.ReadFile(first.FuzzCorpusPath)
	if err != nil {
		t.Fatalf("read conflicting fuzz corpus: %v", err)
	}
	if !bytes.Equal(stillCorrupt, corruptFuzz) {
		t.Fatalf("conflicting fuzz corpus was overwritten")
	}
}
