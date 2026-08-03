package lazily

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
)

type stdlibFixture struct {
	Schema         string `json:"$schema"`
	FixtureVersion int    `json:"fixture_version"`
	Feature        string `json:"feature"`
	// The three floors are the fixture's own "did you actually check anything"
	// guards, and this runner used to drop all three on the floor: the fixture
	// stated a minimum scenario, assertion, and mutation count, and the replay
	// reported green without ever comparing itself against any of them.
	ScenarioFloor  int              `json:"scenario_floor"`
	AssertionFloor int              `json:"assertion_floor"`
	MutationFloor  int              `json:"mutation_floor"`
	Scenarios      []stdlibScenario `json:"scenarios"`
	Mutations      []struct {
		Operator string   `json:"operator"`
		MustFail []string `json:"must_fail"`
	} `json:"mutations"`
}

type stdlibScenario struct {
	ID    string       `json:"id"`
	Steps []stdlibStep `json:"steps"`
}

// stdlibFixtureVersion is the fixture shape this runner understands.
const stdlibFixtureVersion = 1

type stdlibStep struct {
	Op               string          `json:"op"`
	Now              uint64          `json:"now"`
	Duration         uint64          `json:"duration"`
	Operation        string          `json:"operation"`
	Value            string          `json:"value"`
	Cancellation     string          `json:"cancellation"`
	Revision         uint64          `json:"revision"`
	RequiredRevision uint64          `json:"required_revision"`
	ObservedRevision uint64          `json:"observed_revision"`
	Deadline         *uint64         `json:"deadline"`
	Predicate        bool            `json:"predicate"`
	Key              string          `json:"key"`
	Expect           json.RawMessage `json:"expect"`
}

func loadStdlibFixture(t *testing.T, name string) stdlibFixture {
	t.Helper()
	path := filepath.Join("..", "lazily-spec", "conformance", "stdlib", name)
	raw, err := specReadFile(path)
	if err != nil {
		t.Skipf("lazily-spec fixture absent: %s", path)
	}
	var fixture stdlibFixture
	mustStrictJSON(t, name, raw, &fixture)
	if fixture.FixtureVersion != stdlibFixtureVersion {
		t.Fatalf("%s: fixture_version = %d, this runner replays %d", name, fixture.FixtureVersion, stdlibFixtureVersion)
	}
	return fixture
}

func TestStdlibConformance(t *testing.T) {
	for _, name := range []string{"timer.json", "timeout.json", "revision_barrier.json"} {
		name := name
		t.Run(name, func(t *testing.T) {
			fixture := loadStdlibFixture(t, name)
			scenarios := make(map[string]bool, len(fixture.Scenarios))
			assertions := 0
			for index, scenario := range fixture.Scenarios {
				if scenarios[scenario.ID] {
					t.Fatalf("duplicate scenario id %q", scenario.ID)
				}
				scenarios[scenario.ID] = true
				// Rung 4 books at the point of replay, on the payload handed to
				// the replay helper (#lzscenariobodyskip) — never before the
				// duplicate-id check above, which can `t.Fatalf` past it.
				//
				// The floor counts assertions this runner performed, so the
				// replay has to happen inline rather than in a subtest whose
				// tally the parent never sees.
				sv := &typedScenarioView[stdlibScenario]{
					fixture: filepath.Join("stdlib", name), index: index,
					id: scenario.ID, scenario: scenario,
				}
				replayed := sv.Value()
				assertions += replayStdlibScenario(t, replayed.ID, fixture.Feature, replayed.Steps)
			}
			// A mutation counts toward the floor only once every scenario it
			// claims to kill has actually been REPLAYED above. Counted rather
			// than assumed (#lznullformblind): `len(fixture.Mutations)` is the
			// fixture's own array length, so comparing it to the fixture's own
			// `mutation_floor` stays green with the replay loop deleted, and a
			// mutation bound to nothing kills nothing.
			mutationsBound := 0
			for _, mutation := range fixture.Mutations {
				if len(mutation.MustFail) == 0 {
					t.Fatalf("mutation %q has no required kill", mutation.Operator)
				}
				for _, id := range mutation.MustFail {
					if !scenarios[id] {
						t.Fatalf("mutation %q references missing scenario %q", mutation.Operator, id)
					}
				}
				mutationsBound++
			}
			if got := len(scenarios); got < fixture.ScenarioFloor {
				t.Fatalf("replayed %d scenarios, below scenario_floor %d", got, fixture.ScenarioFloor)
			}
			if assertions < fixture.AssertionFloor {
				t.Fatalf("performed %d assertions, below assertion_floor %d", assertions, fixture.AssertionFloor)
			}
			if mutationsBound < fixture.MutationFloor {
				t.Fatalf("%d mutations are bound to scenarios this run replayed, below "+
					"mutation_floor %d", mutationsBound, fixture.MutationFloor)
			}
		})
	}
}

// replayStdlibScenario returns the number of assertions it performed — one per
// key compared in a step's `expect`, matching how the canonical lazily-spec
// runner counts toward `assertion_floor`.
func replayStdlibScenario(t *testing.T, id, feature string, steps []stdlibStep) int {
	t.Helper()
	switch feature {
	case "stdlib_timer_v1":
		return replayStdlibTimer(t, id, steps)
	case "stdlib_timeout_v1":
		return replayStdlibTimeout(t, id, steps)
	case "stdlib_revision_barrier_v1":
		return replayStdlibBarrier(t, id, steps)
	default:
		t.Fatalf("unsupported stdlib feature %q", feature)
		return 0
	}
}

func replayStdlibTimer(t *testing.T, id string, steps []stdlibStep) int {
	asserted := 0
	var timer *Timer
	for index, step := range steps {
		var actual map[string]any
		switch step.Op {
		case "start":
			var err error
			timer, err = NewTimer(step.Now, step.Duration)
			if err == TimerDeadlineOverflow {
				actual = map[string]any{"outcome": "unavailable", "reason": string(TimerDeadlineOverflow)}
			} else if err != nil {
				t.Fatalf("step %d: start: %v", index, err)
			} else {
				deadline, _ := CheckedDeadline(step.Now, step.Duration)
				actual = map[string]any{"outcome": "pending", "deadline": deadline}
			}
		case "observe":
			if timer == nil {
				t.Fatalf("step %d: observe before successful start", index)
			}
			observation, err := timer.Observe(step.Now)
			actual = timerJSON(observation, err)
		default:
			t.Fatalf("step %d: unsupported timer op %q", index, step.Op)
		}
		asserted += assertStdlibExpectation(t, id, index, step.Expect, actual)
	}
	return asserted
}

func timerJSON(observation TimerObservation, err error) map[string]any {
	if err != nil {
		return map[string]any{
			"outcome": "unavailable", "reason": err.Error(), "deadline": observation.Deadline,
		}
	}
	if observation.Outcome == "fired" {
		return map[string]any{"outcome": "fired", "fired_at": observation.FiredAt}
	}
	return map[string]any{"outcome": "pending", "deadline": observation.Deadline}
}

func replayStdlibTimeout(t *testing.T, id string, steps []stdlibStep) int {
	asserted := 0
	var timeout *Timeout[string]
	for index, step := range steps {
		var actual map[string]any
		switch step.Op {
		case "start":
			var err error
			timeout, err = NewTimeout[string](step.Now, step.Duration)
			if err != nil {
				actual = map[string]any{"outcome": "unavailable", "reason": err.Error()}
			} else {
				deadline, _ := CheckedDeadline(step.Now, step.Duration)
				actual = map[string]any{"outcome": "pending", "deadline": deadline}
			}
		case "poll":
			if timeout == nil {
				t.Fatalf("step %d: poll before start", index)
			}
			operationCalls, cancellationCalls := 0, 0
			observation := timeout.Poll(step.Now, func() TimeoutOperation[string] {
				operationCalls++
				switch step.Operation {
				case "pending":
					return PendingOperation[string]()
				case "completed":
					return CompletedOperation(step.Value)
				case "unavailable":
					return UnavailableOperation[string]()
				default:
					t.Fatalf("step %d: unsupported operation %q", index, step.Operation)
					return PendingOperation[string]()
				}
			}, func() TimeoutCancellation {
				cancellationCalls++
				return TimeoutCancellation(step.Cancellation)
			})
			actual = timeoutJSON(observation, operationCalls, cancellationCalls)
		default:
			t.Fatalf("step %d: unsupported timeout op %q", index, step.Op)
		}
		asserted += assertStdlibExpectation(t, id, index, step.Expect, actual)
	}
	return asserted
}

func timeoutJSON(
	observation TimeoutObservation[string],
	operationCalls, cancellationCalls int,
) map[string]any {
	if observation.Outcome == "pending" {
		return map[string]any{
			"outcome": "pending", "deadline": observation.Deadline,
			"operation_calls": operationCalls, "cancellation_calls": cancellationCalls,
		}
	}
	actual := map[string]any{
		"outcome":         observation.Outcome,
		"operation_calls": operationCalls, "cancellation_calls": cancellationCalls,
	}
	if observation.Outcome == "completed" {
		actual["value"] = observation.Value
	}
	if observation.Reason != "" {
		actual["reason"] = observation.Reason
	}
	return actual
}

func replayStdlibBarrier(t *testing.T, id string, steps []stdlibStep) int {
	asserted := 0
	var barrier *RevisionBarrier
	for index, step := range steps {
		cancellationCalls := 0
		var observation RevisionBarrierObservation
		switch step.Op {
		case "start":
			barrier = NewRevisionBarrier(step.Revision, step.RequiredRevision, step.Deadline)
			observation = barrier.Receipt("")
		case "observe":
			if barrier == nil {
				t.Fatalf("step %d: observe before start", index)
			}
			observation = barrier.Observe(step.Now, step.Predicate, func() TimeoutCancellation {
				cancellationCalls++
				return TimeoutCancellation(step.Cancellation)
			})
		case "register_recheck":
			observation = barrier.RegisterRecheck(step.Now, step.ObservedRevision, step.Predicate)
		case "advance":
			observation = barrier.Advance(step.Revision, step.Predicate)
		case "dispose":
			observation = barrier.Dispose()
		case "receipt":
			observation = barrier.Receipt(step.Key)
		default:
			t.Fatalf("step %d: unsupported barrier op %q", index, step.Op)
		}
		actual := barrierJSON(observation)
		if step.Op == "observe" {
			actual["cancellation_calls"] = cancellationCalls
		}
		asserted += assertStdlibExpectation(t, id, index, step.Expect, actual)
	}
	return asserted
}

func barrierJSON(observation RevisionBarrierObservation) map[string]any {
	actual := map[string]any{
		"outcome":  observation.Outcome,
		"revision": observation.Revision, "generation": observation.Generation,
	}
	if observation.Reason != "" {
		actual["reason"] = observation.Reason
	}
	return actual
}

// assertStdlibExpectation compares the whole `expect` object — an unmodelled key
// in the fixture shows up as a DeepEqual mismatch, so this block already fails
// closed — and returns how many keys it compared.
func assertStdlibExpectation(t *testing.T, id string, step int, expectedRaw json.RawMessage, actual any) int {
	t.Helper()
	var expectedKeys map[string]json.RawMessage
	if err := json.Unmarshal(expectedRaw, &expectedKeys); err != nil {
		t.Fatalf("%s step %d: expect must be an object: %v", id, step, err)
	}
	if len(expectedKeys) == 0 {
		t.Fatalf("%s step %d: expect is empty — the step asserts nothing", id, step)
	}
	var expected any
	decoder := json.NewDecoder(bytes.NewReader(expectedRaw))
	decoder.UseNumber()
	if err := decoder.Decode(&expected); err != nil {
		t.Fatalf("step %d: decode expected: %v", step, err)
	}
	actualRaw, err := json.Marshal(actual)
	if err != nil {
		t.Fatalf("step %d: encode actual: %v", step, err)
	}
	var normalized any
	decoder = json.NewDecoder(bytes.NewReader(actualRaw))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		t.Fatalf("step %d: decode actual: %v", step, err)
	}
	if !reflect.DeepEqual(normalized, expected) {
		t.Fatalf("%s step %d:\n  got  %s\n  want %s", id, step, actualRaw, expectedRaw)
	}
	return len(expectedKeys)
}
