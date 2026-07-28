package lazily

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
)

type stdlibFixture struct {
	Feature   string `json:"feature"`
	Scenarios []struct {
		ID    string       `json:"id"`
		Steps []stdlibStep `json:"steps"`
	} `json:"scenarios"`
	Mutations []struct {
		Operator string   `json:"operator"`
		MustFail []string `json:"must_fail"`
	} `json:"mutations"`
}

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
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return fixture
}

func TestStdlibConformance(t *testing.T) {
	for _, name := range []string{"timer.json", "timeout.json", "revision_barrier.json"} {
		name := name
		t.Run(name, func(t *testing.T) {
			fixture := loadStdlibFixture(t, name)
			scenarios := make(map[string]bool, len(fixture.Scenarios))
			for _, scenario := range fixture.Scenarios {
				scenarios[scenario.ID] = true
				t.Run(scenario.ID, func(t *testing.T) {
					replayStdlibScenario(t, fixture.Feature, scenario.Steps)
				})
			}
			for _, mutation := range fixture.Mutations {
				if len(mutation.MustFail) == 0 {
					t.Fatalf("mutation %q has no required kill", mutation.Operator)
				}
				for _, id := range mutation.MustFail {
					if !scenarios[id] {
						t.Fatalf("mutation %q references missing scenario %q", mutation.Operator, id)
					}
				}
			}
		})
	}
}

func replayStdlibScenario(t *testing.T, feature string, steps []stdlibStep) {
	t.Helper()
	switch feature {
	case "stdlib_timer_v1":
		replayStdlibTimer(t, steps)
	case "stdlib_timeout_v1":
		replayStdlibTimeout(t, steps)
	case "stdlib_revision_barrier_v1":
		replayStdlibBarrier(t, steps)
	default:
		t.Fatalf("unsupported stdlib feature %q", feature)
	}
}

func replayStdlibTimer(t *testing.T, steps []stdlibStep) {
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
		assertStdlibExpectation(t, index, step.Expect, actual)
	}
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

func replayStdlibTimeout(t *testing.T, steps []stdlibStep) {
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
		assertStdlibExpectation(t, index, step.Expect, actual)
	}
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

func replayStdlibBarrier(t *testing.T, steps []stdlibStep) {
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
		assertStdlibExpectation(t, index, step.Expect, actual)
	}
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

func assertStdlibExpectation(t *testing.T, step int, expectedRaw json.RawMessage, actual any) {
	t.Helper()
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
		t.Fatalf("step %d:\n  got  %s\n  want %s", step, actualRaw, expectedRaw)
	}
}
