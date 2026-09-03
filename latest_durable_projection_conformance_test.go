package lazily

import (
	"fmt"
	"testing"
)

const latestDurableFixture = "egress/latest_durable_projection.json"

type latestDurableFixtureRevision struct {
	Epoch uint64 `json:"epoch"`
	Value string `json:"value"`
}

type latestDurableFixtureEnvelope struct {
	Generation uint64 `json:"generation"`
	Key        string `json:"key"`
	Epoch      uint64 `json:"epoch"`
	Value      string `json:"value"`
}

type latestDurableFixtureEntry struct {
	Key            string                        `json:"key"`
	Desired        *latestDurableFixtureRevision `json:"desired"`
	Inflight       *latestDurableFixtureEnvelope `json:"inflight"`
	DurableThrough *uint64                       `json:"durable_through"`
}

type latestDurableFixtureState struct {
	Generation uint64                      `json:"generation"`
	Entries    []latestDurableFixtureEntry `json:"entries"`
}

type latestDurableFixtureReturn struct {
	Upsert         string                        `json:"upsert,omitempty"`
	Claim          string                        `json:"claim,omitempty"`
	Ack            string                        `json:"ack,omitempty"`
	Failure        string                        `json:"failure,omitempty"`
	Reconnect      string                        `json:"reconnect,omitempty"`
	Envelope       *latestDurableFixtureEnvelope `json:"envelope,omitempty"`
	DurableThrough *uint64                       `json:"durable_through,omitempty"`
	Current        *uint64                       `json:"current,omitempty"`
	Generation     *uint64                       `json:"generation,omitempty"`
	Requeued       *int                          `json:"requeued,omitempty"`
	Superseded     *int                          `json:"superseded,omitempty"`
}

type latestDurableFixtureOp struct {
	Type       string `json:"type"`
	Key        string `json:"key,omitempty"`
	Epoch      uint64 `json:"epoch,omitempty"`
	Value      string `json:"value,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
}

type latestDurableFixtureStep struct {
	Op       latestDurableFixtureOp     `json:"op"`
	Returns  latestDurableFixtureReturn `json:"returns"`
	Expected latestDurableFixtureState  `json:"expected"`
}

type latestDurableFixtureScenario struct {
	ID         string                     `json:"id"`
	Generation uint64                     `json:"generation"`
	Steps      []latestDurableFixtureStep `json:"steps"`
}

type latestDurableFixtureDocument struct {
	Description string                         `json:"description"`
	Kind        string                         `json:"kind"`
	Model       string                         `json:"model"`
	Scenarios   []latestDurableFixtureScenario `json:"scenarios"`
}

type latestDurableFixtureModel interface {
	UpsertDesired(string, uint64, string) LatestDurableUpsert
	Claim(string, uint64) LatestDurableClaim[string, string]
	AckApplied(string, uint64, uint64) LatestDurableAck
	FailRetryable(string, uint64, uint64) LatestDurableFailure
	Reconnect(uint64) LatestDurableReconnect
	Generation() uint64
	Count() int
	State(string) (LatestDurableKeyState[string, string], bool)
}

func requireU64(t *testing.T, value *uint64, field string) uint64 {
	t.Helper()
	if value == nil {
		t.Fatalf("missing return field %s", field)
	}
	return *value
}

func requireInt(t *testing.T, value *int, field string) int {
	t.Helper()
	if value == nil {
		t.Fatalf("missing return field %s", field)
	}
	return *value
}

func assertLatestDurableReturn(t *testing.T, model latestDurableFixtureModel, step latestDurableFixtureStep) {
	t.Helper()
	op, expected := step.Op, step.Returns
	switch op.Type {
	case "upsert_desired":
		actual := model.UpsertDesired(op.Key, op.Epoch, op.Value)
		if string(actual.Kind) != expected.Upsert {
			t.Fatalf("upsert=%q, want %q", actual.Kind, expected.Upsert)
		}
		switch actual.Kind {
		case LatestDurableUpsertAlreadyDurable:
			if actual.DurableThrough != requireU64(t, expected.DurableThrough, "durable_through") {
				t.Fatalf("durable_through=%d, want %d", actual.DurableThrough, *expected.DurableThrough)
			}
		case LatestDurableUpsertStaleEpoch:
			if actual.Current != requireU64(t, expected.Current, "current") {
				t.Fatalf("current=%d, want %d", actual.Current, *expected.Current)
			}
		}
	case "claim":
		actual := model.Claim(op.Key, op.Generation)
		if string(actual.Kind) != expected.Claim {
			t.Fatalf("claim=%q, want %q", actual.Kind, expected.Claim)
		}
		if actual.Kind == LatestDurableClaimClaimed {
			want := expected.Envelope
			if want == nil || actual.Envelope.Generation != want.Generation || actual.Envelope.Key != want.Key || actual.Envelope.Epoch != want.Epoch || actual.Envelope.Value != want.Value {
				t.Fatalf("envelope=%+v, want %+v", actual.Envelope, want)
			}
		} else if actual.Kind == LatestDurableClaimStaleGeneration && actual.Current != requireU64(t, expected.Current, "current") {
			t.Fatalf("current=%d, want %d", actual.Current, *expected.Current)
		}
	case "ack_applied":
		actual := model.AckApplied(op.Key, op.Generation, op.Epoch)
		if string(actual.Kind) != expected.Ack {
			t.Fatalf("ack=%q, want %q", actual.Kind, expected.Ack)
		}
		if actual.Kind == LatestDurableAckAdvanced || actual.Kind == LatestDurableAckUnchanged {
			if actual.DurableThrough != requireU64(t, expected.DurableThrough, "durable_through") {
				t.Fatalf("durable_through=%d, want %d", actual.DurableThrough, *expected.DurableThrough)
			}
		} else if actual.Kind == LatestDurableAckStaleGeneration && actual.Current != requireU64(t, expected.Current, "current") {
			t.Fatalf("current=%d, want %d", actual.Current, *expected.Current)
		}
	case "fail_retryable":
		actual := model.FailRetryable(op.Key, op.Generation, op.Epoch)
		if string(actual.Kind) != expected.Failure {
			t.Fatalf("failure=%q, want %q", actual.Kind, expected.Failure)
		}
		if actual.Kind == LatestDurableFailureStaleGeneration && actual.Current != requireU64(t, expected.Current, "current") {
			t.Fatalf("current=%d, want %d", actual.Current, *expected.Current)
		}
	case "reconnect":
		actual := model.Reconnect(op.Generation)
		if string(actual.Kind) != expected.Reconnect {
			t.Fatalf("reconnect=%q, want %q", actual.Kind, expected.Reconnect)
		}
		switch actual.Kind {
		case LatestDurableReconnectAdvanced:
			if actual.Generation != requireU64(t, expected.Generation, "generation") || actual.Requeued != requireInt(t, expected.Requeued, "requeued") || actual.Superseded != requireInt(t, expected.Superseded, "superseded") {
				t.Fatalf("reconnect=%+v, want %+v", actual, expected)
			}
		case LatestDurableReconnectUnchanged:
			if actual.Generation != requireU64(t, expected.Generation, "generation") {
				t.Fatalf("generation=%d, want %d", actual.Generation, *expected.Generation)
			}
		case LatestDurableReconnectStaleGeneration:
			if actual.Current != requireU64(t, expected.Current, "current") {
				t.Fatalf("current=%d, want %d", actual.Current, *expected.Current)
			}
		}
	default:
		t.Fatalf("unknown operation %q", op.Type)
	}
}

func assertLatestDurableState(t *testing.T, model latestDurableFixtureModel, expected latestDurableFixtureState) {
	t.Helper()
	if model.Generation() != expected.Generation || model.Count() != len(expected.Entries) {
		t.Fatalf("state shape=(generation %d, entries %d), want (%d, %d)", model.Generation(), model.Count(), expected.Generation, len(expected.Entries))
	}
	for _, want := range expected.Entries {
		actual, ok := model.State(want.Key)
		if !ok {
			t.Fatalf("missing key %q", want.Key)
		}
		if (actual.Desired == nil) != (want.Desired == nil) || (actual.Inflight == nil) != (want.Inflight == nil) || (actual.DurableThrough == nil) != (want.DurableThrough == nil) {
			t.Fatalf("key %q optional shape=%+v, want %+v", want.Key, actual, want)
		}
		if want.Desired != nil && (actual.Desired.Epoch != want.Desired.Epoch || actual.Desired.Value != want.Desired.Value) {
			t.Fatalf("key %q desired=%+v, want %+v", want.Key, actual.Desired, want.Desired)
		}
		if want.Inflight != nil && (actual.Inflight.Generation != want.Inflight.Generation || actual.Inflight.Key != want.Inflight.Key || actual.Inflight.Epoch != want.Inflight.Epoch || actual.Inflight.Value != want.Inflight.Value) {
			t.Fatalf("key %q inflight=%+v, want %+v", want.Key, actual.Inflight, want.Inflight)
		}
		if want.DurableThrough != nil && *actual.DurableThrough != *want.DurableThrough {
			t.Fatalf("key %q durable_through=%d, want %d", want.Key, *actual.DurableThrough, *want.DurableThrough)
		}
	}
}

func replayLatestDurable(t *testing.T, model latestDurableFixtureModel, scenario latestDurableFixtureScenario) int {
	t.Helper()
	for index, step := range scenario.Steps {
		t.Run(fmt.Sprintf("%02d_%s", index, step.Op.Type), func(t *testing.T) {
			assertLatestDurableReturn(t, model, step)
			assertLatestDurableState(t, model, step.Expected)
		})
	}
	return len(scenario.Steps)
}

func TestLatestDurableProjectionFixtureAllFlavors(t *testing.T) {
	data := loadConformanceFixture(t, "egress", "latest_durable_projection.json")
	var fixture latestDurableFixtureDocument
	mustStrictJSON(t, latestDurableFixture, data, &fixture)
	if fixture.Description == "" {
		t.Fatal("fixture description is empty")
	}
	if fixture.Kind != "LatestDurableProjection" || fixture.Model != "LatestDurableProjectionCore" {
		t.Fatalf("fixture kind/model=(%q,%q)", fixture.Kind, fixture.Model)
	}
	for index, scenario := range fixture.Scenarios {
		recordScenarioAt(latestDurableFixture, index, scenario.ID, "")
		t.Run(scenario.ID, func(t *testing.T) {
			ctx := NewContext()
			syncModel := NewLatestDurableProjection[string, string](ctx, scenario.Generation)
			threadModel := NewThreadSafeLatestDurableProjection[string, string](NewThreadSafeContext(), scenario.Generation)
			asyncCtx := NewAsyncContext()
			defer asyncCtx.Close()
			asyncModel := NewAsyncLatestDurableProjection[string, string](asyncCtx, scenario.Generation)

			syncSteps := replayLatestDurable(t, syncModel, scenario)
			threadSteps := replayLatestDurable(t, threadModel, scenario)
			asyncSteps := replayLatestDurable(t, asyncModel, scenario)
			if syncSteps == 0 || threadSteps != syncSteps || asyncSteps != syncSteps {
				t.Fatalf("replay steps sync/thread/async=%d/%d/%d", syncSteps, threadSteps, asyncSteps)
			}
		})
	}
}

func TestLatestDurableProjectionReactiveChangeBoundary(t *testing.T) {
	projection := NewLatestDurableProjection[string, string](NewContext(), 1)
	if got := projection.StateReader().Peek(); got != 0 {
		t.Fatalf("initial state version=%d", got)
	}
	projection.UpsertDesired("doc", 1, "A")
	projection.UpsertDesired("doc", 1, "A")
	if got := projection.StateReader().Peek(); got != 1 {
		t.Fatalf("state version=%d, want one true transition", got)
	}
}

func TestLatestDurableProjectionCoreRejectsConflictsAndPreservesSupersession(t *testing.T) {
	core := NewLatestDurableProjectionCore[string, string](7)
	_, accepted := core.UpsertDesired("doc", 1, "A")
	_, unchanged := core.UpsertDesired("doc", 1, "A")
	_, conflict := core.UpsertDesired("doc", 1, "other")
	_, staleClaim := core.Claim("doc", 6)
	if accepted.Kind != LatestDurableUpsertAccepted || unchanged.Kind != LatestDurableUpsertUnchanged || conflict.Kind != LatestDurableUpsertEpochConflict || staleClaim.Kind != LatestDurableClaimStaleGeneration {
		t.Fatalf("unexpected outcomes accepted=%+v unchanged=%+v conflict=%+v stale=%+v", accepted, unchanged, conflict, staleClaim)
	}
	core.Claim("doc", 7)
	core.UpsertDesired("doc", 2, "B")
	_, failed := core.FailRetryable("doc", 7, 1)
	state, _ := core.State("doc")
	if failed.Kind != LatestDurableFailureSuperseded || state.Desired == nil || state.Desired.Epoch != 2 || state.Inflight != nil {
		t.Fatalf("superseded failure=%+v state=%+v", failed, state)
	}
	_, unchangedReconnect := core.Reconnect(7)
	_, staleReconnect := core.Reconnect(6)
	if unchangedReconnect.Kind != LatestDurableReconnectUnchanged || staleReconnect.Kind != LatestDurableReconnectStaleGeneration {
		t.Fatalf("reconnect outcomes unchanged=%+v stale=%+v", unchangedReconnect, staleReconnect)
	}
}
