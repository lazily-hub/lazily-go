package lazily

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

const simTestSeedHex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

func mustSimTestSeed(t *testing.T) SimSeed {
	t.Helper()
	seed, err := ParseSimSeed(simTestSeedHex)
	if err != nil {
		t.Fatalf("ParseSimSeed: %v", err)
	}
	return seed
}

func mustBuildSimTestWorld(t *testing.T, spec SimWorldSpec) *SimWorld {
	t.Helper()
	world, err := BuildSimWorld(spec)
	if err != nil {
		t.Fatalf("BuildSimWorld: %v", err)
	}
	return world
}

func simTestAction(id, actorID string, payload any) SimAction {
	return SimAction{ID: id, ActorID: actorID, Kind: "test.action", Payload: payload}
}

func mustSimUint64(t *testing.T, stream *SimRandomStream) uint64 {
	t.Helper()
	value, err := stream.Uint64()
	if err != nil {
		t.Fatalf("Uint64: %v", err)
	}
	return value
}

func TestSimWorldSameSeedAndInputsProduceByteIdenticalTraceWithoutWallClockDependence(t *testing.T) {
	run := func() ([]byte, string) {
		world := mustBuildSimTestWorld(t, SimWorldSpec{
			Seed:           mustSimTestSeed(t),
			SubjectKind:    "counter",
			SubjectVersion: "1",
		})
		value := 0
		actor := SimActorFuncs{
			DecideFunc: func(action SimAction) (SimDecision, error) {
				delta, ok := action.Payload.(int)
				if !ok {
					return SimDecision{}, fmt.Errorf("payload is %T, want int", action.Payload)
				}
				value += delta
				return SimDecision{Accepted: true}, nil
			},
			ObserveFunc: func() map[string]any { return map[string]any{"value": value} },
		}
		if err := world.RegisterActor("counter", actor); err != nil {
			t.Fatalf("RegisterActor: %v", err)
		}
		if _, err := world.Schedule(3, simTestAction("add.one", "counter", 1)); err != nil {
			t.Fatalf("schedule add.one: %v", err)
		}
		if _, err := world.Schedule(3, simTestAction("add.two", "counter", 2)); err != nil {
			t.Fatalf("schedule add.two: %v", err)
		}
		result, err := world.RunUntilIdle(4)
		if err != nil {
			t.Fatalf("RunUntilIdle: %v", err)
		}
		if result.Status != SimRunIdle || value != 3 {
			t.Fatalf("run = %+v, value = %d; want idle and 3", result, value)
		}
		traceBytes, err := world.TraceBytes()
		if err != nil {
			t.Fatalf("TraceBytes: %v", err)
		}
		digest, err := world.TraceDigest()
		if err != nil {
			t.Fatalf("TraceDigest: %v", err)
		}
		return traceBytes, digest
	}

	firstBytes, firstDigest := run()
	// Crossing a real wall-clock boundary must not affect a logical-time world.
	time.Sleep(2 * time.Millisecond)
	secondBytes, secondDigest := run()
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("identical seed and inputs produced different trace bytes")
	}
	if firstDigest != secondDigest {
		t.Fatalf("identical seed and inputs produced digests %q and %q", firstDigest, secondDigest)
	}
}

func TestSimWorldOrdersByTimeThenOrdinalIncludingSameTickDecisionEnqueue(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	var executed []SimScheduledItem
	actor := SimActorFuncs{
		DecideFunc: func(action SimAction) (SimDecision, error) {
			if action.ID == "root" {
				return SimDecision{
					Accepted: true,
					Schedule: []SimScheduledAction{{
						At:     7,
						Action: simTestAction("child", "worker", nil),
					}},
				}, nil
			}
			return SimDecision{Accepted: true}, nil
		},
	}
	if err := world.RegisterActor("worker", actor); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	root, err := world.Schedule(7, simTestAction("root", "worker", nil))
	if err != nil {
		t.Fatalf("schedule root: %v", err)
	}
	peer, err := world.Schedule(7, simTestAction("peer", "worker", nil))
	if err != nil {
		t.Fatalf("schedule peer: %v", err)
	}
	early, err := world.Schedule(6, simTestAction("early", "worker", nil))
	if err != nil {
		t.Fatalf("schedule early: %v", err)
	}
	if root.Ordinal != 0 || peer.Ordinal != 1 || early.Ordinal != 2 {
		t.Fatalf("enqueue ordinals = (%d,%d,%d), want (0,1,2)", root.Ordinal, peer.Ordinal, early.Ordinal)
	}

	for world.Pending() > 0 {
		step, stepErr := world.Step()
		if stepErr != nil {
			t.Fatalf("Step: %v", stepErr)
		}
		executed = append(executed, step.Item)
	}
	gotIDs := make([]string, len(executed))
	for i, item := range executed {
		gotIDs[i] = item.Action.ID
	}
	wantIDs := []string{"early", "root", "peer", "child"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("execution order = %v, want %v", gotIDs, wantIDs)
	}
	if executed[3].At != 7 || executed[3].Ordinal != 3 {
		t.Fatalf("same-tick child = %+v, want at=7 ordinal=3", executed[3])
	}
	if executed[3].Action.CauseID != "root" {
		t.Fatalf("child cause = %q, want root", executed[3].Action.CauseID)
	}

	var dequeued []struct{ at, ordinal uint64 }
	for _, entry := range world.Trace().Entries {
		if entry.Kind == "dequeue" {
			dequeued = append(dequeued, struct{ at, ordinal uint64 }{entry.At, entry.Ordinal})
		}
	}
	wantDequeued := []struct{ at, ordinal uint64 }{{6, 2}, {7, 0}, {7, 1}, {7, 3}}
	if !reflect.DeepEqual(dequeued, wantDequeued) {
		t.Fatalf("dequeue keys = %v, want %v", dequeued, wantDequeued)
	}
}

func TestSimWorldRejectsPastSchedulingWithoutMovingLogicalTime(t *testing.T) {
	world := mustBuildSimTestWorld(t, SimWorldSpec{Seed: mustSimTestSeed(t), InitialTime: 10})
	if err := world.RegisterActor("worker", SimActorFuncs{
		DecideFunc: func(SimAction) (SimDecision, error) { return SimDecision{Accepted: true}, nil },
	}); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	_, err := world.Schedule(9, simTestAction("too.early", "worker", nil))
	if err == nil || !errors.Is(err, ErrSimulation) || !strings.Contains(err.Error(), "past tick") {
		t.Fatalf("past schedule error = %v", err)
	}
	if world.Now() != 10 || world.Pending() != 0 || world.Steps() != 0 {
		t.Fatalf("past schedule mutated world: now=%d pending=%d steps=%d", world.Now(), world.Pending(), world.Steps())
	}
	entries := world.Trace().Entries
	if len(entries) == 0 || entries[len(entries)-1].Kind != "error" {
		t.Fatalf("past schedule was not made visible in trace: %+v", entries)
	}
}

func TestSimWorldBoundedRunnersExposeExhaustionAndIdle(t *testing.T) {
	newChainWorld := func() *SimWorld {
		world := NewSimWorld(mustSimTestSeed(t))
		actor := SimActorFuncs{
			DecideFunc: func(action SimAction) (SimDecision, error) {
				remaining := action.Payload.(int)
				decision := SimDecision{Accepted: true}
				if remaining > 1 {
					decision.Schedule = []SimScheduledAction{{
						At:     world.Now(),
						Action: simTestAction(fmt.Sprintf("chain.%d", remaining-1), "worker", remaining-1),
					}}
				}
				return decision, nil
			},
		}
		if err := world.RegisterActor("worker", actor); err != nil {
			t.Fatalf("RegisterActor: %v", err)
		}
		if _, err := world.Submit(simTestAction("chain.3", "worker", 3)); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		return world
	}

	bounded := newChainWorld()
	result, err := bounded.RunUntilIdle(2)
	if err != nil {
		t.Fatalf("RunUntilIdle bounded: %v", err)
	}
	if result.Status != SimRunBoundExhausted || result.Limit != "max_steps" || result.Steps != 2 {
		t.Fatalf("bounded result = %+v", result)
	}
	if bounded.Pending() != 1 || !bounded.Trace().HasTerminal || bounded.Trace().Terminal.Status != SimRunBoundExhausted {
		t.Fatalf("bounded world did not retain visible exhaustion: pending=%d trace=%+v", bounded.Pending(), bounded.Trace().Terminal)
	}

	stepped := newChainWorld()
	stepResult, err := stepped.RunSteps(1)
	if err != nil {
		t.Fatalf("RunSteps bounded: %v", err)
	}
	if stepResult.Status != SimRunBoundExhausted || stepResult.Steps != 1 || stepped.Pending() != 1 {
		t.Fatalf("RunSteps result = %+v pending=%d", stepResult, stepped.Pending())
	}

	idle := newChainWorld()
	idleResult, err := idle.RunUntilIdle(3)
	if err != nil {
		t.Fatalf("RunUntilIdle idle: %v", err)
	}
	if idleResult.Status != SimRunIdle || idleResult.Steps != 3 || idle.Pending() != 0 {
		t.Fatalf("idle result = %+v pending=%d", idleResult, idle.Pending())
	}
}

func TestSimWorldActorLifecycleGatesDomainActionsAndReplayProjection(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	decisions := 0
	if err := world.RegisterActor("worker", SimActorFuncs{
		DecideFunc: func(SimAction) (SimDecision, error) {
			decisions++
			return SimDecision{Accepted: true}, nil
		},
	}); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	if _, err := world.ScheduleLifecycle(0, "worker", SimActorCrashed); err != nil {
		t.Fatalf("schedule crash: %v", err)
	}
	if _, err := world.Submit(simTestAction("while.crashed", "worker", nil)); err != nil {
		t.Fatalf("submit while crashed: %v", err)
	}
	if _, err := world.ScheduleLifecycle(0, "worker", SimActorRunning); err != nil {
		t.Fatalf("schedule restart: %v", err)
	}
	if _, err := world.Submit(simTestAction("after.restart", "worker", nil)); err != nil {
		t.Fatalf("submit after restart: %v", err)
	}
	if _, err := world.ScheduleLifecycle(0, "worker", SimActorStopped); err != nil {
		t.Fatalf("schedule stop: %v", err)
	}
	if _, err := world.Submit(simTestAction("after.stop", "worker", nil)); err != nil {
		t.Fatalf("submit after stop: %v", err)
	}
	result, err := world.RunUntilIdle(6)
	if err != nil {
		t.Fatalf("RunUntilIdle: %v", err)
	}
	if result.Status != SimRunIdle || decisions != 1 {
		t.Fatalf("result=%+v decisions=%d, want idle and one production decision", result, decisions)
	}
	if status, ok := world.ActorStatus("worker"); !ok || status != SimActorStopped {
		t.Fatalf("actor status = %q, %v; want stopped", status, ok)
	}

	rejected := 0
	for _, entry := range world.Trace().Entries {
		if entry.Kind == "action_rejected" {
			rejected++
		}
	}
	if rejected != 2 {
		t.Fatalf("rejected action count = %d, want 2", rejected)
	}
	replay, err := world.ReplayLog()
	if err != nil {
		t.Fatalf("ReplayLog: %v", err)
	}
	if replay.Len() != 1 {
		t.Fatalf("replay length = %d, want one accepted domain action", replay.Len())
	}
	payload, ok := replay.Event(0).Payload.(SimReplayAction)
	if !ok || payload.Action.ID != "after.restart" {
		t.Fatalf("replay payload = %#v, want after.restart", replay.Event(0).Payload)
	}
}

func TestSimWorldCheckpointIncludesActorDecisionAndRegisteredObservations(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	value := 1
	actor := SimActorFuncs{
		DecideFunc: func(action SimAction) (SimDecision, error) {
			value += action.Payload.(int)
			return SimDecision{
				Accepted: true,
				Observations: []SimObservation{{
					ID: "step.outcome", Kind: "test.outcome", Version: "1", Value: "accepted",
				}},
			}, nil
		},
		ObserveFunc: func() map[string]any { return map[string]any{"state": value} },
	}
	if err := world.RegisterActor("machine", actor); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	if err := world.RegisterObservation("world.total", func() any { return value * 2 }); err != nil {
		t.Fatalf("RegisterObservation: %v", err)
	}
	if _, err := world.Schedule(4, simTestAction("advance", "machine", 2)); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	step, err := world.Step()
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !step.Executed || step.Checkpoint.Now != 4 || value != 3 {
		t.Fatalf("step=%+v value=%d", step, value)
	}
	gotIDs := make([]string, len(step.Checkpoint.Values))
	for i, observed := range step.Checkpoint.Values {
		gotIDs[i] = observed.ID
	}
	wantIDs := []string{"machine.state", "step.outcome", "world.total"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("checkpoint observation ids = %v, want %v", gotIDs, wantIDs)
	}
	entries := world.Trace().Entries
	if int(step.Checkpoint.TraceSeq) != len(entries)-1 || entries[len(entries)-1].Kind != "checkpoint" {
		t.Fatalf("checkpoint trace sequence = %d over %d entries", step.Checkpoint.TraceSeq, len(entries))
	}
	if step.Checkpoint.Digest == "" || step.Checkpoint.WorldDigest != entries[len(entries)-1].WorldDigest {
		t.Fatalf("checkpoint digests not bound to trace: %+v last=%+v", step.Checkpoint, entries[len(entries)-1])
	}
}

func TestSimStateMachineActorInvokesProductionTransitionExactlyOnce(t *testing.T) {
	ctx := NewContext()
	transitionCalls := 0
	transition := func(state, event int) (int, bool) {
		transitionCalls++
		return state + event, true
	}
	actor := NewSimStateMachineActor(
		ctx,
		10,
		transition,
		func(action SimAction) (int, error) { return action.Payload.(int), nil },
		func(state int) map[string]any { return map[string]any{"state": state} },
	)
	world := NewSimWorld(mustSimTestSeed(t))
	if err := world.RegisterActor("machine", actor); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	if _, err := world.Submit(simTestAction("add.five", "machine", 5)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	result, err := world.RunUntilIdle(1)
	if err != nil {
		t.Fatalf("RunUntilIdle: %v", err)
	}
	if result.Status != SimRunIdle || transitionCalls != 1 || actor.Machine().State() != 15 {
		t.Fatalf("result=%+v transitionCalls=%d state=%d", result, transitionCalls, actor.Machine().State())
	}
	replay, err := world.ReplayLog()
	if err != nil {
		t.Fatalf("ReplayLog: %v", err)
	}
	if replay.Len() != 1 || replay.Event(0).Name != "test.action" {
		t.Fatalf("production transition replay = %+v", replay.Events())
	}
}

func TestSimWorldNamedRNGAlgorithmAndStreamIsolationAreStable(t *testing.T) {
	if SimRNGAlgorithm != "lazily-sim-rng-v1" {
		t.Fatalf("RNG algorithm = %q", SimRNGAlgorithm)
	}
	world := NewSimWorld(mustSimTestSeed(t))
	stream, err := world.RandomStream("actor", "choice")
	if err != nil {
		t.Fatalf("RandomStream: %v", err)
	}
	if got := mustSimUint64(t, stream); got != 2508641176608179600 {
		t.Fatalf("first pinned draw = %d", got)
	}
	if got := mustSimUint64(t, stream); got != 8672581271090373035 {
		t.Fatalf("second pinned draw = %d", got)
	}
	again, err := world.RandomStream("actor", "choice")
	if err != nil {
		t.Fatalf("RandomStream repeat: %v", err)
	}
	if again != stream {
		t.Fatalf("same named stream returned a different stream instance")
	}
	if got := mustSimUint64(t, again); got != 1329232830732953697 {
		t.Fatalf("continued pinned draw = %d", got)
	}

	other, err := world.RandomStream("actor", "other")
	if err != nil {
		t.Fatalf("other RandomStream: %v", err)
	}
	if got := mustSimUint64(t, other); got != 15153126490100069942 {
		t.Fatalf("other named stream first draw = %d", got)
	}
	fresh := NewSimWorld(mustSimTestSeed(t))
	freshStream, err := fresh.RandomStream("actor", "choice")
	if err != nil {
		t.Fatalf("fresh RandomStream: %v", err)
	}
	if got := mustSimUint64(t, freshStream); got != 2508641176608179600 {
		t.Fatalf("fresh world first draw = %d", got)
	}
	if _, err := stream.Uint64N(0); err == nil || !errors.Is(err, ErrSimulation) {
		t.Fatalf("Uint64N(0) error = %v", err)
	}
}

func TestSimWorldPostDecisionValidationFailureKeepsAcceptedActionTruthful(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	state := 0
	actor := SimActorFuncs{
		DecideFunc: func(SimAction) (SimDecision, error) {
			state++
			return SimDecision{
				Accepted: true,
				Schedule: []SimScheduledAction{{
					At:     0,
					Action: simTestAction("invalid.child", "missing", nil),
				}},
			}, nil
		},
		ObserveFunc: func() map[string]any { return map[string]any{"state": state} },
	}
	if err := world.RegisterActor("worker", actor); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	if _, err := world.Submit(simTestAction("root", "worker", nil)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	step, err := world.Step()
	if err == nil || !errors.Is(err, ErrSimulation) {
		t.Fatalf("Step error = %v, want simulation validation failure", err)
	}
	if !step.Executed || step.Item.Action.ID != "root" || state != 1 {
		t.Fatalf("failed step = %+v, state = %d; reducer mutation must remain visible", step, state)
	}

	trace := world.Trace()
	if !trace.HasTerminal || trace.Terminal.Status != SimRunError {
		t.Fatalf("terminal = %+v, want error", trace.Terminal)
	}
	accepted, failed := -1, -1
	for i, entry := range trace.Entries {
		if entry.ActionID != "root" {
			continue
		}
		switch entry.Kind {
		case "action_accepted":
			accepted = i
		case "error":
			failed = i
		}
	}
	if accepted < 0 || failed <= accepted {
		t.Fatalf("trace does not truthfully record accepted-then-error: %+v", trace.Entries)
	}
	replay, replayErr := world.ReplayLog()
	if replayErr != nil {
		t.Fatalf("ReplayLog: %v", replayErr)
	}
	if replay.Len() != 1 || replay.Event(0).Name != "test.action" {
		t.Fatalf("accepted reducer invocation missing from replay: %+v", replay.Events())
	}
	observed, observeErr := world.Observe()
	if observeErr != nil || observed["worker.state"] != 1 {
		t.Fatalf("observed post-failure state = %v, err = %v", observed, observeErr)
	}
	digest, digestErr := world.WorldDigest()
	if digestErr != nil || digest != trace.Terminal.WorldDigest {
		t.Fatalf("terminal digest = %q, world digest = %q, err = %v", trace.Terminal.WorldDigest, digest, digestErr)
	}
}

func TestSimWorldTerminalRejectsMutationAndKeepsDigestsStable(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	if err := world.RegisterActor("worker", SimActorFuncs{
		DecideFunc: func(SimAction) (SimDecision, error) { return SimDecision{Accepted: true}, nil },
	}); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	stream, err := world.RandomStream("worker", "before-terminal")
	if err != nil {
		t.Fatalf("RandomStream: %v", err)
	}
	if _, err := world.RunUntilIdle(0); err != nil {
		t.Fatalf("idle RunUntilIdle(0): %v", err)
	}
	beforeTrace, err := world.TraceBytes()
	if err != nil {
		t.Fatalf("TraceBytes before mutation attempts: %v", err)
	}
	beforeTraceDigest, err := world.TraceDigest()
	if err != nil {
		t.Fatalf("TraceDigest before mutation attempts: %v", err)
	}
	beforeWorldDigest, err := world.WorldDigest()
	if err != nil {
		t.Fatalf("WorldDigest before mutation attempts: %v", err)
	}

	assertRejected := func(name string, err error) {
		t.Helper()
		if err == nil || !errors.Is(err, ErrSimulation) {
			t.Errorf("%s error = %v, want ErrSimulation", name, err)
		}
	}
	assertRejected("RegisterActor", world.RegisterActor("late", SimActorFuncs{}))
	assertRejected("RegisterActorFactory", world.RegisterActorFactory("late.factory", func() (SimActor, error) {
		return SimActorFuncs{}, nil
	}))
	assertRejected("RegisterObservation", world.RegisterObservation("late.observation", func() any { return 1 }))
	_, err = world.RandomStream("late")
	assertRejected("RandomStream", err)
	_, err = stream.Uint64()
	assertRejected("existing RandomStream.Uint64", err)
	_, err = world.Schedule(0, simTestAction("late.schedule", "worker", nil))
	assertRejected("Schedule", err)
	_, err = world.Submit(simTestAction("late.submit", "worker", nil))
	assertRejected("Submit", err)
	_, err = world.Send(0, simTestAction("late.send", "worker", nil))
	assertRejected("Send", err)
	_, err = world.ScheduleLifecycle(0, "worker", SimActorStopped)
	assertRejected("ScheduleLifecycle", err)
	_, err = world.Step()
	assertRejected("Step", err)
	_, err = world.RunUntilIdle(1)
	assertRejected("RunUntilIdle", err)

	afterTrace, err := world.TraceBytes()
	if err != nil {
		t.Fatalf("TraceBytes after mutation attempts: %v", err)
	}
	afterTraceDigest, err := world.TraceDigest()
	if err != nil {
		t.Fatalf("TraceDigest after mutation attempts: %v", err)
	}
	afterWorldDigest, err := world.WorldDigest()
	if err != nil {
		t.Fatalf("WorldDigest after mutation attempts: %v", err)
	}
	if !bytes.Equal(beforeTrace, afterTrace) || beforeTraceDigest != afterTraceDigest || beforeWorldDigest != afterWorldDigest {
		t.Fatalf("terminal digests changed: trace %q -> %q, world %q -> %q", beforeTraceDigest, afterTraceDigest, beforeWorldDigest, afterWorldDigest)
	}
}

func TestSimWorldActorFactoryRestartReconstructsEphemeralState(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	var instances []*int
	factory := func() (SimActor, error) {
		ephemeral := new(int)
		instances = append(instances, ephemeral)
		return SimActorFuncs{
			DecideFunc: func(SimAction) (SimDecision, error) {
				*ephemeral++
				return SimDecision{Accepted: true}, nil
			},
			ObserveFunc: func() map[string]any { return map[string]any{"ephemeral": *ephemeral} },
		}, nil
	}
	if err := world.RegisterActorFactory("worker", factory); err != nil {
		t.Fatalf("RegisterActorFactory: %v", err)
	}
	if _, err := world.Schedule(0, simTestAction("before.crash", "worker", nil)); err != nil {
		t.Fatalf("schedule before crash: %v", err)
	}
	if _, err := world.ScheduleLifecycle(1, "worker", SimActorCrashed); err != nil {
		t.Fatalf("schedule crash: %v", err)
	}
	if _, err := world.ScheduleLifecycle(2, "worker", SimActorRunning); err != nil {
		t.Fatalf("schedule restart: %v", err)
	}
	if _, err := world.Schedule(3, simTestAction("after.restart", "worker", nil)); err != nil {
		t.Fatalf("schedule after restart: %v", err)
	}
	result, err := world.RunUntilIdle(4)
	if err != nil || result.Status != SimRunIdle {
		t.Fatalf("RunUntilIdle = %+v, err = %v", result, err)
	}
	if len(instances) != 2 || *instances[0] != 1 || *instances[1] != 1 {
		t.Fatalf("factory instances = %d with states %v; want two fresh instances at state 1", len(instances), func() []int {
			states := make([]int, len(instances))
			for i, instance := range instances {
				states[i] = *instance
			}
			return states
		}())
	}
	observed, err := world.Observe()
	if err != nil || observed["worker.ephemeral"] != 1 {
		t.Fatalf("post-restart observation = %v, err = %v", observed, err)
	}
}

func TestSimWorldPayloadOwnershipPreventsInputAndOutputAliases(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	var actorInput []int
	if err := world.RegisterActor("worker", SimActorFuncs{
		DecideFunc: func(action SimAction) (SimDecision, error) {
			payload := action.Payload.([]int)
			actorInput = append([]int(nil), payload...)
			payload[0] = 700
			return SimDecision{Accepted: true}, nil
		},
	}); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}

	input := []int{1, 2}
	returned, err := world.Schedule(0, simTestAction("owned", "worker", input))
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	input[0] = 100
	returned.Action.Payload.([]int)[0] = 200
	listed := world.Scheduled()
	listed[0].Action.Payload.([]int)[0] = 300

	step, err := world.Step()
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !reflect.DeepEqual(actorInput, []int{1, 2}) {
		t.Fatalf("actor input = %v, want frozen schedule input [1 2]", actorInput)
	}
	if got := step.Item.Action.Payload.([]int); !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("step payload = %v, want immutable execution payload [1 2]", got)
	}
	step.Item.Action.Payload.([]int)[0] = 400

	replay, err := world.ReplayLog()
	if err != nil {
		t.Fatalf("ReplayLog: %v", err)
	}
	payload := replay.Event(0).Payload.(SimReplayAction).Action.Payload.([]int)
	if !reflect.DeepEqual(payload, []int{1, 2}) {
		t.Fatalf("replay payload = %v, want [1 2]", payload)
	}
	payload[0] = 500
	replayedAgain, err := world.ReplayLog()
	if err != nil {
		t.Fatalf("ReplayLog again: %v", err)
	}
	gotAgain := replayedAgain.Event(0).Payload.(SimReplayAction).Action.Payload.([]int)
	if !reflect.DeepEqual(gotAgain, []int{1, 2}) {
		t.Fatalf("mutating replay output changed stored replay payload to %v", gotAgain)
	}
}

func TestSimWorldQueueAndTraceBoundsTerminalizeWithLimit(t *testing.T) {
	t.Run("queue", func(t *testing.T) {
		world := mustBuildSimTestWorld(t, SimWorldSpec{Seed: mustSimTestSeed(t), MaxQueue: 1})
		if err := world.RegisterActor("worker", SimActorFuncs{
			DecideFunc: func(SimAction) (SimDecision, error) {
				return SimDecision{Accepted: true, Schedule: []SimScheduledAction{
					{At: 0, Action: simTestAction("child.one", "worker", nil)},
					{At: 0, Action: simTestAction("child.two", "worker", nil)},
				}}, nil
			},
		}); err != nil {
			t.Fatalf("RegisterActor: %v", err)
		}
		if _, err := world.Submit(simTestAction("root", "worker", nil)); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		result, err := world.RunUntilIdle(1)
		if err != nil {
			t.Fatalf("RunUntilIdle: %v", err)
		}
		trace := world.Trace()
		if result.Status != SimRunBoundExhausted || result.Limit != "max_queue" || !trace.HasTerminal || trace.Terminal.Status != SimRunBoundExhausted || trace.Terminal.Limit != "max_queue" {
			t.Fatalf("queue bound result = %+v, terminal = %+v", result, trace.Terminal)
		}
	})

	t.Run("trace", func(t *testing.T) {
		world := mustBuildSimTestWorld(t, SimWorldSpec{Seed: mustSimTestSeed(t), MaxTraceEntries: 6})
		if err := world.RegisterActor("worker", SimActorFuncs{
			DecideFunc:  func(SimAction) (SimDecision, error) { return SimDecision{Accepted: true}, nil },
			ObserveFunc: func() map[string]any { return map[string]any{"state": 1} },
		}); err != nil {
			t.Fatalf("RegisterActor: %v", err)
		}
		if _, err := world.Submit(simTestAction("root", "worker", nil)); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		result, err := world.RunUntilIdle(1)
		if err != nil {
			t.Fatalf("RunUntilIdle: %v", err)
		}
		trace := world.Trace()
		if result.Status != SimRunBoundExhausted || result.Limit != "max_trace_entries" || !trace.HasTerminal || trace.Terminal.Status != SimRunBoundExhausted || trace.Terminal.Limit != "max_trace_entries" {
			t.Fatalf("trace bound result = %+v, terminal = %+v", result, trace.Terminal)
		}
	})
}

func TestSimWorldRunUntilIdleZeroIsIdleWhenQueueIsEmpty(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	result, err := world.RunUntilIdle(0)
	if err != nil {
		t.Fatalf("RunUntilIdle(0): %v", err)
	}
	trace := world.Trace()
	if result.Status != SimRunIdle || result.Steps != 0 || !trace.HasTerminal || trace.Terminal.Status != SimRunIdle {
		t.Fatalf("result = %+v, terminal = %+v; want zero-step idle", result, trace.Terminal)
	}
}

func TestSimWorldRejectsDuplicateDottedObservationIDs(t *testing.T) {
	t.Run("actor-first", func(t *testing.T) {
		world := NewSimWorld(mustSimTestSeed(t))
		if err := world.RegisterActor("worker", SimActorFuncs{
			DecideFunc:  func(SimAction) (SimDecision, error) { return SimDecision{}, nil },
			ObserveFunc: func() map[string]any { return map[string]any{"state": 1} },
		}); err != nil {
			t.Fatalf("RegisterActor: %v", err)
		}
		if err := world.RegisterObservation("worker.state", func() any { return 2 }); err != nil {
			t.Fatalf("RegisterObservation: %v", err)
		}
		_, err := world.Observe()
		if err == nil || !errors.Is(err, ErrSimulation) || !strings.Contains(err.Error(), "duplicate observation id") {
			t.Fatalf("Observe collision error = %v", err)
		}
	})

	t.Run("observation-first", func(t *testing.T) {
		world := NewSimWorld(mustSimTestSeed(t))
		if err := world.RegisterObservation("worker.state", func() any { return 2 }); err != nil {
			t.Fatalf("RegisterObservation: %v", err)
		}
		err := world.RegisterActor("worker", SimActorFuncs{
			DecideFunc:  func(SimAction) (SimDecision, error) { return SimDecision{}, nil },
			ObserveFunc: func() map[string]any { return map[string]any{"state": 1} },
		})
		if err == nil || !errors.Is(err, ErrSimulation) || !strings.Contains(err.Error(), "duplicate observation id") {
			t.Fatalf("RegisterActor collision error = %v", err)
		}
		if _, ok := world.ActorStatus("worker"); ok {
			t.Fatalf("actor registration collision left actor installed")
		}
	})
}
