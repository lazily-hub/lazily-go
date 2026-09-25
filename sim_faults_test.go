package lazily

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func mustSimFaultPlan(t *testing.T, rules []SimFaultRule) SimFaultPlan {
	t.Helper()
	plan, err := NewSimFaultPlan("test.fault.plan", "1", mustSimTestSeed(t).String(), 0, rules)
	if err != nil {
		t.Fatalf("NewSimFaultPlan: %v", err)
	}
	return plan
}

func TestMaterializeSimFaultPlanIsDeterministicPureOrderIndependentAndReplayable(t *testing.T) {
	makeTemplates := func(order []string, payloadDraws int) []SimFaultTemplate {
		weights := map[string]uint64{"delay": 1, "drop": 3}
		templates := make([]SimFaultTemplate, 0, len(order))
		for _, name := range order {
			templateName := name
			templates = append(templates, SimFaultTemplate{
				Name: templateName, Weight: weights[templateName],
				Build: func(random *SimRandomStream, ordinal uint64) (SimFaultRule, error) {
					var payload uint64
					for i := 0; i < payloadDraws; i++ {
						value, err := random.Uint64()
						if err != nil {
							return SimFaultRule{}, err
						}
						payload = value
					}
					kind := SimFaultDrop
					if templateName == "delay" {
						kind = SimFaultDelay
					}
					return SimFaultRule{
						ID: fmt.Sprintf("%s.rule.%d", templateName, ordinal), Kind: kind,
						Port: "materialized.port", CallID: fmt.Sprintf("call.%d", ordinal),
						DelayTicks: payload%7 + 1, Payload: []uint64{payload},
					}, nil
				},
			})
		}
		return templates
	}
	world := NewSimWorld(mustSimTestSeed(t))
	before := mustSimGeneratorWorldDigest(t, world)
	first, err := MaterializeSimFaultPlan(world, "materialized.plan", "1", 11, 32, makeTemplates([]string{"delay", "drop"}, 1))
	if err != nil {
		t.Fatalf("MaterializeSimFaultPlan first: %v", err)
	}
	second, err := MaterializeSimFaultPlan(world, "materialized.plan", "1", 11, 32, makeTemplates([]string{"delay", "drop"}, 1))
	if err != nil {
		t.Fatalf("MaterializeSimFaultPlan repeat: %v", err)
	}
	reversed, err := MaterializeSimFaultPlan(world, "materialized.plan", "1", 11, 32, makeTemplates([]string{"drop", "delay"}, 1))
	if err != nil {
		t.Fatalf("MaterializeSimFaultPlan reversed: %v", err)
	}
	after := mustSimGeneratorWorldDigest(t, world)
	firstBytes, err := first.CanonicalBytes()
	if err != nil {
		t.Fatalf("first CanonicalBytes: %v", err)
	}
	secondBytes, err := second.CanonicalBytes()
	if err != nil {
		t.Fatalf("second CanonicalBytes: %v", err)
	}
	reversedBytes, err := reversed.CanonicalBytes()
	if err != nil {
		t.Fatalf("reversed CanonicalBytes: %v", err)
	}
	if !bytes.Equal(firstBytes, secondBytes) || !bytes.Equal(firstBytes, reversedBytes) {
		t.Fatalf("same seed/index or reversed template declaration changed materialized plan")
	}
	firstDigest, err := first.Digest()
	if err != nil {
		t.Fatalf("first Digest: %v", err)
	}
	secondDigest, err := second.Digest()
	if err != nil || firstDigest != secondDigest {
		t.Fatalf("plan digests = %q and %q, err = %v", firstDigest, secondDigest, err)
	}
	if before != after {
		t.Fatalf("materialization mutated runtime world digest: %q -> %q", before, after)
	}

	extraDraws, err := MaterializeSimFaultPlan(world, "materialized.plan", "1", 11, 32, makeTemplates([]string{"delay", "drop"}, 9))
	if err != nil {
		t.Fatalf("MaterializeSimFaultPlan extra payload draws: %v", err)
	}
	for i := range first.Rules {
		if first.Rules[i].Kind != extraDraws.Rules[i].Kind {
			t.Fatalf("payload draw count perturbed fault selection at %d: %q != %q", i, first.Rules[i].Kind, extraDraws.Rules[i].Kind)
		}
	}

	replay := func() ([]SimFaultDisposition, []byte) {
		t.Helper()
		replayWorld := NewSimWorld(mustSimTestSeed(t))
		controller, err := NewSimFaultController(replayWorld, first)
		if err != nil {
			t.Fatalf("NewSimFaultController: %v", err)
		}
		dispositions := make([]SimFaultDisposition, 0, len(first.Rules))
		for _, rule := range first.Rules {
			disposition, err := controller.Apply(rule.Port, rule.CallID, 0)
			if err != nil {
				t.Fatalf("Apply %q: %v", rule.ID, err)
			}
			dispositions = append(dispositions, disposition)
		}
		traceBytes, err := replayWorld.TraceBytes()
		if err != nil {
			t.Fatalf("TraceBytes: %v", err)
		}
		return dispositions, traceBytes
	}
	firstDispositions, firstTrace := replay()
	secondDispositions, secondTrace := replay()
	if !reflect.DeepEqual(firstDispositions, secondDispositions) || !bytes.Equal(firstTrace, secondTrace) {
		t.Fatalf("materialized plan replay was not deterministic")
	}
}

func TestSimFaultControllerDispositionKindsAndTraceVisibility(t *testing.T) {
	tests := []struct {
		name          string
		kind          SimFaultKind
		delay, copies uint64
		wantAt        uint64
		wantCopies    uint64
		wantInvoke    bool
		wantStale     bool
		wantOutcome   string
	}{
		{"delay", SimFaultDelay, 3, 0, 8, 1, true, false, "success"},
		{"drop", SimFaultDrop, 0, 0, 5, 0, false, false, "dropped"},
		{"duplicate", SimFaultDuplicate, 0, 2, 5, 3, true, false, "success"},
		{"reorder", SimFaultReorder, 4, 0, 9, 1, true, false, "success"},
		{"timeout", SimFaultTimeout, 0, 0, 5, 1, false, false, "timeout"},
		{"cancel", SimFaultCancel, 0, 0, 5, 1, false, false, "canceled"},
		{"stale", SimFaultStaleResponse, 0, 0, 5, 1, false, true, "stale"},
		{"conflict", SimFaultOptimisticConflict, 0, 0, 5, 1, false, false, "optimistic_conflict"},
		{"commit", SimFaultCommit, 0, 0, 5, 1, true, false, "committed"},
		{"rollback", SimFaultRollback, 0, 0, 5, 1, false, false, "rolled_back"},
		{"clock", SimFaultClockAdvance, 7, 0, 12, 1, true, false, "success"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			world := NewSimWorld(mustSimTestSeed(t))
			plan := mustSimFaultPlan(t, []SimFaultRule{{
				ID: "fault.rule", Kind: test.kind, Port: "test.port", CallID: "test.call",
				DelayTicks: test.delay, Copies: test.copies, Payload: "payload." + test.name,
			}})
			controller, err := NewSimFaultController(world, plan)
			if err != nil {
				t.Fatalf("NewSimFaultController: %v", err)
			}
			disposition, err := controller.Apply("test.port", "test.call", 5)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if disposition.At != test.wantAt || disposition.Copies != test.wantCopies || disposition.Invoke != test.wantInvoke || disposition.UseStale != test.wantStale || disposition.Outcome != test.wantOutcome {
				t.Fatalf("disposition = %+v", disposition)
			}
			if !reflect.DeepEqual(disposition.RuleIDs, []string{"fault.rule"}) || disposition.Payload != "payload."+test.name {
				t.Fatalf("rule identity/payload = %+v", disposition)
			}
			trace := world.Trace()
			if len(trace.Entries) != 1 || trace.Entries[0].Kind != "fault_injected" || trace.Entries[0].ActionKind != string(test.kind) {
				t.Fatalf("fault trace = %+v", trace.Entries)
			}
		})
	}
}

func TestSimFaultDelayReorderAndClockAdvanceControlExecutionOrder(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	type execution struct {
		id string
		at uint64
	}
	var executed []execution
	if err := world.RegisterActor("worker", SimActorFuncs{
		DecideFunc: func(action SimAction) (SimDecision, error) {
			executed = append(executed, execution{action.ID, world.Now()})
			return SimDecision{Accepted: true}, nil
		},
	}); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	plan := mustSimFaultPlan(t, []SimFaultRule{
		{ID: "delay.rule", Kind: SimFaultDelay, Port: "transport", CallID: "delay.call", DelayTicks: 10},
		{ID: "reorder.rule", Kind: SimFaultReorder, Port: "transport", CallID: "reorder.call", DelayTicks: 5},
		{ID: "clock.rule", Kind: SimFaultClockAdvance, Port: "transport", CallID: "clock.call", DelayTicks: 7},
	})
	controller, err := NewSimFaultController(world, plan)
	if err != nil {
		t.Fatalf("NewSimFaultController: %v", err)
	}
	port := SimFaultPort[int, int]{
		Name: "transport", Controller: controller,
		Invoke: func(request int) (int, error) { return request, nil },
		ResponseAction: func(callID string, copyIndex uint64, outcome SimPortOutcome[int]) SimAction {
			return simTestAction(fmt.Sprintf("%s.copy.%d", callID, copyIndex), "worker", outcome)
		},
	}
	if _, err := world.Schedule(2, simTestAction("baseline", "worker", nil)); err != nil {
		t.Fatalf("schedule baseline: %v", err)
	}
	for i, callID := range []string{"delay.call", "reorder.call", "clock.call"} {
		items, err := port.Call(0, callID, i)
		if err != nil || len(items) != 1 {
			t.Fatalf("Call %q items=%v err=%v", callID, items, err)
		}
	}
	result, err := world.RunUntilIdle(4)
	if err != nil || result.Status != SimRunIdle {
		t.Fatalf("RunUntilIdle = %+v, err = %v", result, err)
	}
	want := []execution{{"baseline", 2}, {"reorder.call.copy.0", 5}, {"clock.call.copy.0", 7}, {"delay.call.copy.0", 10}}
	if !reflect.DeepEqual(executed, want) || world.Now() != 10 {
		t.Fatalf("execution order = %v, now=%d; want %v, now=10", executed, world.Now(), want)
	}
}

func TestSimFaultDropAndDuplicateControlDelivery(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	var delivered []string
	if err := world.RegisterActor("worker", SimActorFuncs{
		DecideFunc: func(action SimAction) (SimDecision, error) {
			delivered = append(delivered, action.ID)
			return SimDecision{Accepted: true}, nil
		},
	}); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	plan := mustSimFaultPlan(t, []SimFaultRule{
		{ID: "drop.rule", Kind: SimFaultDrop, Port: "delivery", CallID: "drop.call"},
		{ID: "duplicate.rule", Kind: SimFaultDuplicate, Port: "delivery", CallID: "duplicate.call", Copies: 2},
	})
	controller, err := NewSimFaultController(world, plan)
	if err != nil {
		t.Fatalf("NewSimFaultController: %v", err)
	}
	invocations := 0
	port := SimFaultPort[int, int]{
		Name: "delivery", Controller: controller,
		Invoke: func(request int) (int, error) { invocations++; return request, nil },
		ResponseAction: func(callID string, copyIndex uint64, outcome SimPortOutcome[int]) SimAction {
			return simTestAction(fmt.Sprintf("%s.copy.%d", callID, copyIndex), "worker", outcome)
		},
	}
	dropped, err := port.Call(1, "drop.call", 1)
	if err != nil || len(dropped) != 0 || invocations != 0 {
		t.Fatalf("drop call items=%v invocations=%d err=%v", dropped, invocations, err)
	}
	duplicated, err := port.Call(3, "duplicate.call", 2)
	if err != nil || len(duplicated) != 3 || invocations != 1 {
		t.Fatalf("duplicate call items=%v invocations=%d err=%v", duplicated, invocations, err)
	}
	result, err := world.RunUntilIdle(3)
	if err != nil || result.Status != SimRunIdle {
		t.Fatalf("RunUntilIdle = %+v, err = %v", result, err)
	}
	want := []string{"duplicate.call.copy.0", "duplicate.call.copy.1", "duplicate.call.copy.2"}
	if !reflect.DeepEqual(delivered, want) {
		t.Fatalf("delivered = %v, want %v", delivered, want)
	}
}

func TestSimFaultPartitionAndHealGatePortAndTraceRecovery(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	plan := mustSimFaultPlan(t, []SimFaultRule{
		{ID: "partition.rule", Kind: SimFaultPartition, Port: "network", CallID: "cut.call"},
		{ID: "heal.rule", CauseID: "partition.rule", Kind: SimFaultHeal, Port: "network", CallID: "heal.call"},
	})
	controller, err := NewSimFaultController(world, plan)
	if err != nil {
		t.Fatalf("NewSimFaultController: %v", err)
	}
	cut, err := controller.Apply("network", "cut.call", 0)
	if err != nil || cut.Invoke || cut.Copies != 0 || cut.Outcome != "partitioned" {
		t.Fatalf("partition disposition = %+v, err=%v", cut, err)
	}
	blocked, err := controller.Apply("network", "blocked.call", 1)
	if err != nil || blocked.Invoke || blocked.Copies != 0 || blocked.Outcome != "partitioned" || len(blocked.RuleIDs) != 0 {
		t.Fatalf("blocked disposition = %+v, err=%v", blocked, err)
	}
	healed, err := controller.Apply("network", "heal.call", 2)
	if err != nil || !healed.Invoke || healed.Copies != 1 || healed.Outcome != "healed" {
		t.Fatalf("heal disposition = %+v, err=%v", healed, err)
	}
	normal, err := controller.Apply("network", "normal.call", 3)
	if err != nil || !normal.Invoke || normal.Outcome != "success" {
		t.Fatalf("post-heal disposition = %+v, err=%v", normal, err)
	}
	var kinds []string
	for _, entry := range world.Trace().Entries {
		kinds = append(kinds, entry.Kind+":"+entry.ActionKind)
	}
	want := []string{"fault_injected:partition", "fault_effect:partition", "fault_recovered:heal"}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("partition/heal trace = %v, want %v", kinds, want)
	}
}

func TestSimFaultCrashAndRestartIntegrateWithActorLifecycle(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	factoryCalls := 0
	if err := world.RegisterActorFactory("worker", func() (SimActor, error) {
		factoryCalls++
		return SimActorFuncs{DecideFunc: func(SimAction) (SimDecision, error) {
			return SimDecision{Accepted: true}, nil
		}}, nil
	}); err != nil {
		t.Fatalf("RegisterActorFactory: %v", err)
	}
	plan := mustSimFaultPlan(t, []SimFaultRule{
		{ID: "crash.rule", Kind: SimFaultCrash, Port: "lifecycle.port", CallID: "crash.call", ActorID: "worker"},
		{ID: "restart.rule", CauseID: "crash.rule", Kind: SimFaultRestart, Port: "lifecycle.port", CallID: "restart.call", ActorID: "worker"},
	})
	controller, err := NewSimFaultController(world, plan)
	if err != nil {
		t.Fatalf("NewSimFaultController: %v", err)
	}
	if _, err := controller.Apply("lifecycle.port", "crash.call", 1); err != nil {
		t.Fatalf("apply crash: %v", err)
	}
	if _, err := controller.Apply("lifecycle.port", "restart.call", 2); err != nil {
		t.Fatalf("apply restart: %v", err)
	}
	if _, err := world.Step(); err != nil {
		t.Fatalf("step crash: %v", err)
	}
	if status, _ := world.ActorStatus("worker"); status != SimActorCrashed {
		t.Fatalf("status after crash = %q", status)
	}
	if _, err := world.Step(); err != nil {
		t.Fatalf("step restart: %v", err)
	}
	if status, _ := world.ActorStatus("worker"); status != SimActorRunning || factoryCalls != 2 {
		t.Fatalf("status after restart = %q, factoryCalls=%d", status, factoryCalls)
	}
	var faultEntries []string
	for _, entry := range world.Trace().Entries {
		if entry.Kind == "fault_injected" || entry.Kind == "fault_recovered" {
			faultEntries = append(faultEntries, entry.Kind+":"+entry.ActionKind)
		}
	}
	want := []string{"fault_injected:crash", "fault_recovered:restart"}
	if !reflect.DeepEqual(faultEntries, want) {
		t.Fatalf("lifecycle fault trace = %v, want %v", faultEntries, want)
	}
}

func TestSimFaultPortMaterializesTimeoutCancelStaleConflictCommitAndRollbackOutcomes(t *testing.T) {
	world := NewSimWorld(mustSimTestSeed(t))
	var outcomes []SimPortOutcome[int]
	if err := world.RegisterActor("worker", SimActorFuncs{
		DecideFunc: func(action SimAction) (SimDecision, error) {
			outcomes = append(outcomes, action.Payload.(SimPortOutcome[int]))
			return SimDecision{Accepted: true}, nil
		},
	}); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	rules := []SimFaultRule{
		{ID: "stale.rule", Kind: SimFaultStaleResponse, Port: "service", CallID: "stale.call", Payload: "payload.stale"},
		{ID: "timeout.rule", Kind: SimFaultTimeout, Port: "service", CallID: "timeout.call", Payload: "payload.timeout"},
		{ID: "cancel.rule", Kind: SimFaultCancel, Port: "service", CallID: "cancel.call", Payload: "payload.cancel"},
		{ID: "conflict.rule", Kind: SimFaultOptimisticConflict, Port: "service", CallID: "conflict.call", Payload: "payload.conflict"},
		{ID: "commit.rule", Kind: SimFaultCommit, Port: "service", CallID: "commit.call", Payload: "payload.commit"},
		{ID: "rollback.rule", Kind: SimFaultRollback, Port: "service", CallID: "rollback.call", Payload: "payload.rollback"},
	}
	controller, err := NewSimFaultController(world, mustSimFaultPlan(t, rules))
	if err != nil {
		t.Fatalf("NewSimFaultController: %v", err)
	}
	invocations := 0
	port := SimFaultPort[int, int]{
		Name: "service", Controller: controller,
		Invoke: func(request int) (int, error) { invocations++; return request * 10, nil },
		ResponseAction: func(callID string, copyIndex uint64, outcome SimPortOutcome[int]) SimAction {
			return simTestAction(fmt.Sprintf("%s.copy.%d", callID, copyIndex), "worker", outcome)
		},
	}
	calls := []struct {
		at      uint64
		id      string
		request int
	}{
		{0, "fresh.call", 2},
		{1, "stale.call", 9},
		{2, "timeout.call", 9},
		{3, "cancel.call", 9},
		{4, "conflict.call", 9},
		{5, "commit.call", 3},
		{6, "rollback.call", 9},
	}
	for _, call := range calls {
		items, err := port.Call(call.at, call.id, call.request)
		if err != nil || len(items) != 1 {
			t.Fatalf("Call %q items=%v err=%v", call.id, items, err)
		}
	}
	result, err := world.RunUntilIdle(uint64(len(calls)))
	if err != nil || result.Status != SimRunIdle {
		t.Fatalf("RunUntilIdle = %+v, err = %v", result, err)
	}
	statuses := make([]string, len(outcomes))
	values := make([]int, len(outcomes))
	payloads := make([]any, len(outcomes))
	for i, outcome := range outcomes {
		statuses[i], values[i], payloads[i] = outcome.Status, outcome.Value, outcome.FaultPayload
	}
	wantStatuses := []string{"success", "stale", "timeout", "canceled", "optimistic_conflict", "committed", "rolled_back"}
	wantValues := []int{20, 20, 0, 0, 0, 30, 0}
	wantPayloads := []any{nil, "payload.stale", "payload.timeout", "payload.cancel", "payload.conflict", "payload.commit", "payload.rollback"}
	if invocations != 2 || !reflect.DeepEqual(statuses, wantStatuses) || !reflect.DeepEqual(values, wantValues) || !reflect.DeepEqual(payloads, wantPayloads) {
		t.Fatalf("invocations=%d statuses=%v values=%v payloads=%v", invocations, statuses, values, payloads)
	}
}

func TestShrinkSimFaultPlanPreservesStableCausalDependenciesAndActuallyMinimizes(t *testing.T) {
	plan := mustSimFaultPlan(t, []SimFaultRule{
		{ID: "noise.a", Kind: SimFaultDelay, Port: "shrink.port", DelayTicks: 1},
		{ID: "root", Kind: SimFaultPartition, Port: "shrink.port"},
		{ID: "child", CauseID: "root", Kind: SimFaultDelay, Port: "shrink.port", DelayTicks: 2},
		{ID: "target", CauseID: "child", Kind: SimFaultTimeout, Port: "shrink.port"},
		{ID: "noise.b", Kind: SimFaultRollback, Port: "shrink.port"},
	})
	fails := func(candidate SimFaultPlan) (bool, error) {
		for _, rule := range candidate.Rules {
			if rule.ID == "target" {
				return true, nil
			}
		}
		return false, nil
	}
	shrunk, err := ShrinkSimFaultPlan(plan, fails)
	if err != nil {
		t.Fatalf("ShrinkSimFaultPlan: %v", err)
	}
	ids := make([]string, len(shrunk.Rules))
	for i, rule := range shrunk.Rules {
		ids[i] = rule.ID
	}
	want := []string{"root", "child", "target"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("shrunk rule IDs = %v, want %v", ids, want)
	}
	if shrunk.Name != plan.Name || shrunk.Version != plan.Version || shrunk.SeedHex != plan.SeedHex || shrunk.PlanIndex != plan.PlanIndex {
		t.Fatalf("shrinker changed stable plan identity: before=%+v after=%+v", plan, shrunk)
	}
	if _, err := NewSimFaultPlan(shrunk.Name, shrunk.Version, shrunk.SeedHex, shrunk.PlanIndex, shrunk.Rules); err != nil {
		t.Fatalf("shrunk plan lost dependency validity: %v", err)
	}
	for i := range shrunk.Rules {
		rules := append([]SimFaultRule(nil), shrunk.Rules[:i]...)
		rules = append(rules, shrunk.Rules[i+1:]...)
		candidate, candidateErr := NewSimFaultPlan(shrunk.Name, shrunk.Version, shrunk.SeedHex, shrunk.PlanIndex, rules)
		if candidateErr == nil {
			stillFails, err := fails(candidate)
			if err != nil {
				t.Fatalf("failure predicate after removing %q: %v", shrunk.Rules[i].ID, err)
			}
			if stillFails {
				t.Fatalf("shrunk plan is not one-removal-minimal; removing %q still fails", shrunk.Rules[i].ID)
			}
		}
	}
}
