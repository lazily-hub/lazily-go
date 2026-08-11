package lazily

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

// ---------------------------------------------------------------------------
// The independent interpreter (#lzstdlibmutantsallbindings)
// ---------------------------------------------------------------------------
//
// Each stdlib fixture declares a `mutations` ledger: "mutate the implementation
// THIS named way and exactly these scenarios must fail". Nothing in this binding
// applied any of it — `TestStdlibConformance` checks that every entry's
// `must_fail` is non-empty, that its ids resolve to scenarios the run replayed,
// and that the entry count clears `mutation_floor`. All three are claims about
// the ledger's own bookkeeping
// (feedback_conformance_tests_drive_real_behavior_not_runner_bookkeeping), so an
// operator rebound to a scenario it does not break stayed green and the ledger's
// central claim was untested.
//
// The reference shape is lazily-py `tests/test_stdlib_conformance.py` (ed812ab)
// and lazily-rs `tests/stdlib_conformance.rs` (`independent_failures`). The
// design point worth restating: the operator perturbs an INDEPENDENT model of the
// feature, never the shipped `stdlib.go` implementation. Mutating production code
// to test the corpus would test the mutation harness, would need the library to
// carry seams that exist only for tests, and would say nothing about whether the
// corpus can TELL a correct implementation from a wrong one — which is the only
// thing the ledger claims.
//
// Three properties carried over from the python landing, each load-bearing:
//
//   - The set of IMPLEMENTED operators is DERIVED from the branches the replay
//     actually consulted (`stdlibMutationProbe.consulted`), never from a
//     hand-maintained registry. A registry is one more piece of bookkeeping that
//     drifts out of step with the code — which is the same defect one level up.
//   - An operator the corpus names with NO interpreter arm is a HARD failure that
//     names itself and lists what was consulted. Never a skip: a silently
//     unimplemented operator is exactly the vacuity this file exists to end.
//   - The complement is NOT asserted. `must_fail` is a lower bound on detection
//     ("these scenarios catch it"), not a partition — see
//     TestStdlibMutationComplementIsNotAssertedByThisCorpus below, which records
//     a concrete counter-instance so the day the corpus does become a partition,
//     somebody has to decide deliberately whether to tighten this.
//
// These fixtures are read WITHOUT the assertion-key seams the canonical runner
// uses (`bindBlockBytes` in `assertStdlibExpectation`). The interpreter replays
// every scenario once per declared operator and most of those replays are
// expected to DIVERGE; booking their keys as asserted would credit the corpus on
// the strength of runs whose whole point is that they do not conform. The tracked
// reading of these bytes is `TestStdlibConformance`, which replays production.

// stdlibFixtureNames lists the three stdlib fixtures, in the order the canonical
// runner replays them.
var stdlibFixtureNames = []string{"timer.json", "timeout.json", "revision_barrier.json"}

// stdlibMutationPairFloor is the number of (operator, scenario) pairs this
// corpus declares today: timer 4 + timeout 5 + revision_barrier 6. A floor, not
// an equality — the corpus may grow pairs, and this run must never apply fewer
// than it does today.
const stdlibMutationPairFloor = 15

// stdlibMutationProbe is the operator under test, consulted BY NAME at every
// perturbable branch of the interpreter.
//
// `consulted` is what makes an unimplemented operator loud rather than silent: it
// is produced by the branches the replay really evaluated, so an operator no arm
// knows about ends the run naming itself.
type stdlibMutationProbe struct {
	operator  string
	consulted map[string]bool
}

// newStdlibMutationProbe builds a probe for `operator`; the empty string means
// "no operator applied", which no branch name can collide with.
func newStdlibMutationProbe(operator string) *stdlibMutationProbe {
	return &stdlibMutationProbe{operator: operator, consulted: map[string]bool{}}
}

func (p *stdlibMutationProbe) applied(name string) bool {
	p.consulted[name] = true
	return p.operator == name
}

func (p *stdlibMutationProbe) consultedNames() []string {
	out := make([]string, 0, len(p.consulted))
	for name := range p.consulted {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// stdlibModelState is the interpreter's whole memory. Presence flags rather than
// pointers where the corpus distinguishes "absent" from "zero": a latched
// observation carries `fired_at`/`value`/`reason` only when the model set them.
type stdlibModelState struct {
	status      string
	deadline    uint64
	hasDeadline bool
	lastNow     uint64
	hasLastNow  bool
	firedAt     uint64
	hasFiredAt  bool
	value       string
	hasValue    bool
	reason      string

	revision        uint64
	generation      uint64
	required        uint64
	barrierDeadline *uint64
}

// stdlibTerminal is the latched observation: whatever this feature carries, plus
// no adapter calls.
func stdlibTerminal(state *stdlibModelState, adapterCounts bool) map[string]any {
	out := map[string]any{"outcome": state.status}
	if state.hasFiredAt {
		out["fired_at"] = state.firedAt
	}
	if state.hasValue {
		out["value"] = state.value
	}
	if state.reason != "" {
		out["reason"] = state.reason
	}
	if adapterCounts {
		out["operation_calls"] = 0
		out["cancellation_calls"] = 0
	}
	return out
}

// stdlibOptionalDeadline renders the recorded deadline, or JSON null when the
// model never recorded one.
func stdlibOptionalDeadline(state *stdlibModelState) any {
	if !state.hasDeadline {
		return nil
	}
	return state.deadline
}

// stdlibModelOp returns the step's op, CHECKED against the ops this model
// implements (#lzscenariobodyskip).
//
// The models used to test `op == "start"` and let everything else fall through to
// the second op without checking it, so an op the corpus grows would be replayed
// as an observe/poll and its expectation compared against the wrong transition.
func stdlibModelOp(t *testing.T, step stdlibStep, known ...string) string {
	t.Helper()
	for _, name := range known {
		if step.Op == name {
			return step.Op
		}
	}
	t.Fatalf("unknown model op %q (known: %v)", step.Op, known)
	return ""
}

func stdlibModelTimer(
	t *testing.T, state *stdlibModelState, step stdlibStep, mutated *stdlibMutationProbe,
) map[string]any {
	if stdlibModelOp(t, step, "start", "observe") == "start" {
		deadline := step.Now + step.Duration
		if deadline < step.Now { // u64 wrap IS the overflow the corpus types
			state.status, state.reason = "unavailable", "deadline_overflow"
			return stdlibTerminal(state, false)
		}
		state.status, state.deadline, state.hasDeadline = "pending", deadline, true
		state.lastNow, state.hasLastNow = step.Now, true
		return map[string]any{"outcome": "pending", "deadline": deadline}
	}
	if mutated.applied("fixture_bookkeeping") {
		return map[string]any{"outcome": "pending", "deadline": stdlibOptionalDeadline(state)}
	}
	latched := mutated.applied("terminal_not_latched")
	if state.status != "pending" && !latched {
		return stdlibTerminal(state, false)
	}
	if latched {
		state.status = "pending"
	}
	now := step.Now
	if now < state.lastNow {
		return map[string]any{
			"outcome": "unavailable", "reason": "clock_regression", "deadline": state.deadline,
		}
	}
	state.lastNow = now
	reached := now >= state.deadline
	if mutated.applied("deadline_strict_greater") {
		reached = now > state.deadline
	}
	if !reached {
		return map[string]any{"outcome": "pending", "deadline": state.deadline}
	}
	state.status, state.firedAt, state.hasFiredAt = "fired", now, true
	return stdlibTerminal(state, false)
}

func stdlibModelTimeout(
	t *testing.T, state *stdlibModelState, step stdlibStep, mutated *stdlibMutationProbe,
) map[string]any {
	if stdlibModelOp(t, step, "start", "poll") == "start" {
		deadline := step.Now + step.Duration
		if deadline < step.Now {
			state.status, state.reason = "unavailable", "deadline_overflow"
			return stdlibTerminal(state, false)
		}
		state.status, state.deadline, state.hasDeadline = "pending", deadline, true
		state.lastNow, state.hasLastNow = step.Now, true
		return map[string]any{"outcome": "pending", "deadline": deadline}
	}
	if mutated.applied("fixture_bookkeeping") {
		return map[string]any{
			"outcome": "pending", "deadline": stdlibOptionalDeadline(state),
			"operation_calls": 0, "cancellation_calls": 0,
		}
	}
	latched := mutated.applied("terminal_not_latched")
	if state.status != "pending" && !latched {
		return stdlibTerminal(state, true)
	}
	if latched {
		state.status = "pending"
	}
	now := step.Now
	if now < state.lastNow {
		state.status, state.reason = "unavailable", "clock_regression"
		return map[string]any{
			"outcome": "unavailable", "reason": "clock_regression",
			"operation_calls": 0, "cancellation_calls": 0,
		}
	}
	state.lastNow = now
	reached := now >= state.deadline
	if mutated.applied("deadline_strict_greater") {
		reached = now > state.deadline
	}
	if reached {
		state.status = "timed_out"
		return map[string]any{
			"outcome": "timed_out", "operation_calls": 0, "cancellation_calls": 0,
		}
	}
	// Both drive if-chains whose tail ASSUMES `pending`; validate the spelling so
	// an unknown one names itself instead of quietly meaning "pending"
	// (#lzscenariobodyskip).
	switch step.Operation {
	case "completed", "pending", "unavailable":
	default:
		t.Fatalf("unknown timeout operation %q", step.Operation)
	}
	switch step.Cancellation {
	case "cancelled", "pending", "unavailable":
	default:
		t.Fatalf("unknown timeout cancellation %q", step.Cancellation)
	}
	if mutated.applied("cancellation_before_completion") && step.Cancellation == "cancelled" {
		state.status = "cancelled"
		return map[string]any{
			"outcome": "cancelled", "operation_calls": 1, "cancellation_calls": 1,
		}
	}
	if step.Operation == "completed" {
		state.status, state.value, state.hasValue = "completed", step.Value, true
		return map[string]any{
			"outcome": "completed", "value": step.Value,
			"operation_calls": 1, "cancellation_calls": 1,
		}
	}
	if step.Operation == "unavailable" {
		state.status, state.reason = "unavailable", "operation_unavailable"
		return map[string]any{
			"outcome": "unavailable", "reason": "operation_unavailable",
			"operation_calls": 1, "cancellation_calls": 1,
		}
	}
	if step.Cancellation == "cancelled" {
		state.status = "cancelled"
		return map[string]any{
			"outcome": "cancelled", "operation_calls": 1, "cancellation_calls": 1,
		}
	}
	if step.Cancellation == "unavailable" {
		state.status, state.reason = "unavailable", "cancellation_unavailable"
		return map[string]any{
			"outcome": "unavailable", "reason": "cancellation_unavailable",
			"operation_calls": 1, "cancellation_calls": 1,
		}
	}
	return map[string]any{
		"outcome": "pending", "deadline": state.deadline,
		"operation_calls": 1, "cancellation_calls": 1,
	}
}

func stdlibBarrierObservation(state *stdlibModelState) map[string]any {
	out := map[string]any{
		"outcome": state.status, "revision": state.revision, "generation": state.generation,
	}
	if state.reason != "" {
		out["reason"] = state.reason
	}
	return out
}

func stdlibModelBarrier(
	t *testing.T, state *stdlibModelState, step stdlibStep, mutated *stdlibMutationProbe,
) map[string]any {
	op := stdlibModelOp(
		t, step, "start", "register_recheck", "advance", "observe", "dispose", "receipt",
	)
	if op == "start" {
		state.status = "pending"
		state.revision, state.generation = step.Revision, 0
		state.required = step.RequiredRevision
		state.barrierDeadline = step.Deadline
		state.hasLastNow = false
		return stdlibBarrierObservation(state)
	}
	if mutated.applied("fixture_bookkeeping") {
		state.status = "pending"
		return stdlibBarrierObservation(state)
	}
	latched := mutated.applied("terminal_not_latched")
	if state.status != "pending" && !latched {
		result := stdlibBarrierObservation(state)
		if op == "observe" {
			result["cancellation_calls"] = 0
		}
		return result
	}
	if latched {
		state.status = "pending"
	}
	if op == "dispose" {
		state.status = "disposed"
		return stdlibBarrierObservation(state)
	}
	if op == "receipt" {
		// An application-owned effect receipt is NOT barrier authority: it wakes
		// the waiter and changes no revision. The operator makes it authority.
		if mutated.applied("receipt_is_authority") {
			state.revision = state.required
			state.generation++
			state.status = "satisfied"
		}
		return stdlibBarrierObservation(state)
	}
	if op == "advance" {
		if step.Revision > state.revision {
			state.revision = step.Revision
		}
		state.generation++
		if state.revision >= state.required && step.Predicate {
			state.status = "satisfied"
		}
		return stdlibBarrierObservation(state)
	}
	now := step.Now
	regressed := state.hasLastNow && now < state.lastNow
	if regressed && !mutated.applied("barrier_accept_clock_regression") {
		state.status, state.reason = "unavailable", "clock_regression"
		result := stdlibBarrierObservation(state)
		if op == "observe" {
			result["cancellation_calls"] = 0
		}
		return result
	}
	state.lastNow, state.hasLastNow = now, true
	if op == "register_recheck" {
		state.generation++
		if !mutated.applied("barrier_skip_post_registration_recheck") {
			if step.ObservedRevision > state.revision {
				state.revision = step.ObservedRevision
			}
			if state.revision >= state.required && step.Predicate {
				state.status = "satisfied"
			}
		}
		return stdlibBarrierObservation(state)
	}
	reached := false
	if state.barrierDeadline != nil {
		if mutated.applied("deadline_strict_greater") {
			reached = now > *state.barrierDeadline
		} else {
			reached = now >= *state.barrierDeadline
		}
	}
	if reached {
		state.status = "timed_out"
		result := stdlibBarrierObservation(state)
		result["cancellation_calls"] = 0
		return result
	}
	if state.revision >= state.required && step.Predicate {
		state.status = "satisfied"
		result := stdlibBarrierObservation(state)
		result["cancellation_calls"] = 0
		return result
	}
	// Fail-closed tail (#lzscenariobodyskip): a cancellation spelling this model
	// does not know must not behave like `pending`.
	switch step.Cancellation {
	case "cancelled":
		state.status = "cancelled"
	case "unavailable":
		state.status, state.reason = "unavailable", "cancellation_unavailable"
	case "pending":
	default:
		t.Fatalf("unknown barrier cancellation %q", step.Cancellation)
	}
	result := stdlibBarrierObservation(state)
	result["cancellation_calls"] = 1
	return result
}

// stdlibIndependentFailures replays every scenario through the model, perturbed
// by `operator` (empty string for none). It returns the scenario ids that
// DIVERGED from their declared `expect`, and the probe whose `consulted` set is
// the derived registry of operators this interpreter implements.
func stdlibIndependentFailures(
	t *testing.T, fixture stdlibFixture, operator string,
) (map[string]bool, *stdlibMutationProbe) {
	t.Helper()
	var model func(*testing.T, *stdlibModelState, stdlibStep, *stdlibMutationProbe) map[string]any
	switch fixture.Feature {
	case "stdlib_timer_v1":
		model = stdlibModelTimer
	case "stdlib_timeout_v1":
		model = stdlibModelTimeout
	case "stdlib_revision_barrier_v1":
		model = stdlibModelBarrier
	default:
		t.Fatalf("unsupported stdlib feature %q", fixture.Feature)
	}
	probe := newStdlibMutationProbe(operator)
	failed := map[string]bool{}
	for _, scenario := range fixture.Scenarios {
		state := &stdlibModelState{}
		for _, step := range scenario.Steps {
			if !stdlibModelAgrees(t, step.Expect, model(t, state, step, probe)) {
				failed[scenario.ID] = true
			}
		}
	}
	return failed, probe
}

// stdlibModelAgrees reports whether the model's observation equals the step's
// declared `expect`, comparing the WHOLE object in both directions — an
// unmodelled key is a mismatch, so a model that drops a key diverges rather than
// passing.
func stdlibModelAgrees(t *testing.T, expectedRaw json.RawMessage, actual map[string]any) bool {
	t.Helper()
	var expected any
	decoder := json.NewDecoder(bytes.NewReader(expectedRaw))
	decoder.UseNumber()
	if err := decoder.Decode(&expected); err != nil {
		t.Fatalf("decode expected %s: %v", expectedRaw, err)
	}
	actualRaw, err := json.Marshal(actual)
	if err != nil {
		t.Fatalf("encode model observation: %v", err)
	}
	var normalized any
	decoder = json.NewDecoder(bytes.NewReader(actualRaw))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		t.Fatalf("decode model observation %s: %v", actualRaw, err)
	}
	return reflect.DeepEqual(normalized, expected)
}

func sortedStdlibIDs(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// TestStdlibIndependentInterpreterMatchesUnperturbedCorpus is the non-vacuity
// control. Without it a mutation proves nothing: a scenario that fails whether or
// not the operator is applied is not evidence that the operator broke it.
func TestStdlibIndependentInterpreterMatchesUnperturbedCorpus(t *testing.T) {
	for _, name := range stdlibFixtureNames {
		name := name
		t.Run(name, func(t *testing.T) {
			fixture := loadStdlibFixture(t, name)
			if len(fixture.Scenarios) == 0 {
				t.Fatalf("stdlib/%s: no scenarios to replay", name)
			}
			failed, _ := stdlibIndependentFailures(t, fixture, "")
			if len(failed) != 0 {
				t.Fatalf("stdlib/%s: the independent interpreter diverged from the "+
					"canonical corpus with NO operator applied, on %v",
					name, sortedStdlibIDs(failed))
			}
		})
	}
}

// TestStdlibDeclaredMutationsAreObservedByIndependentInterpreter APPLIES every
// operator the corpus names and holds the ledger to its own claim
// (#lzstdlibmutantsallbindings).
func TestStdlibDeclaredMutationsAreObservedByIndependentInterpreter(t *testing.T) {
	pairs := 0
	for _, name := range stdlibFixtureNames {
		fixture := loadStdlibFixture(t, name)
		baseline, _ := stdlibIndependentFailures(t, fixture, "")
		if len(baseline) != 0 {
			t.Fatalf("stdlib/%s: unperturbed replay already fails on %v — no mutation "+
				"below can prove anything", name, sortedStdlibIDs(baseline))
		}
		if len(fixture.Mutations) == 0 {
			t.Fatalf("stdlib/%s: empty mutation ledger", name)
		}
		fixturePairs := 0
		for _, mutation := range fixture.Mutations {
			failed, probe := stdlibIndependentFailures(t, fixture, mutation.Operator)
			// An operator with no interpreter arm is a HARD failure, never a
			// skip: a silently unimplemented operator is the same vacuity as a
			// ledger checked against itself.
			if !probe.consulted[mutation.Operator] {
				t.Fatalf("stdlib/%s: mutation operator %q is declared by the corpus but "+
					"no arm of the independent interpreter implements it; the replay "+
					"consulted %v", name, mutation.Operator, probe.consultedNames())
			}
			if len(mutation.MustFail) == 0 {
				t.Fatalf("stdlib/%s: mutation %q names no scenario", name, mutation.Operator)
			}
			for _, id := range mutation.MustFail {
				if !failed[id] {
					t.Errorf("stdlib/%s: mutation %q did NOT break scenario %q — the "+
						"ledger claims that scenario detects it",
						name, mutation.Operator, id)
				}
				// Redundant given the baseline check above, but it names the
				// PAIR rather than the fixture when it fires.
				if baseline[id] {
					t.Errorf("stdlib/%s: %q/%q fails with the operator applied AND "+
						"without it, so the mutation proves nothing",
						name, mutation.Operator, id)
				}
				// The per-pair non-vacuity record, printed under `-v`: the pair
				// is green with no operator applied and red with it. Logged
				// rather than left implicit so the evidence for "this run
				// applied N pairs" is enumerable rather than a bare count.
				t.Logf("pair %2d  stdlib/%-21s %-38s %-52s unperturbed=pass perturbed=%s",
					pairs+fixturePairs+1, name, mutation.Operator, id,
					map[bool]string{true: "fail", false: "PASS(!)"}[failed[id]])
				fixturePairs++
			}
		}
		// Every entry contributes at least one (operator, scenario) pair, so the
		// corpus's own `mutation_floor` is also a floor on what this run APPLIED
		// — not merely on what the file lists.
		if fixturePairs < fixture.MutationFloor {
			t.Fatalf("stdlib/%s: applied %d (operator, scenario) pairs, below the "+
				"declared mutation_floor %d", name, fixturePairs, fixture.MutationFloor)
		}
		pairs += fixturePairs
	}
	if pairs < stdlibMutationPairFloor {
		t.Fatalf("applied only %d (operator, scenario) pairs, below the %d this corpus "+
			"declares today", pairs, stdlibMutationPairFloor)
	}
	t.Logf("applied %d (operator, scenario) pairs across %d stdlib fixtures",
		pairs, len(stdlibFixtureNames))
}

// TestStdlibMutationComplementIsNotAssertedByThisCorpus records, as a test with a
// concrete instance, WHY the obvious complement is absent above.
//
// The complement — "a scenario NOT named in `must_fail` survives the operator" —
// is FALSE for this corpus, and asserting it would invent a claim the fixtures
// never make. `deadline_strict_greater` on timer.json also breaks
// `clock_regression_is_rejected_without_state_change`, whose final step observes
// exactly at the deadline. `must_fail` is a lower bound on detection, not a
// partition; lazily-rs makes the same choice (`must_fail.is_subset(&failed)`,
// not equality) and so does lazily-py.
//
// Written as a test rather than a comment so the day the corpus DOES become a
// partition, this stops being true and someone has to decide deliberately whether
// to tighten the assertion above.
func TestStdlibMutationComplementIsNotAssertedByThisCorpus(t *testing.T) {
	const operator = "deadline_strict_greater"
	fixture := loadStdlibFixture(t, "timer.json")
	failed, _ := stdlibIndependentFailures(t, fixture, operator)
	named := map[string]bool{}
	found := false
	for _, mutation := range fixture.Mutations {
		if mutation.Operator != operator {
			continue
		}
		found = true
		for _, id := range mutation.MustFail {
			named[id] = true
		}
	}
	if !found {
		t.Fatalf("timer.json no longer declares a %q mutation", operator)
	}
	unnamed := map[string]bool{}
	for id := range failed {
		if !named[id] {
			unnamed[id] = true
		}
	}
	want := []string{"clock_regression_is_rejected_without_state_change"}
	if got := sortedStdlibIDs(unnamed); !reflect.DeepEqual(got, want) {
		t.Fatalf("timer.json/%s breaks %v beyond its `must_fail`; this test recorded %v.\n"+
			"%s", operator, got, want,
			fmt.Sprint("If the corpus has become a partition, decide deliberately whether ",
				"TestStdlibDeclaredMutationsAreObservedByIndependentInterpreter should ",
				"assert equality rather than a subset — do not just update this list."))
	}
}
