package lazily

// The generic effect-CAS corpus (#lzeffectcas), conformance/egress/effect_cas.json,
// replayed against EffectCasCore, ThreadSafeEffectCas, and a core that is torn
// down and HYDRATED from its durable image after every single step
// (#lzeffectcasdurable) — so the corpus also proves that a restart between any
// two operations is unobservable.
//
// Every scenario is booked in the scenario ledger at the point of replay, every
// step's outcome is compared WHOLE against `returns`, and every key of `final`
// is compared against the library's own snapshot, with the KEY SET of `final`
// checked against the keys the core actually holds. Each model returns the
// number of scenarios and steps it EXECUTED and the gate requires both to equal
// the corpus totals.

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

const effectCasFixtureName = "effect_cas.json"

type effectCasTestCore = EffectCasCore[string, uint64, string, string, string]

type effectCasModel interface {
	upsert(intent EffectIntent[string, uint64, string]) EffectCasUpsert
	claim(key string, generation, now uint64) EffectCasClaim[string, uint64, string, string]
	applied(key string, attempt EffectAttemptIdentity, receipt string, authority uint64) EffectCasApplied
	superseded(key string, attempt EffectAttemptIdentity, observed uint64) EffectCasFailure
	retryable(key string, attempt EffectAttemptIdentity, err string, retryAt uint64) EffectCasFailure
	rejected(key string, attempt EffectAttemptIdentity, err string) EffectCasFailure
	reconnect(generation uint64) EffectCasReconnect
	snapshot() EffectCasSnapshot[string, uint64, string, string, string]
}

// --- model 1: the pure core --------------------------------------------------

type coreEffectCasModel struct{ core *effectCasTestCore }

func (m *coreEffectCasModel) upsert(intent EffectIntent[string, uint64, string]) EffectCasUpsert {
	_, outcome := m.core.UpsertDesired(intent)
	return outcome
}
func (m *coreEffectCasModel) claim(key string, generation, now uint64) EffectCasClaim[string, uint64, string, string] {
	_, outcome := m.core.Claim(key, generation, now)
	return outcome
}
func (m *coreEffectCasModel) applied(key string, attempt EffectAttemptIdentity, receipt string, authority uint64) EffectCasApplied {
	_, outcome := m.core.RecordApplied(key, attempt, receipt, authority)
	return outcome
}
func (m *coreEffectCasModel) superseded(key string, attempt EffectAttemptIdentity, observed uint64) EffectCasFailure {
	_, outcome := m.core.RecordSuperseded(key, attempt, observed)
	return outcome
}
func (m *coreEffectCasModel) retryable(key string, attempt EffectAttemptIdentity, err string, retryAt uint64) EffectCasFailure {
	_, outcome := m.core.RecordRetryable(key, attempt, err, retryAt)
	return outcome
}
func (m *coreEffectCasModel) rejected(key string, attempt EffectAttemptIdentity, err string) EffectCasFailure {
	_, outcome := m.core.RecordRejected(key, attempt, err)
	return outcome
}
func (m *coreEffectCasModel) reconnect(generation uint64) EffectCasReconnect {
	_, outcome := m.core.Reconnect(generation)
	return outcome
}
func (m *coreEffectCasModel) snapshot() EffectCasSnapshot[string, uint64, string, string, string] {
	return m.core.Snapshot()
}

// --- model 2: restart after every step ---------------------------------------

// restartingEffectCasModel applies each operation, then replaces the core with
// one hydrated from its durable image. A field the image dropped would surface
// as a wrong outcome or final state on a later step.
type restartingEffectCasModel struct {
	t    *testing.T
	core *effectCasTestCore
}

func (m *restartingEffectCasModel) restart() {
	m.t.Helper()
	image := m.core.DurableState()
	restored, err := HydrateEffectCasCore(image)
	if err != nil {
		m.t.Fatalf("hydrate refused the core's own durable image: %v", err)
	}
	if !reflect.DeepEqual(restored.DurableState(), image) {
		m.t.Fatalf("hydrate is lossy: %+v != %+v", restored.DurableState(), image)
	}
	m.core = restored
}

func (m *restartingEffectCasModel) upsert(intent EffectIntent[string, uint64, string]) EffectCasUpsert {
	defer m.restart()
	_, outcome := m.core.UpsertDesired(intent)
	return outcome
}
func (m *restartingEffectCasModel) claim(key string, generation, now uint64) EffectCasClaim[string, uint64, string, string] {
	defer m.restart()
	_, outcome := m.core.Claim(key, generation, now)
	return outcome
}
func (m *restartingEffectCasModel) applied(key string, attempt EffectAttemptIdentity, receipt string, authority uint64) EffectCasApplied {
	defer m.restart()
	_, outcome := m.core.RecordApplied(key, attempt, receipt, authority)
	return outcome
}
func (m *restartingEffectCasModel) superseded(key string, attempt EffectAttemptIdentity, observed uint64) EffectCasFailure {
	defer m.restart()
	_, outcome := m.core.RecordSuperseded(key, attempt, observed)
	return outcome
}
func (m *restartingEffectCasModel) retryable(key string, attempt EffectAttemptIdentity, err string, retryAt uint64) EffectCasFailure {
	defer m.restart()
	_, outcome := m.core.RecordRetryable(key, attempt, err, retryAt)
	return outcome
}
func (m *restartingEffectCasModel) rejected(key string, attempt EffectAttemptIdentity, err string) EffectCasFailure {
	defer m.restart()
	_, outcome := m.core.RecordRejected(key, attempt, err)
	return outcome
}
func (m *restartingEffectCasModel) reconnect(generation uint64) EffectCasReconnect {
	defer m.restart()
	_, outcome := m.core.Reconnect(generation)
	return outcome
}
func (m *restartingEffectCasModel) snapshot() EffectCasSnapshot[string, uint64, string, string, string] {
	return m.core.Snapshot()
}

// --- model 3: the thread-safe reactive shell ---------------------------------

type tsEffectCasModel struct {
	ts    *ThreadSafeContext
	shell *ThreadSafeEffectCas[string, uint64, string, string, string]
}

func (m *tsEffectCasModel) upsert(intent EffectIntent[string, uint64, string]) EffectCasUpsert {
	return m.shell.UpsertDesired(intent)
}
func (m *tsEffectCasModel) claim(key string, generation, now uint64) EffectCasClaim[string, uint64, string, string] {
	return m.shell.Claim(key, generation, now)
}
func (m *tsEffectCasModel) applied(key string, attempt EffectAttemptIdentity, receipt string, authority uint64) EffectCasApplied {
	return m.shell.RecordApplied(key, attempt, receipt, authority)
}
func (m *tsEffectCasModel) superseded(key string, attempt EffectAttemptIdentity, observed uint64) EffectCasFailure {
	return m.shell.RecordSuperseded(key, attempt, observed)
}
func (m *tsEffectCasModel) retryable(key string, attempt EffectAttemptIdentity, err string, retryAt uint64) EffectCasFailure {
	return m.shell.RecordRetryable(key, attempt, err, retryAt)
}
func (m *tsEffectCasModel) rejected(key string, attempt EffectAttemptIdentity, err string) EffectCasFailure {
	return m.shell.RecordRejected(key, attempt, err)
}
func (m *tsEffectCasModel) reconnect(generation uint64) EffectCasReconnect {
	return m.shell.Reconnect(generation)
}
func (m *tsEffectCasModel) snapshot() EffectCasSnapshot[string, uint64, string, string, string] {
	// The reactive reader, not the core: the shell's projection is what is
	// under test here.
	return m.shell.Snapshot(nil)
}

type effectCasModelBuilder struct {
	name  string
	build func(t *testing.T, generation uint64) effectCasModel
}

var effectCasModelBuilders = []effectCasModelBuilder{
	{"core", func(_ *testing.T, generation uint64) effectCasModel {
		return &coreEffectCasModel{core: NewEffectCasCore[string, uint64, string, string, string](generation)}
	}},
	{"restart-every-step", func(t *testing.T, generation uint64) effectCasModel {
		return &restartingEffectCasModel{t: t, core: NewEffectCasCore[string, uint64, string, string, string](generation)}
	}},
	{"thread-safe", func(_ *testing.T, generation uint64) effectCasModel {
		ts := NewThreadSafeContext()
		return &tsEffectCasModel{ts: ts, shell: NewThreadSafeEffectCas[string, uint64, string, string, string](ts, generation)}
	}},
}

// --- outcome rendering -------------------------------------------------------

func effectCasUpsertValue(t *testing.T, outcome EffectCasUpsert) map[string]any {
	t.Helper()
	switch outcome.Kind {
	case EffectCasUpsertAccepted, EffectCasUpsertUnchanged, EffectCasUpsertEpochConflict:
		return map[string]any{"upsert": string(outcome.Kind)}
	case EffectCasUpsertAlreadyApplied:
		return map[string]any{"upsert": "already_applied", "applied_through": outcome.AppliedThrough}
	case EffectCasUpsertStaleEpoch:
		return map[string]any{"upsert": "stale_epoch", "current": outcome.Current}
	}
	t.Fatalf("unrendered upsert kind %q", outcome.Kind)
	return nil
}

func effectCasClaimValue(t *testing.T, outcome EffectCasClaim[string, uint64, string, string]) map[string]any {
	t.Helper()
	switch outcome.Kind {
	case EffectCasClaimClaimed:
		return map[string]any{
			"claim":    "claimed",
			"identity": outcome.Attempt.Identity.Effect.String(),
			"step":     outcome.Attempt.Identity.Step,
			"attempt":  outcome.Attempt.Identity.Attempt,
			"expected": outcome.Attempt.Expected,
			"payload":  outcome.Attempt.Payload,
		}
	case EffectCasClaimEmpty, EffectCasClaimBusy:
		return map[string]any{"claim": string(outcome.Kind)}
	case EffectCasClaimWaiting:
		return map[string]any{"claim": "waiting", "retry_at": outcome.RetryAt}
	case EffectCasClaimStaleGeneration:
		return map[string]any{"claim": "stale_generation", "current": outcome.Current}
	}
	t.Fatalf("unrendered claim kind %q", outcome.Kind)
	return nil
}

func effectCasAppliedValue(t *testing.T, outcome EffectCasApplied) map[string]any {
	t.Helper()
	switch outcome.Kind {
	case EffectCasAppliedStepApplied:
		return map[string]any{"applied": "step_applied", "next_step": outcome.NextStep}
	case EffectCasAppliedCompleted:
		return map[string]any{"applied": "completed", "applied_through": outcome.AppliedThrough}
	case EffectCasAppliedSuperseded, EffectCasAppliedDuplicate, EffectCasAppliedUnknownAttempt:
		return map[string]any{"applied": string(outcome.Kind)}
	case EffectCasAppliedStaleGeneration:
		return map[string]any{"applied": "stale_generation", "current": outcome.Current}
	}
	t.Fatalf("unrendered applied kind %q", outcome.Kind)
	return nil
}

func effectCasFailureValue(t *testing.T, outcome EffectCasFailure) map[string]any {
	t.Helper()
	switch outcome.Kind {
	case EffectCasFailureSuperseded, EffectCasFailureRejected, EffectCasFailureUnknownAttempt:
		return map[string]any{"failure": string(outcome.Kind)}
	case EffectCasFailureRetrying:
		return map[string]any{"failure": "retrying", "retry_at": outcome.RetryAt}
	case EffectCasFailureStaleGeneration:
		return map[string]any{"failure": "stale_generation", "current": outcome.Current}
	}
	t.Fatalf("unrendered failure kind %q", outcome.Kind)
	return nil
}

func effectCasReconnectValue(t *testing.T, outcome EffectCasReconnect) map[string]any {
	t.Helper()
	switch outcome.Kind {
	case EffectCasReconnectAdvanced:
		return map[string]any{
			"reconnect":  "advanced",
			"generation": outcome.Generation,
			"requeued":   outcome.Requeued,
			"superseded": outcome.Superseded,
		}
	case EffectCasReconnectUnchanged:
		return map[string]any{"reconnect": "unchanged", "generation": outcome.Generation}
	case EffectCasReconnectStaleGeneration:
		return map[string]any{"reconnect": "stale_generation", "current": outcome.Current}
	}
	t.Fatalf("unrendered reconnect kind %q", outcome.Kind)
	return nil
}

func effectCasKeyStateValue(state EffectCasKeyState[string, uint64, string, string, string]) map[string]any {
	value := map[string]any{
		"pending_identity":  nil,
		"inflight_identity": nil,
		"applied_through":   nil,
		"terminal":          nil,
		"receipt_count":     0,
		"retry_at":          nil,
	}
	if state.Pending != nil {
		value["pending_identity"] = state.Pending.Intent.Identity.String()
	}
	if state.Inflight != nil {
		value["inflight_identity"] = state.Inflight.Identity.Effect.String()
	}
	if state.AppliedThrough.Present {
		value["applied_through"] = state.AppliedThrough.Value
	}
	if state.Terminal != nil {
		value["terminal"] = string(state.Terminal.Kind)
		value["receipt_count"] = len(state.Terminal.Receipts)
	}
	if state.Retry != nil {
		value["retry_at"] = state.Retry.RetryAt
	}
	return value
}

// --- the replay --------------------------------------------------------------

func runEffectCasStep(
	t *testing.T,
	label string,
	model effectCasModel,
	attempts map[string]EffectAttemptIdentity,
	step map[string]any,
	op map[string]any,
) map[string]any {
	t.Helper()
	input := "replay input: an argument of the operation applied to the authority; its effect is asserted through this step's returns and the scenario's final block"
	u := func(key string) uint64 { return uint64(jsInt(op[key])) }
	attempt := func() EffectAttemptIdentity {
		saved, ok := attempts[jsStr(op["attempt"])]
		if !ok {
			t.Fatalf("%s: attempt label %q was never saved by an earlier claim", label, jsStr(op["attempt"]))
		}
		return saved
	}
	opType := jsStr(op["type"])
	switch opType {
	case "upsert":
		consumeKeys(t, label+" op", op, "type", "identity", "key", "epoch", "expected", "plan")
		excuseKeys(t, op, input, "type", "identity", "key", "epoch", "expected", "plan")
		identity, err := NewEffectIdentity(jsStr(op["identity"]))
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		plan, err := NewEffectPlan(jsStrList(op["plan"]))
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		return effectCasUpsertValue(t, model.upsert(EffectIntent[string, uint64, string]{
			Identity: identity,
			Epoch:    u("epoch"),
			Key:      jsStr(op["key"]),
			Expected: u("expected"),
			Plan:     plan,
		}))
	case "claim":
		consumeKeys(t, label+" op", op, "type", "key", "generation", "now")
		excuseKeys(t, op, input, "type", "key", "generation", "now")
		claim := model.claim(jsStr(op["key"]), u("generation"), u("now"))
		if saveAs, saves := step["save"]; saves {
			if claim.Kind != EffectCasClaimClaimed {
				t.Fatalf("%s: a claim saved as %v must succeed, got %q", label, saveAs, claim.Kind)
			}
			attempts[jsStr(saveAs)] = claim.Attempt.Identity
		}
		return effectCasClaimValue(t, claim)
	case "applied":
		consumeKeys(t, label+" op", op, "type", "key", "attempt", "receipt", "authority")
		excuseKeys(t, op, input, "type", "key", "attempt", "receipt", "authority")
		return effectCasAppliedValue(t, model.applied(jsStr(op["key"]), attempt(), jsStr(op["receipt"]), u("authority")))
	case "superseded":
		consumeKeys(t, label+" op", op, "type", "key", "attempt", "observed", "backend_before", "backend_after")
		excuseKeys(t, op, input, "type", "key", "attempt", "observed")
		// The core never touches the backend: zero backend mutation on a compare
		// mismatch is the ADAPTER's obligation, proven against a real adapter in
		// TestEffectCasCompareMismatchMutatesNoBackend. Here the corpus states it
		// and the runner holds the corpus to its own claim.
		excuseKeys(t, op, "adapter-side evidence: the backend authority before and after the mismatched compare; the pure core owns no backend, so zero mutation is proven by TestEffectCasCompareMismatchMutatesNoBackend",
			"backend_before", "backend_after")
		if jsInt(op["backend_before"]) != jsInt(op["backend_after"]) {
			t.Fatalf("%s: corpus states a mismatched compare that mutated the backend (%v -> %v)",
				label, op["backend_before"], op["backend_after"])
		}
		return effectCasFailureValue(t, model.superseded(jsStr(op["key"]), attempt(), u("observed")))
	case "retryable":
		consumeKeys(t, label+" op", op, "type", "key", "attempt", "error", "retry_at")
		excuseKeys(t, op, input, "type", "key", "attempt", "error", "retry_at")
		return effectCasFailureValue(t, model.retryable(jsStr(op["key"]), attempt(), jsStr(op["error"]), u("retry_at")))
	case "rejected":
		consumeKeys(t, label+" op", op, "type", "key", "attempt", "error")
		excuseKeys(t, op, input, "type", "key", "attempt", "error")
		return effectCasFailureValue(t, model.rejected(jsStr(op["key"]), attempt(), jsStr(op["error"])))
	case "reconnect":
		consumeKeys(t, label+" op", op, "type", "generation")
		excuseKeys(t, op, input, "type", "generation")
		return effectCasReconnectValue(t, model.reconnect(u("generation")))
	}
	t.Fatalf("%s: unknown effect-CAS op %q", label, opType)
	return nil
}

func loadEffectCasFixture(t *testing.T) map[string]any {
	t.Helper()
	data := loadEgressFixtureBytes(t, effectCasFixtureName)
	var fixture map[string]any
	mustStrictJSON(t, effectCasFixtureName, data, &fixture)
	consumeFixtureKeys(t, effectCasFixtureName, fixture, "$schema", "scenarios")
	excuseKey(t, fixture, "$schema", "schema pointer: names the JSON schema the corpus validates this fixture against in lazily-spec CI; it states nothing a replay observes")
	excuseKey(t, fixture, "scenarios", "container: each scenario is booked and replayed below, its steps and final block asserted there")
	if fixture["kind"] != "EffectCas" || fixture["model"] != "EffectCasCore" {
		t.Fatalf("%s: kind/model=(%v,%v), want (EffectCas,EffectCasCore)", effectCasFixtureName, fixture["kind"], fixture["model"])
	}
	return fixture
}

// replayEffectCasCorpus replays every scenario against one model and returns
// the scenarios and steps EXECUTED.
func replayEffectCasCorpus(t *testing.T, builder effectCasModelBuilder) (scenarios, steps int) {
	t.Helper()
	fixture := loadEffectCasFixture(t)
	for _, sv := range scenarioViews("egress/"+effectCasFixtureName, jsList(fixture["scenarios"])) {
		scenario := sv.Map()
		label := fmt.Sprintf("%s %s", builder.name, sv.Label())
		consumeKeys(t, label, scenario, "id", "generation", "steps", "final")
		excuseKey(t, scenario, "id", "scenario label: booked in the scenario ledger and used as the failure label")
		excuseKey(t, scenario, "generation", "replay input: the actor generation the authority is constructed at")
		excuseKey(t, scenario, "steps", "container: each step's returns is asserted in the loop below")

		model := builder.build(t, uint64(jsInt(scenario["generation"])))
		attempts := map[string]EffectAttemptIdentity{}
		for index, raw := range jsList(scenario["steps"]) {
			step := jsMap(raw)
			op := jsMap(step["op"])
			stepLabel := fmt.Sprintf("%s step %d (%s)", label, index, jsStr(op["type"]))
			if _, saves := step["save"]; saves {
				consumeKeys(t, stepLabel, step, "op", "returns", "save")
				excuseKey(t, step, "save", "replay input: the label under which this claim's exact attempt identity is retained for a later receipt")
			} else {
				consumeKeys(t, stepLabel, step, "op", "returns")
			}
			excuseKey(t, step, "op", "replay input: the operation applied to the authority; its effect is asserted through returns and final")
			got := runEffectCasStep(t, stepLabel, model, attempts, step, op)
			assertKey(t, step, "returns", got)
			// Warm the reader after every step, so the final read below is served
			// from a cache that only a correct invalidation refreshes — a shell
			// that never invalidated would report the state of an earlier step.
			_ = model.snapshot()
			steps++
		}

		snapshot := model.snapshot()
		observed := map[string]bool{}
		byKey := map[string]EffectCasKeyState[string, uint64, string, string, string]{}
		for _, state := range snapshot.Keys {
			observed[state.Key] = true
			byKey[state.Key] = state
		}
		assertKeySet(t, scenario, "final", observed, func(key string, raw any) {
			want := consumeKeys(t, label+" final."+key, jsMap(raw),
				"pending_identity", "inflight_identity", "applied_through", "terminal", "receipt_count", "retry_at")
			got := effectCasKeyStateValue(byKey[key])
			for _, field := range []string{"pending_identity", "inflight_identity", "applied_through", "terminal", "receipt_count", "retry_at"} {
				assertKey(t, want, field, got[field])
			}
		})
		scenarios++
	}
	return scenarios, steps
}

func TestEffectCasConformanceCorpusReplaysExactly(t *testing.T) {
	fixture := loadEffectCasFixture(t)
	// Counting the raw list is not replaying; every scenario is booked by the
	// replays below, at the point each one is executed.
	wantScenarios, wantSteps := 0, 0
	for _, raw := range jsList(fixture["scenarios"]) {
		wantScenarios++
		wantSteps += len(jsList(jsMap(raw)["steps"]))
	}
	if wantScenarios == 0 || wantSteps == 0 {
		t.Fatalf("%s carries %d scenarios / %d steps; a vacuous corpus", effectCasFixtureName, wantScenarios, wantSteps)
	}
	for _, builder := range effectCasModelBuilders {
		t.Run(builder.name, func(t *testing.T) {
			scenarios, steps := replayEffectCasCorpus(t, builder)
			if scenarios != wantScenarios || steps != wantSteps {
				t.Fatalf("%s executed %d scenarios / %d steps, want the corpus totals %d / %d",
					builder.name, scenarios, steps, wantScenarios, wantSteps)
			}
			t.Logf("effect-cas[%s]: %d scenarios, %d steps executed", builder.name, scenarios, steps)
		})
	}
}

// --- adapter boundary, reactive shell, durable image -------------------------

// casBackend is a minimal atomic compare-and-apply backend: one authority token
// per key, bumped by every successful apply.
type casBackend struct {
	authority map[string]uint64
	writes    int
}

func (b *casBackend) Observe(key string) (uint64, error) { return b.authority[key], nil }

func (b *casBackend) CompareAndApply(attempt EffectAttempt[string, uint64, string, string]) EffectApply[string, uint64, string] {
	current := b.authority[attempt.Key]
	if current != attempt.Expected {
		return EffectApply[string, uint64, string]{Kind: EffectApplySuperseded, Observed: current}
	}
	b.writes++
	b.authority[attempt.Key] = current + 1
	return EffectApply[string, uint64, string]{
		Kind:      EffectApplyApplied,
		Receipt:   fmt.Sprintf("%s@%d", attempt.Payload, current+1),
		Authority: current + 1,
	}
}

func mustIntent(t *testing.T, id string, epoch uint64, key string, expected uint64, steps ...string) EffectIntent[string, uint64, string] {
	t.Helper()
	identity, err := NewEffectIdentity(id)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewEffectPlan(steps)
	if err != nil {
		t.Fatal(err)
	}
	return EffectIntent[string, uint64, string]{Identity: identity, Epoch: epoch, Key: key, Expected: expected, Plan: plan}
}

func TestEffectCasCompareMismatchMutatesNoBackend(t *testing.T) {
	ts := NewThreadSafeContext()
	shell := NewThreadSafeEffectCas[string, uint64, string, string, string](ts, 1)
	backend := &casBackend{authority: map[string]uint64{"doc": 5}}
	shell.UpsertDesired(mustIntent(t, "mismatch", 1, "doc", 4, "write"))
	claim, outcome, ran := shell.RunOnce("doc", 0, 0, backend)
	if !ran || claim.Kind != EffectCasClaimClaimed {
		t.Fatalf("claim=%+v ran=%v", claim, ran)
	}
	if outcome.IsApplied || outcome.Failure.Kind != EffectCasFailureSuperseded {
		t.Fatalf("outcome=%+v, want superseded", outcome)
	}
	if backend.writes != 0 || backend.authority["doc"] != 5 {
		t.Fatalf("a compare mismatch mutated the backend: writes=%d authority=%d", backend.writes, backend.authority["doc"])
	}
	state, _ := shell.State("doc")
	if state.AppliedThrough.Present || state.Terminal == nil || state.Terminal.Kind != EffectTerminalSuperseded {
		t.Fatalf("state=%+v", state)
	}
}

func TestEffectCasOrderedPlanRunsThroughTheAdapterWithReceiptLineage(t *testing.T) {
	ts := NewThreadSafeContext()
	shell := NewThreadSafeEffectCas[string, uint64, string, string, string](ts, 3)
	backend := &casBackend{authority: map[string]uint64{"doc": 10}}
	shell.UpsertDesired(mustIntent(t, "plan", 1, "doc", 10, "canonical", "snapshot"))
	if keys := shell.PendingKeys(0); len(keys) != 1 || keys[0] != "doc" {
		t.Fatalf("pending keys=%v", keys)
	}
	_, first, _ := shell.RunOnce("doc", 0, 0, backend)
	if !first.IsApplied || first.Applied.Kind != EffectCasAppliedStepApplied {
		t.Fatalf("first=%+v", first)
	}
	second, done, _ := shell.RunOnce("doc", 0, 0, backend)
	if second.Attempt.Expected != 11 || len(second.Attempt.PriorReceipts) != 1 {
		t.Fatalf("second step must expect the prior receipt's authority: %+v", second.Attempt)
	}
	if !done.IsApplied || done.Applied.Kind != EffectCasAppliedCompleted || done.Applied.AppliedThrough != 1 {
		t.Fatalf("done=%+v", done)
	}
	if backend.authority["doc"] != 12 || backend.writes != 2 {
		t.Fatalf("backend authority=%d writes=%d", backend.authority["doc"], backend.writes)
	}
}

func TestThreadSafeEffectCasInvalidatesOnlyForStateChanges(t *testing.T) {
	ts := NewThreadSafeContext()
	shell := NewThreadSafeEffectCas[string, uint64, string, string, string](ts, 1)
	runs := 0
	var effect *Effect
	ts.WithLock(func(ctx *Context) {
		effect = NewEffect(ctx, func(c *Compute) func() {
			Get[EffectCasSnapshot[string, uint64, string, string, string]](c, shell.StateHandle())
			runs++
			return nil
		})
	})
	defer ts.WithLock(func(*Context) { effect.Dispose() })
	intent := mustIntent(t, "shell-effect", 1, "doc", 1, "write")
	if got := shell.UpsertDesired(intent); got.Kind != EffectCasUpsertAccepted {
		t.Fatalf("upsert=%+v", got)
	}
	if got := shell.UpsertDesired(intent); got.Kind != EffectCasUpsertUnchanged {
		t.Fatalf("re-upsert=%+v", got)
	}
	if got := shell.Claim("doc", 9, 0); got.Kind != EffectCasClaimStaleGeneration {
		t.Fatalf("stale claim=%+v", got)
	}
	if runs != 2 {
		t.Fatalf("effect ran %d times, want 2 (initial + one true transition)", runs)
	}
}

func TestEffectCasCrashReplayResumesTheSameIdentityAndCursor(t *testing.T) {
	core := NewEffectCasCore[string, uint64, string, string, string](1)
	core.UpsertDesired(mustIntent(t, "plan", 7, "doc", 10, "canonical", "snapshot"))
	_, first := core.Claim("doc", 1, 0)
	core.RecordApplied("doc", first.Attempt.Identity, "r0", 11)
	_, crashed := core.Claim("doc", 1, 0)
	image := core.DurableState()

	successor, err := HydrateEffectCasCore(image)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(successor.Snapshot(), core.Snapshot()) {
		t.Fatalf("hydrated snapshot differs")
	}
	if _, reconnect := successor.Reconnect(2); reconnect.Requeued != 1 {
		t.Fatalf("reconnect=%+v", reconnect)
	}
	if _, late := successor.RecordApplied("doc", crashed.Attempt.Identity, "late", 12); late.Kind != EffectCasAppliedStaleGeneration {
		t.Fatalf("the crashed actor's receipt must be fenced: %+v", late)
	}
	_, resumed := successor.Claim("doc", 2, 0)
	if resumed.Attempt.Identity.Effect != crashed.Attempt.Identity.Effect ||
		resumed.Attempt.Identity.Step != 1 ||
		resumed.Attempt.Identity.Attempt != crashed.Attempt.Identity.Attempt+1 ||
		resumed.Attempt.Expected != 11 ||
		len(resumed.Attempt.PriorReceipts) != 1 {
		t.Fatalf("resumed=%+v, want the same identity at step 1, attempt+1, expecting 11", resumed.Attempt)
	}
}

func TestEffectCasHydrateRefusesCorruptImages(t *testing.T) {
	core := NewEffectCasCore[string, uint64, string, string, string](2)
	core.UpsertDesired(mustIntent(t, "e", 1, "doc", 0, "write"))
	core.Claim("doc", 2, 0)
	good := core.DurableState()

	corrupt := func(edit func(*EffectCasDurableState[string, uint64, string, string, string])) error {
		image := core.DurableState()
		edit(&image)
		_, err := HydrateEffectCasCore(image)
		return err
	}
	cases := []struct {
		name string
		edit func(*EffectCasDurableState[string, uint64, string, string, string])
		want error
	}{
		{"duplicate key", func(s *EffectCasDurableState[string, uint64, string, string, string]) {
			s.Keys = append(s.Keys, s.Keys[0])
		}, ErrEffectCasDuplicateKey},
		{"key mismatch", func(s *EffectCasDurableState[string, uint64, string, string, string]) {
			s.Keys[0].Key = "other"
		}, ErrEffectCasKeyMismatch},
		{"cursor out of range", func(s *EffectCasDurableState[string, uint64, string, string, string]) {
			s.Keys[0].Inflight.Retained.NextStep = 5
		}, ErrEffectCasCursorOutOfRange},
		{"attempt mismatch", func(s *EffectCasDurableState[string, uint64, string, string, string]) {
			s.Keys[0].Inflight.Attempt.Identity.Attempt = 9
		}, ErrEffectCasAttemptMismatch},
		{"future generation", func(s *EffectCasDurableState[string, uint64, string, string, string]) {
			s.Generation = 1
		}, ErrEffectCasFutureGeneration},
	}
	for _, tc := range cases {
		if err := corrupt(tc.edit); !errors.Is(err, tc.want) {
			t.Fatalf("%s: err=%v, want %v", tc.name, err, tc.want)
		}
	}
	if _, err := HydrateEffectCasCore(good); err != nil {
		t.Fatalf("the uncorrupted image must hydrate: %v", err)
	}
	if _, err := NewEffectIdentity(""); !errors.Is(err, ErrEmptyEffectIdentity) {
		t.Fatalf("empty identity accepted: %v", err)
	}
	if _, err := NewEffectPlan[string](nil); !errors.Is(err, ErrEmptyEffectPlan) {
		t.Fatalf("empty plan accepted: %v", err)
	}
	keys := []string{}
	for _, k := range good.Keys {
		keys = append(keys, k.Key)
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatalf("durable keys unordered: %v", keys)
	}
}

// TestEffectCasHydratePreservesTerminalIntent pins the one retained field the
// reader snapshot never shows: the last terminal INTENT. It decides whether a
// re-upsert of the same epoch is idempotent (Unchanged) or fresh work, so an
// image that dropped it would silently re-run a superseded effect after a
// restart.
func TestEffectCasHydratePreservesTerminalIntent(t *testing.T) {
	core := NewEffectCasCore[string, uint64, string, string, string](1)
	intent := mustIntent(t, "once", 1, "doc", 4, "write")
	core.UpsertDesired(intent)
	_, claim := core.Claim("doc", 1, 0)
	core.RecordSuperseded("doc", claim.Attempt.Identity, 5)
	restored, err := HydrateEffectCasCore(core.DurableState())
	if err != nil {
		t.Fatal(err)
	}
	if _, live := core.UpsertDesired(intent); live.Kind != EffectCasUpsertUnchanged {
		t.Fatalf("live re-upsert=%+v, want unchanged", live)
	}
	if _, again := restored.UpsertDesired(intent); again.Kind != EffectCasUpsertUnchanged {
		t.Fatalf("re-upsert after restart=%+v, want unchanged: the image lost the terminal intent", again)
	}
}
