package lazily

// The transport-agnostic egress contract (#lzegress), replayed against EVERY
// flavor this binding ships — EgressCell / ThreadSafeEgressCell /
// AsyncEgressCell — matching the reactive-egress, egress-thread-safe and
// egress-async coverage rows.
//
// The flavor axis lives in the RUNNER, not the corpus: the fixtures name the
// `EgressCore` model and no execution model, and one egressFixtureModel interface
// replays the same JSON against each shell. The delivery authority is
// synchronous in every flavor (only the async transport ATTACHMENT is
// async-coloured), so there is no settle step below.
//
// What keeps this suite from reporting green while testing nothing:
//
//   - TestEgressCapabilityLedgerMatchesSource greps the package sources for each
//     flavor's type declaration, in BOTH directions.
//   - Every replay returns the number of steps it EXECUTED, and every flavor
//     asserts that count is non-zero and equal to the corpus total; an unknown
//     op is a t.Fatalf, never a skip. A fixture missing from a PRESENT corpus is
//     a failure (loadEgressFixtureBytes), not the loader's usual skip.
//   - `invalidates` is asserted in BOTH directions, per reader kind, through a
//     recompute-count probe in the library's own graph. A step expecting false
//     FAILS if the shell invalidated anyway. The probe is pinned by
//     TestEgressConformanceInvalidationProbeDiscriminates.
//   - The returned outcome is compared as a WHOLE object against the fixture's
//     `returns`, so an extra or missing outcome field is a mismatch.

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
)

// egressFixtures is every fixture the egress corpus ships for EgressCore. Named
// explicitly: a fixture added upstream and not here is caught by the runtime
// conformance manifest, not by a silently shorter run.
var egressFixtures = []string{
	"egress_ordered_ack.json",
	"egress_inflight_window.json",
	"egress_retry_budget.json",
	"egress_generation_fence.json",
}

// egressReaderKinds is the reader-kind axis, in the order the fixtures name it.
var egressReaderKinds = []string{"pending", "inflight", "acked_through", "retry"}

type egressCapability struct {
	flavor, marker string
	shipped        bool
}

var egressCapabilities = []egressCapability{
	{"sync", "type EgressCell[", true},
	{"thread-safe", "type ThreadSafeEgressCell[", true},
	{"async", "type AsyncEgressCell[", true},
}

func TestEgressCapabilityLedgerMatchesSource(t *testing.T) {
	sources := packageSources(t)
	if sources == "" {
		t.Fatal("read no package sources; capability ledger would be vacuous")
	}
	shipped := 0
	for _, capability := range egressCapabilities {
		defined := strings.Contains(sources, capability.marker)
		if defined != capability.shipped {
			t.Fatalf("%s: source marker %q present=%v, ledger shipped=%v; fix the ledger or extend the runner",
				capability.flavor, capability.marker, defined, capability.shipped)
		}
		if capability.shipped {
			shipped++
		}
	}
	if shipped != 3 {
		t.Fatalf("egress ledger ships %d flavors, want 3", shipped)
	}
}

// --- fixture loading ---------------------------------------------------------

// loadEgressFixtureBytes reads one egress fixture. When a corpus is PRESENT (or
// under CI) a missing fixture is a failure: the shared loader's skip is for an
// absent sibling checkout, not for a corpus that lost the file this runner
// exists to replay.
func loadEgressFixtureBytes(t *testing.T, name string) []byte {
	t.Helper()
	info, err := os.Stat(specDir())
	corpusPresent := err == nil && info.IsDir()
	if corpusPresent || os.Getenv("CI") != "" {
		for _, path := range specCandidatePaths("egress", name) {
			if data, readErr := specReadFile(path); readErr == nil {
				return data
			}
		}
		t.Fatalf("egress fixture %s is missing from the corpus at %s; a missing fixture is a failure, not a skip",
			name, specDir())
	}
	return loadConformanceFixture(t, "egress", name)
}

func loadEgressFixture(t *testing.T, name string) map[string]any {
	t.Helper()
	data := loadEgressFixtureBytes(t, name)
	var fixture map[string]any
	mustStrictJSON(t, name, data, &fixture)
	consumeFixtureKeys(t, name, fixture, "generation", "policy", "steps")
	excuseKeys(t, fixture, "replay input: the generation and policy the egress is CONSTRUCTED with; what they produce is asserted through every step's expected block",
		"generation", "policy")
	excuseKey(t, fixture, "steps", "replay input: the step list drives the loop, and each step's own returns/expected blocks are asserted there")
	if got := fixture["kind"]; got != "Egress" {
		t.Fatalf("%s: fixture kind=%v, want Egress", name, got)
	}
	if got := fixture["model"]; got != "EgressCore" {
		t.Fatalf("%s: fixture model=%v, want EgressCore", name, got)
	}
	if len(jsList(fixture["steps"])) == 0 {
		t.Fatalf("%s: a fixture with no steps is a vacuous replay", name)
	}
	return fixture
}

func expectedEgressStepTotal(t *testing.T) int {
	t.Helper()
	total := 0
	for _, name := range egressFixtures {
		total += len(jsList(loadEgressFixture(t, name)["steps"]))
	}
	return total
}

func egressPolicyOf(t *testing.T, raw any) EgressPolicy {
	t.Helper()
	policy := consumeKeys(t, "egress policy", jsMap(raw),
		"inflight_limit", "retry_budget", "retry_base", "retry_ceiling")
	excuseKeys(t, policy, "replay input: folded into the EgressPolicy the cell is constructed with; its effect is asserted through window_full, retry backoff and exhaustion expectations",
		"inflight_limit", "retry_budget", "retry_base", "retry_ceiling")
	return EgressPolicy{
		InflightLimit: jsInt(policy["inflight_limit"]),
		RetryBudget:   uint32(jsInt(policy["retry_budget"])),
		RetryBase:     uint64(jsInt(policy["retry_base"])),
		RetryCeiling:  uint64(jsInt(policy["retry_ceiling"])),
	}
}

// --- outcome rendering -------------------------------------------------------
//
// Each outcome is rendered to the corpus's JSON shape and compared WHOLE against
// `returns`, so a field the library returns that the corpus does not name — or
// the reverse — is a mismatch, not a silent pass.

func egressEnvelopeValue(envelope EgressEnvelope[uint64]) map[string]any {
	return map[string]any{
		"generation": envelope.Generation,
		"sequence":   envelope.Sequence,
		"attempt":    envelope.Attempt,
		"payload":    envelope.Payload,
	}
}

func egressEnvelopesValue(envelopes []EgressEnvelope[uint64]) []any {
	out := make([]any, len(envelopes))
	for i, envelope := range envelopes {
		out[i] = egressEnvelopeValue(envelope)
	}
	return out
}

func egressRetryValue(retry EgressRetry) map[string]any {
	return map[string]any{
		"sequence":  retry.Sequence,
		"attempt":   retry.Attempt,
		"backoff":   retry.Backoff,
		"exhausted": retry.Exhausted,
	}
}

func egressClaimValue(t *testing.T, claim EgressClaim[uint64]) map[string]any {
	t.Helper()
	switch claim.Kind {
	case EgressClaimClaimed:
		return map[string]any{"claim": "claimed", "envelope": egressEnvelopeValue(claim.Envelope)}
	case EgressClaimEmpty, EgressClaimWindowFull:
		return map[string]any{"claim": string(claim.Kind)}
	case EgressClaimStaleGeneration:
		return map[string]any{"claim": "stale_generation", "current": claim.Current}
	}
	t.Fatalf("unrendered claim kind %q", claim.Kind)
	return nil
}

func egressAckValue(t *testing.T, ack EgressAck) map[string]any {
	t.Helper()
	switch ack.Kind {
	case EgressAckAdvanced, EgressAckUnchanged:
		return map[string]any{"ack": string(ack.Kind), "through": ack.Through}
	case EgressAckStaleGeneration:
		return map[string]any{"ack": "stale_generation", "current": ack.Current}
	case EgressAckBeyondSent:
		return map[string]any{"ack": "beyond_sent", "next_sequence": ack.NextSequence}
	}
	t.Fatalf("unrendered ack kind %q", ack.Kind)
	return nil
}

func egressFailureValue(t *testing.T, failure EgressFailure) map[string]any {
	t.Helper()
	switch failure.Kind {
	case EgressFailureRetrying, EgressFailureExhausted:
		return map[string]any{"failure": string(failure.Kind), "retry": egressRetryValue(failure.Retry)}
	case EgressFailureUnknownSequence:
		return map[string]any{"failure": "unknown_sequence"}
	case EgressFailureStaleGeneration:
		return map[string]any{"failure": "stale_generation", "current": failure.Current}
	}
	t.Fatalf("unrendered failure kind %q", failure.Kind)
	return nil
}

func egressRetryActionValue(t *testing.T, action EgressRetryAction) map[string]any {
	t.Helper()
	switch action.Kind {
	case EgressRetryScheduled:
		return map[string]any{"retry_action": "scheduled", "sequence": action.Sequence}
	case EgressRetryUnknownSequence:
		return map[string]any{"retry_action": "unknown_sequence"}
	case EgressRetryStaleGeneration:
		return map[string]any{"retry_action": "stale_generation", "current": action.Current}
	}
	t.Fatalf("unrendered retry action kind %q", action.Kind)
	return nil
}

func egressReconnectValue(t *testing.T, reconnect EgressReconnect) map[string]any {
	t.Helper()
	switch reconnect.Kind {
	case EgressReconnectAdvanced:
		return map[string]any{"reconnect": "advanced", "generation": reconnect.Generation, "replayed": reconnect.Replayed}
	case EgressReconnectUnchanged:
		return map[string]any{"reconnect": "unchanged", "generation": reconnect.Generation}
	case EgressReconnectStaleGeneration:
		return map[string]any{"reconnect": "stale_generation", "current": reconnect.Current}
	}
	t.Fatalf("unrendered reconnect kind %q", reconnect.Kind)
	return nil
}

// --- the flavor-neutral model ------------------------------------------------

type egressFixtureModel interface {
	flavor() string

	enqueue(payload uint64) uint64
	claim(generation uint64) EgressClaim[uint64]
	ack(generation, through uint64) EgressAck
	fail(generation, sequence uint64) EgressFailure
	retryNow(generation, sequence uint64) EgressRetryAction
	reconnect(generation uint64) EgressReconnect

	generation() uint64
	nextSequence() uint64
	pending() []EgressEnvelope[uint64]
	inflight() []EgressEnvelope[uint64]
	ackedThrough() Opt[uint64]
	retry() Opt[EgressRetry]

	// probe reports how many times a dependent of the given reader kind
	// recomputed — the cache-validity probe `invalidates` is asserted through.
	probe(kind string) qfReader
}

type egressModelBuilder func(t *testing.T, generation uint64, policy EgressPolicy) egressFixtureModel

// --- flavor 1: single-threaded -----------------------------------------------

type syncEgressFixtureModel struct {
	ctx    *Context
	cell   *EgressCell[uint64]
	probes map[string]qfReader
}

func newSyncEgressFixtureModel(t *testing.T, generation uint64, policy EgressPolicy) egressFixtureModel {
	t.Helper()
	ctx := NewContext()
	cell, err := NewEgressCell[uint64](ctx, generation, policy)
	if err != nil {
		t.Fatalf("sync egress build: %v", err)
	}
	readers := cell.Readers()
	return &syncEgressFixtureModel{ctx: ctx, cell: cell, probes: map[string]qfReader{
		"pending": newQFSyncReader(ctx, func(c *Compute) int {
			return len(Get[[]EgressEnvelope[uint64]](c, readers.Pending))
		}),
		"inflight": newQFSyncReader(ctx, func(c *Compute) int {
			return len(Get[[]EgressEnvelope[uint64]](c, readers.Inflight))
		}),
		"acked_through": newQFSyncReader(ctx, func(c *Compute) int {
			return int(Get[Opt[uint64]](c, readers.AckedThrough).Value)
		}),
		"retry": newQFSyncReader(ctx, func(c *Compute) int {
			return int(Get[Opt[EgressRetry]](c, readers.Retry).Value.Attempt)
		}),
	}}
}

func (m *syncEgressFixtureModel) flavor() string                     { return "sync" }
func (m *syncEgressFixtureModel) enqueue(p uint64) uint64            { return m.cell.Enqueue(p) }
func (m *syncEgressFixtureModel) claim(g uint64) EgressClaim[uint64] { return m.cell.Claim(g) }
func (m *syncEgressFixtureModel) ack(g, through uint64) EgressAck    { return m.cell.Ack(g, through) }
func (m *syncEgressFixtureModel) fail(g, s uint64) EgressFailure     { return m.cell.Fail(g, s) }
func (m *syncEgressFixtureModel) retryNow(g, s uint64) EgressRetryAction {
	return m.cell.RetryNow(g, s)
}
func (m *syncEgressFixtureModel) reconnect(g uint64) EgressReconnect { return m.cell.Reconnect(g) }
func (m *syncEgressFixtureModel) generation() uint64                 { return m.cell.Generation() }
func (m *syncEgressFixtureModel) nextSequence() uint64               { return m.cell.NextSequence() }
func (m *syncEgressFixtureModel) pending() []EgressEnvelope[uint64]  { return m.cell.Pending(m.ctx) }
func (m *syncEgressFixtureModel) inflight() []EgressEnvelope[uint64] { return m.cell.Inflight(m.ctx) }
func (m *syncEgressFixtureModel) ackedThrough() Opt[uint64]          { return m.cell.AckedThrough(m.ctx) }
func (m *syncEgressFixtureModel) retry() Opt[EgressRetry]            { return m.cell.Retry(m.ctx) }
func (m *syncEgressFixtureModel) probe(kind string) qfReader         { return m.probes[kind] }

// --- flavor 2: thread-safe ---------------------------------------------------

type tsEgressFixtureModel struct {
	ts     *ThreadSafeContext
	cell   *ThreadSafeEgressCell[uint64]
	probes map[string]qfReader
}

func newTSEgressFixtureModel(t *testing.T, generation uint64, policy EgressPolicy) egressFixtureModel {
	t.Helper()
	ts := NewThreadSafeContext()
	cell, err := NewThreadSafeEgressCell[uint64](ts, generation, policy)
	if err != nil {
		t.Fatalf("thread-safe egress build: %v", err)
	}
	readers := cell.Readers()
	return &tsEgressFixtureModel{ts: ts, cell: cell, probes: map[string]qfReader{
		"pending": newQFTSReader(ts, func(c *Compute) int {
			return len(Get[[]EgressEnvelope[uint64]](c, readers.Pending))
		}),
		"inflight": newQFTSReader(ts, func(c *Compute) int {
			return len(Get[[]EgressEnvelope[uint64]](c, readers.Inflight))
		}),
		"acked_through": newQFTSReader(ts, func(c *Compute) int {
			return int(Get[Opt[uint64]](c, readers.AckedThrough).Value)
		}),
		"retry": newQFTSReader(ts, func(c *Compute) int {
			return int(Get[Opt[EgressRetry]](c, readers.Retry).Value.Attempt)
		}),
	}}
}

func (m *tsEgressFixtureModel) flavor() string                     { return "thread-safe" }
func (m *tsEgressFixtureModel) enqueue(p uint64) uint64            { return m.cell.Enqueue(p) }
func (m *tsEgressFixtureModel) claim(g uint64) EgressClaim[uint64] { return m.cell.Claim(g) }
func (m *tsEgressFixtureModel) ack(g, through uint64) EgressAck    { return m.cell.Ack(g, through) }
func (m *tsEgressFixtureModel) fail(g, s uint64) EgressFailure     { return m.cell.Fail(g, s) }
func (m *tsEgressFixtureModel) retryNow(g, s uint64) EgressRetryAction {
	return m.cell.RetryNow(g, s)
}
func (m *tsEgressFixtureModel) reconnect(g uint64) EgressReconnect { return m.cell.Reconnect(g) }
func (m *tsEgressFixtureModel) generation() uint64                 { return m.cell.Generation() }
func (m *tsEgressFixtureModel) nextSequence() uint64               { return m.cell.NextSequence() }
func (m *tsEgressFixtureModel) pending() []EgressEnvelope[uint64]  { return m.cell.Pending(nil) }
func (m *tsEgressFixtureModel) inflight() []EgressEnvelope[uint64] { return m.cell.Inflight(nil) }
func (m *tsEgressFixtureModel) ackedThrough() Opt[uint64]          { return m.cell.AckedThrough(nil) }
func (m *tsEgressFixtureModel) retry() Opt[EgressRetry]            { return m.cell.Retry(nil) }
func (m *tsEgressFixtureModel) probe(kind string) qfReader         { return m.probes[kind] }

// --- flavor 3: async ---------------------------------------------------------

type asyncEgressFixtureModel struct {
	ctx    *AsyncContext
	cell   *AsyncEgressCell[uint64]
	probes map[string]qfReader
}

func newAsyncEgressFixtureModel(t *testing.T, generation uint64, policy EgressPolicy) egressFixtureModel {
	t.Helper()
	ctx := NewAsyncContext()
	t.Cleanup(func() { _ = ctx.Close() })
	cell, err := NewAsyncEgressCell[uint64](ctx, generation, policy)
	if err != nil {
		t.Fatalf("async egress build: %v", err)
	}
	readers := cell.Readers()
	track := func(body func(cc *AsyncComputeContext) (int, error)) qfReader {
		return newQFAsyncReader(ctx, func(cc *AsyncComputeContext) int {
			value, err := body(cc)
			if err != nil {
				panic(err)
			}
			return value
		})
	}
	return &asyncEgressFixtureModel{ctx: ctx, cell: cell, probes: map[string]qfReader{
		"pending": track(func(cc *AsyncComputeContext) (int, error) {
			value, err := TrackComputed(cc, readers.Pending)
			return len(value), err
		}),
		"inflight": track(func(cc *AsyncComputeContext) (int, error) {
			value, err := TrackComputed(cc, readers.Inflight)
			return len(value), err
		}),
		"acked_through": track(func(cc *AsyncComputeContext) (int, error) {
			value, err := TrackComputed(cc, readers.AckedThrough)
			return int(value.Value), err
		}),
		"retry": track(func(cc *AsyncComputeContext) (int, error) {
			value, err := TrackComputed(cc, readers.Retry)
			return int(value.Value.Attempt), err
		}),
	}}
}

func (m *asyncEgressFixtureModel) flavor() string                     { return "async" }
func (m *asyncEgressFixtureModel) enqueue(p uint64) uint64            { return m.cell.Enqueue(p) }
func (m *asyncEgressFixtureModel) claim(g uint64) EgressClaim[uint64] { return m.cell.Claim(g) }
func (m *asyncEgressFixtureModel) ack(g, through uint64) EgressAck    { return m.cell.Ack(g, through) }
func (m *asyncEgressFixtureModel) fail(g, s uint64) EgressFailure     { return m.cell.Fail(g, s) }
func (m *asyncEgressFixtureModel) retryNow(g, s uint64) EgressRetryAction {
	return m.cell.RetryNow(g, s)
}
func (m *asyncEgressFixtureModel) reconnect(g uint64) EgressReconnect { return m.cell.Reconnect(g) }
func (m *asyncEgressFixtureModel) generation() uint64                 { return m.cell.Generation() }
func (m *asyncEgressFixtureModel) nextSequence() uint64               { return m.cell.NextSequence() }
func (m *asyncEgressFixtureModel) pending() []EgressEnvelope[uint64]  { return m.cell.Pending(nil) }
func (m *asyncEgressFixtureModel) inflight() []EgressEnvelope[uint64] { return m.cell.Inflight(nil) }
func (m *asyncEgressFixtureModel) ackedThrough() Opt[uint64]          { return m.cell.AckedThrough(nil) }
func (m *asyncEgressFixtureModel) retry() Opt[EgressRetry]            { return m.cell.Retry(nil) }
func (m *asyncEgressFixtureModel) probe(kind string) qfReader         { return m.probes[kind] }

// --- the replay --------------------------------------------------------------

// materializeEgress reads every reader kind so the caches are warm and the next
// step's probe measures THAT step's invalidation and nothing else.
func materializeEgress(model egressFixtureModel) {
	_ = model.pending()
	_ = model.inflight()
	_ = model.ackedThrough()
	_ = model.retry()
	for _, kind := range egressReaderKinds {
		model.probe(kind).drive()
	}
}

// runEgressOp executes one fixture op against the model and renders what the
// library returned. An unknown op fails; it is never skipped.
func runEgressOp(t *testing.T, label string, model egressFixtureModel, op map[string]any) map[string]any {
	t.Helper()
	opType := jsStr(op["type"])
	u := func(key string) uint64 { return uint64(jsInt(op[key])) }
	reason := "replay input: an argument of the operation applied to the cell; its effect is asserted through this step's returns and expected blocks"
	switch opType {
	case "enqueue":
		consumeKeys(t, label+" op", op, "type", "payload")
		excuseKeys(t, op, reason, "type", "payload")
		return map[string]any{"sequence": model.enqueue(u("payload"))}
	case "claim":
		consumeKeys(t, label+" op", op, "type", "generation")
		excuseKeys(t, op, reason, "type", "generation")
		return egressClaimValue(t, model.claim(u("generation")))
	case "ack":
		consumeKeys(t, label+" op", op, "type", "generation", "through")
		excuseKeys(t, op, reason, "type", "generation", "through")
		return egressAckValue(t, model.ack(u("generation"), u("through")))
	case "fail":
		consumeKeys(t, label+" op", op, "type", "generation", "sequence")
		excuseKeys(t, op, reason, "type", "generation", "sequence")
		return egressFailureValue(t, model.fail(u("generation"), u("sequence")))
	case "retry_now":
		consumeKeys(t, label+" op", op, "type", "generation", "sequence")
		excuseKeys(t, op, reason, "type", "generation", "sequence")
		return egressRetryActionValue(t, model.retryNow(u("generation"), u("sequence")))
	case "reconnect":
		consumeKeys(t, label+" op", op, "type", "generation")
		excuseKeys(t, op, reason, "type", "generation")
		return egressReconnectValue(t, model.reconnect(u("generation")))
	}
	t.Fatalf("%s: unknown egress op %q", label, opType)
	return nil
}

// replayEgressFixture replays one fixture against one flavor and returns the
// number of steps EXECUTED.
func replayEgressFixture(t *testing.T, name string, fixture map[string]any, build egressModelBuilder) int {
	t.Helper()
	model := build(t, uint64(jsInt(fixture["generation"])), egressPolicyOf(t, fixture["policy"]))
	materializeEgress(model)

	steps := 0
	for i, raw := range jsList(fixture["steps"]) {
		step := jsMap(raw)
		op := jsMap(step["op"])
		label := fmt.Sprintf("%s %s step %d (%s)", model.flavor(), name, i, jsStr(op["type"]))
		consumeKeys(t, label, step, "op", "returns", "expected")
		excuseKey(t, step, "op", "replay input: the operation applied to the cell; its effect is asserted through this step's returns and expected blocks")
		excuseKey(t, step, "expected", "container: asserted key-by-key against the expected block below")
		expected := consumeKeys(t, label+" expected", jsMap(step["expected"]),
			"generation", "next_sequence", "pending", "inflight", "acked_through", "retry", "invalidates")

		before := map[string]int{}
		for _, kind := range egressReaderKinds {
			before[kind] = model.probe(kind).drive()
		}

		got := runEgressOp(t, label, model, op)
		assertKey(t, step, "returns", got)

		// `invalidates`, per reader kind, in BOTH directions.
		invalidates := assertKeySub(t, expected, "invalidates", egressReaderKinds...)
		if invalidates == nil {
			t.Fatalf("%s: expected.invalidates is missing or null", label)
		}
		for _, kind := range egressReaderKinds {
			readerKind := kind
			assertKeyWith(t, invalidates, readerKind, func(wantValue fixtureValue) {
				want, ok := wantValue.Value().(bool)
				if !ok {
					t.Fatalf("%s: invalidates.%s is not a boolean", label, readerKind)
				}
				assertInvalidationDelta(t, label+" invalidates."+readerKind,
					model.probe(readerKind), before[readerKind], want)
			})
		}

		assertEgressState(t, model, expected, label)
		materializeEgress(model)
		steps++
	}
	return steps
}

func assertEgressState(t *testing.T, model egressFixtureModel, expected map[string]any, label string) {
	t.Helper()
	assertKey(t, expected, "generation", model.generation())
	assertKey(t, expected, "next_sequence", model.nextSequence())
	assertKey(t, expected, "pending", egressEnvelopesValue(model.pending()))
	assertKey(t, expected, "inflight", egressEnvelopesValue(model.inflight()))
	assertKeyWith(t, expected, "acked_through", func(wantValue fixtureValue) {
		want := wantValue.Value()
		got := model.ackedThrough()
		switch {
		case want == nil && got.Present:
			t.Errorf("%s: acked_through=%d, want null", label, got.Value)
		case want != nil && (!got.Present || float64(got.Value) != want.(float64)):
			t.Errorf("%s: acked_through=%+v, want %v", label, got, want)
		}
	})
	retry := model.retry()
	wantRetry := assertKeySub(t, expected, "retry", "sequence", "attempt", "backoff", "exhausted")
	if wantRetry == nil {
		if retry.Present {
			t.Errorf("%s: retry=%+v, want null", label, retry.Value)
		}
		return
	}
	if !retry.Present {
		t.Fatalf("%s: retry absent, want %v", label, wantRetry)
	}
	assertKey(t, wantRetry, "sequence", retry.Value.Sequence)
	assertKey(t, wantRetry, "attempt", retry.Value.Attempt)
	assertKey(t, wantRetry, "backoff", retry.Value.Backoff)
	assertKey(t, wantRetry, "exhausted", retry.Value.Exhausted)
}

func replayEgressCorpus(t *testing.T, build egressModelBuilder) int {
	t.Helper()
	steps := 0
	for _, name := range egressFixtures {
		steps += replayEgressFixture(t, name, loadEgressFixture(t, name), build)
	}
	return steps
}

func requireEgressCorpusReplay(t *testing.T, flavor string, build egressModelBuilder) {
	t.Helper()
	steps := replayEgressCorpus(t, build)
	if steps == 0 {
		t.Fatalf("the %s flavor replayed 0 egress steps; the corpus was never opened", flavor)
	}
	if want := expectedEgressStepTotal(t); steps != want {
		t.Fatalf("the %s flavor replayed %d egress steps, want the corpus total %d", flavor, steps, want)
	}
	// Evidence line the CI zero-skip step greps per flavor.
	t.Logf("egress[%s]: %d fixtures, %d steps executed", flavor, len(egressFixtures), steps)
}

// --- the per-flavor gates ----------------------------------------------------

func TestEgressConformanceSyncFlavorReplaysCorpus(t *testing.T) {
	requireEgressCorpusReplay(t, "sync", newSyncEgressFixtureModel)
}

func TestEgressConformanceThreadSafeFlavorReplaysCorpus(t *testing.T) {
	requireEgressCorpusReplay(t, "thread-safe", newTSEgressFixtureModel)
}

func TestEgressConformanceAsyncFlavorReplaysCorpus(t *testing.T) {
	requireEgressCorpusReplay(t, "async", newAsyncEgressFixtureModel)
}

// TestEgressConformanceInvalidationProbeDiscriminates pins the probe itself: the
// corpus asserts NEGATIVE invalidation, so a probe that never moved would pass
// every `false` for free.
func TestEgressConformanceInvalidationProbeDiscriminates(t *testing.T) {
	for _, flavor := range []struct {
		name  string
		build egressModelBuilder
	}{
		{"sync", newSyncEgressFixtureModel},
		{"thread-safe", newTSEgressFixtureModel},
		{"async", newAsyncEgressFixtureModel},
	} {
		t.Run(flavor.name, func(t *testing.T) {
			model := flavor.build(t, 1, DefaultEgressPolicy())
			materializeEgress(model)
			pending, acked := model.probe("pending"), model.probe("acked_through")
			beforePending, beforeAcked := pending.drive(), acked.drive()
			model.enqueue(1)
			if after := pending.drive(); after != beforePending+1 {
				t.Fatalf("an enqueue must invalidate the pending reader: %d -> %d", beforePending, after)
			}
			if after := acked.drive(); after != beforeAcked {
				t.Fatalf("an enqueue must NOT invalidate the acked_through reader: %d -> %d", beforeAcked, after)
			}
		})
	}
}

// --- transport attachment (one Effect per incarnation) -----------------------

type recordingEgressTransport struct {
	mu       sync.Mutex
	sent     []uint64
	failNext bool
}

func (r *recordingEgressTransport) Send(envelope EgressEnvelope[int]) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failNext {
		r.failNext = false
		return false
	}
	r.sent = append(r.sent, envelope.Sequence)
	return true
}

func (r *recordingEgressTransport) snapshot() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.sent...)
}

func equalSequences(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestEgressCellAttachmentProjectsPendingToTransport(t *testing.T) {
	ctx := NewContext()
	egress, err := NewEgressCell[int](ctx, 1, DefaultEgressPolicy())
	if err != nil {
		t.Fatal(err)
	}
	transport := &recordingEgressTransport{}
	effect := egress.AttachTransport(transport)
	defer effect.Dispose()
	egress.Enqueue(10)
	egress.Enqueue(20)
	if got := transport.snapshot(); !equalSequences(got, []uint64{0, 1}) {
		t.Fatalf("sent=%v, want [0 1]", got)
	}
	if len(egress.Pending(ctx)) != 0 || len(egress.Inflight(ctx)) != 2 {
		t.Fatalf("pending=%v inflight=%v", egress.Pending(ctx), egress.Inflight(ctx))
	}
}

func TestEgressCellReconnectMakesOldAttachmentInert(t *testing.T) {
	ctx := NewContext()
	egress, err := NewEgressCell[int](ctx, 3, DefaultEgressPolicy())
	if err != nil {
		t.Fatal(err)
	}
	old := &recordingEgressTransport{}
	oldEffect := egress.AttachTransport(old)
	egress.Enqueue(10)
	egress.Reconnect(4)
	egress.Enqueue(20)
	if got := old.snapshot(); !equalSequences(got, []uint64{0}) {
		t.Fatalf("old attachment sent=%v, want only [0]", got)
	}
	oldEffect.Dispose()
	fresh := &recordingEgressTransport{}
	freshEffect := egress.AttachTransport(fresh)
	defer freshEffect.Dispose()
	if got := fresh.snapshot(); !equalSequences(got, []uint64{0, 1}) {
		t.Fatalf("fresh attachment sent=%v, want the replayed [0 1]", got)
	}
}

func TestEgressCellAckReopensTheWindowReactively(t *testing.T) {
	ctx := NewContext()
	policy := DefaultEgressPolicy()
	policy.InflightLimit = 1
	egress, err := NewEgressCell[int](ctx, 1, policy)
	if err != nil {
		t.Fatal(err)
	}
	transport := &recordingEgressTransport{}
	effect := egress.AttachTransport(transport)
	defer effect.Dispose()
	egress.Enqueue(10)
	egress.Enqueue(20)
	if got := transport.snapshot(); !equalSequences(got, []uint64{0}) {
		t.Fatalf("sent=%v, want [0] with a window of one", got)
	}
	if ack := egress.Ack(1, 0); ack.Kind != EgressAckAdvanced || ack.Through != 0 {
		t.Fatalf("ack=%+v", ack)
	}
	if got := transport.snapshot(); !equalSequences(got, []uint64{0, 1}) {
		t.Fatalf("sent=%v, want the ack to re-run the attachment: [0 1]", got)
	}
}

func TestEgressCellRefusedSendParksTheRecord(t *testing.T) {
	ctx := NewContext()
	egress, err := NewEgressCell[int](ctx, 1, DefaultEgressPolicy())
	if err != nil {
		t.Fatal(err)
	}
	transport := &recordingEgressTransport{failNext: true}
	effect := egress.AttachTransport(transport)
	defer effect.Dispose()
	egress.Enqueue(10)
	retry := egress.Retry(ctx)
	if !retry.Present || retry.Value.Sequence != 0 || retry.Value.Attempt != 1 || retry.Value.Exhausted {
		t.Fatalf("retry=%+v, want a parked attempt-1 retry of sequence 0", retry)
	}
	if action := egress.RetryNow(1, 0); action.Kind != EgressRetryScheduled {
		t.Fatalf("retry_now=%+v", action)
	}
	if got := transport.snapshot(); !equalSequences(got, []uint64{0}) {
		t.Fatalf("sent=%v, want the rescheduled record re-sent", got)
	}
}

func TestThreadSafeEgressCellAttachmentDrivesTheSharedProjection(t *testing.T) {
	ts := NewThreadSafeContext()
	egress, err := NewThreadSafeEgressCell[int](ts, 1, DefaultEgressPolicy())
	if err != nil {
		t.Fatal(err)
	}
	transport := &recordingEgressTransport{}
	effect := egress.AttachTransport(transport)
	defer egress.DisposeTransport(effect)
	egress.Enqueue(7)
	if got := transport.snapshot(); !equalSequences(got, []uint64{0}) {
		t.Fatalf("sent=%v, want [0]", got)
	}
	if got := len(egress.Inflight(nil)); got != 1 {
		t.Fatalf("inflight=%d, want 1", got)
	}
}

func TestAsyncEgressCellAttachmentDrivesTheAsyncProjection(t *testing.T) {
	ctx := NewAsyncContext()
	defer ctx.Close()
	egress, err := NewAsyncEgressCell[int](ctx, 1, DefaultEgressPolicy())
	if err != nil {
		t.Fatal(err)
	}
	transport := &recordingEgressTransport{}
	effect := egress.AttachTransport(transport)
	defer effect.Dispose()
	egress.Enqueue(7)
	egress.Enqueue(8)
	// The attachment runs off the event loop; wait (bounded) for the sends it
	// drives, then assert the library's own projection.
	waitFor(t, func() bool { return len(transport.snapshot()) >= 2 })
	if got := transport.snapshot(); !equalSequences(got, []uint64{0, 1}) {
		t.Fatalf("sent=%v, want [0 1]", got)
	}
	if got := len(egress.Inflight(nil)); got != 2 {
		t.Fatalf("inflight=%d, want 2", got)
	}
}

// TestThreadSafeEgressCellConcurrentReadsAndTransitionsAreRaceFree is the -race
// gate for this flavor (see the ingress twin for why reads must lock too): a
// small hot state, readers of every kind AND mutators running together, and a
// soak that must leave real state behind.
func TestThreadSafeEgressCellConcurrentReadsAndTransitionsAreRaceFree(t *testing.T) {
	ts := NewThreadSafeContext()
	policy := DefaultEgressPolicy()
	policy.InflightLimit = 4
	egress, err := NewThreadSafeEgressCell[int](ts, 1, policy)
	if err != nil {
		t.Fatal(err)
	}
	transport := &recordingEgressTransport{}
	effect := egress.AttachTransport(transport)
	defer egress.DisposeTransport(effect)

	const rounds = 200
	var wg sync.WaitGroup
	for reader := 0; reader < 3; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				_ = egress.Pending(nil)
				_ = egress.Inflight(nil)
				_ = egress.AckedThrough(nil)
				_ = egress.Retry(nil)
				_ = egress.NextSequence()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 0; r < rounds; r++ {
			egress.Enqueue(r)
			if r%4 == 3 {
				egress.Ack(1, uint64(r-1))
			}
		}
	}()
	wg.Wait()

	if egress.NextSequence() != rounds {
		t.Fatalf("next_sequence=%d, want %d", egress.NextSequence(), rounds)
	}
	if len(transport.snapshot()) == 0 {
		t.Fatal("the soak sent nothing; it exercised no attachment")
	}
	if !egress.AckedThrough(nil).Present {
		t.Fatal("the soak acknowledged nothing")
	}
}

func TestEgressCoreRejectsInvalidPolicy(t *testing.T) {
	for _, tc := range []struct {
		policy EgressPolicy
		want   error
	}{
		{EgressPolicy{InflightLimit: 0, RetryBase: 1, RetryCeiling: 1}, ErrEgressZeroInflightLimit},
		{EgressPolicy{InflightLimit: 1, RetryBase: 0, RetryCeiling: 1}, ErrEgressZeroRetryBase},
		{EgressPolicy{InflightLimit: 1, RetryBase: 5, RetryCeiling: 4}, ErrEgressRetryCeilingBelowBase},
	} {
		if _, err := NewEgressCore[int](1, tc.policy); err != tc.want {
			t.Fatalf("policy %+v: err=%v, want %v", tc.policy, err, tc.want)
		}
	}
}

// TestEgressCoreRefusesAckBeyondSent pins fix 1 directly: an acknowledgement
// through a sequence never assigned is refused, moves no state, and reports no
// invalidation — even before any record was ever acknowledged.
func TestEgressCoreRefusesAckBeyondSent(t *testing.T) {
	core, err := NewEgressCore[int](1, DefaultEgressPolicy())
	if err != nil {
		t.Fatal(err)
	}
	change, ack := core.Ack(1, 0)
	if ack.Kind != EgressAckBeyondSent || ack.NextSequence != 0 || !change.IsEmpty() {
		t.Fatalf("ack on an empty egress=%+v change=%+v, want beyond_sent(0) with no change", ack, change)
	}
	core.Enqueue(10)
	core.Claim(1)
	change, ack = core.Ack(1, 1)
	if ack.Kind != EgressAckBeyondSent || ack.NextSequence != 1 || !change.IsEmpty() {
		t.Fatalf("ack through 1 with next_sequence 1=%+v change=%+v, want beyond_sent(1)", ack, change)
	}
	if core.AckedThrough().Present || len(core.Inflight()) != 1 {
		t.Fatalf("a refused ack moved state: acked=%+v inflight=%v", core.AckedThrough(), core.Inflight())
	}
	if _, ack = core.Ack(1, 0); ack.Kind != EgressAckAdvanced || ack.Through != 0 {
		t.Fatalf("ack through the last assigned sequence=%+v, want advanced(0)", ack)
	}
}

// TestEgressCoreReconnectInvalidatesPendingWithoutReplay pins fix 2: a
// reconnect with nothing in flight still re-stamps every pending record with
// the new generation, so it must report the pending projection as changed.
func TestEgressCoreReconnectInvalidatesPendingWithoutReplay(t *testing.T) {
	core, err := NewEgressCore[int](4, DefaultEgressPolicy())
	if err != nil {
		t.Fatal(err)
	}
	core.Enqueue(12)
	change, reconnect := core.Reconnect(5)
	if reconnect.Kind != EgressReconnectAdvanced || reconnect.Replayed != 0 {
		t.Fatalf("reconnect=%+v", reconnect)
	}
	if !change.Pending || change.Inflight {
		t.Fatalf("change=%+v, want pending invalidated and inflight untouched", change)
	}
	if pending := core.Pending(); len(pending) != 1 || pending[0].Generation != 5 {
		t.Fatalf("pending=%+v, want re-stamped at generation 5", pending)
	}
	empty, _ := NewEgressCore[int](1, DefaultEgressPolicy())
	if change, _ := empty.Reconnect(2); change.Pending {
		t.Fatalf("reconnect of an empty egress invalidated pending: %+v", change)
	}
}
