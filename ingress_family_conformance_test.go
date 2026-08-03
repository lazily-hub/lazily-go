package lazily

// The transport-agnostic ingress contract (#designimplementtransport), replayed
// against EVERY flavor this binding ships — with a ledger that is *enforced*
// rather than advisory.
//
// lazily-go ships all three: IngressCell / ThreadSafeIngressCell /
// AsyncIngressCell, matching the three README coverage rows and the contract
// lazily-spec/docs/transport-ingress.md declares REQUIRED of every binding ×
// every flavor.
//
// The flavor axis lives in the RUNNER, not the corpus: the fixtures carry a
// `model` field naming the primitive and no execution-model field, and one
// ingressFixtureModel interface replays the same JSON against each shell. Nothing
// in the interface is async-coloured, which is the finding rather than an
// oversight — an admission decision is a function of the fence, the watermark, the
// reorder buffer, and the observed clock, so there is nothing to await and no
// settle step anywhere below.
//
// Three things keep this suite from reporting green while testing nothing — each
// one a failure mode this family of suites has actually shipped:
//
//   - TestIngressCapabilityLedgerMatchesSource greps the package sources for each
//     flavor's type declaration, in BOTH directions. A ledger row marked shipped
//     whose type does not exist fails; a type that exists while its row says
//     unshipped fails and names the runner to extend. The ledger cannot rot,
//     because the filesystem enforces it.
//   - Every replay returns its step count, and every flavor asserts that count is
//     non-zero and equal to the corpus total. An absence guard proves the fixtures
//     exist on disk; only a positive count proves this binary opened them.
//   - `invalidates` is asserted in BOTH directions, per reader kind, through a
//     recompute-count probe. A step expecting false FAILS if the shell invalidated
//     anyway, so over-invalidation is as visible as under-. The probe is itself
//     pinned by TestIngressInvalidationProbeDiscriminates.
//
// Receipt invalidation is asserted per CHANNEL, never by receipt count: a stale
// cache recomputes to the right count, so a count-only gate reports green.
//
// Every gate below was mutation-checked; the file tail lists the probes and what
// each one turned red.

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// ingressFixtures is every fixture the ingress corpus ships. Named explicitly
// rather than globbed: a fixture added to the corpus and not to this list is a
// *missing replay*, and the runtime conformance manifest is what should notice,
// not a silently shorter run.
var ingressFixtures = []string{
	"ingress_ordered_delivery.json",
	"ingress_reorder_and_duplication.json",
	"ingress_reorder_window_overflow.json",
	"ingress_disconnect_replay.json",
	"ingress_backpressure.json",
	"ingress_generation_handoff.json",
	"ingress_freshness_and_retry.json",
}

// ingressCapability is an enforced (primitive, flavor) ledger. A source marker
// must exist exactly when the capability is shipped.
type ingressCapability struct {
	flavor, primitive, marker string
	shipped                   bool
}

var ingressCapabilities = []ingressCapability{
	{"sync", "IngressCell", "type IngressCell[", true},
	{"thread-safe", "IngressCell", "type ThreadSafeIngressCell[", true},
	{"async", "IngressCell", "type AsyncIngressCell[", true},
}

func TestIngressCapabilityLedgerMatchesSource(t *testing.T) {
	sources := packageSources(t)
	if sources == "" {
		t.Fatal("read no package sources; capability ledger would be vacuous")
	}
	seen := map[string]bool{}
	shipped := 0
	for _, capability := range ingressCapabilities {
		key := capability.flavor + "/" + capability.primitive
		if seen[key] {
			t.Fatalf("duplicate ingress ledger entry %q", key)
		}
		seen[key] = true
		defined := strings.Contains(sources, capability.marker)
		if defined != capability.shipped {
			t.Fatalf("%s: source marker %q present=%v, ledger shipped=%v; "+
				"fix the ledger or extend the runner",
				key, capability.marker, defined, capability.shipped)
		}
		if capability.shipped {
			shipped++
		}
	}
	if len(seen) != 3 {
		t.Fatalf("ingress ledger has %d entries, want 1 primitive x 3 flavors", len(seen))
	}
	if shipped == 0 {
		t.Fatal("all ingress capabilities are staged; suite would test nothing")
	}
}

// --- fixture loading ---------------------------------------------------------

func loadIngressFixture(t *testing.T, name string) map[string]any {
	t.Helper()
	data := loadConformanceFixture(t, "ingress", name)
	var fixture map[string]any
	mustStrictJSON(t, name, data, &fixture)
	consumeFixtureKeys(t, name, fixture,
		"policy", "merge", "transport", "poll_interval", "steps")
	excuseKeys(t, fixture, "replay input: the shell the IngressCell is CONSTRUCTED with; what it produces is asserted through each step's expected block",
		"policy", "merge", "transport", "poll_interval")
	excuseKey(t, fixture, "steps", "replay input: the step list drives the loop, and each step's own `expected` block is asserted there")
	if got := fixture["model"]; got != "IngressCell" {
		t.Fatalf("%s: fixture model=%v, want IngressCell", name, got)
	}
	// The nesting is load-bearing: `invalidates` belongs under `expected`, and a
	// runner reading it from the step level would silently assert nothing.
	for i, raw := range jsList(fixture["steps"]) {
		step := jsMap(raw)
		if _, bad := step["invalidates"]; bad {
			t.Fatalf("%s step %d: invalidates is at step level; runner reads expected.invalidates",
				name, i)
		}
		expected := jsMap(step["expected"])
		invalidates, ok := expected["invalidates"]
		if !ok {
			t.Fatalf("%s step %d: expected.invalidates is missing", name, i)
		}
		nested := jsMap(invalidates)
		if _, ok := nested["scopes"]; !ok {
			t.Fatalf("%s step %d: expected.invalidates.scopes is missing", name, i)
		}
		if _, ok := nested["receipts"]; !ok {
			t.Fatalf("%s step %d: expected.invalidates.receipts is missing", name, i)
		}
	}
	return fixture
}

func expectedIngressStepTotal(t *testing.T) int {
	t.Helper()
	total := 0
	for _, name := range ingressFixtures {
		total += len(jsList(loadIngressFixture(t, name)["steps"]))
	}
	return total
}

func TestIngressCorpusIsPresentAndNonTrivial(t *testing.T) {
	total := expectedIngressStepTotal(t)
	if total < 30 {
		t.Fatalf("the ingress corpus replays only %d steps; that is not the named schedule set",
			total)
	}
}

// --- decoding ----------------------------------------------------------------

func jsOptU64(v any) Opt[uint64] {
	if v == nil {
		return None[uint64]()
	}
	return Some(uint64(jsInt(v)))
}

func ingressOverflowOf(t *testing.T, text string) Overflow {
	t.Helper()
	switch text {
	case "block":
		return OverflowBlock
	case "drop_newest":
		return OverflowDropNewest
	case "drop_oldest":
		return OverflowDropOldest
	case "conflate":
		return OverflowConflate
	case "spill":
		return OverflowSpill
	}
	t.Fatalf("unknown overflow %q", text)
	return ""
}

func ingressTransportOf(t *testing.T, text string) IngressTransportKind {
	t.Helper()
	switch text {
	case "event_channel":
		return IngressTransportEventChannel
	case "rpc_triggered":
		return IngressTransportRpcTriggered
	case "bounded_polling":
		return IngressTransportBoundedPolling
	}
	t.Fatalf("unknown transport %q", text)
	return ""
}

func ingressErrorOf(t *testing.T, text string) IngressError {
	t.Helper()
	switch text {
	case "transport_closed":
		return IngressErrorTransportClosed
	case "decode_failed":
		return IngressErrorDecodeFailed
	case "authority_lost":
		return IngressErrorAuthorityLost
	}
	t.Fatalf("unknown error %q", text)
	return ""
}

func ingressDropReasonOf(t *testing.T, text string) IngressDropReason {
	t.Helper()
	switch text {
	case "stale_generation":
		return IngressDropStaleGeneration
	case "duplicate_sequence":
		return IngressDropDuplicateSequence
	case "duplicate_buffered":
		return IngressDropDuplicateBuffered
	case "reorder_window_overflow":
		return IngressDropReorderWindowOverflow
	case "expired":
		return IngressDropExpired
	case "backpressure":
		return IngressDropBackpressure
	case "scope_closed":
		return IngressDropScopeClosed
	}
	t.Fatalf("unknown drop reason %q", text)
	return ""
}

func ingressLifecycleOf(t *testing.T, text string) IngressLifecycle {
	t.Helper()
	switch text {
	case "opening":
		return IngressLifecycleOpening
	case "live":
		return IngressLifecycleLive
	case "suspended":
		return IngressLifecycleSuspended
	case "closed":
		return IngressLifecycleClosed
	}
	t.Fatalf("unknown lifecycle %q", text)
	return ""
}

func ingressReadinessOf(t *testing.T, text string) IngressReadiness {
	t.Helper()
	switch text {
	case "unknown":
		return IngressReadinessUnknown
	case "warming":
		return IngressReadinessWarming
	case "ready":
		return IngressReadinessReady
	case "stale":
		return IngressReadinessStale
	case "suspended":
		return IngressReadinessSuspended
	case "closed":
		return IngressReadinessClosed
	}
	t.Fatalf("unknown readiness %q", text)
	return ""
}

func ingressPolicyOf(t *testing.T, raw any) IngressPolicy {
	t.Helper()
	policy := consumeKeys(t, "ingress policy", jsMap(raw),
		"reorder_window", "freshness_horizon", "high_water", "overflow",
		"receipt_capacity", "retry_base", "retry_ceiling")
	excuseKeys(t, policy, "replay input: every field is folded into the IngressPolicy the cell is constructed with, and its effect is asserted through the admission and lifecycle expectations each step states",
		"reorder_window", "freshness_horizon", "high_water", "overflow",
		"receipt_capacity", "retry_base", "retry_ceiling")
	return IngressPolicy{
		ReorderWindow:    jsInt(policy["reorder_window"]),
		FreshnessHorizon: uint64(jsInt(policy["freshness_horizon"])),
		HighWater:        uint64(jsInt(policy["high_water"])),
		Overflow:         ingressOverflowOf(t, jsStr(policy["overflow"])),
		ReceiptCapacity:  jsInt(policy["receipt_capacity"]),
		RetryBase:        uint64(jsInt(policy["retry_base"])),
		RetryCeiling:     uint64(jsInt(policy["retry_ceiling"])),
	}
}

func ingressMergeOf(t *testing.T, text string) MergePolicy[uint64] {
	t.Helper()
	switch text {
	case "sum":
		return Sum[uint64]()
	case "keep_latest":
		return KeepLatest[uint64]()
	}
	t.Fatalf("unknown merge %q", text)
	return MergePolicy[uint64]{}
}

func ingressExpectedAdmission(t *testing.T, raw any) IngressAdmission {
	t.Helper()
	want := consumeKeys(t, "ingress returns", jsMap(raw),
		"admission", "delivered_through", "gap_from", "from", "to", "reason")
	// This block is DECODED into the expected admission, which the caller then
	// compares against the live one: the fixture's values reach a comparison at
	// the call site rather than here.
	excuseKeys(t, want, "folded into the expected IngressAdmission that replayIngressFixture compares against the admission the cell returned",
		"admission", "delivered_through", "gap_from", "from", "to", "reason")
	switch kind := jsStr(want["admission"]); kind {
	case "accepted":
		return IngressAdmitted(uint64(jsInt(want["delivered_through"])))
	case "conflated":
		return IngressCoalesced(uint64(jsInt(want["delivered_through"])))
	case "buffered":
		return IngressHeld(uint64(jsInt(want["gap_from"])))
	case "generation_handoff":
		return IngressHandedOff(uint64(jsInt(want["from"])), uint64(jsInt(want["to"])))
	case "dropped":
		return IngressRefused(ingressDropReasonOf(t, jsStr(want["reason"])))
	case "blocked":
		return IngressBackpressured()
	default:
		t.Fatalf("unknown admission %q", kind)
		return IngressAdmission{}
	}
}

func ingressExpectedReplay(raw any) (ReplayRequest, bool) {
	if raw == nil {
		return ReplayRequest{}, false
	}
	want := jsMap(raw)
	return ReplayRequest{
		Generation:   uint64(jsInt(want["generation"])),
		FromSequence: uint64(jsInt(want["from_sequence"])),
	}, true
}

// --- the flavor-neutral model ------------------------------------------------

// ingressScopeReaderKinds is the reader-kind axis, in the order the fixtures name
// them.
var ingressScopeReaderKinds = []string{"value", "readiness", "authority", "retry"}

// ingressReceiptChannels is the receipt-channel axis, in the order the fixtures
// name them. They are three separate reader kinds because they have three
// separate consumers.
var ingressReceiptChannels = []string{"accepted", "dropped", "error"}

// ingressFixtureModel is what every ingress flavor must be able to do for the
// corpus to replay against it.
//
// The probe accessors are the whole reason this is an interface rather than three
// copies of the runner: `invalidates` is a claim about the GRAPH, and only the
// shell can answer it.
type ingressFixtureModel interface {
	flavor() string

	open(key string, generation uint64)
	admit(envelope IngressEnvelope[string, uint64]) IngressAdmission
	suspend(key string) (ReplayRequest, bool)
	reconnect(key string, generation uint64) ReplayRequest
	closeScope(key string)
	fail(key string, err IngressError)
	tick(now uint64)
	drain(key string) (uint64, bool)

	value(key string) (uint64, bool)
	readiness(key string) IngressReadiness
	authority(key string) (IngressAuthority, bool)
	retry(key string) (IngressRetry, bool)
	acceptedLen() int
	droppedLen() int
	errorsLen() int
	schedule() IngressSchedule
	view(key string) (IngressScopeView, bool)

	// scopeProbe reports whether the given reader kind recomputed — the
	// cache-validity probe the fixtures' `invalidates` flags are asserted through.
	scopeProbe(key, kind string) qfReader
	receiptProbe(channel string) qfReader
}

type ingressModelBuilder func(
	t *testing.T,
	policy IngressPolicy,
	merge MergePolicy[uint64],
	transport IngressTransportKind,
	pollInterval uint64,
	keys []string,
) ingressFixtureModel

// --- flavor 1: single-threaded ----------------------------------------------

type syncIngressFixtureModel struct {
	ctx      *Context
	cell     *IngressCell[string, uint64]
	scope    map[string]map[string]qfReader
	receipts map[string]qfReader
}

func newSyncIngressFixtureModel(
	t *testing.T,
	policy IngressPolicy,
	merge MergePolicy[uint64],
	transport IngressTransportKind,
	pollInterval uint64,
	keys []string,
) ingressFixtureModel {
	t.Helper()
	ctx := NewContext()
	cell, err := NewIngressCell[string, uint64](ctx, policy, merge, transport, pollInterval)
	if err != nil {
		t.Fatalf("sync ingress build: %v", err)
	}
	m := &syncIngressFixtureModel{
		ctx:      ctx,
		cell:     cell,
		scope:    map[string]map[string]qfReader{},
		receipts: map[string]qfReader{},
	}
	for _, key := range keys {
		readers := cell.ScopeReaders(key)
		m.scope[key] = map[string]qfReader{
			"value": newQFSyncReader(ctx, func(c *Compute) int {
				window := Get[IngressWindow[uint64]](c, readers.Value)
				if !window.Present {
					return -1
				}
				return int(window.Value)
			}),
			"readiness": newQFSyncReader(ctx, func(c *Compute) int {
				return len(Get[IngressReadiness](c, readers.Readiness))
			}),
			"authority": newQFSyncReader(ctx, func(c *Compute) int {
				return int(Get[Opt[IngressAuthority]](c, readers.Authority).Value.Generation)
			}),
			"retry": newQFSyncReader(ctx, func(c *Compute) int {
				return int(Get[Opt[IngressRetry]](c, readers.Retry).Value.Attempt)
			}),
		}
	}
	channels := cell.ReceiptReaders()
	m.receipts["accepted"] = newQFSyncReader(ctx, func(c *Compute) int {
		return len(Get[[]IngressReceipt[string]](c, channels.Accepted))
	})
	m.receipts["dropped"] = newQFSyncReader(ctx, func(c *Compute) int {
		return len(Get[[]IngressReceipt[string]](c, channels.Dropped))
	})
	m.receipts["error"] = newQFSyncReader(ctx, func(c *Compute) int {
		return len(Get[[]IngressReceipt[string]](c, channels.Errors))
	})
	return m
}

func (m *syncIngressFixtureModel) flavor() string { return "sync" }

func (m *syncIngressFixtureModel) open(key string, generation uint64) {
	m.cell.Open(key, generation)
}

func (m *syncIngressFixtureModel) admit(
	envelope IngressEnvelope[string, uint64],
) IngressAdmission {
	return m.cell.Admit(envelope)
}

func (m *syncIngressFixtureModel) suspend(key string) (ReplayRequest, bool) {
	return m.cell.Suspend(key)
}

func (m *syncIngressFixtureModel) reconnect(key string, generation uint64) ReplayRequest {
	return m.cell.Reconnect(key, generation)
}

func (m *syncIngressFixtureModel) closeScope(key string)           { m.cell.Close(key) }
func (m *syncIngressFixtureModel) fail(key string, e IngressError) { m.cell.Fail(key, e) }
func (m *syncIngressFixtureModel) tick(now uint64)                 { m.cell.Tick(now) }
func (m *syncIngressFixtureModel) drain(key string) (uint64, bool) { return m.cell.Drain(key) }
func (m *syncIngressFixtureModel) value(key string) (uint64, bool) { return m.cell.Value(m.ctx, key) }
func (m *syncIngressFixtureModel) acceptedLen() int                { return len(m.cell.Accepted(m.ctx)) }
func (m *syncIngressFixtureModel) droppedLen() int                 { return len(m.cell.Dropped(m.ctx)) }
func (m *syncIngressFixtureModel) errorsLen() int                  { return len(m.cell.Errors(m.ctx)) }
func (m *syncIngressFixtureModel) schedule() IngressSchedule       { return m.cell.Schedule(m.ctx) }

func (m *syncIngressFixtureModel) readiness(key string) IngressReadiness {
	return m.cell.Readiness(m.ctx, key)
}

func (m *syncIngressFixtureModel) authority(key string) (IngressAuthority, bool) {
	return m.cell.Authority(m.ctx, key)
}

func (m *syncIngressFixtureModel) retry(key string) (IngressRetry, bool) {
	return m.cell.Retry(m.ctx, key)
}

func (m *syncIngressFixtureModel) view(key string) (IngressScopeView, bool) {
	return m.cell.View(key)
}

func (m *syncIngressFixtureModel) scopeProbe(key, kind string) qfReader {
	return m.scope[key][kind]
}

func (m *syncIngressFixtureModel) receiptProbe(channel string) qfReader {
	return m.receipts[channel]
}

// --- flavor 2: thread-safe ---------------------------------------------------

type tsIngressFixtureModel struct {
	ts       *ThreadSafeContext
	cell     *ThreadSafeIngressCell[string, uint64]
	scope    map[string]map[string]qfReader
	receipts map[string]qfReader
}

func newTSIngressFixtureModel(
	t *testing.T,
	policy IngressPolicy,
	merge MergePolicy[uint64],
	transport IngressTransportKind,
	pollInterval uint64,
	keys []string,
) ingressFixtureModel {
	t.Helper()
	ts := NewThreadSafeContext()
	cell, err := NewThreadSafeIngressCell[string, uint64](
		ts, policy, merge, transport, pollInterval)
	if err != nil {
		t.Fatalf("thread-safe ingress build: %v", err)
	}
	m := &tsIngressFixtureModel{
		ts:       ts,
		cell:     cell,
		scope:    map[string]map[string]qfReader{},
		receipts: map[string]qfReader{},
	}
	for _, key := range keys {
		readers := cell.ScopeReaders(key)
		m.scope[key] = map[string]qfReader{
			"value": newQFTSReader(ts, func(c *Compute) int {
				window := Get[IngressWindow[uint64]](c, readers.Value)
				if !window.Present {
					return -1
				}
				return int(window.Value)
			}),
			"readiness": newQFTSReader(ts, func(c *Compute) int {
				return len(Get[IngressReadiness](c, readers.Readiness))
			}),
			"authority": newQFTSReader(ts, func(c *Compute) int {
				return int(Get[Opt[IngressAuthority]](c, readers.Authority).Value.Generation)
			}),
			"retry": newQFTSReader(ts, func(c *Compute) int {
				return int(Get[Opt[IngressRetry]](c, readers.Retry).Value.Attempt)
			}),
		}
	}
	channels := cell.ReceiptReaders()
	m.receipts["accepted"] = newQFTSReader(ts, func(c *Compute) int {
		return len(Get[[]IngressReceipt[string]](c, channels.Accepted))
	})
	m.receipts["dropped"] = newQFTSReader(ts, func(c *Compute) int {
		return len(Get[[]IngressReceipt[string]](c, channels.Dropped))
	})
	m.receipts["error"] = newQFTSReader(ts, func(c *Compute) int {
		return len(Get[[]IngressReceipt[string]](c, channels.Errors))
	})
	return m
}

func (m *tsIngressFixtureModel) flavor() string { return "thread-safe" }

func (m *tsIngressFixtureModel) open(key string, generation uint64) {
	m.cell.Open(key, generation)
}

func (m *tsIngressFixtureModel) admit(
	envelope IngressEnvelope[string, uint64],
) IngressAdmission {
	return m.cell.Admit(envelope)
}

func (m *tsIngressFixtureModel) suspend(key string) (ReplayRequest, bool) {
	return m.cell.Suspend(key)
}

func (m *tsIngressFixtureModel) reconnect(key string, generation uint64) ReplayRequest {
	return m.cell.Reconnect(key, generation)
}

func (m *tsIngressFixtureModel) closeScope(key string)           { m.cell.Close(key) }
func (m *tsIngressFixtureModel) fail(key string, e IngressError) { m.cell.Fail(key, e) }
func (m *tsIngressFixtureModel) tick(now uint64)                 { m.cell.Tick(now) }
func (m *tsIngressFixtureModel) drain(key string) (uint64, bool) { return m.cell.Drain(key) }

func (m *tsIngressFixtureModel) value(key string) (uint64, bool) {
	return m.cell.Value(m.ts.Context(), key)
}

func (m *tsIngressFixtureModel) readiness(key string) IngressReadiness {
	return m.cell.Readiness(m.ts.Context(), key)
}

func (m *tsIngressFixtureModel) authority(key string) (IngressAuthority, bool) {
	return m.cell.Authority(m.ts.Context(), key)
}

func (m *tsIngressFixtureModel) retry(key string) (IngressRetry, bool) {
	return m.cell.Retry(m.ts.Context(), key)
}

func (m *tsIngressFixtureModel) acceptedLen() int {
	return len(m.cell.Accepted(m.ts.Context()))
}

func (m *tsIngressFixtureModel) droppedLen() int {
	return len(m.cell.Dropped(m.ts.Context()))
}

func (m *tsIngressFixtureModel) errorsLen() int {
	return len(m.cell.Errors(m.ts.Context()))
}

func (m *tsIngressFixtureModel) schedule() IngressSchedule {
	return m.cell.Schedule(m.ts.Context())
}

func (m *tsIngressFixtureModel) view(key string) (IngressScopeView, bool) {
	return m.cell.View(key)
}

func (m *tsIngressFixtureModel) scopeProbe(key, kind string) qfReader {
	return m.scope[key][kind]
}

func (m *tsIngressFixtureModel) receiptProbe(channel string) qfReader {
	return m.receipts[channel]
}

// --- flavor 3: async ---------------------------------------------------------

type asyncIngressFixtureModel struct {
	ctx      *AsyncContext
	cell     *AsyncIngressCell[string, uint64]
	scope    map[string]map[string]qfReader
	receipts map[string]qfReader
}

func newAsyncIngressFixtureModel(
	t *testing.T,
	policy IngressPolicy,
	merge MergePolicy[uint64],
	transport IngressTransportKind,
	pollInterval uint64,
	keys []string,
) ingressFixtureModel {
	t.Helper()
	ctx := NewAsyncContext()
	cell, err := NewAsyncIngressCell[string, uint64](
		ctx, policy, merge, transport, pollInterval)
	if err != nil {
		t.Fatalf("async ingress build: %v", err)
	}
	t.Cleanup(func() { _ = ctx.Close() })
	m := &asyncIngressFixtureModel{
		ctx:      ctx,
		cell:     cell,
		scope:    map[string]map[string]qfReader{},
		receipts: map[string]qfReader{},
	}
	for _, key := range keys {
		readers := cell.ScopeReaders(key)
		m.scope[key] = map[string]qfReader{
			"value": newQFAsyncReader(ctx, func(cc *AsyncComputeContext) int {
				window, err := TrackComputed(cc, readers.Value)
				if err != nil {
					panic(err)
				}
				if !window.Present {
					return -1
				}
				return int(window.Value)
			}),
			"readiness": newQFAsyncReader(ctx, func(cc *AsyncComputeContext) int {
				readiness, err := TrackComputed(cc, readers.Readiness)
				if err != nil {
					panic(err)
				}
				return len(readiness)
			}),
			"authority": newQFAsyncReader(ctx, func(cc *AsyncComputeContext) int {
				authority, err := TrackComputed(cc, readers.Authority)
				if err != nil {
					panic(err)
				}
				return int(authority.Value.Generation)
			}),
			"retry": newQFAsyncReader(ctx, func(cc *AsyncComputeContext) int {
				retry, err := TrackComputed(cc, readers.Retry)
				if err != nil {
					panic(err)
				}
				return int(retry.Value.Attempt)
			}),
		}
	}
	channels := cell.ReceiptReaders()
	asyncReceiptProbe := func(reader *AsyncComputed[[]IngressReceipt[string]]) qfReader {
		return newQFAsyncReader(ctx, func(cc *AsyncComputeContext) int {
			receipts, err := TrackComputed(cc, reader)
			if err != nil {
				panic(err)
			}
			return len(receipts)
		})
	}
	m.receipts["accepted"] = asyncReceiptProbe(channels.Accepted)
	m.receipts["dropped"] = asyncReceiptProbe(channels.Dropped)
	m.receipts["error"] = asyncReceiptProbe(channels.Errors)
	return m
}

func (m *asyncIngressFixtureModel) flavor() string { return "async" }

func (m *asyncIngressFixtureModel) open(key string, generation uint64) {
	m.cell.Open(key, generation)
}

func (m *asyncIngressFixtureModel) admit(
	envelope IngressEnvelope[string, uint64],
) IngressAdmission {
	return m.cell.Admit(envelope)
}

func (m *asyncIngressFixtureModel) suspend(key string) (ReplayRequest, bool) {
	return m.cell.Suspend(key)
}

func (m *asyncIngressFixtureModel) reconnect(key string, generation uint64) ReplayRequest {
	return m.cell.Reconnect(key, generation)
}

func (m *asyncIngressFixtureModel) closeScope(key string)           { m.cell.Close(key) }
func (m *asyncIngressFixtureModel) fail(key string, e IngressError) { m.cell.Fail(key, e) }
func (m *asyncIngressFixtureModel) tick(now uint64)                 { m.cell.Tick(now) }
func (m *asyncIngressFixtureModel) drain(key string) (uint64, bool) { return m.cell.Drain(key) }

func (m *asyncIngressFixtureModel) value(key string) (uint64, bool) {
	return m.cell.Value(nil, key)
}

func (m *asyncIngressFixtureModel) readiness(key string) IngressReadiness {
	return m.cell.Readiness(nil, key)
}

func (m *asyncIngressFixtureModel) authority(key string) (IngressAuthority, bool) {
	return m.cell.Authority(nil, key)
}

func (m *asyncIngressFixtureModel) retry(key string) (IngressRetry, bool) {
	return m.cell.Retry(nil, key)
}

func (m *asyncIngressFixtureModel) acceptedLen() int { return len(m.cell.Accepted(nil)) }
func (m *asyncIngressFixtureModel) droppedLen() int  { return len(m.cell.Dropped(nil)) }
func (m *asyncIngressFixtureModel) errorsLen() int   { return len(m.cell.Errors(nil)) }

func (m *asyncIngressFixtureModel) schedule() IngressSchedule {
	return m.cell.Schedule(nil)
}

func (m *asyncIngressFixtureModel) view(key string) (IngressScopeView, bool) {
	return m.cell.View(key)
}

func (m *asyncIngressFixtureModel) scopeProbe(key, kind string) qfReader {
	return m.scope[key][kind]
}

func (m *asyncIngressFixtureModel) receiptProbe(channel string) qfReader {
	return m.receipts[channel]
}

// --- the replay --------------------------------------------------------------

// ingressFixtureKeys is every key the fixture ever mentions, so a reader exists
// (and is probed) from the FIRST step. An absent reader would silently pass a
// `false` invalidation expectation.
func ingressFixtureKeys(fixture map[string]any) []string {
	var keys []string
	add := func(key string) {
		for _, seen := range keys {
			if seen == key {
				return
			}
		}
		keys = append(keys, key)
	}
	for _, raw := range jsList(fixture["steps"]) {
		step := jsMap(raw)
		if key, ok := jsMap(step["op"])["key"]; ok {
			add(jsStr(key))
		}
		for key := range jsMap(jsMap(step["expected"])["scopes"]) {
			add(key)
		}
	}
	return keys
}

// materializeIngress reads every reader kind so the caches are warm and the next
// step's probe measures THAT step's invalidation and nothing else.
func materializeIngress(model ingressFixtureModel, keys []string) {
	for _, key := range keys {
		_, _ = model.value(key)
		_ = model.readiness(key)
		_, _ = model.authority(key)
		_, _ = model.retry(key)
	}
	_ = model.acceptedLen()
	_ = model.droppedLen()
	_ = model.errorsLen()
	_ = model.schedule()
}

// replayIngressFixture replays one fixture against one flavor and returns the
// number of steps executed, so a caller can prove this binary actually opened the
// corpus.
func replayIngressFixture(
	t *testing.T,
	name string,
	fixture map[string]any,
	build ingressModelBuilder,
) int {
	t.Helper()
	policy := ingressPolicyOf(t, fixture["policy"])
	merge := ingressMergeOf(t, jsStr(fixture["merge"]))
	transport := ingressTransportOf(t, jsStr(fixture["transport"]))
	pollInterval := uint64(jsInt(fixture["poll_interval"]))
	keys := ingressFixtureKeys(fixture)
	model := build(t, policy, merge, transport, pollInterval, keys)

	materializeIngress(model, keys)
	for _, key := range keys {
		for _, kind := range ingressScopeReaderKinds {
			model.scopeProbe(key, kind).drive()
		}
	}
	for _, channel := range ingressReceiptChannels {
		model.receiptProbe(channel).drive()
	}

	steps := 0
	for i, raw := range jsList(fixture["steps"]) {
		step := jsMap(raw)
		op := jsMap(step["op"])
		label := fmt.Sprintf("%s %s step %d (%s)", model.flavor(), name, i, jsStr(op["type"]))
		consumeKeys(t, label, step, "op", "returns", "expected")
		excuseKey(t, step, "op", "replay input: the operation applied to the cell; its effect is asserted through this step's expected block")
		excuseKey(t, step, "returns", "compared in the op switch below, through ingressExpectedAdmission / ingressExpectedReplay, which fold the fixture's values into the expected return")
		excuseKey(t, step, "expected", "container: asserted key-by-key against the expected block below")
		expected := consumeKeys(t, label+" expected", jsMap(step["expected"]),
			"invalidates", "scopes", "receipts")
		excuseKey(t, expected, "invalidates", "container: its scope and receipt reader classes are asserted key-by-key against the expected.invalidates block below")

		beforeScope := map[string]map[string]int{}
		for _, key := range keys {
			beforeScope[key] = map[string]int{}
			for _, kind := range ingressScopeReaderKinds {
				beforeScope[key][kind] = model.scopeProbe(key, kind).drive()
			}
		}
		beforeReceipts := map[string]int{}
		for _, channel := range ingressReceiptChannels {
			beforeReceipts[channel] = model.receiptProbe(channel).drive()
		}

		switch opType := jsStr(op["type"]); opType {
		case "admit":
			admission := model.admit(NewIngressEnvelope(
				jsStr(op["key"]),
				uint64(jsInt(op["generation"])),
				uint64(jsInt(op["sequence"])),
				uint64(jsInt(op["stamped_at"])),
				uint64(jsInt(op["payload"])),
			))
			if want, ok := step["returns"]; ok {
				if got, expect := admission, ingressExpectedAdmission(t, want); got != expect {
					t.Errorf("%s: admission=%+v, want %+v", label, got, expect)
				}
			}
		case "open":
			model.open(jsStr(op["key"]), uint64(jsInt(op["generation"])))
		case "drain":
			value, present := model.drain(jsStr(op["key"]))
			if want, ok := step["returns"]; ok {
				wantValue := jsOptU64(jsMap(want)["drained"])
				got := Opt[uint64]{Present: present, Value: value}
				if !present {
					got = None[uint64]()
				}
				if got != wantValue {
					t.Errorf("%s: drained=%+v, want %+v", label, got, wantValue)
				}
			}
		case "suspend":
			request, present := model.suspend(jsStr(op["key"]))
			if want, ok := step["returns"]; ok {
				wantRequest, wantPresent := ingressExpectedReplay(jsMap(want)["replay"])
				if present != wantPresent || (present && request != wantRequest) {
					t.Errorf("%s: replay=(%+v,%v), want (%+v,%v)",
						label, request, present, wantRequest, wantPresent)
				}
			}
		case "reconnect":
			request := model.reconnect(jsStr(op["key"]), uint64(jsInt(op["generation"])))
			if want, ok := step["returns"]; ok {
				wantRequest, wantPresent := ingressExpectedReplay(jsMap(want)["replay"])
				if !wantPresent {
					t.Errorf("%s: reconnect always returns a replay request; fixture says null",
						label)
				} else if request != wantRequest {
					t.Errorf("%s: replay=%+v, want %+v", label, request, wantRequest)
				}
			}
		case "close":
			model.closeScope(jsStr(op["key"]))
		case "fail":
			model.fail(jsStr(op["key"]), ingressErrorOf(t, jsStr(op["error"])))
		case "tick":
			model.tick(uint64(jsInt(op["now"])))
		default:
			t.Fatalf("%s: unknown op %q", label, opType)
		}

		// `invalidates`, asserted per reader kind in BOTH directions. A step
		// expecting false FAILS if the shell invalidated anyway.
		invalidates := consumeKeys(t, label+" expected.invalidates",
			jsMap(expected["invalidates"]), "scopes", "receipts")
		// Walked by the tracker rather than by a loop written here
		// (#lzsubblockkeyset): every scope the block names reaches the probe below,
		// so a scope added upstream cannot go unread.
		scopesSeen := 0
		assertKeyEach(t, invalidates, "scopes", func(key string, rawWant any) {
			scopesSeen++
			want := consumeKeys(t, label+" expected.invalidates.scopes."+key,
				jsMap(rawWant), ingressScopeReaderKinds...)
			if _, probed := beforeScope[key]; !probed {
				t.Fatalf("%s: invalidates names unprobed scope %q", label, key)
			}
			for _, kind := range ingressScopeReaderKinds {
				if _, ok := want[kind]; !ok {
					t.Fatalf("%s: invalidates.scopes.%s.%s is missing", label, key, kind)
				}
				scopeKey, readerKind := key, kind
				assertKeyWith(t, want, kind, func(flag any) {
					assertInvalidationDelta(t,
						label+" invalidates.scopes."+scopeKey+"."+readerKind,
						model.scopeProbe(scopeKey, readerKind), beforeScope[scopeKey][readerKind], flag == true)
				})
			}
		})
		if scopesSeen == 0 {
			t.Fatalf("%s: expected.invalidates.scopes is empty", label)
		}
		receiptWants := assertKeySub(t, invalidates, "receipts", ingressReceiptChannels...)
		for _, channel := range ingressReceiptChannels {
			if _, ok := receiptWants[channel]; !ok {
				t.Fatalf("%s: invalidates.receipts.%s is missing", label, channel)
			}
			receiptChannel := channel
			assertKeyWith(t, receiptWants, channel, func(flag any) {
				assertInvalidationDelta(t,
					label+" invalidates.receipts."+receiptChannel,
					model.receiptProbe(receiptChannel), beforeReceipts[receiptChannel], flag == true)
			})
		}

		assertIngressState(t, model, expected, label)
		materializeIngress(model, keys)
		steps++
	}
	return steps
}

func assertIngressState(
	t *testing.T,
	model ingressFixtureModel,
	expected map[string]any,
	label string,
) {
	t.Helper()
	assertKeyEach(t, expected, "scopes", func(key string, rawWant any) {
		assertIngressScope(t, model, key, rawWant, label)
	})

	receipts := assertKeySub(t, expected, "receipts", "accepted", "dropped", "error")
	assertKey(t, receipts, "accepted", model.acceptedLen())
	assertKey(t, receipts, "dropped", model.droppedLen())
	assertKey(t, receipts, "error", model.errorsLen())
}

// assertIngressScope checks one scope's expected view against the live cell.
func assertIngressScope(
	t *testing.T,
	model ingressFixtureModel,
	key string,
	rawWant any,
	label string,
) {
	t.Helper()
	want := consumeKeys(t, label+" expected.scopes."+key, jsMap(rawWant),
		"lifecycle", "generation", "delivered_through", "buffered",
		"consecutive_errors", "window", "readiness", "authority", "retry")
	view, ok := model.view(key)
	if !ok {
		t.Fatalf("%s: scope %s absent", label, key)
	}
	assertKeyWith(t, want, "lifecycle", func(raw any) {
		if got, expect := view.Lifecycle, ingressLifecycleOf(t, jsStr(raw)); got != expect {
			t.Errorf("%s: %s lifecycle=%s, want %s", label, key, got, expect)
		}
	})
	assertKey(t, want, "generation", view.Generation)
	assertKeyWith(t, want, "delivered_through", func(raw any) {
		if got, expect := view.DeliveredThrough, jsOptU64(raw); got != expect {
			t.Errorf("%s: %s watermark=%+v, want %+v", label, key, got, expect)
		}
	})
	assertKey(t, want, "buffered", view.Buffered)
	assertKey(t, want, "consecutive_errors", int(view.ConsecutiveErrors))

	assertKeyWith(t, want, "window", func(raw any) {
		value, present := model.value(key)
		got := Opt[uint64]{Present: present, Value: value}
		if !present {
			got = None[uint64]()
		}
		if expect := jsOptU64(raw); got != expect {
			t.Errorf("%s: %s window=%+v, want %+v", label, key, got, expect)
		}
	})
	assertKeyWith(t, want, "readiness", func(raw any) {
		if got, expect := model.readiness(key), ingressReadinessOf(t, jsStr(raw)); got != expect {
			t.Errorf("%s: %s readiness=%s, want %s", label, key, got, expect)
		}
	})

	// `authority` and `retry` are DESCENDED into (#lzsubblockkeyset) rather than
	// read field-by-field out of an assertKeyWith callback: the child block owns
	// the unconsumed-key check, so a fourth field added to either one fails here
	// instead of being folded into nothing.
	authority, hasAuthority := model.authority(key)
	wantAuthority := assertKeySub(t, want, "authority", "generation", "delivered_through", "stamped_at")
	if wantAuthority == nil {
		if hasAuthority {
			t.Errorf("%s: %s authority=%+v, want none", label, key, authority)
		}
	} else {
		expectAuthority := IngressAuthority{
			Generation:       uint64(jsInt(wantAuthority["generation"])),
			DeliveredThrough: jsOptU64(wantAuthority["delivered_through"]),
			StampedAt:        uint64(jsInt(wantAuthority["stamped_at"])),
		}
		excuseKeys(t, wantAuthority, "folded into the expected IngressAuthority compared against the live one on the next line",
			"generation", "delivered_through", "stamped_at")
		if !hasAuthority || authority != expectAuthority {
			t.Errorf("%s: %s authority=(%+v,%v), want %+v",
				label, key, authority, hasAuthority, expectAuthority)
		}
	}

	retry, hasRetry := model.retry(key)
	wantRetry := assertKeySub(t, want, "retry", "attempt", "backoff", "resume_from")
	if wantRetry == nil {
		if hasRetry {
			t.Errorf("%s: %s retry=%+v, want none", label, key, retry)
		}
		return
	}
	expectRetry := IngressRetry{
		Attempt:    uint32(jsInt(wantRetry["attempt"])),
		Backoff:    uint64(jsInt(wantRetry["backoff"])),
		ResumeFrom: uint64(jsInt(wantRetry["resume_from"])),
	}
	excuseKeys(t, wantRetry, "folded into the expected IngressRetry compared against the live one on the next line",
		"attempt", "backoff", "resume_from")
	if !hasRetry || retry != expectRetry {
		t.Errorf("%s: %s retry=(%+v,%v), want %+v",
			label, key, retry, hasRetry, expectRetry)
	}
}

func replayIngressCorpus(t *testing.T, build ingressModelBuilder) int {
	t.Helper()
	steps := 0
	for _, name := range ingressFixtures {
		steps += replayIngressFixture(t, name, loadIngressFixture(t, name), build)
	}
	return steps
}

// --- the per-flavor gates ----------------------------------------------------

func TestIngressSyncFlavorReplaysCorpus(t *testing.T) {
	steps := replayIngressCorpus(t, newSyncIngressFixtureModel)
	if steps == 0 {
		t.Fatal("the single-threaded flavor replayed 0 steps; the corpus was never opened")
	}
	if want := expectedIngressStepTotal(t); steps != want {
		t.Fatalf("single-threaded flavor replayed %d steps, want the corpus total %d",
			steps, want)
	}
}

func TestIngressThreadSafeFlavorReplaysCorpus(t *testing.T) {
	steps := replayIngressCorpus(t, newTSIngressFixtureModel)
	if steps == 0 {
		t.Fatal("the thread-safe flavor replayed 0 steps; the corpus was never opened")
	}
	if want := expectedIngressStepTotal(t); steps != want {
		t.Fatalf("thread-safe flavor replayed %d steps, want the corpus total %d", steps, want)
	}
}

func TestIngressAsyncFlavorReplaysCorpus(t *testing.T) {
	steps := replayIngressCorpus(t, newAsyncIngressFixtureModel)
	if steps == 0 {
		t.Fatal("the async flavor replayed 0 steps; the corpus was never opened")
	}
	if want := expectedIngressStepTotal(t); steps != want {
		t.Fatalf("async flavor replayed %d steps, want the corpus total %d", steps, want)
	}
}

// TestIngressInvalidationProbeDiscriminates pins the probe itself.
//
// The corpus asserts NEGATIVE invalidation, so a probe that can never report
// "invalidated" would make every `false` expectation pass for free. This proves
// the probe moves in both directions: a delivery invalidates the value reader, and
// a buffered envelope does not.
func TestIngressInvalidationProbeDiscriminates(t *testing.T) {
	for _, build := range []struct {
		name  string
		build ingressModelBuilder
	}{
		{"sync", newSyncIngressFixtureModel},
		{"thread-safe", newTSIngressFixtureModel},
		{"async", newAsyncIngressFixtureModel},
	} {
		t.Run(build.name, func(t *testing.T) {
			model := build.build(t, DefaultIngressPolicy(), Sum[uint64](),
				IngressTransportEventChannel, 25, []string{"alpha"})
			probe := model.scopeProbe("alpha", "value")
			before := probe.drive()

			model.admit(NewIngressEnvelope[string, uint64]("alpha", 1, 0, 0, 1))
			if after := probe.drive(); after != before+1 {
				t.Fatalf("a delivery must invalidate the value reader: %d -> %d",
					before, after)
			}

			before = probe.drive()
			model.admit(NewIngressEnvelope[string, uint64]("alpha", 1, 5, 0, 1))
			if after := probe.drive(); after != before {
				t.Fatalf("a buffered envelope must NOT invalidate the value reader: %d -> %d",
					before, after)
			}
		})
	}
}

// TestIngressThreadSafeConcurrentReadsAndAdmitsAreRaceFree is the -race gate for
// this flavor, and it exists because lazily-go already shipped the defect once:
// in ThreadSafeReactiveMap every READ ran off the context lock, because a read
// looks passive. It is not — a Get of a stale derived reader runs refresh →
// recomputeNow → Context.newCompute, which bumps computeGen and cachedCount on the
// shared single-threaded Context, and registering a dependency edge mutates the
// edge set. So two goroutines reading two different scopes write the same Context
// fields.
//
// The shape is what makes the detector discriminate, and each part is load-bearing:
//
//   - A SMALL hot key set. Spreading readers over a wide key space makes the
//     collision rare enough that -race misses it — which is exactly how an
//     unlocked read first passed the map's own soak.
//   - Readers AND an admitting mutator running together, so the readers hit
//     refresh/recompute rather than a warm cache. A warm cache reads a field and
//     writes nothing, and races on the cold path only.
//   - Every reader kind driven, not just the value: readiness/authority/retry and
//     the three receipt channels are separate nodes, and the map's first fix
//     covered exactly one of its five call sites.
//
// `make check` runs this without the detector; CI runs the whole package under
// `CGO_ENABLED=1 go test -race ./...`, and `make race` does the same locally.
func TestIngressThreadSafeConcurrentReadsAndAdmitsAreRaceFree(t *testing.T) {
	ts := NewThreadSafeContext()
	cell, err := NewThreadSafeIngressCell[string, uint64](
		ts, DefaultIngressPolicy(), Sum[uint64](),
		IngressTransportEventChannel, 25)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	keys := []string{"alpha", "beta"}
	for _, key := range keys {
		cell.Open(key, 1)
	}

	const rounds = 200
	var wg sync.WaitGroup
	for reader := 0; reader < 3; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				key := keys[r%len(keys)]
				_, _ = cell.Value(ts.Context(), key)
				_ = cell.Readiness(ts.Context(), key)
				_, _ = cell.Authority(ts.Context(), key)
				_, _ = cell.Retry(ts.Context(), key)
				_ = cell.Accepted(ts.Context())
				_ = cell.Dropped(ts.Context())
				_ = cell.Errors(ts.Context())
				_ = cell.Schedule(ts.Context())
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 0; r < rounds; r++ {
			for i, key := range keys {
				cell.Admit(NewIngressEnvelope(key, 1, uint64(r), uint64(r), uint64(i+1)))
			}
			if r%8 == 0 {
				cell.Tick(uint64(r))
				for _, key := range keys {
					cell.Drain(key)
				}
			}
			if r%16 == 0 {
				cell.Fail(keys[0], IngressErrorTransportClosed)
			}
		}
	}()
	wg.Wait()

	// The soak has to leave real state behind, or a deadlocked/no-op run would
	// pass as "race-free".
	for _, key := range keys {
		view, ok := cell.View(key)
		if !ok {
			t.Fatalf("scope %s vanished during the soak", key)
		}
		if !view.DeliveredThrough.Present {
			t.Fatalf("scope %s delivered nothing during the soak", key)
		}
	}
	if len(cell.Accepted(ts.Context())) == 0 {
		t.Fatal("the soak minted no accepted receipts; it exercised nothing")
	}
}

// Mutation-check record (#designimplementtransport). Each deliberate defect was
// introduced, the gate run, and the defect reverted with an mtime bump — a restore
// that preserves mtime lets the build cache reuse the MUTATED artifact and reports
// a false green. All eight were killed:
//
//   - fence checked AFTER dedupe → ingress_generation_handoff step 2 reports
//     duplicate_sequence where the corpus expects stale_generation (all 3 flavors).
//   - handoff keeps the superseded window → ingress_generation_handoff step 4
//     window value (9 becomes 49) and the receipt's conflated flag.
//   - Buffered marks every reader dirty → every `invalidates: false` step in
//     ingress_reorder_and_duplication and ingress_reorder_window_overflow, in all
//     three flavors.
//   - tick marks readiness unconditionally → ingress_freshness_and_retry step 1,
//     the in-horizon tick.
//   - Block advances the watermark → ingress_backpressure step 3 reports
//     duplicate_sequence instead of an accept.
//   - thread-safe apply clears OUTSIDE the batch (ts.WithLock instead of ts.Batch)
//     → TestIngressOneAdmissionIsOneFrontierWalk sees two effect runs for one
//     admission.
//   - the error-receipt channel is never cleared → invalidates.receipts.error.
//     This is why receipt invalidation is asserted per CHANNEL rather than by
//     receipt COUNT: a stale cache recomputes to the right count, so a count-only
//     gate would have called it green.
//   - every thread-safe read taken OFF the graph lock (Read → a bare call) →
//     TestIngressThreadSafeConcurrentReadsAndAdmitsAreRaceFree under -race, the
//     defect ThreadSafeReactiveMap already shipped.
