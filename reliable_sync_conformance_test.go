package lazily

// Conformance replay of the canonical lazily-spec/conformance/reliable-sync/
// fixtures against the native ResyncCoordinator / InMemoryOutbox / OrSet /
// WireLwwRegister, plus a JSON round-trip of the two control frames
// (ResyncRequest / OutboxAck) and SyncDriver loop-shape unit tests.
//
// Cross-language pin with lazily-rs / lazily-kt / lazily-js; correctness backstop
// lazily-formal ReliableSync.lean. Fixtures resolve via loadConformanceFixture
// (sibling ../lazily-spec/conformance/reliable-sync/ or the local committed copy
// ../lazily-spec/conformance/reliable-sync/). The vendored copy under
// test/conformance/ is no longer read by any test — see the note in
// conformance_manifest_test.go about why reading a vendored copy is invisible to
// a source-grep coverage guard.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

// rsFixture is the reliable-sync fixture envelope (scenarios kept raw so each
// scenario can decode into its own shape).
type rsFixture struct {
	conformanceMeta
	ProtocolVersion int    `json:"protocol_version"`
	Kind            string `json:"kind"`
	// Wire is the canonical frame the fixture's `assertions` describe. Both were
	// undeclared, so multi_epoch_delta.json's five root assertions — the span
	// arithmetic the whole file is named for — decoded into nothing.
	Wire       json.RawMessage            `json:"wire"`
	Assertions map[string]json.RawMessage `json:"assertions"`
	Scenarios  []json.RawMessage          `json:"scenarios"`

	// fixtureID is the corpus-relative path this envelope was loaded from. It
	// is unexported and untagged so DisallowUnknownFields never sees it.
	fixtureID string
}

// assertRootDelta cross-checks the fixture's root `assertions` against the root
// `wire` frame AS THE LIBRARY DECODES IT, failing on any key it cannot evaluate.
//
// The values come from `IpcMessageFromWire`, never from a re-parse of the
// fixture's own `wire` object (#lznullformblind). They used to come from an
// ad-hoc struct filled by `mustStrictJSON` on that same object, which made
// `base_epoch`, `epoch` and `op_count` a comparison of the fixture against
// itself: `assertions.base_epoch` versus `wire.Delta.base_epoch`, with the
// library never consulted. Delete the decoder entirely and those three stayed
// green — the vacuity this repo's `anti_vacuity` keys exist to name, in a file
// whose whole point is the span arithmetic over a decoded Delta.
func (fx rsFixture) assertRootDelta(t *testing.T, name string) {
	t.Helper()
	if len(fx.Assertions) == 0 {
		return
	}
	// The strict parse stays as a CORPUS-SHAPE precondition — it is what rejects
	// an unknown field or a non-Delta root before the decode — but nothing
	// asserted below is read out of it.
	var wire struct {
		Delta *struct {
			BaseEpoch Epoch             `json:"base_epoch"`
			Epoch     Epoch             `json:"epoch"`
			Ops       []json.RawMessage `json:"ops"`
		} `json:"Delta"`
		OutboxAck     json.RawMessage `json:"OutboxAck"`
		ResyncRequest json.RawMessage `json:"ResyncRequest"`
	}
	mustStrictJSON(t, name+" wire", fx.Wire, &wire)
	if wire.Delta == nil {
		t.Fatalf("%s: root assertions require a root Delta wire frame", name)
	}

	message, err := IpcMessageFromWire(fx.Wire)
	if err != nil {
		t.Fatalf("%s: decode root wire frame: %v", name, err)
	}
	decoded, ok := message.(IpcMessageDelta)
	if !ok {
		t.Fatalf("%s: the root wire frame decoded to %T, want a Delta", name, message)
	}
	d := decoded.Value
	// Rung 0: the dispatch below fatals on a key it cannot evaluate, so the block
	// is bound (#lzunboundblockguard).
	bindBlockFields("assertions", fx.Assertions)
	for key, raw := range fx.Assertions {
		var actual any
		switch key {
		case "base_epoch":
			actual = d.BaseEpoch
		case "epoch":
			actual = d.Epoch
		case "span":
			actual = d.Span()
		case "is_multi_epoch":
			actual = d.Span() > 1
		case "op_count":
			actual = len(d.Ops)
		default:
			t.Fatalf("%s: unknown root assertion key %q", name, key)
		}
		assertValueEqualsRaw(t, actual, raw, name+":"+key)
	}
}

// rsWireDelta is a fixture-shaped delta: the epoch pair plus the raw op list,
// kept raw so ops can be compared structurally without re-deriving the wire
// encoding.
type rsWireDelta struct {
	BaseEpoch Epoch             `json:"base_epoch"`
	Epoch     Epoch             `json:"epoch"`
	Ops       []json.RawMessage `json:"ops"`
}

// assertUnitFoldEquivalent checks a span delta against the unit fold the fixture
// declares equivalent to it: same op sequence, a contiguous epoch chain over the
// same range, and a receiver that ends in the same place either way.
func assertUnitFoldEquivalent(t *testing.T, from Epoch, span rsWireDelta, fold []rsWireDelta) {
	t.Helper()
	if len(fold) == 0 {
		t.Fatal("fold_equivalent asserted with no equivalent_unit_fold to compare against")
	}

	var folded []json.RawMessage
	prev := span.BaseEpoch
	for i, unit := range fold {
		if unit.BaseEpoch != prev {
			t.Fatalf("unit fold %d starts at base_epoch %d, want %d (not contiguous)", i, unit.BaseEpoch, prev)
		}
		if unit.Epoch != unit.BaseEpoch+1 {
			t.Fatalf("unit fold %d spans %d..%d, want a unit step", i, unit.BaseEpoch, unit.Epoch)
		}
		prev = unit.Epoch
		folded = append(folded, unit.Ops...)
	}
	if prev != span.Epoch {
		t.Fatalf("unit fold ends at epoch %d, want the span end %d", prev, span.Epoch)
	}

	if len(folded) != len(span.Ops) {
		t.Fatalf("unit fold carries %d ops, span carries %d", len(folded), len(span.Ops))
	}
	for i := range folded {
		if !jsonSemanticEqual(t, folded[i], span.Ops[i]) {
			t.Fatalf("unit fold op %d differs from the span op:\n fold: %s\n span: %s",
				i, folded[i], span.Ops[i])
		}
	}

	// Replaying the fold one unit at a time must land the receiver where the
	// single span landed it.
	unitCoord := NewResyncCoordinatorWithEpoch(from)
	for i, unit := range fold {
		act, _ := unitCoord.IngestDelta(Delta{BaseEpoch: unit.BaseEpoch, Epoch: unit.Epoch})
		if act != ResyncActionApply {
			t.Fatalf("unit fold %d ingest = %v, want Apply", i, act)
		}
	}
	if unitCoord.LastEpoch() != span.Epoch {
		t.Fatalf("unit fold left last_epoch at %d, want %d", unitCoord.LastEpoch(), span.Epoch)
	}
}

// rsScenarioHead is the prose/identity preamble every reliable-sync scenario
// carries. Declaring it once keeps each scenario's own struct to the keys it
// actually asserts.
type rsScenarioHead struct {
	conformanceDoc
	Id   string `json:"id"`
	Name string `json:"name"`
}

func loadReliableSyncFixture(t *testing.T, name string) rsFixture {
	t.Helper()
	raw := loadConformanceFixture(t, "reliable-sync", name)
	var fx rsFixture
	mustStrictJSON(t, name, raw, &fx)
	// Unexported and untagged, so the strict decode never sees it: the fixture
	// id the scenario ledger records against (#lzscenariocoverage).
	fx.fixtureID = filepath.Join("reliable-sync", name)
	return fx
}

// scenario returns the raw scenario object with the given name.
//
// Every reliable-sync runner reaches its scenarios through here, which makes it
// the one place rung 4 has to be instrumented for this whole corpus: a scenario
// no runner asks for is never recorded, and that is exactly the report
// (#lzscenariocoverage).
func (fx rsFixture) scenario(t *testing.T, name string) json.RawMessage {
	t.Helper()
	for index, sc := range fx.Scenarios {
		var head struct {
			Id   string `json:"id"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(sc, &head); err != nil {
			t.Fatalf("decode scenario head: %v", err)
		}
		// Matched on `id`, the canonical scenario identity
		// (#recommendedconformanceco). `name` is a prose label, so matching on
		// it means a copy-edit upstream silently stops resolving.
		if head.Id != name {
			continue
		}
		_ = index
		return sc
	}
	t.Fatalf("scenario %q not found", name)
	return nil
}

// replay selects a scenario, BOOKS it, and strictly decodes it into target.
//
// Selecting is not replaying (#lzscenariobodyskip): `scenario` above walks past
// every scenario ahead of its match and hands back undecoded bytes, and a caller
// could select one and never decode it. The decode is the operation only a
// runner about to replay performs, so that is where the ledger books.
func (fx rsFixture) replay(t *testing.T, name string, target any) {
	t.Helper()
	raw := fx.scenario(t, name)
	recordScenario(fx.fixtureID, name)
	mustStrictJSON(t, "reliable-sync scenario "+name, raw, target)
}

func mustMessage(t *testing.T, raw json.RawMessage) IpcMessage {
	t.Helper()
	m, err := IpcMessageFromWire(raw)
	if err != nil {
		t.Fatalf("decode frame %s: %v", raw, err)
	}
	return m
}

// ---------------------------------------------------------------------------
// control-frame serde round-trip
// ---------------------------------------------------------------------------

func TestReliableSyncResyncRequestRoundTripsJSON(t *testing.T) {
	m := IpcMessageResyncRequest{Value: ResyncRequest{FromEpoch: 2}}
	b, err := m.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got, want := string(b), `{"ResyncRequest":{"from_epoch":2}}`; got != want {
		t.Fatalf("wire = %s, want %s", got, want)
	}
	decoded, err := DecodeIpcMessageJSON(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(decoded, m) {
		t.Fatalf("round-trip = %#v, want %#v", decoded, m)
	}
}

func TestReliableSyncOutboxAckRoundTripsJSON(t *testing.T) {
	m := IpcMessageOutboxAck{Value: OutboxAck{ThroughEpoch: 41}}
	b, err := m.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got, want := string(b), `{"OutboxAck":{"through_epoch":41}}`; got != want {
		t.Fatalf("wire = %s, want %s", got, want)
	}
	decoded, err := DecodeIpcMessageJSON(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(decoded, m) {
		t.Fatalf("round-trip = %#v, want %#v", decoded, m)
	}
}

func TestReliableSyncControlFramesClassifyAsKinds(t *testing.T) {
	if got := ipcMessageKind(IpcMessageResyncRequest{Value: ResyncRequest{FromEpoch: 1}}); got != LazilyFfiMessageKindResyncRequest {
		t.Fatalf("ResyncRequest kind = %d, want 4", got)
	}
	if got := ipcMessageKind(IpcMessageOutboxAck{Value: OutboxAck{ThroughEpoch: 1}}); got != LazilyFfiMessageKindOutboxAck {
		t.Fatalf("OutboxAck kind = %d, want 5", got)
	}
}

// ---------------------------------------------------------------------------
// multi_epoch_delta.json
// ---------------------------------------------------------------------------

func TestReliableSyncMultiEpochDelta(t *testing.T) {
	fx := loadReliableSyncFixture(t, "multi_epoch_delta.json")
	if fx.Kind != "ReliableSync" {
		t.Fatalf("kind = %q, want ReliableSync", fx.Kind)
	}

	// The whole point of this scenario is `fold_equivalent`: one span-3 delta
	// must land the receiver where three unit deltas carrying the same ops land
	// it. The runner used to decode `receiver_last_epoch_after` and nothing else,
	// so `delta.ops`, `equivalent_unit_fold`, `action`, `applied`,
	// `atomic_advance` and `fold_equivalent` all went unread — the epoch arithmetic
	// was checked and the equivalence the fixture exists for never was.
	var span struct {
		rsScenarioHead
		ReceiverLastEpoch  Epoch         `json:"receiver_last_epoch"`
		Delta              rsWireDelta   `json:"delta"`
		EquivalentUnitFold []rsWireDelta `json:"equivalent_unit_fold"`
		Expect             struct {
			Action                 string `json:"action"`
			Applied                bool   `json:"applied"`
			ReceiverLastEpochAfter Epoch  `json:"receiver_last_epoch_after"`
			AtomicAdvance          bool   `json:"atomic_advance"`
			FoldEquivalent         bool   `json:"fold_equivalent"`
		} `json:"expect"`
	}
	fx.replay(t, "span_3_applies_equal_to_unit_fold", &span)
	if !(span.Delta.Epoch > span.Delta.BaseEpoch+1) {
		t.Fatalf("fixture must pin a multi-epoch span")
	}
	delta := Delta{BaseEpoch: span.Delta.BaseEpoch, Epoch: span.Delta.Epoch}
	if got, want := delta.Span(), span.Delta.Epoch-span.Delta.BaseEpoch; got != want {
		t.Fatalf("span = %d, want %d", got, want)
	}
	coord := NewResyncCoordinatorWithEpoch(span.ReceiverLastEpoch)
	action, _ := coord.IngestDelta(delta)
	if got := action.String(); got != span.Expect.Action {
		t.Fatalf("action = %q, want %q", got, span.Expect.Action)
	}
	if applied := action == ResyncActionApply; applied != span.Expect.Applied {
		t.Fatalf("applied = %v, want %v", applied, span.Expect.Applied)
	}
	if coord.LastEpoch() != span.Expect.ReceiverLastEpochAfter {
		t.Fatalf("last_epoch = %d, want %d", coord.LastEpoch(), span.Expect.ReceiverLastEpochAfter)
	}

	// atomic_advance: one ingest moves the receiver from the span's base straight
	// to its end. No epoch between the two is ever the receiver's last_epoch.
	if span.Expect.AtomicAdvance {
		if span.ReceiverLastEpoch != span.Delta.BaseEpoch {
			t.Fatalf("atomic_advance: receiver starts at %d, not at the span base %d",
				span.ReceiverLastEpoch, span.Delta.BaseEpoch)
		}
		if coord.LastEpoch() != span.Delta.Epoch {
			t.Fatalf("atomic_advance: one ingest left last_epoch at %d, want %d",
				coord.LastEpoch(), span.Delta.Epoch)
		}
	}

	// fold_equivalent: the unit fold chains contiguously across the same range
	// and carries the same ops in the same order, and replaying it leaves an
	// identical receiver.
	if span.Expect.FoldEquivalent {
		assertUnitFoldEquivalent(t, span.ReceiverLastEpoch, span.Delta, span.EquivalentUnitFold)
	}

	var gap struct {
		rsScenarioHead
		ReceiverLastEpoch Epoch       `json:"receiver_last_epoch"`
		Delta             rsWireDelta `json:"delta"`
		Expect            struct {
			Action                 string `json:"action"`
			RequestFrom            Epoch  `json:"request_from"`
			Applied                bool   `json:"applied"`
			ReceiverLastEpochAfter Epoch  `json:"receiver_last_epoch_after"`
		} `json:"expect"`
	}
	fx.replay(t, "gap_rule_unchanged_under_span", &gap)
	gc := NewResyncCoordinatorWithEpoch(gap.ReceiverLastEpoch)
	gapAction, from := gc.IngestDelta(Delta{BaseEpoch: gap.Delta.BaseEpoch, Epoch: gap.Delta.Epoch})
	if got := gapAction.String(); got != gap.Expect.Action {
		t.Fatalf("gap action = %q, want %q", got, gap.Expect.Action)
	}
	if from != gap.Expect.RequestFrom {
		t.Fatalf("gap request_from = %d, want %d", from, gap.Expect.RequestFrom)
	}
	if applied := gapAction == ResyncActionApply; applied != gap.Expect.Applied {
		t.Fatalf("gap applied = %v, want %v", applied, gap.Expect.Applied)
	}
	if gc.LastEpoch() != gap.Expect.ReceiverLastEpochAfter {
		t.Fatalf("gap last_epoch = %d, want %d", gc.LastEpoch(), gap.Expect.ReceiverLastEpochAfter)
	}

	fx.assertRootDelta(t, "multi_epoch_delta.json")
}

// ---------------------------------------------------------------------------
// resync_gap_converge.json
// ---------------------------------------------------------------------------

type rsInbound struct {
	conformanceDoc
	Dropped        bool            `json:"dropped"`
	Reason         string          `json:"reason"`
	Frame          json.RawMessage `json:"frame"`
	ExpectAction   string          `json:"expect_action"`
	RequestFrom    Epoch           `json:"request_from"`
	LastEpochAfter Epoch           `json:"last_epoch_after"`
}

// rsApplyToGraph folds an inbound frame into a node -> payload-bytes map: the
// smallest receiver state that can answer `converged_nodes` and
// `equals_no_drop_receiver`.
func rsApplyToGraph(t *testing.T, graph map[NodeId][]byte, msg IpcMessage) {
	t.Helper()
	switch m := msg.(type) {
	case IpcMessageSnapshot:
		for _, n := range m.Value.Nodes {
			p, ok := n.State.(NodeStatePayload)
			if !ok {
				t.Fatalf("snapshot node %d carries %T, want a Payload", n.Node, n.State)
			}
			graph[n.Node] = p.Bytes
		}
	case IpcMessageDelta:
		for _, op := range m.Value.Ops {
			switch o := op.(type) {
			case DeltaOpCellSet:
				graph[o.Node] = rsInlineBytes(t, o.Payload)
			case DeltaOpSlotValue:
				graph[o.Node] = rsInlineBytes(t, o.Payload)
			}
		}
	default:
		t.Fatalf("cannot fold %T into a receiver graph", msg)
	}
}

func rsInlineBytes(t *testing.T, v IpcValue) []byte {
	t.Helper()
	inline, ok := v.(IpcValueInline)
	if !ok {
		t.Fatalf("payload %T is not Inline", v)
	}
	return inline.Bytes
}

func mustParseNodeId(t *testing.T, s string) uint64 {
	t.Helper()
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatalf("node id %q: %v", s, err)
	}
	return n
}

func TestReliableSyncResyncGapConverge(t *testing.T) {
	fx := loadReliableSyncFixture(t, "resync_gap_converge.json")

	var sc struct {
		rsScenarioHead
		StartLastEpoch Epoch       `json:"start_last_epoch"`
		Inbound        []rsInbound `json:"inbound"`
		Expect         struct {
			FinalLastEpoch        Epoch `json:"final_last_epoch"`
			ResyncRequestsEmitted int   `json:"resync_requests_emitted"`
			// The two keys the fixture is actually named for. Counting resync
			// requests says the gap was noticed; only these say the receiver
			// ended up holding the right graph.
			ConvergedNodes       map[string][]byte `json:"converged_nodes"`
			EqualsNoDropReceiver *bool             `json:"equals_no_drop_receiver"`
		} `json:"expect"`
	}
	fx.replay(t, "drop_suffix_then_resync_converges", &sc)
	coord := NewResyncCoordinatorWithEpoch(sc.StartLastEpoch)
	requests := 0
	// receiver A's graph, and the authoritative graph a receiver that dropped
	// nothing would hold (the sender's latest snapshot).
	graph := map[NodeId][]byte{}
	authoritative := map[NodeId][]byte{}
	for _, frame := range sc.Inbound {
		if frame.Dropped {
			continue
		}
		msg := mustMessage(t, frame.Frame)
		action, from := coord.Ingest(msg)
		if action == ResyncActionApply {
			rsApplyToGraph(t, graph, msg)
		}
		if snap, ok := msg.(IpcMessageSnapshot); ok {
			authoritative = map[NodeId][]byte{}
			rsApplyToGraph(t, authoritative, snap)
		}
		switch frame.ExpectAction {
		case "Apply":
			if action != ResyncActionApply {
				t.Fatalf("action = %v, want Apply", action)
			}
		case "RequestSnapshot":
			requests++
			if action != ResyncActionRequestSnapshot || from != frame.RequestFrom {
				t.Fatalf("action = (%v,%d), want (RequestSnapshot,%d)", action, from, frame.RequestFrom)
			}
		case "Ignore":
			if action != ResyncActionIgnore {
				t.Fatalf("action = %v, want Ignore", action)
			}
		default:
			t.Fatalf("unknown expect_action %q", frame.ExpectAction)
		}
		if coord.LastEpoch() != frame.LastEpochAfter {
			t.Fatalf("last_epoch = %d, want %d", coord.LastEpoch(), frame.LastEpochAfter)
		}
	}
	if coord.LastEpoch() != sc.Expect.FinalLastEpoch {
		t.Fatalf("final last_epoch = %d, want %d", coord.LastEpoch(), sc.Expect.FinalLastEpoch)
	}
	if requests != sc.Expect.ResyncRequestsEmitted {
		t.Fatalf("requests = %d, want %d", requests, sc.Expect.ResyncRequestsEmitted)
	}
	for id, want := range sc.Expect.ConvergedNodes {
		node := NodeId(mustParseNodeId(t, id))
		got, ok := graph[node]
		if !ok {
			t.Fatalf("converged_nodes: node %s absent from the receiver graph", id)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("converged_nodes: node %s = %v, want %v", id, got, want)
		}
	}
	if want := sc.Expect.EqualsNoDropReceiver; want != nil {
		equal := reflect.DeepEqual(graph, authoritative)
		if equal != *want {
			t.Fatalf("equals_no_drop_receiver = %v (%v vs %v), want %v",
				equal, graph, authoritative, *want)
		}
	}

	var single struct {
		rsScenarioHead
		StartLastEpoch Epoch       `json:"start_last_epoch"`
		Inbound        []rsInbound `json:"inbound"`
		Expect         struct {
			FinalLastEpoch        Epoch `json:"final_last_epoch"`
			ResyncRequestsEmitted int   `json:"resync_requests_emitted"`
		} `json:"expect"`
	}
	fx.replay(t, "single_request_per_gap", &single)
	c2 := NewResyncCoordinatorWithEpoch(single.StartLastEpoch)
	req2 := 0
	for i, frame := range single.Inbound {
		action, _ := c2.Ingest(mustMessage(t, frame.Frame))
		if action == ResyncActionRequestSnapshot {
			req2++
		}
		// Per-frame `expect_action` / `last_epoch_after` were declared on
		// rsInbound and then read by only the first scenario in this file.
		if got := action.String(); got != frame.ExpectAction {
			t.Fatalf("single-gap frame %d action = %q, want %q", i, got, frame.ExpectAction)
		}
		if c2.LastEpoch() != frame.LastEpochAfter {
			t.Fatalf("single-gap frame %d last_epoch = %d, want %d", i, c2.LastEpoch(), frame.LastEpochAfter)
		}
	}
	if req2 != single.Expect.ResyncRequestsEmitted {
		t.Fatalf("single-gap requests = %d, want %d", req2, single.Expect.ResyncRequestsEmitted)
	}
	if c2.LastEpoch() != single.Expect.FinalLastEpoch {
		t.Fatalf("single-gap final last_epoch = %d, want %d", c2.LastEpoch(), single.Expect.FinalLastEpoch)
	}
}

// ---------------------------------------------------------------------------
// idempotent_redelivery.json
// ---------------------------------------------------------------------------

func TestReliableSyncIdempotentRedelivery(t *testing.T) {
	fx := loadReliableSyncFixture(t, "idempotent_redelivery.json")
	for _, name := range []string{"replayed_delta_is_ignored", "duplicate_current_head_is_ignored"} {
		var sc struct {
			rsScenarioHead
			StartLastEpoch Epoch             `json:"start_last_epoch"`
			StateBefore    map[string][]byte `json:"state_before"`
			Inbound        []rsInbound       `json:"inbound"`
			Expect         struct {
				FinalLastEpoch Epoch `json:"final_last_epoch"`
				// The redelivery claim itself: the ignored frame left the graph
				// exactly as it was. The runner checked epochs only, which an
				// implementation that ignored the frame *and* corrupted state
				// would have passed.
				StateAfter         map[string][]byte `json:"state_after"`
				NetEffectUnchanged *bool             `json:"net_effect_unchanged"`
			} `json:"expect"`
		}
		fx.replay(t, name, &sc)
		coord := NewResyncCoordinatorWithEpoch(sc.StartLastEpoch)
		graph := map[NodeId][]byte{}
		for id, payload := range sc.StateBefore {
			graph[NodeId(mustParseNodeId(t, id))] = payload
		}
		before := map[NodeId][]byte{}
		for k, v := range graph {
			before[k] = v
		}
		for _, frame := range sc.Inbound {
			msg := mustMessage(t, frame.Frame)
			action, _ := coord.Ingest(msg)
			if got := action.String(); got != frame.ExpectAction {
				t.Fatalf("%s: action = %q, want %q", name, got, frame.ExpectAction)
			}
			if action == ResyncActionApply {
				rsApplyToGraph(t, graph, msg)
			}
			if coord.LastEpoch() != frame.LastEpochAfter {
				t.Fatalf("%s: last_epoch = %d, want %d", name, coord.LastEpoch(), frame.LastEpochAfter)
			}
		}
		if coord.LastEpoch() != sc.Expect.FinalLastEpoch {
			t.Fatalf("%s: final last_epoch = %d, want %d", name, coord.LastEpoch(), sc.Expect.FinalLastEpoch)
		}
		wantAfter := map[NodeId][]byte{}
		for id, payload := range sc.Expect.StateAfter {
			wantAfter[NodeId(mustParseNodeId(t, id))] = payload
		}
		if !reflect.DeepEqual(graph, wantAfter) {
			t.Fatalf("%s: state_after = %v, want %v", name, graph, wantAfter)
		}
		if want := sc.Expect.NetEffectUnchanged; want != nil {
			if unchanged := reflect.DeepEqual(graph, before); unchanged != *want {
				t.Fatalf("%s: net_effect_unchanged = %v, want %v", name, unchanged, *want)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// outbox_replay_after_crash.json — with a reference file-backed DurableOutbox
// ---------------------------------------------------------------------------

// fileOutbox is a reference disk-backed DurableOutbox exercising crash replay:
// each frame is a JSON `[epoch, wire]` line, retention rewrites the file.
type fileOutbox struct {
	path         string
	ackedThrough Epoch
}

func newFileOutbox(t *testing.T, path string) *fileOutbox {
	t.Helper()
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("create outbox file: %v", err)
		}
	}
	return &fileOutbox{path: path}
}

func (o *fileOutbox) readAll(t *testing.T) []OutboxEntry {
	t.Helper()
	data, err := specReadFile(o.path)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	var out []OutboxEntry
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var pair []json.RawMessage
		if err := json.Unmarshal([]byte(line), &pair); err != nil {
			t.Fatalf("decode outbox line: %v", err)
		}
		var epoch Epoch
		if err := json.Unmarshal(pair[0], &epoch); err != nil {
			t.Fatalf("decode outbox epoch: %v", err)
		}
		msg, err := IpcMessageFromWire(pair[1])
		if err != nil {
			t.Fatalf("decode outbox frame: %v", err)
		}
		out = append(out, OutboxEntry{Epoch: epoch, Msg: msg})
	}
	return out
}

func encodeOutboxLine(t *testing.T, e OutboxEntry) string {
	t.Helper()
	wire, err := e.Msg.EncodeJSON()
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	return "[" + strconv.FormatInt(int64(e.Epoch), 10) + "," + string(wire) + "]\n"
}

func (o *fileOutbox) append(t *testing.T, epoch Epoch, msg IpcMessage) {
	t.Helper()
	f, err := os.OpenFile(o.path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open outbox for append: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteString(encodeOutboxLine(t, OutboxEntry{Epoch: epoch, Msg: msg})); err != nil {
		t.Fatalf("append outbox: %v", err)
	}
}

func (o *fileOutbox) ackThrough(t *testing.T, epoch Epoch) {
	t.Helper()
	if epoch > o.ackedThrough {
		o.ackedThrough = epoch
	}
	var buf strings.Builder
	for _, e := range o.readAll(t) {
		if e.Epoch > o.ackedThrough {
			buf.WriteString(encodeOutboxLine(t, e))
		}
	}
	if err := os.WriteFile(o.path, []byte(buf.String()), 0o644); err != nil {
		t.Fatalf("rewrite outbox: %v", err)
	}
}

func (o *fileOutbox) replayFrom(t *testing.T, cursor Epoch) []OutboxEntry {
	t.Helper()
	var out []OutboxEntry
	for _, e := range o.readAll(t) {
		if e.Epoch > cursor {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Epoch < out[j].Epoch })
	return out
}

func (o *fileOutbox) retainedEpochs(t *testing.T) []Epoch {
	t.Helper()
	var es []Epoch
	for _, e := range o.readAll(t) {
		es = append(es, e.Epoch)
	}
	sort.Slice(es, func(i, j int) bool { return es[i] < es[j] })
	return es
}

type rsAppended struct {
	Epoch Epoch           `json:"epoch"`
	Frame json.RawMessage `json:"frame"`
}

func TestReliableSyncOutboxReplayAfterCrash(t *testing.T) {
	fx := loadReliableSyncFixture(t, "outbox_replay_after_crash.json")

	var sc struct {
		rsScenarioHead
		Appended   []rsAppended `json:"appended"`
		AckThrough Epoch        `json:"ack_through"`
		// `crash` is the scenario's whole premise — the runner reopened the
		// durable outbox from disk unconditionally, so a fixture that said "no
		// crash" would have been replayed as a crash anyway.
		Crash           bool  `json:"crash"`
		ReconnectCursor Epoch `json:"reconnect_cursor"`
		Expect          struct {
			RetainedAfterAck       []Epoch `json:"retained_after_ack"`
			ReplayedFromCursor     []Epoch `json:"replayed_from_cursor"`
			ReplayOrder            []Epoch `json:"replay_order"`
			ReceiverApplies        []Epoch `json:"receiver_applies"`
			ReceiverLastEpochAfter Epoch   `json:"receiver_last_epoch_after"`
			OpsLost                int     `json:"ops_lost"`
			OpsDoubled             int     `json:"ops_doubled"`
			ExactlyOnceEffect      *bool   `json:"exactly_once_effect"`
		} `json:"expect"`
	}
	fx.replay(t, "crash_between_append_and_ack_replays_on_reconnect", &sc)

	path := filepath.Join(t.TempDir(), "outbox.jsonl")
	mem := NewInMemoryOutbox()
	file := newFileOutbox(t, path)
	for _, a := range sc.Appended {
		msg := mustMessage(t, a.Frame)
		mem.Append(a.Epoch, msg)
		file.append(t, a.Epoch, msg)
	}
	mem.AckThrough(sc.AckThrough)
	file.ackThrough(t, sc.AckThrough)

	if !reflect.DeepEqual(mem.RetainedEpochs(), sc.Expect.RetainedAfterAck) {
		t.Fatalf("mem retained = %v, want %v", mem.RetainedEpochs(), sc.Expect.RetainedAfterAck)
	}
	if !reflect.DeepEqual(file.retainedEpochs(t), sc.Expect.RetainedAfterAck) {
		t.Fatalf("file retained = %v, want %v", file.retainedEpochs(t), sc.Expect.RetainedAfterAck)
	}

	// "crash": reopen the durable file outbox from disk. Only when the fixture
	// says a crash happened — otherwise the in-process handle survives and the
	// durability claim is not the one under test.
	if sc.Crash {
		file = newFileOutbox(t, path)
	}
	replay := file.replayFrom(t, sc.ReconnectCursor)
	replayEpochs := make([]Epoch, 0, len(replay))
	for _, e := range replay {
		replayEpochs = append(replayEpochs, e.Epoch)
	}
	if !reflect.DeepEqual(replayEpochs, sc.Expect.ReplayedFromCursor) {
		t.Fatalf("replay = %v, want %v", replayEpochs, sc.Expect.ReplayedFromCursor)
	}
	// `replay_order` is the ordering claim, stated separately from the set: the
	// outbox must re-send in append order, not merely re-send the right frames.
	if !reflect.DeepEqual(replayEpochs, sc.Expect.ReplayOrder) {
		t.Fatalf("replay order = %v, want %v", replayEpochs, sc.Expect.ReplayOrder)
	}

	// Feed the replay to a receiver at the reconnect cursor: applies each once.
	coord := NewResyncCoordinatorWithEpoch(sc.ReconnectCursor)
	var applied []Epoch
	deliveries := map[NodeId]int{}
	for _, e := range replay {
		if action, _ := coord.Ingest(e.Msg); action == ResyncActionApply {
			applied = append(applied, coord.LastEpoch())
			for _, node := range rsDeltaNodes(t, e.Msg) {
				deliveries[node]++
			}
		}
	}
	if !reflect.DeepEqual(applied, sc.Expect.ReceiverApplies) {
		t.Fatalf("applied = %v, want %v", applied, sc.Expect.ReceiverApplies)
	}
	if coord.LastEpoch() != sc.Expect.ReceiverLastEpochAfter {
		t.Fatalf("receiver last_epoch = %d, want %d", coord.LastEpoch(), sc.Expect.ReceiverLastEpochAfter)
	}

	// Exactly-once: every op appended after the reconnect cursor lands exactly
	// once at the receiver. `ops_lost` counts those that never arrived,
	// `ops_doubled` those applied more than once.
	expectedNodes := map[NodeId]bool{}
	for _, a := range sc.Appended {
		if a.Epoch <= sc.ReconnectCursor {
			continue
		}
		for _, node := range rsDeltaNodes(t, mustMessage(t, a.Frame)) {
			expectedNodes[node] = true
		}
	}
	lost, doubled := 0, 0
	for node := range expectedNodes {
		switch n := deliveries[node]; {
		case n == 0:
			lost++
		case n > 1:
			doubled += n - 1
		}
	}
	if lost != sc.Expect.OpsLost {
		t.Fatalf("ops_lost = %d, want %d", lost, sc.Expect.OpsLost)
	}
	if doubled != sc.Expect.OpsDoubled {
		t.Fatalf("ops_doubled = %d, want %d", doubled, sc.Expect.OpsDoubled)
	}
	if want := sc.Expect.ExactlyOnceEffect; want != nil {
		got := lost == 0 && doubled == 0
		if got != *want {
			t.Fatalf("exactly_once_effect = %v (lost=%d doubled=%d), want %v", got, lost, doubled, *want)
		}
	}

	// send_failure_retains_frame_for_next_tick
	var sc2 struct {
		rsScenarioHead
		Appended              []rsAppended `json:"appended"`
		AckThrough            Epoch        `json:"ack_through"`
		SendFailsFirstAttempt bool         `json:"send_fails_first_attempt"`
		Expect                struct {
			FrameRetainedAfterFailedSend *bool   `json:"frame_retained_after_failed_send"`
			Retained                     []Epoch `json:"retained"`
			ResentOnNextTick             []Epoch `json:"resent_on_next_tick"`
			PermanentGap                 *bool   `json:"permanent_gap"`
		} `json:"expect"`
	}
	fx.replay(t, "send_failure_retains_frame_for_next_tick", &sc2)
	if !sc2.SendFailsFirstAttempt {
		t.Fatal("send_fails_first_attempt must be set for this scenario to mean anything")
	}
	mem2 := NewInMemoryOutbox()
	for _, a := range sc2.Appended {
		mem2.Append(a.Epoch, mustMessage(t, a.Frame))
	}
	mem2.AckThrough(sc2.AckThrough)
	if !reflect.DeepEqual(mem2.RetainedEpochs(), sc2.Expect.Retained) {
		t.Fatalf("retained = %v, want %v", mem2.RetainedEpochs(), sc2.Expect.Retained)
	}
	// The send failed, so nothing was acked past the cursor: the frame is still
	// there to resend. That is `frame_retained_after_failed_send`.
	if want := sc2.Expect.FrameRetainedAfterFailedSend; want != nil {
		got := len(mem2.RetainedEpochs()) > 0
		if got != *want {
			t.Fatalf("frame_retained_after_failed_send = %v, want %v", got, *want)
		}
	}
	resent := make([]Epoch, 0, len(sc2.Expect.ResentOnNextTick))
	for _, e := range mem2.ReplayFrom(sc2.Expect.Retained[0] - 1) {
		resent = append(resent, e.Epoch)
	}
	if !reflect.DeepEqual(resent, sc2.Expect.ResentOnNextTick) {
		t.Fatalf("resent_on_next_tick = %v, want %v", resent, sc2.Expect.ResentOnNextTick)
	}
	// `permanent_gap` false means the retained frames cover every epoch from the
	// cursor onward with no hole, so the resend closes the gap for good.
	if want := sc2.Expect.PermanentGap; want != nil {
		gap := false
		for i := 1; i < len(resent); i++ {
			if resent[i] != resent[i-1]+1 {
				gap = true
			}
		}
		if len(resent) == 0 {
			gap = true
		}
		if gap != *want {
			t.Fatalf("permanent_gap = %v (resent %v), want %v", gap, resent, *want)
		}
	}
}

// rsDeltaNodes lists the nodes a delta frame writes, so redelivery can be counted
// per op rather than per frame.
func rsDeltaNodes(t *testing.T, msg IpcMessage) []NodeId {
	t.Helper()
	d, ok := msg.(IpcMessageDelta)
	if !ok {
		return nil
	}
	var out []NodeId
	for _, op := range d.Value.Ops {
		switch o := op.(type) {
		case DeltaOpCellSet:
			out = append(out, o.Node)
		case DeltaOpSlotValue:
			out = append(out, o.Node)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// liveness_orset_lww.json
// ---------------------------------------------------------------------------

type stampJSON struct {
	WallTime int64  `json:"wall_time"`
	Logical  int64  `json:"logical"`
	Peer     PeerId `json:"peer"`
}

func (s stampJSON) wire() WireStamp {
	return WireStamp{WallTime: s.WallTime, Logical: s.Logical, Peer: s.Peer}
}

// parsePid strips a "pid" prefix from the trailing key segment.
func parsePid(t *testing.T, s string) PeerId {
	t.Helper()
	seg := s
	if i := strings.LastIndex(s, "/"); i >= 0 {
		seg = s[i+1:]
	}
	seg = strings.TrimPrefix(seg, "pid")
	n, err := strconv.ParseInt(seg, 10, 64)
	if err != nil {
		t.Fatalf("parse pid from %q: %v", s, err)
	}
	return PeerId(n)
}

// rsRequireRegisterKind consumes a scenario's `register_kind`, pinning the CRDT
// the runner is about to replay it through.
func rsRequireRegisterKind(t *testing.T, scenario, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: register_kind = %q, but this replay drives a %q register", scenario, got, want)
	}
}

// rsLivenessOp is one op of the derived-aggregate scenario's stream. The corpus
// interleaves both register kinds in a single list, so one shape carries the
// union of their fields and `register_kind` picks which half applies.
type rsLivenessOp struct {
	RegisterKind string    `json:"register_kind"`
	Key          string    `json:"key"`
	Op           string    `json:"op"`
	Tag          string    `json:"tag"`
	ObservedTags []string  `json:"observed_tags"`
	Value        bool      `json:"value"`
	Stamp        stampJSON `json:"stamp"`
}

// rsDocOf splits the doc out of a `docA/pid100` presence key.
func rsDocOf(key string) string {
	if i := strings.Index(key, "/"); i >= 0 {
		return key[:i]
	}
	return key
}

// rsLivenessReplica is the derived per-doc live aggregate: an OR-set per
// `doc/pid` presence key, an LWW register per peer's `alive/pid`, and the join
// of the two. The library ships neither the composition nor a name for it,
// because which docs are live is the application's derivation over the two wire
// CRDTs rather than anything the wire itself carries.
type rsLivenessReplica struct {
	open  map[string]*OrSet
	alive map[PeerId]*WireLwwRegister[bool]
}

func newRsLivenessReplica() *rsLivenessReplica {
	return &rsLivenessReplica{open: map[string]*OrSet{}, alive: map[PeerId]*WireLwwRegister[bool]{}}
}

// apply folds one op in and reports whether it CHANGED the replica. That is the
// definition `redeliver_applied_count` needs: an op is "applied" when it moved
// the state, so a redelivered op that re-derives the value it already had
// counts as zero, and one that double-inserted would not.
func (r *rsLivenessReplica) apply(t *testing.T, op rsLivenessOp) bool {
	t.Helper()
	before := r.snapshot()
	switch op.RegisterKind {
	case "orset":
		set, ok := r.open[op.Key]
		if !ok {
			set = NewOrSet()
			r.open[op.Key] = set
		}
		switch op.Op {
		case "add":
			set.Add(op.Tag)
		case "remove":
			set.RemoveObserved(op.ObservedTags)
		default:
			t.Fatalf("unknown orset op %q on %s", op.Op, op.Key)
		}
	case "lww":
		pid := parsePid(t, op.Key)
		reg, ok := r.alive[pid]
		if !ok {
			// An unheard-of peer is not yet alive, at the bottom stamp, so the
			// first liveness write it receives always dominates.
			reg = NewWireLwwRegister(WireStamp{}, false)
			r.alive[pid] = reg
		}
		reg.Set(op.Stamp.wire(), op.Value)
	default:
		t.Fatalf("unknown register_kind %q on %s", op.RegisterKind, op.Key)
	}
	return r.snapshot() != before
}

// liveDocs is the derived aggregate: a doc is live iff some presence tag for it
// survives AND the peer holding it is alive.
func (r *rsLivenessReplica) liveDocs(t *testing.T) []string {
	t.Helper()
	docs := map[string]struct{}{}
	for key, set := range r.open {
		if !set.Present() {
			continue
		}
		if reg, ok := r.alive[parsePid(t, key)]; ok && reg.Value() {
			docs[rsDocOf(key)] = struct{}{}
		}
	}
	out := make([]string, 0, len(docs))
	for doc := range docs {
		out = append(out, doc)
	}
	sort.Strings(out)
	return out
}

// snapshot is the replica's full observable state, rendered deterministically.
//
// It reaches into the OR-set's tag sets on purpose: `Present()` alone collapses
// two distinct add-tags into one bit, so an op that grew the tag set without
// flipping presence would read as "applied nothing". Convergence and idempotence
// are claims about the STATE, not about the projection of it.
func (r *rsLivenessReplica) snapshot() string {
	var b strings.Builder
	keys := make([]string, 0, len(r.open))
	for key := range r.open {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		set := r.open[key]
		b.WriteString("open " + key + " add=" + strings.Join(sortedTagSet(set.adds), ",") +
			" rm=" + strings.Join(sortedTagSet(set.removes), ",") + ";")
	}
	pids := make([]int, 0, len(r.alive))
	for pid := range r.alive {
		pids = append(pids, int(pid))
	}
	sort.Ints(pids)
	for _, pid := range pids {
		reg := r.alive[PeerId(pid)]
		stamp := reg.Stamp()
		b.WriteString("alive " + strconv.Itoa(pid) + "=" + strconv.FormatBool(reg.Value()) +
			"@" + strconv.FormatInt(stamp.WallTime, 10) + "." + strconv.FormatInt(stamp.Logical, 10) +
			"." + strconv.FormatInt(int64(stamp.Peer), 10) + ";")
	}
	return b.String()
}

func sortedTagSet(tags map[string]struct{}) []string {
	out := make([]string, 0, len(tags))
	for tag := range tags {
		out = append(out, tag)
	}
	sort.Strings(out)
	return out
}

func TestReliableSyncLivenessOrSetLww(t *testing.T) {
	fx := loadReliableSyncFixture(t, "liveness_orset_lww.json")

	// open_set_add_wins_over_stale_remove
	var add struct {
		rsScenarioHead
		// `register_kind` picks the CRDT the ops replay against, and `key` names
		// the doc/peer the register belongs to. Both were unread, so an `lww`
		// scenario would have been replayed through an OR-set without complaint.
		RegisterKind string `json:"register_kind"`
		Key          string `json:"key"`
		Ops          []struct {
			Op           string    `json:"op"`
			Tag          string    `json:"tag"`
			ObservedTags []string  `json:"observed_tags"`
			Stamp        stampJSON `json:"stamp"`
		} `json:"ops"`
		Expect struct {
			Present               bool   `json:"present"`
			Reason                string `json:"reason"`
			OrderIndependent      *bool  `json:"order_independent"`
			RedeliverAppliedCount *int   `json:"redeliver_applied_count"`
		} `json:"expect"`
	}
	fx.replay(t, "open_set_add_wins_over_stale_remove", &add)
	rsRequireRegisterKind(t, "open_set_add_wins_over_stale_remove", add.RegisterKind, "orset")
	parsePid(t, add.Key) // the key must name a real doc/peer pair
	replayOrSet := func(ops []int) *OrSet {
		set := NewOrSet()
		for _, i := range ops {
			op := add.Ops[i]
			switch op.Op {
			case "add":
				set.Add(op.Tag)
			case "remove":
				set.RemoveObserved(op.ObservedTags)
			default:
				t.Fatalf("unknown orset op %q", op.Op)
			}
		}
		return set
	}
	forward := make([]int, len(add.Ops))
	for i := range forward {
		forward[i] = i
	}
	set := replayOrSet(forward)
	if set.Present() != add.Expect.Present {
		t.Fatalf("present = %v, want %v", set.Present(), add.Expect.Present)
	}
	if add.Expect.Reason == "" {
		t.Fatal("open_set_add_wins_over_stale_remove: expect.reason must say why the doc stays open")
	}
	if want := add.Expect.OrderIndependent; want != nil {
		reversed := make([]int, len(forward))
		for i := range forward {
			reversed[len(forward)-1-i] = forward[i]
		}
		got := replayOrSet(reversed).Present() == set.Present()
		if got != *want {
			t.Fatalf("order_independent = %v, want %v", got, *want)
		}
	}
	if want := add.Expect.RedeliverAppliedCount; want != nil {
		before := set.Present()
		redelivered := replayOrSet(append(append([]int{}, forward...), forward...))
		changed := 0
		if redelivered.Present() != before {
			changed = 1
		}
		if changed != *want {
			t.Fatalf("redeliver_applied_count = %d, want %d", changed, *want)
		}
	}

	// lww_alive_highest_stamp_wins
	var lww struct {
		rsScenarioHead
		RegisterKind string `json:"register_kind"`
		Key          string `json:"key"`
		Ops          []struct {
			Value bool      `json:"value"`
			Stamp stampJSON `json:"stamp"`
		} `json:"ops"`
		Expect struct {
			Value            bool   `json:"value"`
			Resolution       string `json:"resolution"`
			OrderIndependent *bool  `json:"order_independent"`
		} `json:"expect"`
	}
	fx.replay(t, "lww_alive_highest_stamp_wins", &lww)
	rsRequireRegisterKind(t, "lww_alive_highest_stamp_wins", lww.RegisterKind, "lww")
	parsePid(t, lww.Key)
	replayLww := func(order []int) bool {
		reg := NewWireLwwRegister(lww.Ops[order[0]].Stamp.wire(), lww.Ops[order[0]].Value)
		for _, i := range order[1:] {
			reg.Set(lww.Ops[i].Stamp.wire(), lww.Ops[i].Value)
		}
		return reg.Value()
	}
	order := make([]int, len(lww.Ops))
	for i := range order {
		order[i] = i
	}
	got := replayLww(order)
	if got != lww.Expect.Value {
		t.Fatalf("lww value = %v, want %v", got, lww.Expect.Value)
	}
	// `resolution` names the rule; check the winner really is the max-stamp op.
	if lww.Expect.Resolution != "max_stamp" {
		t.Fatalf("unknown lww resolution %q", lww.Expect.Resolution)
	}
	best := 0
	for i := range lww.Ops {
		if lww.Ops[best].Stamp.wire().Greater(lww.Ops[i].Stamp.wire()) {
			continue
		}
		best = i
	}
	if got != lww.Ops[best].Value {
		t.Fatalf("resolution=max_stamp: value = %v, but the max-stamp op carries %v", got, lww.Ops[best].Value)
	}
	if want := lww.Expect.OrderIndependent; want != nil {
		reversed := make([]int, len(order))
		for i := range order {
			reversed[len(order)-1-i] = order[i]
		}
		if indep := replayLww(reversed) == got; indep != *want {
			t.Fatalf("lww order_independent = %v, want %v", indep, *want)
		}
	}

	// whole_editor_death_cascades
	var death struct {
		rsScenarioHead
		OpenSet []struct {
			Key     string `json:"key"`
			Present bool   `json:"present"`
		} `json:"open_set"`
		AliveBefore map[string]bool `json:"alive_before"`
		Op          struct {
			RegisterKind string    `json:"register_kind"`
			Key          string    `json:"key"`
			Value        bool      `json:"value"`
			Stamp        stampJSON `json:"stamp"`
		} `json:"op"`
		Expect struct {
			LiveDocsBefore []string `json:"live_docs_before"`
			LiveDocsAfter  []string `json:"live_docs_after"`
			Cascade        *bool    `json:"cascade"`
			Note           string   `json:"note"`
		} `json:"expect"`
	}
	fx.replay(t, "whole_editor_death_cascades", &death)
	type openEntry struct {
		doc string
		pid PeerId
	}
	var open []openEntry
	for _, e := range death.OpenSet {
		if !e.Present {
			continue
		}
		parts := strings.SplitN(e.Key, "/", 2)
		open = append(open, openEntry{doc: parts[0], pid: parsePid(t, e.Key)})
	}
	alive := map[PeerId]*WireLwwRegister[bool]{}
	for pidStr, v := range death.AliveBefore {
		pid, err := strconv.ParseInt(pidStr, 10, 64)
		if err != nil {
			t.Fatalf("parse alive pid %q: %v", pidStr, err)
		}
		alive[PeerId(pid)] = NewWireLwwRegister(WireStamp{WallTime: 1, Logical: 0, Peer: 1}, v)
	}
	rsRequireRegisterKind(t, "whole_editor_death_cascades", death.Op.RegisterKind, "lww")

	liveDocs := func() []string {
		set := map[string]struct{}{}
		for _, e := range open {
			if reg, ok := alive[e.pid]; ok && reg.Value() {
				set[e.doc] = struct{}{}
			}
		}
		out := make([]string, 0, len(set))
		for doc := range set {
			out = append(out, doc)
		}
		sort.Strings(out)
		return out
	}

	before := liveDocs()
	wantBefore := append([]string(nil), death.Expect.LiveDocsBefore...)
	sort.Strings(wantBefore)
	if !reflect.DeepEqual(before, wantBefore) {
		t.Fatalf("live_docs_before = %v, want %v", before, wantBefore)
	}

	deadPid := parsePid(t, death.Op.Key)
	alive[deadPid].Set(death.Op.Stamp.wire(), death.Op.Value)

	live := liveDocs()
	want := append([]string(nil), death.Expect.LiveDocsAfter...)
	sort.Strings(want)
	if !reflect.DeepEqual(live, want) {
		t.Fatalf("live docs = %v, want %v", live, want)
	}

	// `cascade`: one liveness write moved more than one doc. That is the claim
	// the scenario is named for, and it is the difference between this and a
	// per-doc close.
	if wantCascade := death.Expect.Cascade; wantCascade != nil {
		dropped := 0
		after := map[string]bool{}
		for _, doc := range live {
			after[doc] = true
		}
		for _, doc := range before {
			if !after[doc] {
				dropped++
			}
		}
		if cascaded := dropped > 1; cascaded != *wantCascade {
			t.Fatalf("cascade = %v (%d docs dropped on one write), want %v",
				cascaded, dropped, *wantCascade)
		}
	}
	if death.Expect.Note == "" {
		t.Fatal("whole_editor_death_cascades: expect.note must say which docs drop and why")
	}

	// derived_live_doc_aggregate_converges_under_retry
	//
	// The fourth scenario in this fixture, and the one this binding never
	// replayed (#lzscenariocoverage). It is not a property of OrSet or of
	// WireLwwRegister on its own — it is the property of the DERIVED aggregate
	// the two compose into: a doc is live iff some presence tag for it survives
	// AND the peer holding it is still alive. Each half is a semilattice, so the
	// join is one too, and the scenario states the three consequences: the
	// aggregate is order-independent, re-delivering a seen op applies nothing,
	// and one doc's liveness does not leak into another's.
	//
	// The library ships no type for that join, because it is the application's
	// derivation rather than the wire's. That is why replaying this scenario
	// means building the aggregate here — and why skipping it was easy.
	var derived struct {
		rsScenarioHead
		// The scenario's two replicas exist to be fed the SAME ops in
		// different orders, which is what `reverse_order_equivalent` asks for.
		Replicas               []string       `json:"replicas"`
		Ops                    []rsLivenessOp `json:"ops"`
		ReverseOrderEquivalent bool           `json:"reverse_order_equivalent"`
		Redeliver              bool           `json:"redeliver"`
		Expect                 struct {
			ConvergedLiveDocs     []string `json:"converged_live_docs"`
			OrderIndependent      *bool    `json:"order_independent"`
			RedeliverAppliedCount *int     `json:"redeliver_applied_count"`
			PerDocIsolation       *bool    `json:"per_doc_isolation"`
		} `json:"expect"`
	}
	fx.replay(t, "derived_live_doc_aggregate_converges_under_retry", &derived)
	if len(derived.Replicas) != 2 {
		t.Fatalf("derived_live_doc_aggregate: %d replicas, this replay drives exactly 2 (forward and reverse)",
			len(derived.Replicas))
	}
	if !derived.ReverseOrderEquivalent {
		t.Fatal("derived_live_doc_aggregate: reverse_order_equivalent=false, but the scenario's whole claim is that the two orders agree")
	}

	replayLiveness := func(order []int, skipDoc string) (*rsLivenessReplica, int) {
		replica := newRsLivenessReplica()
		applied := 0
		for _, i := range order {
			op := derived.Ops[i]
			if skipDoc != "" && op.RegisterKind == "orset" && rsDocOf(op.Key) == skipDoc {
				continue
			}
			if replica.apply(t, op) {
				applied++
			}
		}
		return replica, applied
	}

	forwardOrder := make([]int, len(derived.Ops))
	for i := range forwardOrder {
		forwardOrder[i] = i
	}
	reverseOrder := make([]int, len(forwardOrder))
	for i, v := range forwardOrder {
		reverseOrder[len(forwardOrder)-1-i] = v
	}

	r1, _ := replayLiveness(forwardOrder, "")
	r2, _ := replayLiveness(reverseOrder, "")

	wantLive := append([]string(nil), derived.Expect.ConvergedLiveDocs...)
	sort.Strings(wantLive)
	for i, replica := range []*rsLivenessReplica{r1, r2} {
		if got := replica.liveDocs(t); !reflect.DeepEqual(got, wantLive) {
			t.Fatalf("replica %s: converged_live_docs = %v, want %v",
				derived.Replicas[i], got, wantLive)
		}
	}

	// `order_independent` is the semilattice claim itself: the two replicas saw
	// the same ops in opposite orders and must be indistinguishable, not merely
	// each equal to a literal the fixture also carries.
	if want := derived.Expect.OrderIndependent; want != nil {
		if indep := r1.snapshot() == r2.snapshot(); indep != *want {
			t.Fatalf("order_independent = %v, want %v\n  forward %s\n  reverse %s",
				indep, *want, r1.snapshot(), r2.snapshot())
		}
	}

	// Re-delivery: feeding the whole stream again must apply nothing new. This
	// is idempotence measured on the replica's own state, so an implementation
	// that "ignored" a redelivered op by silently rebuilding the same value
	// still counts as unapplied, and one that double-inserted would not.
	if want := derived.Expect.RedeliverAppliedCount; want != nil {
		if !derived.Redeliver {
			t.Fatal("derived_live_doc_aggregate: redeliver_applied_count asserted with redeliver=false")
		}
		reapplied := 0
		for _, i := range forwardOrder {
			if r1.apply(t, derived.Ops[i]) {
				reapplied++
			}
		}
		if reapplied != *want {
			t.Fatalf("redeliver_applied_count = %d, want %d", reapplied, *want)
		}
		if got := r1.liveDocs(t); !reflect.DeepEqual(got, wantLive) {
			t.Fatalf("after redelivery live docs = %v, want %v", got, wantLive)
		}
	}

	// `per_doc_isolation`: the aggregate is per-doc, so dropping one doc's
	// presence ops removes exactly that doc and leaves the rest live. Asserting
	// only the converged list cannot tell a per-doc aggregate from one global
	// flag that happens to be on.
	if want := derived.Expect.PerDocIsolation; want != nil {
		isolated := true
		for _, doc := range wantLive {
			withoutDoc := make([]string, 0, len(wantLive))
			for _, other := range wantLive {
				if other != doc {
					withoutDoc = append(withoutDoc, other)
				}
			}
			partial, _ := replayLiveness(forwardOrder, doc)
			if !reflect.DeepEqual(partial.liveDocs(t), withoutDoc) {
				isolated = false
				break
			}
		}
		if isolated != *want {
			t.Fatalf("per_doc_isolation = %v, want %v", isolated, *want)
		}
	}
}

// ---------------------------------------------------------------------------
// SyncDriver (#sync-driver) loop-shape unit tests over a scripted seam
// ---------------------------------------------------------------------------
//
// A SimWorld-style deterministic transport pair mirroring lazily-rs / lazily-js:
// the sink records what the driver sends (and can be toggled "down" to model a
// disconnect); the source replays a scripted inbound stream (and can inject one
// read error). No goroutines, no real socket — every tick is a pure step. The
// seam carries no wire form of its own, so it has no conformance fixture; these
// unit tests pin the loop shape the spec § SyncDriver requires.

var errSinkDown = errors.New("scripted sink down")
var errSourceRead = errors.New("scripted source read failure")

type testWire struct {
	sent      []IpcMessage
	inbound   []IpcMessage
	up        bool
	sourceErr bool
}

type testSink struct{ w *testWire }

func (s testSink) Send(m IpcMessage) error {
	if !s.w.up {
		return errSinkDown
	}
	s.w.sent = append(s.w.sent, m)
	return nil
}

type testSource struct{ w *testWire }

func (s testSource) Recv() (IpcMessage, bool, error) {
	if s.w.sourceErr {
		s.w.sourceErr = false
		return nil, false, errSourceRead
	}
	if len(s.w.inbound) == 0 {
		return nil, false, nil
	}
	m := s.w.inbound[0]
	s.w.inbound = s.w.inbound[1:]
	return m, true, nil
}

type zeroClock struct{}

func (zeroClock) NowMillis() int64 { return 0 }

// snapAhead answers a ResyncRequest{from} with a snapshot at from + 5.
type snapAhead struct{}

func (snapAhead) Snapshot(from Epoch) IpcMessage {
	return IpcMessageSnapshot{Value: Snapshot{Epoch: from + 5}}
}

func driverAt(w *testWire, lastEpoch Epoch) *SyncDriver {
	return NewSyncDriverWithEpoch(testSink{w}, testSource{w}, NewInMemoryOutbox(), zeroClock{}, snapAhead{}, lastEpoch)
}

func dframe(base, epoch Epoch) IpcMessage {
	return IpcMessageDelta{Value: Delta{BaseEpoch: base, Epoch: epoch}}
}

func mustTick(t *testing.T, d *SyncDriver) Progress {
	t.Helper()
	p, err := d.Tick()
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	return p
}

func TestSyncDriverDrainsAppendBeforeSendAndRetainsUntilAcked(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 0)
	d.Enqueue(1, dframe(0, 1))
	d.Enqueue(2, dframe(1, 2))
	p := mustTick(t, d)
	if p.Sent != 2 {
		t.Fatalf("sent = %d, want 2", p.Sent)
	}
	if len(w.sent) != 2 {
		t.Fatalf("wire sent = %d, want 2", len(w.sent))
	}
	if p.Retained != 2 {
		t.Fatalf("retained = %d, want 2", p.Retained)
	}
	if d.IsStalled() {
		t.Fatalf("driver should not be stalled")
	}

	w.inbound = append(w.inbound, IpcMessageOutboxAck{Value: OutboxAck{ThroughEpoch: 2}})
	p = mustTick(t, d)
	if p.PeerAckedThrough != 2 {
		t.Fatalf("peer_acked_through = %d, want 2", p.PeerAckedThrough)
	}
	if p.Retained != 0 {
		t.Fatalf("retained = %d, want 0 (acked pruned)", p.Retained)
	}
}

func TestSyncDriverRetainsOnSendFailureAndReplaysOnReconnect(t *testing.T) {
	w := &testWire{up: false} // sink down before the first send
	d := driverAt(w, 0)
	d.Enqueue(1, dframe(0, 1))
	p := mustTick(t, d)
	if p.Sent != 0 {
		t.Fatalf("sent = %d, want 0", p.Sent)
	}
	if !d.IsStalled() {
		t.Fatalf("a failed send should stall the driver")
	}
	if p.Retained != 1 {
		t.Fatalf("retained = %d, want 1", p.Retained)
	}
	if len(w.sent) != 0 {
		t.Fatalf("nothing should have reached the sink")
	}
	if got := d.StalledFor(250); got != 250 {
		t.Fatalf("stalled_for = %d, want 250", got)
	}

	w.up = true
	d.OnReconnect()
	p = mustTick(t, d)
	if d.IsStalled() {
		t.Fatalf("reconnect should clear the stall")
	}
	if p.Sent != 1 {
		t.Fatalf("sent = %d, want 1 (retained frame replayed)", p.Sent)
	}
	found := false
	for _, m := range w.sent {
		if dm, ok := m.(IpcMessageDelta); ok && dm.Value.Epoch == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("the replayed delta should reach the sink")
	}
}

func TestSyncDriverAppliesDeltaAndAdvertisesReceiverCursor(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 0)
	w.inbound = append(w.inbound, dframe(0, 1))
	p := mustTick(t, d)
	if len(p.Applied) != 1 {
		t.Fatalf("applied = %d, want 1", len(p.Applied))
	}
	if d.LastEpoch() != 1 {
		t.Fatalf("last_epoch = %d, want 1", d.LastEpoch())
	}
	found := false
	for _, m := range w.sent {
		if am, ok := m.(IpcMessageOutboxAck); ok && am.Value.ThroughEpoch == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("an OutboxAck advertising the new cursor should be sent")
	}
}

func TestSyncDriverRedeliveryIsIdempotentNoOp(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 0)
	w.inbound = append(w.inbound, dframe(0, 1))
	if p := mustTick(t, d); len(p.Applied) != 1 {
		t.Fatalf("applied = %d, want 1", len(p.Applied))
	}
	w.inbound = append(w.inbound, dframe(0, 1))
	p := mustTick(t, d)
	if len(p.Applied) != 0 {
		t.Fatalf("re-delivery applied = %d, want 0", len(p.Applied))
	}
	if d.LastEpoch() != 1 {
		t.Fatalf("last_epoch = %d, want 1 (no double advance)", d.LastEpoch())
	}
}

func TestSyncDriverRequestsSnapshotOnInboundGap(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 2)
	w.inbound = append(w.inbound, dframe(3, 4)) // base 3 > last 2 → gap
	p := mustTick(t, d)
	if !p.ResyncRequested {
		t.Fatalf("resync_requested should be true")
	}
	if len(p.Applied) != 0 {
		t.Fatalf("gapped delta should not be applied")
	}
	found := false
	for _, m := range w.sent {
		if rm, ok := m.(IpcMessageResyncRequest); ok && rm.Value.FromEpoch == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("a ResyncRequest at the current cursor should be emitted")
	}
}

func TestSyncDriverAnswersResyncRequestWithProviderSnapshot(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 0)
	w.inbound = append(w.inbound, IpcMessageResyncRequest{Value: ResyncRequest{FromEpoch: 2}})
	p := mustTick(t, d)
	if p.SnapshotsServed != 1 {
		t.Fatalf("snapshots_served = %d, want 1", p.SnapshotsServed)
	}
	found := false
	for _, m := range w.sent {
		if sm, ok := m.(IpcMessageSnapshot); ok && sm.Value.Epoch == 7 {
			found = true
		}
	}
	if !found {
		t.Fatalf("a covering snapshot (from + 5) should be sent")
	}
}

func TestSyncDriverSurfacesSourceReadError(t *testing.T) {
	w := &testWire{up: true, sourceErr: true}
	d := driverAt(w, 0)
	_, err := d.Tick()
	var de *DriverError
	if !errors.As(err, &de) {
		t.Fatalf("err = %v, want *DriverError", err)
	}
	if !errors.Is(de.Source, errSourceRead) {
		t.Fatalf("DriverError.Source = %v, want scripted source read failure", de.Source)
	}
}

func TestSyncDriverGapThenSnapshotConverges(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 2)
	w.inbound = append(w.inbound, dframe(4, 5)) // gap
	mustTick(t, d)
	if d.LastEpoch() != 2 {
		t.Fatalf("last_epoch = %d, want 2 (stuck at pre-gap cursor)", d.LastEpoch())
	}
	w.inbound = append(w.inbound, IpcMessageSnapshot{Value: Snapshot{Epoch: 5}})
	p := mustTick(t, d)
	if len(p.Applied) != 1 {
		t.Fatalf("applied = %d, want 1", len(p.Applied))
	}
	if d.LastEpoch() != 5 {
		t.Fatalf("last_epoch = %d, want 5 (snapshot restored convergence)", d.LastEpoch())
	}
}
