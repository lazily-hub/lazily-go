package lazily

// Conformance replay of the causal-receipts and distributed CRDT-plane fixtures
// from lazily-spec (conformance/receipts + conformance/distributed).
//
// Mirrors the Dart replay reference lazily-dart/test/distributed_conformance_test.dart:
//   - Receipts: fold each receipt into a ReceiptProjection at the fixture's
//     current generation, then assert receipt_count, current_generation, the
//     per-causation terminal outcome, stale_receipt_ids, and non-terminal enum
//     classification.
//   - Distributed anti-entropy: ingest each scenario's CrdtOps, assert
//     applied_count, idempotent redelivery, order-independence, the converged
//     winners/values, plus membership and per-peer frontier.
//   - CrdtSync frames: decode the externally-tagged {"CrdtSync": ...} wire form,
//     assert the frame shape, and round-trip the JSON.
//
// Fixtures are resolved via a relative-path helper; a test skips (never fails)
// when the spec checkout is absent.

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixture resolution
// ---------------------------------------------------------------------------

// loadConformanceFixture returns the raw bytes of a fixture under
// lazily-spec/conformance, or t.Skip when the spec checkout is absent.
func loadConformanceFixture(t *testing.T, segments ...string) []byte {
	t.Helper()
	rel := filepath.Join(segments...)

	candidates := []string{
		filepath.Join("..", "lazily-spec", "conformance", rel),
		filepath.Join("test", "conformance", rel),
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		dir := filepath.Dir(file)
		candidates = append(candidates,
			filepath.Join(dir, "..", "lazily-spec", "conformance", rel),
		)
	}
	for _, path := range candidates {
		if b, err := specReadFile(path); err == nil {
			return b
		}
	}
	t.Skipf("conformance fixture not found: %s", rel)
	return nil
}

// normalizeJSON (parse into a canonical interface{} tree) is shared with
// ipc_conformance_test.go within this package.

// ---------------------------------------------------------------------------
// Causal receipts
// ---------------------------------------------------------------------------

func TestReceiptsConformance(t *testing.T) {
	raw := loadConformanceFixture(t, "receipts", "causal_receipts.json")

	var fixture struct {
		conformanceMeta
		ProtocolVersion int `json:"protocol_version"`
		Assertions      struct {
			ReceiptCount        int      `json:"receipt_count"`
			CurrentGeneration   int64    `json:"current_generation"`
			CausationId         string   `json:"causation_id"`
			TerminalOutcome     string   `json:"terminal_outcome"`
			StaleReceiptIds     []string `json:"stale_receipt_ids"`
			NonterminalOutcomes []string `json:"nonterminal_outcomes"`
		} `json:"assertions"`
		Wire struct {
			CausalReceipts json.RawMessage `json:"CausalReceipts"`
		} `json:"wire"`
	}
	mustStrictJSON(t, "receipts/causal_receipts.json", raw, &fixture)

	receipts, err := CausalReceiptsFromWire(fixture.Wire.CausalReceipts)
	if err != nil {
		t.Fatalf("CausalReceiptsFromWire: %v", err)
	}
	if got := len(receipts.Receipts); got != fixture.Assertions.ReceiptCount {
		t.Fatalf("receipt_count = %d, want %d", got, fixture.Assertions.ReceiptCount)
	}

	gen := fixture.Assertions.CurrentGeneration
	projection := NewReceiptProjection()
	for _, receipt := range receipts.Receipts {
		projection.Observe(&gen, receipt)
	}

	if got := projection.CurrentGeneration(); got != gen {
		t.Fatalf("current_generation = %d, want %d", got, gen)
	}

	// Per-causation terminal outcome.
	terminal, ok := projection.TerminalFor(fixture.Assertions.CausationId)
	if !ok {
		t.Fatalf("no terminal receipt for causation %q", fixture.Assertions.CausationId)
	}
	if got := terminal.Outcome.Wire(); got != fixture.Assertions.TerminalOutcome {
		t.Fatalf("terminal_outcome = %q, want %q", got, fixture.Assertions.TerminalOutcome)
	}
	if !terminal.IsTerminal() {
		t.Fatalf("terminal receipt outcome %q reports non-terminal", terminal.Outcome)
	}

	// Stale receipt ids: known to the projection and reported in StaleReceiptIds.
	staleSet := map[string]struct{}{}
	for _, id := range projection.StaleReceiptIds() {
		staleSet[id] = struct{}{}
	}
	if got, want := len(staleSet), len(fixture.Assertions.StaleReceiptIds); got != want {
		t.Fatalf("stale id count = %d, want %d (%v)", got, want, projection.StaleReceiptIds())
	}
	for _, id := range fixture.Assertions.StaleReceiptIds {
		if !projection.ContainsReceipt(id) {
			t.Fatalf("stale receipt %q not known to projection", id)
		}
		if _, ok := staleSet[id]; !ok {
			t.Fatalf("stale receipt %q missing from StaleReceiptIds()", id)
		}
	}

	// A stale receipt must not have leaked into the recorded latest/terminal
	// views (it carried a terminal `rejected` outcome at a stale generation).
	for _, id := range fixture.Assertions.StaleReceiptIds {
		if latest, ok := projection.LatestFor(fixture.Assertions.CausationId); ok && latest.ReceiptId == id {
			t.Fatalf("stale receipt %q became the recorded latest", id)
		}
	}

	// Non-terminal outcome classification.
	for _, wire := range fixture.Assertions.NonterminalOutcomes {
		outcome, err := ReceiptOutcomeFromWire(wire)
		if err != nil {
			t.Fatalf("ReceiptOutcomeFromWire(%q): %v", wire, err)
		}
		if outcome.IsTerminal() {
			t.Fatalf("outcome %q classified terminal, want non-terminal", wire)
		}
	}
}

// ---------------------------------------------------------------------------
// Distributed anti-entropy
// ---------------------------------------------------------------------------

type antiEntropyScenario struct {
	conformanceDoc
	Id                     string   `json:"id"`
	Name                   string   `json:"name"`
	Ops                    []CrdtOp `json:"ops"`
	Redeliver              bool     `json:"redeliver"`
	ReverseOrderEquivalent bool     `json:"reverse_order_equivalent"`
	Expect                 struct {
		AppliedCount          int               `json:"applied_count"`
		RedeliverAppliedCount int               `json:"redeliver_applied_count"`
		Converged             []convergedExpect `json:"converged"`
		// `resolution` names the conflict rule the winners must follow and
		// `order_independent` states the convergence claim outright. Both fell
		// through unread: the runner compared winners to a literal list, which
		// is true of any rule that happens to produce that list, and ran the
		// reverse-order replay without ever reporting it as the property the
		// scenario asserts.
		Resolution       string `json:"resolution"`
		OrderIndependent *bool  `json:"order_independent"`
	} `json:"expect"`
}

type convergedExpect struct {
	Node  NodeId          `json:"node"`
	Key   *string         `json:"key"`
	State json.RawMessage `json:"state"`
}

func TestDistributedAntiEntropyConformance(t *testing.T) {
	raw := loadConformanceFixture(t, "distributed", "anti_entropy_converge.json")

	var fixture struct {
		conformanceMeta
		ProtocolVersion int                   `json:"protocol_version"`
		Scenarios       []antiEntropyScenario `json:"scenarios"`
	}
	mustStrictJSON(t, "distributed/anti_entropy_converge.json", raw, &fixture)
	if len(fixture.Scenarios) == 0 {
		t.Fatal("anti_entropy fixture has no scenarios")
	}

	for _, sv := range typedScenarioViews("distributed/anti_entropy_converge.json", fixture.Scenarios,
		func(s antiEntropyScenario) (string, string) { return s.Id, s.Name }) {
		t.Run(sv.Label(), func(t *testing.T) {
			// Rung 4 books HERE (#lzscenariobodyskip), on the payload handoff
			// inside the subtest — never at the loop header, which cannot tell a
			// body that replayed from one that returned early.
			scenario := sv.Value()
			runtime := NewCrdtPlaneRuntime(1)
			defer runtime.Close()

			if got := runtime.IngestOps(scenario.Ops); got != scenario.Expect.AppliedCount {
				t.Fatalf("applied_count = %d, want %d", got, scenario.Expect.AppliedCount)
			}

			if scenario.Redeliver {
				if got := runtime.IngestOps(scenario.Ops); got != scenario.Expect.RedeliverAppliedCount {
					t.Fatalf("redeliver_applied_count = %d, want %d", got, scenario.Expect.RedeliverAppliedCount)
				}
			}

			orderIndependent := false
			if scenario.ReverseOrderEquivalent {
				reversed := make([]CrdtOp, len(scenario.Ops))
				for i, op := range scenario.Ops {
					reversed[len(scenario.Ops)-1-i] = op
				}
				other := NewCrdtPlaneRuntime(1)
				defer other.Close()
				other.IngestOps(reversed)
				orderIndependent = convergedEqual(t, other.Converged(), runtime.Converged())
				if !orderIndependent {
					t.Fatalf("order-dependent convergence: reverse order diverged")
				}
			}
			if want := scenario.Expect.OrderIndependent; want != nil && orderIndependent != *want {
				t.Fatalf("order_independent = %v, want %v (reverse_order_equivalent=%v)",
					orderIndependent, *want, scenario.ReverseOrderEquivalent)
			}
			assertCrdtResolution(t, scenario.Expect.Resolution, scenario.Ops, runtime.Converged())

			// Converged winners + values.
			actual := runtime.Converged()
			if len(actual) != len(scenario.Expect.Converged) {
				t.Fatalf("converged length = %d, want %d", len(actual), len(scenario.Expect.Converged))
			}
			for i, exp := range scenario.Expect.Converged {
				got := actual[i]
				if got.Node != exp.Node {
					t.Fatalf("converged[%d].node = %d, want %d", i, got.Node, exp.Node)
				}
				if exp.Key != nil {
					if got.Key == nil || *got.Key != *exp.Key {
						t.Fatalf("converged[%d].key = %v, want %q", i, got.Key, *exp.Key)
					}
				}
				gotState, err := json.Marshal(got.State)
				if err != nil {
					t.Fatalf("marshal converged[%d].state: %v", i, err)
				}
				if !reflect.DeepEqual(normalizeJSON(t, gotState), normalizeJSON(t, exp.State)) {
					t.Fatalf("converged[%d].state = %s, want %s", i, gotState, exp.State)
				}

				// WinningOp / Value accessors agree with the converged view.
				op, ok := runtime.WinningOp(exp.Node)
				if !ok {
					t.Fatalf("WinningOp(%d) missing", exp.Node)
				}
				winState, _ := json.Marshal(op.State)
				if !reflect.DeepEqual(normalizeJSON(t, winState), normalizeJSON(t, exp.State)) {
					t.Fatalf("WinningOp(%d).state = %s, want %s", exp.Node, winState, exp.State)
				}
				val, ok := runtime.Value(exp.Node)
				if !ok {
					t.Fatalf("Value(%d) missing", exp.Node)
				}
				valState, _ := json.Marshal(val)
				if !reflect.DeepEqual(normalizeJSON(t, valState), normalizeJSON(t, exp.State)) {
					t.Fatalf("Value(%d) = %s, want %s", exp.Node, valState, exp.State)
				}
			}

			// Membership + frontier derived from the ingested ops.
			wantMembership, wantFrontier := deriveMembershipFrontier(scenario.Ops)
			gotMembership := runtime.Membership()
			if !reflect.DeepEqual(gotMembership, wantMembership) {
				t.Fatalf("membership = %v, want %v", gotMembership, wantMembership)
			}
			gotFrontier := runtime.FrontierEntries()
			if !reflect.DeepEqual(gotFrontier, wantFrontier) {
				t.Fatalf("frontier = %v, want %v", gotFrontier, wantFrontier)
			}
		})
	}
}

// deriveMembershipFrontier computes the expected membership (ascending peers)
// and per-peer max-stamp frontier (ascending peers) from a set of ops.
func deriveMembershipFrontier(ops []CrdtOp) ([]PeerId, []StampFrontierEntry) {
	maxByPeer := map[PeerId]HlcStamp{}
	for _, op := range ops {
		s := HlcStampFromWire(op.Stamp)
		if cur, ok := maxByPeer[s.Peer]; !ok || s.Greater(cur) {
			maxByPeer[s.Peer] = s
		}
	}
	// Build the frontier via the public API for parity with the runtime.
	f := NewStampFrontier()
	for peer, stamp := range maxByPeer {
		f.Observe(peer, stamp)
	}
	membership := make([]PeerId, 0, len(maxByPeer))
	for peer := range maxByPeer {
		membership = append(membership, peer)
	}
	sortPeers(membership)
	return membership, f.ToWire()
}

func sortPeers(peers []PeerId) {
	for i := 1; i < len(peers); i++ {
		for j := i; j > 0 && peers[j-1] > peers[j]; j-- {
			peers[j-1], peers[j] = peers[j], peers[j-1]
		}
	}
}

// assertCrdtResolution checks the converged winners follow the conflict rule the
// fixture names in `expect.resolution`, rather than merely happening to equal a
// literal list. Under "max_stamp" the surviving state for every address must be
// the state of the op with the greatest HLC stamp among the ops addressing it.
func assertCrdtResolution(t *testing.T, rule string, ops []CrdtOp, converged []ConvergedEntry) {
	t.Helper()
	if rule == "" {
		t.Fatal("expect.resolution is empty — the conflict rule must be stated")
	}
	if rule != "max_stamp" {
		t.Fatalf("unknown conflict resolution rule %q", rule)
	}

	type addr struct {
		node NodeId
		key  string
	}
	winner := map[addr]CrdtOp{}
	for _, op := range ops {
		a := addr{node: op.Node}
		if op.Key != nil {
			a.key = op.Key.Path()
		}
		prev, seen := winner[a]
		if !seen || HlcStampFromWire(prev.Stamp).Less(HlcStampFromWire(op.Stamp)) {
			winner[a] = op
		}
	}

	for _, entry := range converged {
		a := addr{node: entry.Node}
		if entry.Key != nil {
			a.key = *entry.Key
		}
		want, ok := winner[a]
		if !ok {
			t.Fatalf("converged entry %v has no op in the fixture", a)
		}
		gotState, err := json.Marshal(entry.State)
		if err != nil {
			t.Fatalf("marshal converged state: %v", err)
		}
		wantState, err := json.Marshal(want.State)
		if err != nil {
			t.Fatalf("marshal winning op state: %v", err)
		}
		if !reflect.DeepEqual(normalizeJSON(t, gotState), normalizeJSON(t, wantState)) {
			t.Fatalf("resolution=%s: node %d key %q converged to %s, but the max-stamp op %v carries %s",
				rule, entry.Node, a.key, gotState, want.Stamp, wantState)
		}
	}
}

func convergedEqual(t *testing.T, a, b []ConvergedEntry) bool {
	t.Helper()
	aw, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("marshal converged a: %v", err)
	}
	bw, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal converged b: %v", err)
	}
	return reflect.DeepEqual(normalizeJSON(t, aw), normalizeJSON(t, bw))
}

// ---------------------------------------------------------------------------
// CrdtSync frame serde
// ---------------------------------------------------------------------------

// canonicalizeCrdtSyncWire normalizes a fixture wire into the same canonical form
// the binding emits: an omitted `CrdtSync.frontier` is filled in as `[]`, which
// schemas/distributed.json declares equivalent (#lzspecfrontiersuppress).
func canonicalizeCrdtSyncWire(t *testing.T, wire json.RawMessage) any {
	t.Helper()
	normalized := normalizeJSON(t, wire)
	envelope, ok := normalized.(map[string]any)
	if !ok {
		return normalized
	}
	inner, ok := envelope["CrdtSync"].(map[string]any)
	if !ok {
		return normalized
	}
	if _, present := inner["frontier"]; !present {
		inner["frontier"] = []any{}
	}
	return normalized
}

func TestDistributedCrdtSyncFramesConformance(t *testing.T) {
	raw := loadConformanceFixture(t, "distributed", "crdt_sync_frames.json")

	var fixture struct {
		conformanceDoc
		ProtocolVersion int    `json:"protocol_version"`
		Kind            string `json:"kind"`
		Frames          []struct {
			Label      string `json:"label"`
			Assertions struct {
				FrontierLen     *int  `json:"frontier_len"`
				FrontierOmitted *bool `json:"frontier_omitted"`
				OpCount         int   `json:"op_count"`
				HasKeyedOp      *bool `json:"has_keyed_op"`
				HasKeylessOp    *bool `json:"has_keyless_op"`
			} `json:"assertions"`
			Wire json.RawMessage `json:"wire"`
		} `json:"frames"`
	}
	mustStrictJSON(t, "distributed/crdt_sync_frames.json", raw, &fixture)
	if len(fixture.Frames) == 0 {
		t.Fatal("crdt_sync_frames fixture has no frames")
	}

	for _, frame := range fixture.Frames {
		frame := frame
		t.Run(frame.Label, func(t *testing.T) {
			msg, err := IpcMessageFromWire(frame.Wire)
			if err != nil {
				t.Fatalf("IpcMessageFromWire: %v", err)
			}
			sync, ok := msg.(IpcMessageCrdtSync)
			if !ok {
				t.Fatalf("decoded %T, want IpcMessageCrdtSync", msg)
			}

			if frame.Assertions.FrontierLen != nil {
				if got := len(sync.Value.Frontier); got != *frame.Assertions.FrontierLen {
					t.Fatalf("frontier_len = %d, want %d", got, *frame.Assertions.FrontierLen)
				}
			}
			// #lzspecfrontiersuppress: an omitted frontier decodes as empty.
			if frame.Assertions.FrontierOmitted != nil {
				if !*frame.Assertions.FrontierOmitted {
					t.Fatal("frontier_omitted must assert true")
				}
				if len(sync.Value.Frontier) != 0 {
					t.Fatalf("frontier = %v, want empty for an omitted frontier", sync.Value.Frontier)
				}
			}
			if got := len(sync.Value.Ops); got != frame.Assertions.OpCount {
				t.Fatalf("op_count = %d, want %d", got, frame.Assertions.OpCount)
			}

			keyed, keyless := false, false
			for _, op := range sync.Value.Ops {
				if op.Key != nil {
					keyed = true
				} else {
					keyless = true
				}
			}
			if frame.Assertions.HasKeyedOp != nil && keyed != *frame.Assertions.HasKeyedOp {
				t.Fatalf("has_keyed_op = %v, want %v", keyed, *frame.Assertions.HasKeyedOp)
			}
			if frame.Assertions.HasKeylessOp != nil && keyless != *frame.Assertions.HasKeylessOp {
				t.Fatalf("has_keyless_op = %v, want %v", keyless, *frame.Assertions.HasKeylessOp)
			}

			// JSON round-trip: re-encode and compare structurally to the fixture.
			// Byte-for-byte except for schema-declared-equivalent encodings (see
			// lazily-spec docs/conformance.md § Round-trip equivalence exemptions):
			// `CrdtSync.frontier` omitted is equivalent to `[]`.
			encoded, err := msg.EncodeJSON()
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			want := canonicalizeCrdtSyncWire(t, frame.Wire)
			if !reflect.DeepEqual(normalizeJSON(t, encoded), want) {
				t.Fatalf("round-trip mismatch:\n got: %s\nwant: %s", encoded, frame.Wire)
			}
		})
	}
}
