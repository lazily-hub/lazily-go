package lazily

// Frame-codec round-trip conformance (#lzmsgpackparity).
//
// protocol.md § Frame codecs makes `json` (the reference codec) and `msgpack`
// (the cross-language binary default) MUST-level for every binding, and
// requires every frame to round-trip through both for all three IpcMessage
// variants. That requirement lived only in prose. The four conformance rungs —
// was the fixture OPENED, were its keys CONSUMED, were they ASSERTED, was every
// SCENARIO replayed — all reason about fixture *content*, and content replay
// never exercises a codec, so a binding could carve out a MUST-level codec and
// stay green on every rung.
//
// lazily-go implements the `json` half. `msgpack` is an explicit carve-out
// (declared in cmd/lazily-interop-peer and now in
// scripts/check-conformance-coverage.sh), so codec/frame_roundtrip_msgpack.json
// is listed as known-uncovered rather than silently ignored.
//
// The runner decodes `wire`, RE-ENCODES the decoded message, decodes again, and
// checks every `expect` key against that second decode. Asserting against the
// fixture literal would prove nothing: the literal never passed through an
// encoder.

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
)

const codecJSONFixture = "codec/frame_roundtrip_json.json"

func loadCodecFixture(t *testing.T, name string) (map[string]any, bool) {
	t.Helper()
	candidates := []string{
		filepath.Join("..", "lazily-spec", "conformance", name),
		filepath.Join("conformance", name),
		filepath.Join("testdata", "conformance", name),
	}
	for _, path := range candidates {
		data, err := specReadFile(path)
		if err != nil {
			continue
		}
		var fixture map[string]any
		mustStrictJSON(t, name, data, &fixture)
		return fixture, true
	}
	t.Skipf("codec fixture not found: %s", name)
	return nil, false
}

// codecVariant names the IpcMessage arm, matching the fixture's `variant`.
func codecVariant(t *testing.T, m IpcMessage) string {
	t.Helper()
	switch m.(type) {
	case IpcMessageSnapshot:
		return "Snapshot"
	case IpcMessageDelta:
		return "Delta"
	case IpcMessageCrdtSync:
		return "CrdtSync"
	default:
		t.Fatalf("codec fixture pins no runner for %T", m)
		return ""
	}
}

func codecOpVariant(t *testing.T, op DeltaOp) string {
	t.Helper()
	switch op.(type) {
	case DeltaOpCellSet:
		return "CellSet"
	case DeltaOpSlotValue:
		return "SlotValue"
	case DeltaOpInvalidate:
		return "Invalidate"
	case DeltaOpNodeAdd:
		return "NodeAdd"
	case DeltaOpNodeRemove:
		return "NodeRemove"
	case DeltaOpEdgeAdd:
		return "EdgeAdd"
	case DeltaOpEdgeRemove:
		return "EdgeRemove"
	default:
		t.Fatalf("unknown DeltaOp %T", op)
		return ""
	}
}

// ints widens an integer slice to the []any the fixture's JSON decodes to.
func ints[T ~int | ~int64 | ~uint64 | ~uint8](vs []T) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = float64(v)
	}
	return out
}

func strs(vs []string) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = v
	}
	return out
}

func assertCodecSnapshot(t *testing.T, block map[string]any, snap Snapshot) {
	t.Helper()
	assertKey(t, block, "epoch", float64(snap.Epoch))
	assertKey(t, block, "node_count", float64(len(snap.Nodes)))
	assertKey(t, block, "edge_count", float64(len(snap.Edges)))
	assertKey(t, block, "root_count", float64(len(snap.Roots)))
	assertKey(t, block, "first_node_type_tag", snap.Nodes[0].TypeTag)
	payload, ok := snap.Nodes[0].State.(NodeStatePayload)
	if !ok {
		t.Fatalf("first node should carry Payload bytes, got %T", snap.Nodes[0].State)
	}
	assertKey(t, block, "first_node_payload", ints(payload.Bytes))

	var opaque NodeSnapshot
	found := false
	for _, n := range snap.Nodes {
		if _, isOpaque := n.State.(NodeStateOpaque); isOpaque {
			opaque, found = n, true
			break
		}
	}
	if !found {
		t.Fatal("fixture pins an Opaque node")
	}
	assertKey(t, block, "opaque_node_id", float64(opaque.Node))
	// The externally-tagged UNIT variant is the shape most likely to decay into
	// {"Opaque": null} under a re-encode, so name it rather than infer it.
	raw, err := opaque.State.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal Opaque state: %v", err)
	}
	var tag any
	if err := json.Unmarshal(raw, &tag); err != nil {
		t.Fatalf("decode Opaque state: %v", err)
	}
	assertKey(t, block, "opaque_node_state_tag", tag)

	assertKey(t, block, "first_edge", ints([]NodeId{snap.Edges[0].Dependent, snap.Edges[0].Dependency}))
	assertKey(t, block, "roots", ints(snap.Roots))
}

func assertCodecDelta(t *testing.T, block map[string]any, delta Delta) {
	t.Helper()
	assertKey(t, block, "base_epoch", float64(delta.BaseEpoch))
	assertKey(t, block, "epoch", float64(delta.Epoch))
	assertKey(t, block, "op_count", float64(len(delta.Ops)))

	variants := make([]string, len(delta.Ops))
	for i, op := range delta.Ops {
		variants[i] = codecOpVariant(t, op)
	}
	assertKey(t, block, "op_variants", strs(variants))

	cellSet, ok := delta.Ops[0].(DeltaOpCellSet)
	if !ok {
		t.Fatalf("first op should be a CellSet, got %T", delta.Ops[0])
	}
	inline, ok := cellSet.Payload.(IpcValueInline)
	if !ok {
		t.Fatalf("first op payload should be Inline, got %T", cellSet.Payload)
	}
	assertKey(t, block, "first_op_payload", ints(inline.Bytes))

	typeTag := ""
	for _, op := range delta.Ops {
		if add, isAdd := op.(DeltaOpNodeAdd); isAdd {
			typeTag = add.TypeTag
			break
		}
	}
	if typeTag == "" {
		t.Fatal("fixture pins a NodeAdd op")
	}
	assertKey(t, block, "node_add_type_tag", typeTag)
}

func assertCodecCrdtSync(t *testing.T, block map[string]any, sync CrdtSync) {
	t.Helper()
	assertKey(t, block, "frontier_len", float64(len(sync.Frontier)))
	assertKey(t, block, "frontier_first_peer", float64(sync.Frontier[0].Peer))
	assertKey(t, block, "frontier_first_stamp_wall_time", float64(sync.Frontier[0].Stamp.WallTime))
	assertKey(t, block, "op_count", float64(len(sync.Ops)))
	assertKey(t, block, "first_op_node", float64(sync.Ops[0].Node))
	// Decoded-value assertion, not an encoding one: both self-describing codecs
	// WRITE `key` for a CrdtOp (null when unset — an anti-entropy op's
	// addressing is part of its merge identity). What must survive the round
	// trip is that the decoder reads that null back as absent.
	assertKey(t, block, "first_op_key_absent", sync.Ops[0].Key == nil)
	assertKey(t, block, "second_op_node", float64(sync.Ops[1].Node))
	if sync.Ops[1].Key == nil {
		t.Fatal("second op should carry a NodeKey")
	}
	assertKey(t, block, "second_op_key", sync.Ops[1].Key.ToWire())
	assertKey(t, block, "second_op_stamp_peer", float64(sync.Ops[1].Stamp.Peer))
}

func assertCodecValues(t *testing.T, block map[string]any, m IpcMessage) {
	t.Helper()
	switch msg := m.(type) {
	case IpcMessageSnapshot:
		assertCodecSnapshot(t, block, msg.Value)
	case IpcMessageDelta:
		assertCodecDelta(t, block, msg.Value)
	case IpcMessageCrdtSync:
		assertCodecCrdtSync(t, block, msg.Value)
	default:
		t.Fatalf("codec fixture pins no runner for %T", m)
	}
}

// TestCodecJSONFramesRoundTrip replays the `json` half of the codec obligation.
func TestCodecJSONFramesRoundTrip(t *testing.T) {
	fixture, ok := loadCodecFixture(t, codecJSONFixture)
	if !ok {
		return
	}
	if got := jsInt(fixture["protocol_version"]); got != 1 {
		t.Fatalf("protocol_version = %d, want 1", got)
	}
	if got := jsStr(fixture["kind"]); got != "FrameCodecRoundTrip" {
		t.Fatalf("kind = %q, want FrameCodecRoundTrip", got)
	}
	if got := jsStr(fixture["codec"]); got != "json" {
		t.Fatalf("codec = %q, want json", got)
	}

	consumeFixtureKeys(t, codecJSONFixture, fixture, "scenarios", "assertions", "codec", "protocol_version")
	excuseKey(t, fixture, "scenarios", "replay input: each scenario's own expect block is asserted in the subtest below")
	excuseKey(t, fixture, "assertions", "container: asserted key-by-key in the fixture-level block below")
	excuseKey(t, fixture, "codec", "discriminator: cross-checked above against the file this fixture lives in and against assertions.codec")
	excuseKey(t, fixture, "protocol_version", "envelope version: a load precondition checked above, not an expectation about the codec")

	// The fixture-level block pins the codec's identity and the two distinct
	// senses of "canonical" protocol.md keeps apart (`role` = the required
	// interop floor, `byte_canonical` = one deterministic byte form per
	// message). Unread until now — an unread block is the drift
	// #lzassertunknownkeys exists to catch.
	fixtureBlock := jsMap(fixture["assertions"])
	assertKey(t, fixtureBlock, "codec", "json")
	assertKey(t, fixtureBlock, "self_describing", true)
	assertKey(t, fixtureBlock, "byte_canonical", true)
	assertKey(t, fixtureBlock, "required_of_binding", "MUST")
	assertKey(t, fixtureBlock, "role", "reference")
	assertKey(t, fixtureBlock, "scenario_count", float64(len(jsList(fixture["scenarios"]))))
	excuseKey(t, fixtureBlock, "note", "prose: documents the reference-vs-byte-canonical distinction, states nothing the replay observes")

	replayed := 0
	for index, rawScenario := range jsList(fixture["scenarios"]) {
		scenario := jsMap(rawScenario)
		// Rung 4 (#lzscenariocoverage): record at the point of replay, so a
		// scenario this loop stops reaching stops being recorded.
		label := recordScenarioMap(codecJSONFixture, index, scenario)
		t.Run(label, func(t *testing.T) {
			consumeKeys(t, codecJSONFixture+" scenario", scenario, "id", "name", "description", "variant", "expect", "wire")
			excuseKeys(t, scenario, "narration: names and describes the scenario, states nothing the replay must observe", "id", "name", "description")
			excuseKey(t, scenario, "wire", "replay input: the frame fed through the codec; what survives is asserted through the expect block")
			excuseKey(t, scenario, "variant", "discriminator: cross-checked against the decoded frame below, not an expectation")
			excuseKey(t, scenario, "expect", "container: asserted key-by-key against the ROUND-TRIPPED frame in assertCodecValues")

			wire, err := json.Marshal(scenario["wire"])
			if err != nil {
				t.Fatalf("re-marshal wire: %v", err)
			}
			source, err := IpcMessageFromWire(wire)
			if err != nil {
				t.Fatalf("decode wire: %v", err)
			}
			if got, want := codecVariant(t, source), jsStr(scenario["variant"]); got != want {
				t.Fatalf("fixture `variant` = %q, decoded frame is %q", want, got)
			}

			// Encode the DECODED message and decode the result. The fixture
			// literal is never re-asserted, so a codec that silently drops a
			// field cannot be masked by reading the input back.
			encoded, err := json.Marshal(source)
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}
			roundTripped, err := IpcMessageFromWire(encoded)
			if err != nil {
				t.Fatalf("decode re-encoded frame: %v", err)
			}

			block := jsMap(scenario["expect"])
			assertKey(t, block, "round_trip_equals_source", reflect.DeepEqual(roundTripped, source))
			assertCodecValues(t, block, roundTripped)
		})
		replayed++
	}
	if replayed != 3 {
		t.Fatalf("replayed %d scenarios, want one per IpcMessage variant", replayed)
	}
}
