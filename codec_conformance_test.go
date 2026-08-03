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
// lazily-go implements BOTH halves: `json` in ipc.go and `msgpack` in
// msgpack_codec.go (#lzmsgpackseven). Neither is a carve-out any more.
//
// Each runner decodes `wire`, RE-ENCODES the decoded message, decodes again, and
// checks every `expect` key against that second decode. Asserting against the
// fixture literal would prove nothing: the literal never passed through an
// encoder.
//
// The msgpack runner additionally introspects the ENCODED BYTES schema-lessly.
// Value round-tripping alone cannot see the named-field rule — a positional
// array encoder round-trips every value correctly — so `encoded_envelope_key`
// and the SORTED `encoded_body_field_names` are read back out of the bytes
// themselves. Sorted because a MessagePack map's key order is encoder-defined
// (§ Frame codecs: msgpack is NOT byte-canonical).

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

const (
	codecJSONFixture    = "codec/frame_roundtrip_json.json"
	codecMsgpackFixture = "codec/frame_roundtrip_msgpack.json"
)

func loadCodecFixture(t *testing.T, name string) (map[string]any, bool) {
	t.Helper()
	candidates := []string{
		filepath.Join("..", "lazily-spec", "conformance", name),
		// The vendored mirror, in the same place loadConformanceFixture reads
		// it from, so the offline fallback is a real one rather than a copy
		// nothing loads. TestVendoredFixturesMatchCanonical holds it
		// byte-identical to the canonical corpus whenever the sibling is present.
		filepath.Join("test", "conformance", name),
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
	fixtureBlock := consumeKeys(t, codecJSONFixture+" assertions", jsMap(fixture["assertions"]),
		"prose", "codec", "self_describing", "byte_canonical", "required_of_binding", "role",
		"scenario_count", "note")
	assertKey(t, fixtureBlock, "codec", "json")
	assertKey(t, fixtureBlock, "self_describing", true)
	assertKey(t, fixtureBlock, "byte_canonical", true)
	assertKey(t, fixtureBlock, "required_of_binding", "MUST")
	assertKey(t, fixtureBlock, "role", "reference")
	assertKey(t, fixtureBlock, "scenario_count", float64(len(jsList(fixture["scenarios"]))))
	// `note` is a declared prose key here, not a reserved annotation: it states
	// the obligation to keep `role` and `byte_canonical` apart, and both are
	// pinned, so it is discharged by naming them (#lzprosekeyconvention).
	proseKey(t, fixtureBlock, "note", "role", "byte_canonical")

	replayed := 0
	for _, sv := range scenarioViews(codecJSONFixture, jsList(fixture["scenarios"])) {
		t.Run(sv.Label(), func(t *testing.T) {
			// Rung 4 books HERE (#lzscenariobodyskip), on the first read of the
			// PAYLOAD inside the subtest — never at the loop header, which
			// cannot tell a body that replayed from one that returned early.
			scenario := sv.Map()
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

	// The replay is finished, so every key the `note` discharge names has either
	// been asserted or has not (#lzprosekeyconvention).
	verifyProse(t, codecJSONFixture)
}

// ---------------------------------------------------------------------------
// `msgpack` — the cross-language binary default (#lzmsgpackseven)
// ---------------------------------------------------------------------------

// msgpackFieldNames returns the SORTED field names of an encoded MessagePack
// map. Sorted because map key order is encoder-defined; the assertion is about
// WHICH names the encoder wrote, never the order it wrote them in.
//
// The type assertion is the executable form of the named-field rule: a
// positional encoder produces an array here and dies on this line, having
// round-tripped every value correctly.
func msgpackFieldNames(t *testing.T, value any, what string) []any {
	t.Helper()
	fields, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s: msgpack encodes each struct as a MAP KEYED BY THE JSON FIELD NAME, "+
			"not as a positional array (got %T)", what, value)
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	return strs(names)
}

// msgpackEnvelope decodes the encoded frame schema-lessly and splits the
// externally-tagged one-entry envelope into its variant NAME and its body.
func msgpackEnvelope(t *testing.T, encoded []byte) (string, any) {
	t.Helper()
	tree, err := msgpackDecodeValue(encoded)
	if err != nil {
		t.Fatalf("schema-less decode of the encoded frame: %v", err)
	}
	envelope, ok := tree.(map[string]any)
	if !ok {
		t.Fatalf("the frame envelope is externally tagged — a one-entry map keyed by the "+
			"variant NAME, never an integer discriminator or a positional array (got %T)", tree)
	}
	if len(envelope) != 1 {
		t.Fatalf("the frame envelope carries %d entries, want exactly one (external tagging)", len(envelope))
	}
	for tag, body := range envelope {
		return tag, body
	}
	return "", nil
}

func msgpackBodyField(t *testing.T, body any, field string) any {
	t.Helper()
	fields, ok := body.(map[string]any)
	if !ok {
		t.Fatalf("frame body is a %T, want a named-field map", body)
	}
	value, present := fields[field]
	if !present {
		t.Fatalf("frame body carries no %q field", field)
	}
	return value
}

func msgpackElement(t *testing.T, value any, index int, what string) any {
	t.Helper()
	list, ok := value.([]any)
	if !ok {
		t.Fatalf("%s: want a msgpack array, got %T", what, value)
	}
	if index >= len(list) {
		t.Fatalf("%s: only %d element(s) encoded", what, len(list))
	}
	return list[index]
}

// assertCodecEncoding reads the ENCODED BYTES back schema-lessly. This is the
// only place the named-field rule is observable: every `expect` key in
// assertCodecValues passes just as well under a positional encoder.
func assertCodecEncoding(t *testing.T, block map[string]any, variant string, encoded []byte) {
	t.Helper()
	tag, body := msgpackEnvelope(t, encoded)
	assertKey(t, block, "encoded_envelope_key", tag)
	assertKey(t, block, "encoded_body_field_names", msgpackFieldNames(t, body, "frame body"))

	switch variant {
	case "Snapshot":
		// No `key`: NodeSnapshot.key is optional and a SELF-DESCRIBING codec
		// OMITS it when absent (protocol.md § NodeKey). Writing it as nil
		// instead would round-trip fine and redden exactly this key.
		first := msgpackElement(t, msgpackBodyField(t, body, "nodes"), 0, "nodes")
		assertKey(t, block, "first_node_encoded_field_names", msgpackFieldNames(t, first, "first node"))
	case "CrdtSync":
		// `key` IS present on both ops, including the one whose key is unset:
		// a CrdtOp always writes it (nil when unset) because an anti-entropy
		// op's addressing is part of its merge identity.
		ops := msgpackBodyField(t, body, "ops")
		assertKey(t, block, "first_op_encoded_field_names",
			msgpackFieldNames(t, msgpackElement(t, ops, 0, "ops"), "first op"))
		assertKey(t, block, "second_op_encoded_field_names",
			msgpackFieldNames(t, msgpackElement(t, ops, 1, "ops"), "second op"))
	case "Delta":
		// No per-variant encoded-field-name key: the Delta scenario pins its op
		// shapes through `op_variants` and `first_op_payload` in
		// assertCodecValues instead. Named explicitly so the `default` below can
		// fail closed.
	default:
		// Fail closed (#lzscenariobodyskip). Without this the switch fell
		// through, so a fixture naming a frame variant this function has no
		// encoding assertion for checked nothing here and still reported the
		// scenario as covered.
		t.Fatalf("unknown frame variant %q", variant)
	}
}

// codecMsgpackExpectKeys is the union of the `expect` keys the three scenarios
// carry. consumeKeys fails on any key the fixture carries that this list does
// not name, and the rung-3 tracker then owes a disposition for each one that is
// actually present — so a key nobody asserts cannot hide in a scenario.
var codecMsgpackExpectKeys = []string{
	// every scenario
	"round_trip_equals_source", "encoded_envelope_key", "encoded_body_field_names",
	// Snapshot
	"first_node_encoded_field_names", "epoch", "node_count", "edge_count", "root_count",
	"first_node_type_tag", "first_node_payload", "opaque_node_id", "opaque_node_state_tag",
	"first_edge", "roots",
	// Delta
	"base_epoch", "op_count", "op_variants", "first_op_payload", "node_add_type_tag",
	// CrdtSync
	"first_op_encoded_field_names", "second_op_encoded_field_names", "frontier_len",
	"frontier_first_peer", "frontier_first_stamp_wall_time", "first_op_node",
	"first_op_key_absent", "second_op_node", "second_op_key", "second_op_stamp_peer",
}

// TestCodecMsgpackFramesRoundTrip replays the `msgpack` half of the codec
// obligation: the externally-tagged frame over named-field maps.
func TestCodecMsgpackFramesRoundTrip(t *testing.T) {
	fixture, ok := loadCodecFixture(t, codecMsgpackFixture)
	if !ok {
		return
	}
	if got := jsInt(fixture["protocol_version"]); got != 1 {
		t.Fatalf("protocol_version = %d, want 1", got)
	}
	if got := jsStr(fixture["kind"]); got != "FrameCodecRoundTrip" {
		t.Fatalf("kind = %q, want FrameCodecRoundTrip", got)
	}
	if got := jsStr(fixture["codec"]); got != "msgpack" {
		t.Fatalf("codec = %q, want msgpack", got)
	}

	consumeFixtureKeys(t, codecMsgpackFixture, fixture, "scenarios", "assertions", "codec", "protocol_version")
	excuseKey(t, fixture, "scenarios", "replay input: each scenario's own expect block is asserted in the subtest below")
	excuseKey(t, fixture, "assertions", "container: asserted key-by-key in the fixture-level block below")
	excuseKey(t, fixture, "codec", "discriminator: cross-checked above against the file this fixture lives in and against assertions.codec")
	excuseKey(t, fixture, "protocol_version", "envelope version: a load precondition checked above, not an expectation about the codec")

	// The fixture-level block pins the codec's identity, its ROLE (the
	// negotiated cross-language binary default, not the reference floor), and
	// `byte_canonical: false` — the reason this fixture pins decoded values and
	// sorted field names instead of a golden byte string.
	fixtureBlock := consumeKeys(t, codecMsgpackFixture+" assertions", jsMap(fixture["assertions"]),
		"prose", "codec", "self_describing", "byte_canonical", "required_of_binding", "role", "scenario_count", "note")
	assertKey(t, fixtureBlock, "codec", "msgpack")
	assertKey(t, fixtureBlock, "self_describing", true)
	assertKey(t, fixtureBlock, "byte_canonical", false)
	assertKey(t, fixtureBlock, "required_of_binding", "MUST")
	assertKey(t, fixtureBlock, "role", "cross_language_binary_default")
	assertKey(t, fixtureBlock, "scenario_count", float64(len(jsList(fixture["scenarios"]))))
	// `note` is a declared prose key here, not a reserved annotation: it states
	// WHY this fixture pins decoded values and named field lists rather than
	// golden bytes, and the executable form of that is `byte_canonical: false`
	// plus the field names read back out of the encoded bytes
	// (#lzprosekeyconvention). `encoded_body_field_names` is asserted per
	// scenario, long after this block is finished — which is why the ledger is
	// fixture-scoped rather than block-scoped.
	proseKey(t, fixtureBlock, "note", "byte_canonical", "encoded_body_field_names", "encoded_envelope_key")

	replayed := 0
	for _, sv := range scenarioViews(codecMsgpackFixture, jsList(fixture["scenarios"])) {
		t.Run(sv.Label(), func(t *testing.T) {
			// Rung 4 books HERE (#lzscenariobodyskip), on the first read of the
			// PAYLOAD inside the subtest — never at the loop header, which
			// cannot tell a body that replayed from one that returned early.
			scenario := sv.Map()
			consumeKeys(t, codecMsgpackFixture+" scenario", scenario, "id", "name", "description", "variant", "expect", "wire")
			excuseKeys(t, scenario, "narration: names and describes the scenario, states nothing the replay must observe", "id", "name", "description")
			excuseKey(t, scenario, "wire", "replay input: the frame fed through the codec; what survives is asserted through the expect block")
			excuseKey(t, scenario, "variant", "discriminator: cross-checked against the decoded frame below, not an expectation")
			excuseKey(t, scenario, "expect", "container: asserted key-by-key against the ROUND-TRIPPED frame and the encoded bytes below")

			// `wire` is written in the REFERENCE json form (protocol.md § Frame
			// codecs: fixtures are authored in the reference codec), so the
			// frame enters through the json decoder and leaves through msgpack.
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

			// Encode the DECODED message to msgpack and decode the bytes back.
			// The fixture literal is never re-asserted, so a codec that silently
			// drops a field cannot be masked by reading the input back.
			encoded, err := EncodeIpcMessageMsgpack(source)
			if err != nil {
				t.Fatalf("msgpack encode: %v", err)
			}
			block := consumeKeys(t, codecMsgpackFixture+" "+sv.Label()+" expect", jsMap(scenario["expect"]), codecMsgpackExpectKeys...)

			// SHAPE of the bytes first, then their meaning. A binding whose
			// codec is positional on both ends round-trips every value
			// correctly, so the decode below cannot see the named-field rule at
			// all — only this schema-less read of the encoding can.
			assertCodecEncoding(t, block, jsStr(scenario["variant"]), encoded)

			roundTripped, err := DecodeIpcMessageMsgpack(encoded)
			if err != nil {
				t.Fatalf("msgpack decode: %v", err)
			}
			assertKey(t, block, "round_trip_equals_source", reflect.DeepEqual(roundTripped, source))
			assertCodecValues(t, block, roundTripped)
		})
		replayed++
	}
	if replayed != 3 {
		t.Fatalf("replayed %d scenarios, want one per IpcMessage variant", replayed)
	}

	// The replay is finished, so every key the `note` discharge names has either
	// been asserted or has not (#lzprosekeyconvention).
	verifyProse(t, codecMsgpackFixture)
}

// TestMsgpackCodecRefusesBin pins the Go-specific trap in the wire itself: a
// byte payload travels as an ARRAY OF INTEGERS, and every Go msgpack library
// encodes a []byte as `bin` by default. Accepting `bin` in a byte-payload
// position would put this binding outside the wire the `msgpack` token names,
// so the decoder refuses it and the encoder refuses a []byte outright.
func TestMsgpackCodecRefusesBin(t *testing.T) {
	// {"Inline": <bin8 [1,2]>} — the shape a default Go encoder would emit for
	// an IpcValue::Inline payload.
	frame := []byte{0x81, 0xa6, 'I', 'n', 'l', 'i', 'n', 'e', 0xc4, 0x02, 0x01, 0x02}
	if _, err := msgpackDecodeValue(frame); err == nil {
		t.Fatal("decoder accepted MessagePack bin in a byte-payload position")
	}
	if _, err := msgpackEncodeValue(map[string]any{"Inline": []byte{1, 2}}); err == nil {
		t.Fatal("encoder accepted a []byte, which every Go msgpack library writes as bin")
	}
}
