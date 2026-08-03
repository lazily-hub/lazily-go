package lazily

// NodeKey null-leniency on decode (#lzkeynullstrict).
//
// protocol.md § NodeKey said a self-describing codec OMITS an absent `key`, and
// that a decoder seeing no `key` field treats it as absent. That settled the
// omitted form and left an explicit `key: null` undefined — and three bindings
// diverged there. The clause is now explicit: omit-when-absent binds the
// ENCODER, and a decoder MUST accept both forms as absent, refusing neither and
// constructing a key from neither.
//
// lazily-go was already lenient: `Key *NodeKey` under encoding/json reads a JSON
// null back as nil. What was NOT held in place is that it stays that way, and
// that the encoder still omits the field — this runner pins both.
//
// The re-encode half is the one a decode assertion cannot reach. A binding that
// reads `null` as absent and writes it straight back out has a correct decoded
// value and a non-conforming encoder, so each scenario re-encodes under its OWN
// codec and inspects the produced frame's field set schema-lessly.

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"testing"
)

const nodeKeyNullFixture = "codec/nodekey_null_leniency.json"

func decodeNodeKeyScenario(t *testing.T, scenario, expect map[string]any) IpcMessage {
	t.Helper()
	// Read off the raw carriage, never restated from the label itself
	// (#lznullformblind).
	codec := scenarioWireCodec(t, scenario)
	assertKey(t, scenario, "codec", codec)
	var (
		msg IpcMessage
		err error
	)
	switch codec {
	case "json":
		excuseKey(t, scenario, "wire_json",
			"the frame under test: the runner's INPUT, proven by the decoded values asserted below")
		raw := []byte(scenario["wire_json"].(string))
		assertKey(t, expect, "wire_input_fnv1a64", wireInputFNV1a64(raw))
		msg, err = DecodeIpcMessageJSON(raw)
	case "msgpack":
		excuseKey(t, scenario, "wire_msgpack_hex",
			"the frame under test: the runner's INPUT, proven by the decoded values asserted below")
		var raw []byte
		raw, err = hex.DecodeString(scenario["wire_msgpack_hex"].(string))
		if err != nil {
			t.Fatalf("wire_msgpack_hex is not hex: %v", err)
		}
		assertKey(t, expect, "wire_input_fnv1a64", wireInputFNV1a64(raw))
		msg, err = DecodeIpcMessageMsgpack(raw)
	default:
		t.Fatalf("unknown codec %q", codec)
	}
	if err != nil {
		t.Fatalf("%v: lazily-go must accept every form in this fixture; got %v", scenario["id"], err)
	}
	return msg
}

// nodeKeySite navigates a generic frame tree to the map that carries the `key`
// slot, dispatching on which optional-key site the scenario exercises. Shared by
// the RAW-WIRE control and the RE-ENCODED inspection so both read the same slot.
func nodeKeySite(t *testing.T, field string, generic map[string]any) map[string]any {
	t.Helper()
	switch field {
	case "snapshot":
		body := generic["Snapshot"].(map[string]any)
		return body["nodes"].([]any)[0].(map[string]any)
	case "node_add":
		body := generic["Delta"].(map[string]any)
		op := body["ops"].([]any)[0].(map[string]any)
		return op["NodeAdd"].(map[string]any)
	}
	t.Fatalf("unknown field %v", field)
	return nil
}

// nodeKeyWireForm reads the `key` slot off the RAW frame, BEFORE the decoder
// runs, and reports which of the three wire forms it holds.
//
// This control is the whole reason `wire_encoding` is dischargeable here. Every
// key in this fixture's `expect` blocks is IDENTICAL for the `omitted` and
// `null` families — `decoded_key` is nil for both, by design, because that is
// the leniency under test — so the four `null` scenarios are the four `omitted`
// ones wearing a different id as far as any post-decode assertion can tell. A
// binding whose decoder collapses the two the instant it touches the value
// (`key ?? null`) satisfies all twelve scenarios while never once distinguishing
// them. Only a read of the raw slot sees the difference the clause is about, and
// it is the same control the sibling blob-backend runner already applies to
// `backend`.
func nodeKeyWireForm(t *testing.T, scenario map[string]any) string {
	t.Helper()
	var tree any
	var witness string
	switch scenarioWireCodec(t, scenario) {
	case "json":
		if err := json.Unmarshal([]byte(scenario["wire_json"].(string)), &tree); err != nil {
			t.Fatalf("wire_json is not JSON: %v", err)
		}
	case "msgpack":
		raw, err := hex.DecodeString(scenario["wire_msgpack_hex"].(string))
		if err != nil {
			t.Fatalf("wire_msgpack_hex is not hex: %v", err)
		}
		// SECOND WITNESS, taken before the decoder is consulted at all.
		witness = nodeKeyMsgpackByteForm(t, raw)
		if tree, err = msgpackDecodeValue(raw); err != nil {
			t.Fatalf("wire_msgpack_hex is not msgpack: %v", err)
		}
	default:
		// Fail closed (#lzscenariobodyskip).
		t.Fatalf("unknown codec %v", scenario["codec"])
	}
	site := nodeKeySite(t, scenario["field"].(string), tree.(map[string]any))
	value, present := site["key"]
	form := "present"
	switch {
	case !present:
		form = "omitted"
	case value == nil:
		form = "null"
	}
	if witness != "" && witness != form {
		t.Fatalf("%v: the msgpack BYTES carry the %q form but msgpackDecodeValue reports %q "+
			"— the control and the thing it controls disagree, so neither can be trusted",
			scenario["id"], witness, form)
	}
	return form
}

// nodeKeyMsgpackByteForm classifies the `key` slot straight out of the msgpack
// BYTES, without going through this binding's decoder.
//
// nodeKeyWireForm's msgpack arm classifies through `msgpackDecodeValue`, which
// is the very layer the fixture suspects: a decoder that collapsed an absent map
// entry into an explicit nil would corrupt the control and the thing controlled
// in the same stroke, and the three-way `key_form` split would agree with itself
// all the way to green. This witness reads the wire directly — locate the
// fixstr header for "key" (0xa3 'k' 'e' 'y') and look at the byte that follows,
// 0xc0 being msgpack nil — so the two classifications are produced by
// independent machinery and are cross-checked against each other.
func nodeKeyMsgpackByteForm(t *testing.T, raw []byte) string {
	t.Helper()
	needle := []byte{0xa3, 'k', 'e', 'y'}
	at := bytes.Index(raw, needle)
	if at < 0 {
		return "omitted"
	}
	if bytes.Contains(raw[at+len(needle):], needle) {
		t.Fatalf("the frame names `key` more than once; this witness cannot tell which " +
			"slot it is reading, so it must not report a form at all")
	}
	next := at + len(needle)
	if next >= len(raw) {
		t.Fatalf("`key` is the last thing in the frame; there is no value byte to read")
	}
	if raw[next] == 0xc0 { // msgpack nil
		return "null"
	}
	return "present"
}

// reencodedNodeFields re-encodes under the scenario's own codec and reads the
// result back into a plain map, so what is inspected is the field set the
// encoder produced rather than a typed view that cannot tell absent from null.
func reencodedNodeFields(t *testing.T, scenario map[string]any, msg IpcMessage) map[string]any {
	t.Helper()
	var generic map[string]any
	switch scenario["codec"].(string) {
	case "json":
		raw, err := msg.MarshalJSON()
		if err != nil {
			t.Fatalf("json encode: %v", err)
		}
		if err := json.Unmarshal(raw, &generic); err != nil {
			t.Fatalf("json re-decode: %v", err)
		}
	case "msgpack":
		raw, err := EncodeIpcMessageMsgpack(msg)
		if err != nil {
			t.Fatalf("msgpack encode: %v", err)
		}
		value, err := msgpackDecodeValue(raw)
		if err != nil {
			t.Fatalf("msgpack re-decode: %v", err)
		}
		generic = value.(map[string]any)
	default:
		// Fail closed (#lzscenariobodyskip). Without this the switch fell
		// through leaving `generic` nil, so a fixture naming a codec this
		// runner does not implement replayed nothing and still reported the
		// scenario as covered.
		t.Fatalf("unknown codec %v", scenario["codec"])
	}
	return nodeKeySite(t, scenario["field"].(string), generic)
}

func decodedNodeKey(t *testing.T, scenario map[string]any, msg IpcMessage) any {
	t.Helper()
	switch scenario["field"].(string) {
	case "snapshot":
		node := msg.(IpcMessageSnapshot).Value.Nodes[0]
		if node.Key == nil {
			return nil
		}
		return node.Key.Path()
	case "node_add":
		op := msg.(IpcMessageDelta).Value.Ops[0].(DeltaOpNodeAdd)
		if op.Key == nil {
			return nil
		}
		return op.Key.Path()
	}
	t.Fatalf("unknown field %v", scenario["field"])
	return nil
}

func TestNodeKeyNullLeniencyConformance(t *testing.T) {
	fixture, ok := loadCodecFixture(t, nodeKeyNullFixture)
	if !ok {
		return
	}
	consumeFixtureKeys(t, nodeKeyNullFixture, fixture, "protocol_version", "assertions", "scenarios")
	assertKey(t, fixture, "protocol_version", float64(1))
	excuseKey(t, fixture, "assertions", "container: asserted key-by-key immediately below")
	excuseKey(t, fixture, "scenarios", "container: every entry is replayed and asserted in the loop below")
	if got := fixture["kind"]; got != "NodeKeyNullLeniency" {
		t.Fatalf("kind = %v, want NodeKeyNullLeniency", got)
	}

	assertions := fixture["assertions"].(map[string]any)
	consumeKeys(t, nodeKeyNullFixture+".assertions", assertions,
		"prose", "clause", "required_of_binding", "codecs", "fields", "key_forms",
		"scenario_count", "wire_encoding", "reencode_obligation", "anti_vacuity", "generator")
	assertKey(t, assertions, "required_of_binding", "MUST")
	excuseKey(t, assertions, "generator",
		"provenance: the path of the script that emitted this file, not a statement "+
			"about the decoder; the corpus does not declare it prose")

	// The vocabularies are asserted AFTER the loop, against what the replay
	// really dispatched on. Compared to a hand-written literal they would be
	// green over a runner that decodes nothing — the exact vacuity
	// `anti_vacuity` exists to name — so naming them in a discharge would
	// discharge nothing (#lzprosekeyconvention).
	var (
		codecsReplayed = map[string]bool{}
		fieldsReplayed = map[string]bool{}
		formsReplayed  = map[string]bool{}
	)

	// The four paragraphs the corpus declares in `assertions.prose`
	// (#lzprosekeyconvention). Each names the executable keys this run asserts
	// that carry its obligation.
	proseKey(t, assertions, "clause",
		// "accept both an omitted `key` and an explicit `key: null` and read
		// both as absent, refusing neither and constructing a key from neither".
		// `key_form` is what proves the two forms were DISTINCT going in;
		// `decoded_key` is what proves they arrive the same.
		"key_form", "decoded_key", "fields")
	proseKey(t, assertions, "wire_encoding",
		// Executable proof that the exact raw text / decoded-hex byte slice
		// reaches the library decoder rather than a reconstructed proxy.
		"wire_input_fnv1a64")
	proseKey(t, assertions, "reencode_obligation",
		// The half a decode assertion cannot reach: the encoder must still emit
		// the OMITTED form.
		"reencoded_key_field_present")
	proseKey(t, assertions, "anti_vacuity",
		// `omitted` forces a real decode and `present` forces a real key
		// through, and both are counted off the raw wire rather than off the
		// fixture's labels. `scenario_count` carries the paragraph's OTHER half
		// — "one that never decodes at all satisfies all of them" — now that it
		// is compared against the scenarios this run really replayed rather than
		// against the fixture's own array length (#lznullformblind). Named here
		// only because of that: as a self-comparison it discharged nothing.
		"key_form", "key_forms", "decoded_key", "reencoded_key_field_present",
		"scenario_count")

	scenarios := fixture["scenarios"].([]any)

	// Anti-vacuity in both directions. A runner that never decodes reports
	// "absent" for every scenario and satisfies all six null/omitted cases; the
	// `present` count is what only a real decode can produce.
	keysDecoded := 0
	// `scenario_count` is asserted AFTER the loop, against the scenarios this
	// run really replayed (#lznullformblind). Against `len(scenarios)` it
	// restated the fixture's own array length back to itself and was green over
	// a runner that decodes nothing.
	scenariosReplayed := 0

	for _, sv := range scenarioViews(nodeKeyNullFixture, scenarios) {
		id := sv.Label()
		// Rung 4 books on the first PAYLOAD read (#lzscenariobodyskip), not on
		// the label: a loop that reads `id` and skips has replayed nothing.
		scenario := sv.Map()
		scenariosReplayed++

		consumeKeys(t, id, scenario,
			"id", "name", "codec", "field", "key_form", "variant", "description",
			"expect", "wire_json", "wire_msgpack_hex")
		assertKey(t, scenario, "name", scenario["id"])
		excuseKey(t, scenario, "id", "the ledger key this loop records; it names the scenario rather than asserting it")
		excuseKey(t, scenario, "expect", "container: asserted key-by-key against the DECODED and RE-ENCODED frames below")
		excuseKey(t, scenario, "field", "a selector: it chooses which optional-key site this scenario exercises, not a value to compare")

		codecsReplayed[scenario["codec"].(string)] = true
		fieldsReplayed[scenario["field"].(string)] = true
		// The control, read off the RAW frame before the decoder runs. Not a
		// selector: a scenario tagged `null` whose frame omits the entry — or a
		// decoder-side collapse that made the two families indistinguishable —
		// reddens HERE, which is the only place it can.
		wireForm := nodeKeyWireForm(t, scenario)
		formsReplayed[wireForm] = true
		assertKey(t, scenario, "key_form", wireForm)

		expect := scenario["expect"].(map[string]any)
		consumeKeys(t, id+".expect", expect,
			"decoded_key", "reencoded_key_field_present", "node", "type_tag", "payload", "epoch",
			"wire_input_fnv1a64")

		msg := decodeNodeKeyScenario(t, scenario, expect)
		assertKey(t, scenario, "variant", codecVariant(t, msg))

		key := decodedNodeKey(t, scenario, msg)
		if key != nil {
			keysDecoded++
		}
		// The decode half: omitted and explicit-null must both arrive absent.
		assertKey(t, expect, "decoded_key", key)

		node := reencodedNodeFields(t, scenario, msg)
		// The encode half, invisible to every assertion above.
		encoded, present := node["key"]
		assertKey(t, expect, "reencoded_key_field_present", present && encoded != nil)

		assertKey(t, expect, "node", node["node"])
		assertKey(t, expect, "type_tag", node["type_tag"])
		assertKey(t, expect, "payload", node["state"].(map[string]any)["Payload"])
		switch m := msg.(type) {
		case IpcMessageSnapshot:
			assertKey(t, expect, "epoch", float64(m.Value.Epoch))
		case IpcMessageDelta:
			assertKey(t, expect, "epoch", float64(m.Value.Epoch))
		default:
			t.Fatalf("%s: unexpected variant", id)
		}
	}

	// The count the replay produced, not the length of the array it read
	// (#lznullformblind). Asserted BEFORE the decode-count gate below, so a short
	// replay is reported against the corpus's own number rather than swallowed by
	// a runner-side literal that fatals first.
	assertKey(t, assertions, "scenario_count", float64(scenariosReplayed))

	if keysDecoded != 4 {
		t.Fatalf("decoded %d keys, want 4: only the `present` scenarios carry one, so a "+
			"runner reporting absent for everything satisfies the null cases trivially", keysDecoded)
	}

	// The three vocabularies, asserted as SETS against what the loop really
	// dispatched on — `key_forms` off the raw wire, not off the fixture's own
	// labels. Compared to literals these were green over a runner that decodes
	// nothing.
	assertKeyWith(t, assertions, "codecs", func(want any) {
		t.Helper()
		assertSameStringSet(t, "codecs", stringSlice(want), codecsReplayed)
	})
	assertKeyWith(t, assertions, "fields", func(want any) {
		t.Helper()
		assertSameStringSet(t, "fields", stringSlice(want), fieldsReplayed)
	})
	assertKeyWith(t, assertions, "key_forms", func(want any) {
		t.Helper()
		assertSameStringSet(t, "key_forms", stringSlice(want), formsReplayed)
	})

	// The replay is finished, so every key a discharge names has either been
	// asserted or has not (#lzprosekeyconvention).
	verifyProse(t, nodeKeyNullFixture)
}
