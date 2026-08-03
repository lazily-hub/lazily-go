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
	"encoding/hex"
	"encoding/json"
	"testing"
)

const nodeKeyNullFixture = "codec/nodekey_null_leniency.json"

func decodeNodeKeyScenario(t *testing.T, scenario map[string]any) IpcMessage {
	t.Helper()
	codec := scenario["codec"].(string)
	assertKeyWith(t, scenario, "codec", func(want any) {
		t.Helper()
		if want != codec {
			t.Errorf("codec = %v, want %v", codec, want)
		}
	})
	var (
		msg IpcMessage
		err error
	)
	switch codec {
	case "json":
		excuseKey(t, scenario, "wire_json",
			"the frame under test: the runner's INPUT, proven by the decoded values asserted below")
		msg, err = DecodeIpcMessageJSON([]byte(scenario["wire_json"].(string)))
	case "msgpack":
		excuseKey(t, scenario, "wire_msgpack_hex",
			"the frame under test: the runner's INPUT, proven by the decoded values asserted below")
		var raw []byte
		raw, err = hex.DecodeString(scenario["wire_msgpack_hex"].(string))
		if err != nil {
			t.Fatalf("wire_msgpack_hex is not hex: %v", err)
		}
		msg, err = DecodeIpcMessageMsgpack(raw)
	default:
		t.Fatalf("unknown codec %q", codec)
	}
	if err != nil {
		t.Fatalf("%v: lazily-go must accept every form in this fixture; got %v", scenario["id"], err)
	}
	return msg
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
	switch scenario["field"].(string) {
	case "snapshot":
		body := generic["Snapshot"].(map[string]any)
		return body["nodes"].([]any)[0].(map[string]any)
	case "node_add":
		body := generic["Delta"].(map[string]any)
		op := body["ops"].([]any)[0].(map[string]any)
		return op["NodeAdd"].(map[string]any)
	}
	t.Fatalf("unknown field %v", scenario["field"])
	return nil
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
	assertKey(t, assertions, "codecs", []any{"json", "msgpack"})
	assertKey(t, assertions, "fields", []any{"snapshot", "node_add"})
	assertKey(t, assertions, "key_forms", []any{"omitted", "null", "present"})
	excuseKey(t, assertions, "generator",
		"provenance: the path of the script that emitted this file, not a statement "+
			"about the decoder; the corpus does not declare it prose")

	// The four paragraphs the corpus declares in `assertions.prose`
	// (#lzprosekeyconvention). Each names the executable keys this run asserts
	// that carry its obligation.
	proseKey(t, assertions, "clause",
		// "accept both an omitted `key` and an explicit `key: null` and read
		// both as absent, refusing neither and constructing a key from neither".
		"decoded_key", "key_forms", "fields")
	proseKey(t, assertions, "wire_encoding",
		// Raw text / lowercase hex is what carries the ABSENT-entry versus
		// explicit-nil distinction into the runner in both codecs.
		"decoded_key", "codecs")
	proseKey(t, assertions, "reencode_obligation",
		// The half a decode assertion cannot reach: the encoder must still emit
		// the OMITTED form.
		"reencoded_key_field_present")
	proseKey(t, assertions, "anti_vacuity",
		// `omitted` forces a real decode and `present` forces a real key through.
		"decoded_key", "key_forms", "scenario_count")

	scenarios := fixture["scenarios"].([]any)
	assertKey(t, assertions, "scenario_count", float64(len(scenarios)))

	// Anti-vacuity in both directions. A runner that never decodes reports
	// "absent" for every scenario and satisfies all six null/omitted cases; the
	// `present` count is what only a real decode can produce.
	keysDecoded := 0

	for _, sv := range scenarioViews(nodeKeyNullFixture, scenarios) {
		id := sv.Label()
		// Rung 4 books on the first PAYLOAD read (#lzscenariobodyskip), not on
		// the label: a loop that reads `id` and skips has replayed nothing.
		scenario := sv.Map()

		consumeKeys(t, id, scenario,
			"id", "name", "codec", "field", "key_form", "variant", "description",
			"expect", "wire_json", "wire_msgpack_hex")
		assertKey(t, scenario, "name", scenario["id"])
		excuseKey(t, scenario, "id", "the ledger key this loop records; it names the scenario rather than asserting it")
		excuseKey(t, scenario, "expect", "container: asserted key-by-key against the DECODED and RE-ENCODED frames below")
		excuseKey(t, scenario, "field", "a selector: it chooses which optional-key site this scenario exercises, not a value to compare")
		excuseKey(t, scenario, "key_form", "a selector: the wire form is proven by `decoded_key` and `reencoded_key_field_present` below")

		expect := scenario["expect"].(map[string]any)
		consumeKeys(t, id+".expect", expect,
			"decoded_key", "reencoded_key_field_present", "node", "type_tag", "payload", "epoch")

		msg := decodeNodeKeyScenario(t, scenario)
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

	if keysDecoded != 4 {
		t.Fatalf("decoded %d keys, want 4: only the `present` scenarios carry one, so a "+
			"runner reporting absent for everything satisfies the null cases trivially", keysDecoded)
	}

	// The replay is finished, so every key a discharge names has either been
	// asserted or has not (#lzprosekeyconvention).
	verifyProse(t, nodeKeyNullFixture)
}
