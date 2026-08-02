package lazily

// Blob-backend discriminator strictness on decode (#lzblobbackendstrict).
//
// protocol.md § Shared-memory payload path makes `backend` optional with a
// default of `shm`, and that OPTIONALITY is the whole of the field's
// forward-compatibility: it carries every descriptor minted before the field
// existed. A value that is PRESENT but outside the enum is a different fact and
// gets the opposite answer — the decode MUST fail, naming the offending token.
//
// lazily-go used to normalize an unknown token to `shm`, documented as
// deliberate. It is not conforming: `shm` is a backend this build genuinely
// resolves, so reading an unknown kind as `shm` routes a non-shm descriptor into
// the shm table. That is exactly the misroute `resolve_wrong_backend`
// (docs/zero-copy-transport.md) rules out structurally, downgraded to a
// probabilistic guarantee discharged by a 64-bit checksum.
//
// Three things this runner asserts that a decode assertion alone cannot reach:
//
//   - `error_names_token`: a decoder that refuses the frame because it mis-parsed
//     `checksum` satisfies a bare is-error check while implementing none of the
//     clause. The token has to appear in the message.
//   - `reencoded_backend_field_present`: the ENCODER half. A binding must not
//     satisfy the clause by echoing back whatever it received — `shm` is omitted
//     on the way out, `arrow` is not — so each scenario is re-encoded under its
//     OWN codec and the produced frame is inspected schema-lessly.
//   - `backend_form`: read back off the RAW wire rather than trusted from the
//     fixture's label, so a scenario that does not actually carry the form it
//     claims cannot make a probe pass by aiming at a value the input never had.

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

const blobBackendFixture = "codec/blob_backend_discriminator.json"

// blobBackendWireTree decodes the scenario's RAW wire — text for json, hex for
// msgpack — into a generic tree, without going anywhere near the library's
// typed decoder.
//
// The wire is carried raw on purpose: `schemas/defs.json` closes `backend` to an
// enum, so the reject frames are schema-INVALID by design and cannot be embedded
// as parsed objects. Re-serializing a parsed object here would silently repair
// them and the reject scenarios would test nothing.
func blobBackendWireTree(t *testing.T, scenario map[string]any) any {
	t.Helper()
	switch scenario["codec"].(string) {
	case "json":
		var tree any
		if err := json.Unmarshal([]byte(scenario["wire_json"].(string)), &tree); err != nil {
			t.Fatalf("wire_json is not JSON: %v", err)
		}
		return tree
	case "msgpack":
		raw, err := hex.DecodeString(scenario["wire_msgpack_hex"].(string))
		if err != nil {
			t.Fatalf("wire_msgpack_hex is not hex: %v", err)
		}
		tree, err := msgpackDecodeValue(raw)
		if err != nil {
			t.Fatalf("wire_msgpack_hex is not msgpack: %v", err)
		}
		return tree
	default:
		// Fail closed (#lzscenariobodyskip): a codec this runner does not
		// implement must not replay as an empty tree and still book.
		t.Fatalf("unknown codec %v", scenario["codec"])
		return nil
	}
}

// blobBackendDecode runs the scenario's raw wire through the library's decoder
// for its own codec. Both are exercised: msgpack is a MUST-level codec here.
func blobBackendDecode(t *testing.T, scenario map[string]any) (IpcMessage, error) {
	t.Helper()
	switch scenario["codec"].(string) {
	case "json":
		return DecodeIpcMessageJSON([]byte(scenario["wire_json"].(string)))
	case "msgpack":
		raw, err := hex.DecodeString(scenario["wire_msgpack_hex"].(string))
		if err != nil {
			t.Fatalf("wire_msgpack_hex is not hex: %v", err)
		}
		return DecodeIpcMessageMsgpack(raw)
	default:
		t.Fatalf("unknown codec %v", scenario["codec"])
		return nil, nil
	}
}

// blobBackendReencode re-encodes a decoded frame under the scenario's own codec
// and reads it back into a plain tree, so what is inspected is the field set the
// ENCODER produced rather than a typed view that cannot tell absent from
// present.
func blobBackendReencode(t *testing.T, scenario map[string]any, msg IpcMessage) any {
	t.Helper()
	switch scenario["codec"].(string) {
	case "json":
		raw, err := msg.MarshalJSON()
		if err != nil {
			t.Fatalf("json encode: %v", err)
		}
		var tree any
		if err := json.Unmarshal(raw, &tree); err != nil {
			t.Fatalf("json re-decode: %v", err)
		}
		return tree
	case "msgpack":
		raw, err := EncodeIpcMessageMsgpack(msg)
		if err != nil {
			t.Fatalf("msgpack encode: %v", err)
		}
		tree, err := msgpackDecodeValue(raw)
		if err != nil {
			t.Fatalf("msgpack re-decode: %v", err)
		}
		return tree
	default:
		t.Fatalf("unknown codec %v", scenario["codec"])
		return nil
	}
}

// blobBackendEnvelopeKey returns the single variant key of a wire tree
// ("Delta"). It reads the RAW frame, so it is available for the reject
// scenarios too, which never produce a decoded message to inspect.
func blobBackendEnvelopeKey(t *testing.T, tree any) string {
	t.Helper()
	obj, ok := tree.(map[string]any)
	if !ok || len(obj) != 1 {
		t.Fatalf("frame is not a single-key envelope: %v", tree)
	}
	for k := range obj {
		return k
	}
	return ""
}

// blobBackendWireDescriptor digs the SharedBlob descriptor out of a wire tree:
// <Variant>.ops[0].SlotValue.payload.SharedBlob.
func blobBackendWireDescriptor(t *testing.T, tree any) map[string]any {
	t.Helper()
	body := jsMap(tree)[blobBackendEnvelopeKey(t, tree)]
	ops, ok := jsMap(body)["ops"].([]any)
	if !ok || len(ops) == 0 {
		t.Fatalf("frame carries no ops: %v", body)
	}
	slot := jsMap(jsMap(ops[0])["SlotValue"])
	payload := jsMap(slot["payload"])
	blob, ok := payload["SharedBlob"]
	if !ok {
		t.Fatalf("op payload is not a SharedBlob: %v", payload)
	}
	return jsMap(blob)
}

// blobBackendWireForm reads the wire form of `backend` back off the RAW frame:
// "omitted" when the map carries no entry, else the token as written. This is
// what makes `backend_form` an assertion rather than a label — and it is the
// guard against a probe aimed at a value the input never carries.
func blobBackendWireForm(t *testing.T, tree any) string {
	t.Helper()
	descriptor := blobBackendWireDescriptor(t, tree)
	value, present := descriptor["backend"]
	if !present {
		return "omitted"
	}
	token, ok := value.(string)
	if !ok {
		t.Fatalf("wire `backend` is not a string: %v", value)
	}
	return token
}

func TestBlobBackendDiscriminatorConformance(t *testing.T) {
	fixture, ok := loadCodecFixture(t, blobBackendFixture)
	if !ok {
		return
	}
	consumeFixtureKeys(t, blobBackendFixture, fixture, "protocol_version", "assertions", "scenarios")
	assertKey(t, fixture, "protocol_version", float64(1))
	excuseKey(t, fixture, "assertions", "container: asserted key-by-key immediately below")
	excuseKey(t, fixture, "scenarios", "container: every entry is replayed and asserted in the loop below")
	if got := fixture["kind"]; got != "BlobBackendDiscriminator" {
		t.Fatalf("kind = %v, want BlobBackendDiscriminator", got)
	}

	assertions := fixture["assertions"].(map[string]any)
	consumeKeys(t, blobBackendFixture+".assertions", assertions,
		"clause", "required_of_binding", "codecs", "backends", "outcomes",
		"scenario_count", "wire_encoding", "reject_obligation", "anti_vacuity",
		"theorem", "generator")
	assertKey(t, assertions, "required_of_binding", "MUST")
	// The enum, asserted against the LIBRARY's own three values rather than a
	// literal: a binding that dropped one would redden here.
	assertKey(t, assertions, "backends", []any{
		string(BackendShm), string(BackendArrow), string(BackendInProcess),
	})
	for _, prose := range []string{
		"clause", "wire_encoding", "reject_obligation", "anti_vacuity", "theorem", "generator",
	} {
		excuseKey(t, assertions, prose,
			"prose: it states WHY the fixture is shaped this way; the behaviour it "+
				"describes is asserted by the per-scenario decode, rejection and "+
				"re-encode below")
	}

	scenarios := fixture["scenarios"].([]any)
	assertKey(t, assertions, "scenario_count", float64(len(scenarios)))

	// Anti-vacuity counters, each defeating a different way to pass without
	// implementing the clause. They are asserted after the loop.
	var (
		codecsReplayed   = map[string]bool{}
		outcomesReplayed = map[string]bool{}
		accepted         int
		rejected         int
		nonShmDecoded    int
		fieldReencoded   int
	)

	for _, sv := range scenarioViews(blobBackendFixture, scenarios) {
		id := sv.Label()
		// Rung 4 books on the first PAYLOAD read (#lzscenariobodyskip), not on
		// the label: a loop that reads `id` and skips has replayed nothing.
		scenario := sv.Map()

		consumeKeys(t, id, scenario,
			"id", "name", "codec", "backend_form", "outcome", "variant", "description",
			"expect", "wire_json", "wire_msgpack_hex")
		assertKey(t, scenario, "name", scenario["id"])
		excuseKey(t, scenario, "id",
			"the ledger key this loop records; it names the scenario rather than asserting it")
		excuseKey(t, scenario, "expect",
			"container: asserted key-by-key against the DECODED frame, the REJECTION, and the RE-ENCODED frame below")

		codec := scenario["codec"].(string)
		codecsReplayed[codec] = true
		assertKeyWith(t, scenario, "codec", func(want any) {
			t.Helper()
			if want != codec {
				t.Errorf("%s: codec = %v, want %v", id, codec, want)
			}
		})
		switch codec {
		case "json":
			excuseKey(t, scenario, "wire_json",
				"the frame under test: the runner's INPUT. Its content is proven by "+
					"`backend_form`, which is read back off this same raw text, and by "+
					"the decoded values asserted below")
		case "msgpack":
			excuseKey(t, scenario, "wire_msgpack_hex",
				"the frame under test: the runner's INPUT. Its content is proven by "+
					"`backend_form`, which is read back off these same raw bytes, and by "+
					"the decoded values asserted below")
		}

		wire := blobBackendWireTree(t, scenario)
		// The wire form is READ, not taken on the fixture's word, so a scenario
		// whose frame does not carry the form it advertises fails here rather
		// than quietly making a downstream probe vacuous.
		assertKey(t, scenario, "backend_form", blobBackendWireForm(t, wire))
		// Available for accept and reject alike, because it comes off the raw
		// frame rather than a decoded message.
		assertKey(t, scenario, "variant", blobBackendEnvelopeKey(t, wire))

		expect := jsMap(scenario["expect"])
		consumeKeys(t, id+".expect", expect,
			"decoded_backend", "reencoded_backend_field_present",
			"node", "offset", "len", "generation", "epoch", "checksum",
			"rejected", "error_names_token")

		msg, err := blobBackendDecode(t, scenario)

		switch outcome := scenario["outcome"].(string); outcome {
		case "reject":
			rejected++
			outcomesReplayed[outcome] = true
			assertKey(t, scenario, "outcome", "reject")
			if err == nil {
				t.Fatalf("%s: the frame decoded to %v; a `backend` outside the enum "+
					"MUST be rejected, not normalized", id, msg)
			}
			assertKey(t, expect, "rejected", true)
			// The rejection must be FOR THE STATED REASON. A decoder that
			// refuses this frame because it mis-parsed some other field passes a
			// bare is-error assertion while implementing none of the clause.
			assertKeyWith(t, expect, "error_names_token", func(want any) {
				t.Helper()
				token, ok := want.(string)
				if !ok {
					t.Fatalf("%s: error_names_token is not a string: %v", id, want)
				}
				if !strings.Contains(err.Error(), token) {
					t.Errorf("%s: rejection %q does not name the offending token %q",
						id, err.Error(), token)
				}
			})

		case "accept":
			accepted++
			outcomesReplayed[outcome] = true
			assertKey(t, scenario, "outcome", "accept")
			if err != nil {
				t.Fatalf("%s: lazily-go must accept this frame; got %v", id, err)
			}

			delta, ok := msg.(IpcMessageDelta)
			if !ok {
				t.Fatalf("%s: decoded %T, want a Delta", id, msg)
			}
			op, ok := delta.Value.Ops[0].(DeltaOpSlotValue)
			if !ok {
				t.Fatalf("%s: first op is %T, want SlotValue", id, delta.Value.Ops[0])
			}
			blob, ok := op.Payload.(IpcValueSharedBlob)
			if !ok {
				t.Fatalf("%s: op payload is %T, want a SharedBlob", id, op.Payload)
			}

			// The decode half. `backend_arrow` is what forces the field to be
			// READ: a decoder that ignores it and hardcodes `shm` passes the
			// omitted and explicit-shm scenarios and fails here.
			decoded := blob.Blob.Backend.Normalized()
			if decoded != BackendShm {
				nonShmDecoded++
			}
			assertKey(t, expect, "decoded_backend", string(decoded))

			// The rest of the descriptor, so "decoded the backend and dropped
			// the frame" is not a passing implementation.
			assertKey(t, expect, "node", uint64(op.Node))
			assertKey(t, expect, "offset", blob.Blob.Offset)
			assertKey(t, expect, "len", blob.Blob.Len)
			assertKey(t, expect, "generation", blob.Blob.Generation)
			assertKey(t, expect, "epoch", blob.Blob.Epoch)
			assertKey(t, expect, "checksum", blob.Blob.Checksum)

			// The ENCODER half, invisible to every assertion above: a conforming
			// encoder OMITS `backend` when it is `shm`, so a binding cannot
			// satisfy the clause by round-tripping whatever it received.
			reencoded := blobBackendWireDescriptor(t, blobBackendReencode(t, scenario, msg))
			value, present := reencoded["backend"]
			if present {
				fieldReencoded++
				if token, _ := value.(string); token != string(decoded) {
					t.Errorf("%s: re-encoded backend %v, want %q", id, value, decoded)
				}
			}
			assertKey(t, expect, "reencoded_backend_field_present", present)

		default:
			// Fail closed (#lzscenariobodyskip): an outcome this runner does not
			// implement must not fall through as replayed.
			t.Fatalf("%s: unknown outcome %q", id, outcome)
		}
	}

	// `codecs` and `outcomes` are asserted against what the loop ACTUALLY
	// dispatched, so a runner that silently implemented one arm cannot satisfy
	// them.
	assertKey(t, assertions, "codecs", sortedKeys(codecsReplayed))
	assertKey(t, assertions, "outcomes", sortedOutcomes(outcomesReplayed))

	if accepted != 6 || rejected != 2 {
		t.Fatalf("replayed %d accepts and %d rejects, want 6 and 2", accepted, rejected)
	}
	// A decoder that ignores `backend` and hardcodes `shm` reports shm for every
	// scenario. Only a real read of the field produces these.
	if nonShmDecoded != 2 {
		t.Fatalf("decoded %d non-shm backends, want 2 (the two `arrow` scenarios): a "+
			"decoder that hardcodes shm satisfies the other four accepts trivially", nonShmDecoded)
	}
	// ...and the mirror image on the encoder side: exactly the arrow scenarios
	// emit the field. A binding that echoes the received value back out emits it
	// for `backend_shm_explicit` too.
	if fieldReencoded != 2 {
		t.Fatalf("re-encoded the `backend` field in %d scenarios, want 2 (arrow only): "+
			"an encoder that echoes what it received emits it for explicit shm as well",
			fieldReencoded)
	}
}

// sortedOutcomes orders the outcomes the way the fixture lists them
// (accept before reject) rather than alphabetically by accident.
func sortedOutcomes(set map[string]bool) []string {
	var out []string
	for _, outcome := range []string{"accept", "reject"} {
		if set[outcome] {
			out = append(out, outcome)
		}
	}
	return out
}
