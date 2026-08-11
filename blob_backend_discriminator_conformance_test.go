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
// Six things this runner asserts that a decode assertion alone cannot reach:
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
//   - VOCABULARY COMPLETENESS: every backend in `assertions.backends` must turn
//     up as the `decoded_backend` of some accept scenario. A binding that knows
//     only {shm, arrow} rejects `in_process` — naming the token, conformingly by
//     the letter of the clause — and passed all of fixture v1 while implementing
//     a smaller enum than the clause declares. lazily-go was one of the bindings
//     that reported that hole; the guard is here so the next one reddens instead.
//     No scenario count reaches it.
//   - `rejection_is_decode_error`: the refusal has to arrive through the family
//     every caller already guards a decode with. See blobBackendDecode — in Go
//     that family is the returned `error`, and the way out of it is a panic.
//   - `frame_epoch` vs `blob_epoch`: two DIFFERENT numbers from two different
//     places. v1 carried 9 in both, so a runner reading the Delta's epoch and one
//     reading the descriptor's both satisfied the single `expect.epoch` it
//     offered. Both are asserted here, each against its own source, and the loop
//     additionally records that the fixture keeps them distinct — a fixture that
//     re-collapsed them would make the two assertions indistinguishable again
//     without failing either one.
//
// TWO CODECS ARE NOT TWO IMPLEMENTATIONS HERE, and this runner records that
// rather than banking it. lazily-go's msgpack decoder bridges into the very DOM
// the json decoder consumes (DecodeIpcMessageMsgpack -> IpcMessageFromWire), so
// the msgpack half of every scenario pair re-runs ONE discriminator verdict. It
// still covers the bridge and the encoder, and the ledger assertion at the foot
// of this file pins the sharing so that splitting the codecs apart reddens here
// and gets this paragraph rewritten, instead of quietly turning 14 green
// scenarios into a claim they never made.

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
)

const blobBackendFixture = "codec/blob_backend_discriminator.json"

// blobBackendWireTree decodes the scenario's RAW wire — text for json, hex for
// msgpack — into a generic tree, without going anywhere near the library's
// typed decoder.
//
// The wire is carried raw on purpose: `schemas/defs.json` closes `backend` to an
// enum, so the reject frames AND the explicit-null frames are schema-INVALID by
// design and cannot be embedded as parsed objects. Re-serializing a parsed object
// here would silently repair them and those scenarios would test nothing.
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

// blobBackendOutcome is what the library's decode entry point actually DID with
// a frame, including the way out that a plain `(IpcMessage, error)` cannot show.
//
// Go has no exception hierarchy, so "the codec's documented decode-error family"
// (`assertions.non_string_form`) is the error VALUE returned from the documented
// entry point — and the way a refusal escapes that family is a PANIC, which
// unwinds straight past every `if err != nil` a caller wrote. That is the Go
// shape of "the frame is still refused and the peer still never sees the error",
// so the panic is captured rather than allowed to abort the run, and a scenario
// that refuses by panicking fails `rejection_is_decode_error` instead of
// disappearing into a stack trace.
type blobBackendOutcome struct {
	msg      IpcMessage
	err      error
	panicked any
}

// blobBackendDecode runs the scenario's raw wire through the library's decoder
// for its own codec. Both are exercised: msgpack is a MUST-level codec here.
func blobBackendDecode(t *testing.T, scenario, expect map[string]any) (out blobBackendOutcome) {
	t.Helper()
	var raw []byte
	var decode func([]byte) (IpcMessage, error)
	switch scenario["codec"].(string) {
	case "json":
		raw, decode = []byte(scenario["wire_json"].(string)), DecodeIpcMessageJSON
	case "msgpack":
		decoded, err := hex.DecodeString(scenario["wire_msgpack_hex"].(string))
		if err != nil {
			t.Fatalf("wire_msgpack_hex is not hex: %v", err)
		}
		raw, decode = decoded, DecodeIpcMessageMsgpack
	default:
		t.Fatalf("unknown codec %v", scenario["codec"])
		return
	}
	assertKey(t, expect, "wire_input_fnv1a64", wireInputFNV1a64(raw))
	defer func() {
		if r := recover(); r != nil {
			out.panicked = r
		}
	}()
	out.msg, out.err = decode(raw)
	return
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

// blobBackendWireForm reads the wire form of `backend` back off the RAW frame,
// covering all seven shapes `assertions.backend_forms` names: "omitted" when the
// map carries no entry at all, "null" for an explicit nil, "non_string" for a
// value in the slot that is not a token, and otherwise the token as written.
//
// This is what makes `backend_form` an assertion rather than a label — and it is
// the guard against a probe aimed at a value the input never carries. The three
// non-token shapes matter most: "omitted" and "null" are DIFFERENT wire facts
// that the clause deliberately gives the same answer, so a runner that could not
// tell them apart would replay one of them twice and never notice.
func blobBackendWireForm(t *testing.T, tree any) string {
	t.Helper()
	descriptor := blobBackendWireDescriptor(t, tree)
	value, present := descriptor["backend"]
	switch {
	case !present:
		return "omitted"
	case value == nil:
		return "null"
	}
	token, ok := value.(string)
	if !ok {
		return "non_string"
	}
	return token
}

// blobBackendWireRejectionKind derives which of the two refusals a reject
// scenario is asking for from the RAW frame, not from the fixture's own label,
// for the same reason `backend_form` is read rather than trusted: a scenario
// tagged `non_string` whose frame carries a string would otherwise let the
// non-string arm pass while never seeing a non-string.
func blobBackendWireRejectionKind(t *testing.T, tree any) string {
	t.Helper()
	if _, isToken := blobBackendWireDescriptor(t, tree)["backend"].(string); isToken {
		return "unknown_token"
	}
	return "non_string"
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
		"prose", "clause", "required_of_binding", "codecs", "backends", "backend_forms",
		"outcomes", "rejection_kinds", "scenario_count", "wire_encoding",
		"backend_form_vocabulary", "null_form", "non_string_form",
		"epoch_disambiguation", "reject_obligation", "anti_vacuity",
		"theorem")
	assertKey(t, assertions, "required_of_binding", "MUST")

	// The nine paragraphs the corpus declares in `assertions.prose`
	// (#lzprosekeyconvention). Each names the executable keys THIS run asserts
	// that carry its obligation; the tracker checks the naming at fixture end,
	// which is what makes the claim falsifiable rather than merely well worded.
	proseKey(t, assertions, "clause",
		// omitted/null decode as shm; an unknown token is refused through the
		// decode-error family and never normalized.
		"decoded_backend", "rejected", "rejection_kind", "rejection_is_decode_error")
	proseKey(t, assertions, "wire_encoding",
		// Executable proof that the exact raw text / decoded-hex byte slice
		// reaches the library decoder rather than a reconstructed proxy.
		"wire_input_fnv1a64")
	proseKey(t, assertions, "backend_form_vocabulary",
		// "every backend in `backends` is the `decoded_backend` of some accept
		// scenario" — the assertion that would have caught v1's missing
		// `in_process`.
		"backends", "backend_forms", "decoded_backend")
	proseKey(t, assertions, "reject_obligation",
		// "refused for the STATED reason", not merely refused.
		"error_names_token", "rejection_kind", "rejection_kinds")
	proseKey(t, assertions, "null_form",
		// null is the ABSENT form: decodes as shm and does not survive the round
		// trip.
		"backend_form", "decoded_backend", "reencoded_backend_field_present")
	proseKey(t, assertions, "non_string_form",
		// Refused through the same decode-error family, naming no token.
		"rejection_kind", "rejection_is_decode_error")
	proseKey(t, assertions, "epoch_disambiguation",
		// Two different numbers from two different places.
		"frame_epoch", "blob_epoch")
	proseKey(t, assertions, "anti_vacuity",
		// The four controls, in the order the paragraph states them, plus
		// `scenario_count` — which joins them only now that it counts the
		// scenarios this run really replayed instead of restating the fixture's
		// own array length (#lznullformblind). As a self-comparison it was
		// itself an instance of the pass-without-implementing the paragraph
		// names, so naming it here would have discharged nothing.
		"backend_forms", "backends", "reencoded_backend_field_present", "outcomes",
		"scenario_count")
	proseKey(t, assertions, "theorem",
		// PROXY. `resolve_wrong_backend` is a Lean theorem in lazily-formal; a
		// run in this repository cannot prove it, only its CONSEQUENCE — that an
		// unknown kind is refused rather than normalized to `shm` and routed.
		// These are the keys that pin the refusal.
		"rejected", "rejection_kind")

	scenarios := fixture["scenarios"].([]any)

	// Anti-vacuity counters, each defeating a different way to pass without
	// implementing the clause. They are asserted after the loop. `replayed`
	// carries `scenario_count`, which used to compare the fixture's own count to
	// the length of the fixture's own array (#lznullformblind) — green over a
	// runner that decodes nothing, which is what `anti_vacuity` exists to name.
	var (
		replayed          int
		codecsReplayed    = map[string]bool{}
		outcomesReplayed  = map[string]bool{}
		formsReplayed     = map[string]bool{}
		kindsReplayed     = map[string]bool{}
		decodedBackends   = map[string]bool{}
		refusalTypes      = map[string]map[string]bool{}
		accepted          int
		rejected          int
		nonShmDecoded     int
		fieldReencoded    int
		decodeFamilyRefus int
		epochsDistinct    int
	)

	for _, sv := range scenarioViews(blobBackendFixture, scenarios) {
		id := sv.Label()
		// Rung 4 books on the first PAYLOAD read (#lzscenariobodyskip), not on
		// the label: a loop that reads `id` and skips has replayed nothing.
		scenario := sv.Map()
		replayed++

		consumeKeys(t, id, scenario,
			"id", "name", "codec", "backend_form", "outcome", "variant", "description",
			"expect", "wire_json", "wire_msgpack_hex")
		assertKey(t, scenario, "name", scenario["id"])
		excuseKey(t, scenario, "id",
			"the ledger key this loop records; it names the scenario rather than asserting it")
		excuseKey(t, scenario, "expect",
			"container: asserted key-by-key against the DECODED frame, the REJECTION, and the RE-ENCODED frame below")

		// Read off the raw carriage, never restated from the label itself
		// (#lznullformblind).
		codec := scenarioWireCodec(t, scenario)
		codecsReplayed[codec] = true
		assertKey(t, scenario, "codec", codec)
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
		wireForm := blobBackendWireForm(t, wire)
		formsReplayed[wireForm] = true
		assertKey(t, scenario, "backend_form", wireForm)
		// Available for accept and reject alike, because it comes off the raw
		// frame rather than a decoded message.
		assertKey(t, scenario, "variant", blobBackendEnvelopeKey(t, wire))

		expect := jsMap(scenario["expect"])
		consumeKeys(t, id+".expect", expect,
			"decoded_backend", "reencoded_backend_field_present",
			"node", "offset", "len", "generation", "frame_epoch", "blob_epoch", "checksum",
			"rejected", "rejection_kind", "rejection_is_decode_error", "error_names_token",
			"wire_input_fnv1a64")

		out := blobBackendDecode(t, scenario, expect)

		// THE RUN CHOOSES THE ARM, not the fixture's label (#lznullformblind).
		//
		// This switch used to dispatch on `scenario["outcome"]`, so the fixture
		// decided which assertions a frame owed. A `reject` frame the binding
		// wrongly ACCEPTED still entered the reject arm and was caught only by
		// the guard below — and, worse, `outcomesReplayed`, `accepted` and
		// `rejected` were all booked from the label BEFORE that guard ran. The
		// anti-vacuity counters that protect `assertions.outcomes` and the
		// 10/4 split therefore carried exactly the blindness they exist to
		// remove: they counted what the corpus said happened, not what did.
		//
		// Derived from the decode itself, the label becomes an ASSERTION rather
		// than a selector: a wrongly-accepted reject frame now takes the accept
		// arm, reddens `outcome` against the corpus's own value, and drops out
		// of the `rejected` tally at the same time.
		observed := "accept"
		if out.panicked != nil || out.err != nil {
			observed = "reject"
		}
		outcomesReplayed[observed] = true
		assertKey(t, scenario, "outcome", observed)

		switch observed {
		case "reject":
			rejected++
			assertKey(t, expect, "rejected", true)

			// Which refusal is this? Derived from the wire, like `backend_form`.
			kind := blobBackendWireRejectionKind(t, wire)
			kindsReplayed[kind] = true
			assertKey(t, expect, "rejection_kind", kind)

			// The refusal has to arrive through the family every caller already
			// guards a decode with. In Go that family is the returned `error`, and
			// the way out of it is a panic, which unwinds past `if err != nil`
			// entirely: the frame is still refused and the peer still never sees
			// the error. That is the failure `rejection_is_decode_error` names, in
			// the shape this runtime can produce it.
			assertKeyWith(t, expect, "rejection_is_decode_error", func(wantValue fixtureValue) {
				want := wantValue.Value()
				t.Helper()
				if want != true {
					t.Fatalf("%s: rejection_is_decode_error is %v; this runner only "+
						"implements the true arm", id, want)
				}
				if out.panicked != nil {
					t.Errorf("%s: the refusal escaped as a PANIC (%v) rather than the "+
						"returned error every caller guards a decode with — the frame "+
						"is refused and the peer never sees it", id, out.panicked)
					return
				}
				if out.err == nil {
					t.Errorf("%s: no error returned", id)
					return
				}
				decodeFamilyRefus++
				byCodec := refusalTypes[kind]
				if byCodec == nil {
					byCodec = map[string]bool{}
					refusalTypes[kind] = byCodec
				}
				byCodec[fmt.Sprintf("%s=%T", codec, out.err)] = true
			})

			// The rejection must be FOR THE STATED REASON. A decoder that refuses
			// this frame because it mis-parsed some other field passes a bare
			// is-error assertion while implementing none of the clause.
			//
			// Only the unknown-token refusal carries this key: there is no token to
			// name in the non-string frame, and requiring the field name instead
			// would pin a message format no codec's native type error carries.
			if _, present := expect["error_names_token"]; present {
				if kind != "unknown_token" {
					t.Errorf("%s: rejection_kind is %q but the fixture asks for a named "+
						"token; only unknown_token has one", id, kind)
				}
				assertKeyWith(t, expect, "error_names_token", func(wantValue fixtureValue) {
					want := wantValue.Value()
					t.Helper()
					token, ok := want.(string)
					if !ok {
						t.Fatalf("%s: error_names_token is not a string: %v", id, want)
					}
					if out.err == nil || !strings.Contains(out.err.Error(), token) {
						t.Errorf("%s: rejection %v does not name the offending token %q",
							id, out.err, token)
					}
				})
			} else if kind != "non_string" {
				t.Errorf("%s: rejection_kind %q names a token but the fixture does not "+
					"assert it is in the message", id, kind)
			}

		case "accept":
			accepted++

			delta, ok := out.msg.(IpcMessageDelta)
			if !ok {
				t.Fatalf("%s: decoded %T, want a Delta", id, out.msg)
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
			// omitted, null and explicit-shm scenarios and fails here.
			decoded := blob.Blob.Backend.Normalized()
			decodedBackends[string(decoded)] = true
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
			assertKey(t, expect, "checksum", blob.Blob.Checksum)

			// TWO epochs, from two different places. The Delta's epoch orders
			// deltas; the descriptor's epoch is the arena incarnation the blob was
			// written into. v1 carried the same number in both, so reading either
			// one satisfied the single `expect.epoch` it offered — which is why
			// `expect.epoch` was REMOVED upstream rather than redefined.
			frameEpoch := uint64(delta.Value.Epoch)
			blobEpoch := blob.Blob.Epoch
			assertKey(t, expect, "frame_epoch", frameEpoch)
			assertKey(t, expect, "blob_epoch", blobEpoch)
			if frameEpoch != uint64(blobEpoch) {
				epochsDistinct++
			}

			// The ENCODER half, invisible to every assertion above: a conforming
			// encoder OMITS `backend` when it is `shm`, so a binding cannot
			// satisfy the clause by round-tripping whatever it received — and an
			// explicit null must not survive the trip either.
			reencoded := blobBackendWireDescriptor(t, blobBackendReencode(t, scenario, out.msg))
			value, present := reencoded["backend"]
			if present {
				fieldReencoded++
				if token, _ := value.(string); token != string(decoded) {
					t.Errorf("%s: re-encoded backend %v, want %q", id, value, decoded)
				}
			}
			assertKey(t, expect, "reencoded_backend_field_present", present)

		default:
			// Fail closed (#lzscenariobodyskip). `observed` is derived from the
			// decode and so has only two values today; the arm stays so that
			// widening that derivation cannot silently fall through as replayed.
			t.Fatalf("%s: unhandled observed outcome %q", id, observed)
		}
	}

	// The count the replay produced, not the length of the array it read
	// (#lznullformblind). Asserted BEFORE the accept/reject-split gate below, so
	// a short replay is reported against the corpus's own number rather than
	// swallowed by a runner-side literal that fatals first.
	assertKey(t, assertions, "scenario_count", float64(replayed))

	// `codecs`, `outcomes`, `backend_forms` and `rejection_kinds` are asserted
	// against what the loop ACTUALLY dispatched — and the forms come off the raw
	// wire, not off the fixture's labels — so a runner that silently implemented
	// one arm cannot satisfy them.
	assertKey(t, assertions, "codecs", sortedKeys(codecsReplayed))
	assertKey(t, assertions, "outcomes", sortedOutcomes(outcomesReplayed))
	assertKeyWith(t, assertions, "backend_forms", func(wantValue fixtureValue) {
		want := wantValue.Value()
		t.Helper()
		assertSameStringSet(t, "backend_forms", stringSlice(want), formsReplayed)
	})
	assertKeyWith(t, assertions, "rejection_kinds", func(wantValue fixtureValue) {
		want := wantValue.Value()
		t.Helper()
		assertSameStringSet(t, "rejection_kinds", stringSlice(want), kindsReplayed)
	})

	// `backends` carries TWO facts, and v1 proved they are separable: the enum
	// this build implements, and the enum the corpus actually exercises. v1
	// declared three backends and carried scenarios for two, so a binding knowing
	// only {shm, arrow} was green on every scenario while contradicting the enum
	// the clause declares — reading the discriminator and knowing the vocabulary
	// are different facts. `decodedBackends` is filled from REAL decodes, so a
	// backend that no accept scenario produces reddens here even though the
	// scenario count is untouched.
	assertKeyWith(t, assertions, "backends", func(wantValue fixtureValue) {
		want := wantValue.Value()
		t.Helper()
		declared := stringSlice(want)
		library := []string{string(BackendShm), string(BackendArrow), string(BackendInProcess)}
		if strings.Join(declared, ",") != strings.Join(library, ",") {
			t.Errorf("assertions.backends = %v, but this build implements %v", declared, library)
		}
		for _, backend := range declared {
			if !decodedBackends[backend] {
				t.Errorf("backend %q is declared by the clause but is the decoded_backend "+
					"of NO accept scenario — the vocabulary is not proven complete, and no "+
					"count of scenarios reaches this (fixture v1 shipped exactly this hole "+
					"for `in_process`)", backend)
			}
		}
		if extra := len(decodedBackends) - len(declared); extra > 0 {
			t.Errorf("the accepts decoded %d backend(s) the clause does not declare", extra)
		}
	})

	if accepted != 10 || rejected != 4 {
		t.Fatalf("replayed %d accepts and %d rejects, want 10 and 4", accepted, rejected)
	}
	// A decoder that ignores `backend` and hardcodes `shm` reports shm for every
	// scenario. Only a real read of the field produces these: arrow ×2 and
	// in_process ×2.
	if nonShmDecoded != 4 {
		t.Fatalf("decoded %d non-shm backends, want 4 (arrow and in_process, both codecs): "+
			"a decoder that hardcodes shm satisfies the other six accepts trivially", nonShmDecoded)
	}
	// ...and the mirror image on the encoder side: exactly the non-default
	// scenarios emit the field. A binding that echoes the received value back out
	// emits it for `backend_shm_explicit` and for the explicit null too.
	if fieldReencoded != 4 {
		t.Fatalf("re-encoded the `backend` field in %d scenarios, want 4 (arrow and "+
			"in_process only): an encoder that echoes what it received emits it for "+
			"explicit shm and for the explicit null as well", fieldReencoded)
	}
	// Every refusal came back as a returned error rather than a panic.
	if decodeFamilyRefus != 4 {
		t.Fatalf("%d of %d refusals arrived through the returned-error family, want 4",
			decodeFamilyRefus, rejected)
	}
	// The fixture keeps the two epochs apart in every accept. A corpus that
	// re-collapsed them would satisfy both assertions above with one number and
	// re-open exactly the hole v1 had.
	if epochsDistinct != accepted {
		t.Fatalf("%d of %d accepts carried a frame epoch distinct from the blob epoch; "+
			"a fixture that collapses them makes frame_epoch and blob_epoch "+
			"indistinguishable again", epochsDistinct, accepted)
	}

	// The shared-decode-path ledger (`assertions.anti_vacuity`: two codecs are not
	// two implementations). lazily-go's msgpack decoder bridges into the json
	// decoder's DOM, so both codecs return the SAME concrete error type for a
	// given refusal — which is the evidence of the sharing, recorded here rather
	// than inferred from a scenario count. This assertion is bidirectional on
	// purpose: giving msgpack its own decoder reddens it, and whoever does that
	// gets a red test telling them to rewrite the paragraph at the head of this
	// file instead of banking 14 green scenarios as 14 independent verdicts.
	for kind, seen := range refusalTypes {
		types := map[string]bool{}
		for entry := range seen {
			types[entry[strings.Index(entry, "=")+1:]] = true
		}
		if len(types) != 1 {
			t.Errorf("the %s refusal produced %v across codecs; lazily-go's codecs share "+
				"one decode path, so this ledger entry is now stale — update the header "+
				"comment, do not delete this check", kind, sortedKeys(types))
		}
	}

	// The replay is finished, so every key a discharge names has either been
	// asserted or has not (#lzprosekeyconvention). Armed last so the check runs
	// before the per-block rung-3 teardowns that read the `prose` key it consumes.
	verifyProse(t, blobBackendFixture)
}

// assertSameStringSet compares a fixture-declared vocabulary against the one the
// loop really observed, as a SET: the fixture's order is presentation, and
// pinning it here would make the assertion a restatement of the fixture rather
// than a claim about the run.
func assertSameStringSet(t *testing.T, label string, declared []string, observed map[string]bool) {
	t.Helper()
	want := append([]string{}, declared...)
	sort.Strings(want)
	got := sortedKeys(observed)
	if strings.Join(want, ",") != strings.Join(got, ",") {
		t.Errorf("%s: the fixture declares %v; the loop replayed %v", label, want, got)
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
