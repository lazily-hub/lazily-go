package lazily

// NodeId exact-representation bound (#lzspecdecoderbound).
//
// protocol.md § NodeId / PeerId stated the 2^53 bound as a PRODUCER obligation
// and said nothing about what a decoder does when it receives a violation. The
// clause is now normative: a decoder that cannot represent a received
// identifier exactly MUST reject the frame rather than round it.
//
// lazily-go's NodeId is an int64, so its exact range is [0, 2^63) — narrower
// than the u64 wire type. That is conforming: the clause does not require a
// binding to widen, only to refuse rather than substitute. This runner asserts
// exactly that split — 2^53-1 and 2^53+1 decode exactly, u64::MAX is refused —
// and the refusal has to be an ERROR, never a decoded neighbour.
//
// The fixture carries wire frames as raw text (json) and hex (msgpack) and its
// expected identifier as a decimal string, because the fixture is itself JSON:
// encoding/json decodes a bare 9007199254740993 into `any` as a float64 and
// rounds it to 9007199254740992 while loading. The runner would then compare a
// rounded expectation against a rounded decode and pass.

import (
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
)

const nodeIDExactRangeFixture = "codec/nodeid_exact_range.json"

// maxExactNodeID is the largest identifier lazily-go's int64 NodeId represents.
const maxExactNodeID = uint64(1)<<63 - 1

// decodeNodeIDScenario decodes a scenario's wire frame with the codec it names.
// An error is a conforming outcome for an over-range identifier; the caller
// decides, because that split is the whole point of the fixture.
func decodeNodeIDScenario(t *testing.T, scenario, expect map[string]any) (IpcMessage, error) {
	t.Helper()
	// Read off the raw carriage, never restated from the label itself
	// (#lznullformblind).
	codec := scenarioWireCodec(t, scenario)
	assertKey(t, scenario, "codec", codec)
	switch codec {
	case "json":
		excuseKey(t, scenario, "wire_json",
			"the frame under test, not an expectation: it is the runner's INPUT and is "+
				"proven by the decoded values asserted below")
		raw := []byte(scenario["wire_json"].(string))
		assertKey(t, expect, "wire_input_fnv1a64", wireInputFNV1a64(raw))
		return DecodeIpcMessageJSON(raw)
	case "msgpack":
		excuseKey(t, scenario, "wire_msgpack_hex",
			"the frame under test, not an expectation: it is the runner's INPUT and is "+
				"proven by the decoded values asserted below")
		raw, err := hex.DecodeString(scenario["wire_msgpack_hex"].(string))
		if err != nil {
			t.Fatalf("wire_msgpack_hex is not hex: %v", err)
		}
		assertKey(t, expect, "wire_input_fnv1a64", wireInputFNV1a64(raw))
		return DecodeIpcMessageMsgpack(raw)
	default:
		t.Fatalf("unknown codec %q", codec)
		return nil, nil
	}
}

func TestNodeIdExactRangeConformance(t *testing.T) {
	fixture, ok := loadCodecFixture(t, nodeIDExactRangeFixture)
	if !ok {
		return
	}
	consumeFixtureKeys(t, nodeIDExactRangeFixture, fixture,
		"protocol_version", "assertions", "scenarios")
	assertKey(t, fixture, "protocol_version", float64(1))
	excuseKey(t, fixture, "assertions", "container: asserted key-by-key immediately below")
	excuseKey(t, fixture, "scenarios", "container: every entry is replayed and asserted in the loop below")
	if got := fixture["kind"]; got != "NodeIdExactRange" {
		t.Fatalf("kind = %v, want NodeIdExactRange", got)
	}

	assertions := fixture["assertions"].(map[string]any)
	consumeKeys(t, nodeIDExactRangeFixture+".assertions", assertions,
		"prose", "clause", "required_of_binding", "codecs", "scenario_count",
		"wire_encoding", "outcomes", "anti_vacuity", "generator")
	assertKey(t, assertions, "required_of_binding", "MUST")
	excuseKey(t, assertions, "generator",
		"provenance: the path of the script that emitted this file, not a statement "+
			"about the decoder; the corpus does not declare it prose")

	// `outcomes` is NOT prose (#lzprosekeyconvention): it maps a vocabulary to
	// English glosses, and the assertion is the KEY SET, discharged by this key's
	// own assertion against the outcomes the loop really dispatched on. Both
	// vocabularies are asserted after the loop, against what the replay really
	// did — compared to hand-written literals they are green over a runner that
	// decodes nothing, so naming them in a discharge would discharge nothing.
	outcomesReplayed := map[string]bool{}
	codecsReplayed := map[string]bool{}

	// The three paragraphs the corpus declares in `assertions.prose`. Each names
	// the executable keys this run asserts that carry its obligation.
	proseKey(t, assertions, "clause",
		// "reject rather than round, truncate, saturate or wrap" — the decimal
		// rendering is the only comparison that sees a neighbouring identifier.
		"node_id_decimal", "root_id_decimal", "outcome")
	proseKey(t, assertions, "wire_encoding",
		// Executable proof that the exact raw text / decoded-hex byte slice
		// reaches the library decoder rather than a reconstructed proxy.
		"wire_input_fnv1a64")
	proseKey(t, assertions, "anti_vacuity",
		// The two `exact` scenarios are the control: a runner that never decodes
		// satisfies `exact_or_reject` alone. `node_id_decimal` and
		// `root_id_decimal` are rendered from the decoded frame, so they are the
		// keys only a real decode can produce. `scenario_count` joins them now
		// that it counts the scenarios this run really replayed rather than the
		// length of the fixture's own array (#lznullformblind) — as a
		// self-comparison it was exactly the runner-that-decodes-nothing pass the
		// paragraph names, so naming it discharged nothing.
		"outcome", "outcomes", "node_id_decimal", "root_id_decimal", "scenario_count")

	scenarios := fixture["scenarios"].([]any)

	// Anti-vacuity: a runner that treats every frame as rejected satisfies the
	// `exact_or_reject` scenarios trivially. `accepted` is what proves this one
	// decoded anything at all, and the assertion at the end pins the exact count
	// Go's int64 range implies rather than "at least one".
	accepted := 0
	// `scenario_count` is asserted AFTER the loop, against the scenarios this
	// run really replayed (#lznullformblind). It used to compare the fixture's
	// own count to the length of the fixture's own array — green over a runner
	// that decodes nothing, which is the vacuity `anti_vacuity` exists to name.
	scenariosReplayed := 0

	for _, sv := range scenarioViews(nodeIDExactRangeFixture, scenarios) {
		id := sv.Label()
		// Rung 4 books on the first PAYLOAD read (#lzscenariobodyskip), not on
		// the label: a loop that reads `id` and skips has replayed nothing.
		scenario := sv.Map()
		scenariosReplayed++

		consumeKeys(t, id, scenario,
			"id", "name", "codec", "variant", "description", "expect",
			"wire_json", "wire_msgpack_hex")
		assertKey(t, scenario, "name", scenario["id"])
		excuseKey(t, scenario, "id", "the ledger key this loop records; it names the scenario rather than asserting it")
		excuseKey(t, scenario, "expect", "container: asserted key-by-key against the DECODED frame below")
		codecsReplayed[scenario["codec"].(string)] = true

		expect := scenario["expect"].(map[string]any)
		consumeKeys(t, id+".expect", expect,
			"outcome", "node_id_decimal", "root_id_decimal", "epoch",
			"node_count", "type_tag", "payload", "wire_input_fnv1a64")

		expected, err := strconv.ParseUint(expect["node_id_decimal"].(string), 10, 64)
		if err != nil {
			t.Fatalf("%s: node_id_decimal is not a u64: %v", id, err)
		}
		representable := expected <= maxExactNodeID

		// `outcome` is the corpus-wide statement of what a decoder may do. Go
		// reads it as a constraint on the fixture rather than on itself: an
		// `exact` scenario the binding cannot represent would be a fixture bug.
		assertKeyWith(t, expect, "outcome", func(want any) {
			t.Helper()
			outcome := want.(string)
			outcomesReplayed[outcome] = true
			switch outcome {
			case "exact":
				if !representable {
					t.Errorf("%s: fixture marks an unrepresentable identifier `exact`", id)
				}
			case "exact_or_reject":
			default:
				t.Errorf("%s: unknown outcome %q", id, outcome)
			}
		})

		message, err := decodeNodeIDScenario(t, scenario, expect)
		if err != nil {
			if representable {
				t.Fatalf("%s: lazily-go represents %d exactly, so this frame must decode; got %v",
					id, expected, err)
			}
			// Refusal is the conforming outcome for an over-range identifier.
			// The keys below belong to the accept branch; say so rather than
			// leaving them silently unasserted.
			excuseKey(t, scenario, "variant",
				"the frame was REFUSED, so there is no decoded message whose variant could be "+
					"compared; the variant is asserted by the scenarios inside [0, 2^63)")
			for _, key := range []string{"node_id_decimal", "root_id_decimal", "epoch", "node_count", "type_tag", "payload"} {
				excuseKey(t, expect, key,
					"lazily-go's int64 NodeId cannot represent this identifier, so the frame "+
						"is REFUSED — the conforming outcome. These keys are asserted by the "+
						"scenarios inside [0, 2^63), and the refusal itself is the assertion here.")
			}
			continue
		}
		if !representable {
			t.Fatalf("%s: lazily-go cannot represent %s exactly; decoding it means the "+
				"identifier was rounded, truncated, or wrapped", id, expect["node_id_decimal"])
		}
		accepted++

		snapshot, isSnapshot := message.(IpcMessageSnapshot)
		if !isSnapshot {
			t.Fatalf("%s: fixture declares the Snapshot variant", id)
		}
		assertKey(t, scenario, "variant", codecVariant(t, message))
		body := snapshot.Value

		assertKey(t, expect, "epoch", float64(body.Epoch))
		assertKey(t, expect, "node_count", float64(len(body.Nodes)))

		node := body.Nodes[0]
		// The discriminating assertion. A rounding decoder yields a NEIGHBOURING
		// identifier that decodes cleanly and addresses a different node, so
		// comparing the decimal rendering is the only check that sees it.
		assertKey(t, expect, "node_id_decimal", strconv.FormatInt(node.Node, 10))
		assertKey(t, expect, "type_tag", node.TypeTag)
		payload, isPayload := node.State.(NodeStatePayload)
		if !isPayload {
			t.Fatalf("%s: fixture carries a Payload node state", id)
		}
		assertKey(t, expect, "payload", ints(payload.Bytes))
		if len(body.Roots) != 1 {
			t.Fatalf("%s: roots = %v, want one entry", id, body.Roots)
		}
		assertKey(t, expect, "root_id_decimal", strconv.FormatInt(body.Roots[0], 10))
	}

	// Four of the six scenarios (2^53-1 and 2^53+1, in both codecs) are inside
	// int64; the two u64::MAX ones are not. A change to either half of that
	// split is a real behaviour change and has to be read here, not absorbed.
	// The count the replay produced, not the length of the array it read
	// (#lznullformblind). Asserted BEFORE the accept-count gate below, so a short
	// replay is reported against the corpus's own number rather than swallowed by
	// a runner-side literal that fatals first.
	assertKey(t, assertions, "scenario_count", float64(scenariosReplayed))

	if accepted != 4 {
		t.Fatalf("accepted %d scenarios, want 4: lazily-go's exact range is [0, 2^63)", accepted)
	}

	assertKeyWith(t, assertions, "codecs", func(want any) {
		t.Helper()
		assertSameStringSet(t, "codecs", stringSlice(want), codecsReplayed)
	})

	// The vocabulary, asserted as a SET against the outcomes the loop really
	// dispatched on. The glosses are prose nested inside a data key, so the
	// parent key's own assertion discharges them (#lzprosekeyconvention).
	//
	// Routed through the tracker's key-set entry point rather than a hand-rolled
	// comparison here (#lzsubblockkeyset): the set difference is the same one, but
	// the tracker now RECORDS that this object-valued key got one, so a future site
	// that reaches for plain assertKeyWith on an object value fails at teardown
	// instead of silently comparing sub-fields it happened to name.
	assertKeySet(t, assertions, "outcomes", outcomesReplayed, func(name string, gloss any) {
		t.Helper()
		if text, _ := gloss.(string); strings.TrimSpace(text) == "" {
			t.Errorf("assertions.outcomes[%q] carries no gloss", name)
		}
	})

	// The replay is finished, so every key a discharge names has either been
	// asserted or has not (#lzprosekeyconvention).
	verifyProse(t, nodeIDExactRangeFixture)
}
