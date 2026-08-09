package lazily

import (
	"encoding/json"
	"testing"
)

const capabilityHandshakeFixture = "codec/capability_handshake.json"

func decodeFixtureHandshake(t *testing.T, label string, block map[string]any) CapabilityHandshake {
	t.Helper()
	data, err := json.Marshal(block)
	if err != nil {
		t.Fatalf("%s: encode fixture handshake: %v", label, err)
	}
	handshake, err := DecodeCapabilityHandshakeJSON(data)
	if err != nil {
		t.Fatalf("%s: decode fixture handshake through production codec: %v", label, err)
	}

	assertKey(t, block, "protocol_id", handshake.ProtocolID)
	assertKey(t, block, "protocol_major_version", float64(handshake.ProtocolMajorVersion))
	assertKey(t, block, "codec", handshake.Codec)
	assertKey(t, block, "max_frame_size", float64(handshake.MaxFrameSize))
	assertKey(t, block, "fragmentation_supported", handshake.FragmentationSupported)
	assertKey(t, block, "ordered_reliable", handshake.OrderedReliable)
	assertKey(t, block, "peer_id", float64(handshake.PeerID))
	assertKey(t, block, "session_id", handshake.SessionID)
	features := make([]any, len(handshake.Features))
	for i, feature := range handshake.Features {
		features[i] = feature
	}
	assertKey(t, block, "features", features)
	return handshake
}

func TestCapabilityHandshakeConformance(t *testing.T) {
	path := specPath(capabilityHandshakeFixture)
	data, err := specReadFile(path)
	if err != nil {
		t.Fatalf("canonical capability-handshake fixture not found: %v", err)
	}
	var fixture map[string]any
	mustStrictJSON(t, capabilityHandshakeFixture, data, &fixture)

	consumeFixtureKeys(t, capabilityHandshakeFixture, fixture, "protocol_version", "scenarios")
	assertKey(t, fixture, "protocol_version", float64(1))
	excuseKey(t, fixture, "scenarios",
		"container: every scenario is recorded and replayed through production negotiation below")
	if got := fixture["kind"]; got != "CapabilityHandshake" {
		t.Fatalf("kind = %v, want CapabilityHandshake", got)
	}

	scenarios, ok := fixture["scenarios"].([]any)
	if !ok {
		t.Fatalf("scenarios = %T, want array", fixture["scenarios"])
	}
	for index, raw := range scenarios {
		scenario, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("scenarios[%d] = %T, want object", index, raw)
		}
		id := recordScenarioMap(capabilityHandshakeFixture, index, scenario)
		consumeKeys(t, capabilityHandshakeFixture+" scenarios["+id+"]", scenario,
			"id", "local", "remote", "expected")
		excuseKey(t, scenario, "id", "stable scenario-ledger identifier")

		localBlock := assertKeySub(t, scenario, "local",
			"protocol_id", "protocol_major_version", "codec", "max_frame_size",
			"fragmentation_supported", "ordered_reliable", "peer_id", "session_id", "features")
		remoteBlock := assertKeySub(t, scenario, "remote",
			"protocol_id", "protocol_major_version", "codec", "max_frame_size",
			"fragmentation_supported", "ordered_reliable", "peer_id", "session_id", "features")
		local := decodeFixtureHandshake(t, id+".local", localBlock)
		remote := decodeFixtureHandshake(t, id+".remote", remoteBlock)

		expected := assertKeySub(t, scenario, "expected",
			"compatible", "negotiated_max_frame_size",
			"negotiated_fragmentation_supported", "field")
		negotiation := local.Negotiate(remote)
		assertKey(t, expected, "compatible", negotiation.IsOk())
		if negotiation.IsOk() {
			if negotiation.Capabilities == nil {
				t.Fatalf("%s: successful negotiation retained no capabilities", id)
			}
			assertKey(t, expected, "negotiated_max_frame_size",
				float64(negotiation.Capabilities.MaxFrameSize))
			assertKey(t, expected, "negotiated_fragmentation_supported",
				negotiation.Capabilities.FragmentationSupported)
		} else {
			if negotiation.Capabilities != nil {
				t.Fatalf("%s: failed negotiation retained capabilities", id)
			}
			assertKey(t, expected, "field", negotiation.Check.Field)
		}
	}
}
