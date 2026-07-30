package lazily

// Conformance replay for the shared lazily-spec IPC wire fixtures.
//
// Each fixture under lazily-spec/conformance/ is a self-describing
// {description, protocol_version, kind, assertions, wire} record. This test
// mirrors lazily-dart/test/ipc_test.dart and lazily-kt's conformance replay:
//   1. load the fixture JSON,
//   2. decode the `wire` field into a Go IpcMessage,
//   3. cross-check every `assertions` field against the parsed message,
//   4. re-encode and assert canonical round-trip equality (semantic JSON
//      equality — both sides are normalized to interface{} so key-order and
//      whitespace differences never produce a false negative).
//
// Fixtures are resolved via a relative path helper (../lazily-spec/conformance
// or a local conformance/ copy); the whole suite t.Skip()s when no spec
// checkout is present, matching the Dart pattern.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixture loading + JSON normalize/deep-equal helpers
// ---------------------------------------------------------------------------

// fixtureCandidateDirs lists, in priority order, the directories that may hold
// the conformance fixtures. A local committed copy wins (parity with lazily-kt
// / lazily-dart), then the sibling lazily-spec submodule (dev convenience).
func fixtureCandidateDirs() []string {
	return []string{
		"conformance",
		filepath.Join("..", "lazily-spec", "conformance"),
	}
}

// findFixture returns the on-disk path of a named fixture, or "" if the spec
// checkout is absent.
func findFixture(name string) string {
	for _, dir := range fixtureCandidateDirs() {
		p := filepath.Join(dir, name)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// conformanceFixture is the shared fixture envelope.
type conformanceFixture struct {
	Description     string                     `json:"description"`
	ProtocolVersion int                        `json:"protocol_version"`
	SchemaVersion   string                     `json:"schema_version"`
	Kind            string                     `json:"kind"`
	Assertions      map[string]json.RawMessage `json:"assertions"`
	Wire            json.RawMessage            `json:"wire"`
}

// loadFixture reads and validates a fixture, or skips the test when the spec
// checkout is unavailable.
func loadFixture(t *testing.T, name string) conformanceFixture {
	t.Helper()
	p := findFixture(name)
	if p == "" {
		t.Skipf("conformance fixture %q not found (lazily-spec checkout absent)", name)
	}
	raw, err := specReadFile(p)
	if err != nil {
		t.Fatalf("reading fixture %s: %v", p, err)
	}
	var f conformanceFixture
	mustStrictJSON(t, name, raw, &f)
	if f.ProtocolVersion != 1 {
		t.Fatalf("%s: protocol_version = %d, want 1", name, f.ProtocolVersion)
	}
	return f
}

// normalizeJSON unmarshals JSON bytes into a canonical interface{} tree so two
// encodings can be compared without key-order/whitespace false negatives.
func normalizeJSON(t *testing.T, b []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("normalizing JSON %q: %v", string(b), err)
	}
	return v
}

// jsonEqual reports whether two JSON byte slices are semantically equal.
func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	return reflect.DeepEqual(normalizeJSON(t, a), normalizeJSON(t, b))
}

// assertJSONEqual fails the test unless got and want are semantically equal.
func assertJSONEqual(t *testing.T, got, want []byte, what string) {
	t.Helper()
	if !jsonEqual(t, got, want) {
		t.Fatalf("%s: JSON mismatch\n got: %s\nwant: %s", what, string(got), string(want))
	}
}

// assertValueEqualsRaw marshals a computed Go value and semantically compares it
// to a raw expected JSON assertion value. This keeps int/float/bool/string/array
// comparisons uniform (both sides pass through JSON).
func assertValueEqualsRaw(t *testing.T, actual any, expected json.RawMessage, key string) {
	t.Helper()
	got, err := json.Marshal(actual)
	if err != nil {
		t.Fatalf("assertion %q: marshaling actual value: %v", key, err)
	}
	if !jsonEqual(t, got, expected) {
		t.Fatalf("assertion %q: got %s, want %s", key, string(got), string(expected))
	}
}

// decodeWire decodes a fixture's tagged `wire` envelope into an IpcMessage and
// asserts a byte-for-byte (semantic) round-trip through EncodeJSON/decode.
func decodeWire(t *testing.T, f conformanceFixture) IpcMessage {
	t.Helper()
	msg, err := DecodeIpcMessageJSON(f.Wire)
	if err != nil {
		t.Fatalf("decoding wire (%s): %v", f.Description, err)
	}
	// Re-encode and assert canonical equality with the fixture wire.
	enc, err := msg.EncodeJSON()
	if err != nil {
		t.Fatalf("encoding message (%s): %v", f.Description, err)
	}
	assertJSONEqual(t, enc, f.Wire, "round-trip wire equality")
	// Decode(Encode(msg)) must reproduce the same wire form (idempotent codec).
	msg2, err := DecodeIpcMessageJSON(enc)
	if err != nil {
		t.Fatalf("re-decoding encoded message (%s): %v", f.Description, err)
	}
	enc2, err := msg2.EncodeJSON()
	if err != nil {
		t.Fatalf("re-encoding message (%s): %v", f.Description, err)
	}
	assertJSONEqual(t, enc2, enc, "codec idempotence")
	return msg
}

// ---------------------------------------------------------------------------
// Shape helpers (mirror the Dart nodeStateKind / deltaOpKind helpers)
// ---------------------------------------------------------------------------

func nodeStateKind(t *testing.T, state NodeState) string {
	t.Helper()
	switch state.(type) {
	case NodeStatePayload:
		return "Payload"
	case NodeStateSharedBlob:
		return "SharedBlob"
	case NodeStateOpaque:
		return "Opaque"
	default:
		t.Fatalf("unknown NodeState: %T", state)
		return ""
	}
}

// singleTagKey marshals v (an externally-tagged union member) and returns its
// single object key — the PascalCase variant name.
func singleTagKey(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshaling tagged union %T: %v", v, err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("tagged union %T is not an object: %v", v, err)
	}
	if len(m) != 1 {
		t.Fatalf("tagged union %T must have exactly one key, got %d", v, len(m))
	}
	for k := range m {
		return k
	}
	return ""
}

func deltaOpKind(t *testing.T, op DeltaOp) string { return singleTagKey(t, op) }

func ipcValueKind(t *testing.T, v IpcValue) string { return singleTagKey(t, v) }

// firstSharedBlob returns the ShmBlobRef of the first node's SharedBlob state,
// or nil when the first node is not a shared blob.
func firstSharedBlob(snap Snapshot) *ShmBlobRef {
	if len(snap.Nodes) == 0 {
		return nil
	}
	if sb, ok := snap.Nodes[0].State.(NodeStateSharedBlob); ok {
		return &sb.Blob
	}
	return nil
}

var allDeltaOpKinds = []string{
	"CellSet", "SlotValue", "Invalidate", "NodeAdd", "NodeRemove", "EdgeAdd", "EdgeRemove",
}

// ---------------------------------------------------------------------------
// Assertion cross-checks for the base IPC fixtures
// ---------------------------------------------------------------------------

// snapshotAssertionActual computes the actual value for a snapshot assertion
// key, returning ok=false for an unknown key (so callers can reject it).
func snapshotAssertionActual(t *testing.T, snap Snapshot, key string) (any, bool) {
	t.Helper()
	switch key {
	case "epoch":
		return snap.Epoch, true
	case "node_count":
		return len(snap.Nodes), true
	case "edge_count":
		return len(snap.Edges), true
	case "root_count":
		return len(snap.Roots), true
	case "first_node_type_tag":
		return snap.Nodes[0].TypeTag, true
	case "first_node_state_kind":
		return nodeStateKind(t, snap.Nodes[0].State), true
	case "has_opaque_node":
		for _, n := range snap.Nodes {
			if _, ok := n.State.(NodeStateOpaque); ok {
				return true, true
			}
		}
		return false, true
	case "opaque_node_id":
		var id *NodeId
		for _, n := range snap.Nodes {
			if _, ok := n.State.(NodeStateOpaque); ok {
				v := n.Node
				id = &v
				break
			}
		}
		return id, true
	case "blob_offset":
		return firstSharedBlob(snap).Offset, true
	case "blob_len":
		return firstSharedBlob(snap).Len, true
	case "blob_epoch":
		return firstSharedBlob(snap).Epoch, true
	default:
		return nil, false
	}
}

func assertSnapshotAssertions(t *testing.T, snap Snapshot, assertions map[string]json.RawMessage) {
	t.Helper()
	for key, expected := range assertions {
		actual, ok := snapshotAssertionActual(t, snap, key)
		if !ok {
			t.Fatalf("unknown snapshot assertion key: %q", key)
		}
		assertValueEqualsRaw(t, actual, expected, "snapshot:"+key)
	}
}

func assertDeltaAssertions(t *testing.T, d Delta, assertions map[string]json.RawMessage) {
	t.Helper()
	for key, expected := range assertions {
		var actual any
		switch key {
		case "base_epoch":
			actual = d.BaseEpoch
		case "epoch":
			actual = d.Epoch
		case "is_sequential":
			actual = d.IsNextAfter(d.BaseEpoch)
		case "op_count":
			actual = len(d.Ops)
		case "has_all_op_variants":
			present := map[string]bool{}
			for _, op := range d.Ops {
				present[deltaOpKind(t, op)] = true
			}
			all := true
			for _, k := range allDeltaOpKinds {
				if !present[k] {
					all = false
					break
				}
			}
			actual = all
		case "resync_after_epoch_10":
			actual = d.ApplyStatus(10).IsResyncRequired()
		case "first_op_kind":
			actual = deltaOpKind(t, d.Ops[0])
		case "first_op_payload_kind":
			var payload IpcValue
			switch op := d.Ops[0].(type) {
			case DeltaOpCellSet:
				payload = op.Payload
			case DeltaOpSlotValue:
				payload = op.Payload
			default:
				t.Fatalf("first op %T has no payload", d.Ops[0])
			}
			actual = ipcValueKind(t, payload)
		case "first_op_payload_backend":
			var payload IpcValue
			switch op := d.Ops[0].(type) {
			case DeltaOpCellSet:
				payload = op.Payload
			case DeltaOpSlotValue:
				payload = op.Payload
			default:
				t.Fatalf("first op %T has no payload", d.Ops[0])
			}
			sb, ok := payload.(IpcValueSharedBlob)
			if !ok {
				t.Fatalf("first op payload %T is not a SharedBlob", payload)
			}
			actual = string(sb.Blob.Backend.Normalized())
		default:
			t.Fatalf("unknown delta assertion key: %q", key)
		}
		assertValueEqualsRaw(t, actual, expected, "delta:"+key)
	}
}

// ---------------------------------------------------------------------------
// Base IPC conformance fixtures
// ---------------------------------------------------------------------------

func TestIPCConformanceFixtures(t *testing.T) {
	fixtures := []string{
		"snapshot_minimal.json",
		"snapshot_multi_node.json",
		"snapshot_shared_blob.json",
		"delta_sequential.json",
		"delta_non_sequential.json",
		"delta_shared_blob.json",
		"delta_zero_copy_arrow.json",
	}
	for _, name := range fixtures {
		name := name
		t.Run(name, func(t *testing.T) {
			f := loadFixture(t, name)
			msg := decodeWire(t, f)
			switch m := msg.(type) {
			case IpcMessageSnapshot:
				if f.Kind != "Snapshot" {
					t.Fatalf("%s: decoded Snapshot but kind=%q", name, f.Kind)
				}
				assertSnapshotAssertions(t, m.Value, f.Assertions)
			case IpcMessageDelta:
				if f.Kind != "Delta" {
					t.Fatalf("%s: decoded Delta but kind=%q", name, f.Kind)
				}
				assertDeltaAssertions(t, m.Value, f.Assertions)
			default:
				t.Fatalf("%s: unexpected message type %T", name, msg)
			}
		})
	}
}

// TestIPCConformanceAssertionDrift confirms the assertion cross-check is load
// bearing: a drifted metadata value fails semantic equality, and an unknown
// assertion key is rejected — mirroring the Dart harness's drift guard, but via
// the value-returning path so no failing t.Fatalf machinery is needed.
func TestIPCConformanceAssertionDrift(t *testing.T) {
	f := loadFixture(t, "snapshot_minimal.json")
	msg := decodeWire(t, f)
	snap := msg.(IpcMessageSnapshot).Value

	// The real node_count is 1, so the drifted assertion (999) must not match.
	actual, ok := snapshotAssertionActual(t, snap, "node_count")
	if !ok {
		t.Fatalf("node_count should be a known assertion key")
	}
	got, err := json.Marshal(actual)
	if err != nil {
		t.Fatalf("marshaling node_count: %v", err)
	}
	if jsonEqual(t, got, json.RawMessage("999")) {
		t.Fatalf("drifted node_count (999) unexpectedly matched actual %s", string(got))
	}

	// An unknown assertion key is rejected (new metadata can't be silently ignored).
	if _, ok := snapshotAssertionActual(t, snap, "unexpected_field"); ok {
		t.Fatalf("unknown assertion key unexpectedly accepted")
	}
}

// ---------------------------------------------------------------------------
// agent-doc state-projection conformance fixtures
// ---------------------------------------------------------------------------

// agentDocTypeTagVocabulary loads the pinned eight-value type_tag vocabulary
// from the canonical schema when the spec checkout is present, else falls back
// to the pinned 1.0.0 set (mirroring lazily-kt).
func agentDocTypeTagVocabulary(t *testing.T) map[string]bool {
	t.Helper()
	fallback := []string{
		"agent_doc.document.baseline",
		"agent_doc.queue",
		"agent_doc.queue.head",
		"agent_doc.closeout.cycle",
		"agent_doc.transport.patch",
		"agent_doc.supervisor.owner",
		"agent_doc.route",
		"agent_doc.proof.marker",
	}
	var tags []string
	for _, dir := range []string{
		filepath.Join("schemas"),
		filepath.Join("..", "lazily-spec", "schemas"),
	} {
		p := filepath.Join(dir, "agent-doc-state.json")
		raw, err := specReadFile(p)
		if err != nil {
			continue
		}
		var schema struct {
			Defs struct {
				TypeTag struct {
					Enum []string `json:"enum"`
				} `json:"TypeTag"`
			} `json:"$defs"`
		}
		if err := json.Unmarshal(raw, &schema); err == nil && len(schema.Defs.TypeTag.Enum) > 0 {
			tags = schema.Defs.TypeTag.Enum
			break
		}
	}
	if tags == nil {
		tags = fallback
	}
	set := make(map[string]bool, len(tags))
	for _, tag := range tags {
		set[tag] = true
	}
	return set
}

// payloadBytes extracts the concrete byte payload of a NodeState (Payload) or
// IpcValue (Inline).
func payloadBytesFromState(t *testing.T, state NodeState) []byte {
	t.Helper()
	p, ok := state.(NodeStatePayload)
	if !ok {
		t.Fatalf("expected NodeStatePayload, got %T", state)
	}
	return p.Bytes
}

func payloadBytesFromValue(t *testing.T, v IpcValue) []byte {
	t.Helper()
	inline, ok := v.(IpcValueInline)
	if !ok {
		t.Fatalf("expected IpcValueInline, got %T", v)
	}
	return inline.Bytes
}

// decodePayloadPhase parses serde_json(struct) bytes and returns the "phase"
// field.
func decodePayloadPhase(t *testing.T, bytesData []byte) string {
	t.Helper()
	var obj struct {
		Phase string `json:"phase"`
	}
	if err := json.Unmarshal(bytesData, &obj); err != nil {
		t.Fatalf("decoding payload phase: %v (payload=%q)", err, string(bytesData))
	}
	return obj.Phase
}

func rawString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("expected JSON string, got %s: %v", string(raw), err)
	}
	return s
}

func rawStringSet(t *testing.T, raw json.RawMessage) map[string]bool {
	t.Helper()
	var arr []string
	if err := json.Unmarshal(raw, &arr); err != nil {
		t.Fatalf("expected JSON string array, got %s: %v", string(raw), err)
	}
	set := make(map[string]bool, len(arr))
	for _, s := range arr {
		set[s] = true
	}
	return set
}

func TestIPCConformanceAgentDocSnapshot(t *testing.T) {
	f := loadFixture(t, filepath.Join("agent-doc", "snapshot_agent_doc_state.json"))
	if f.Kind != "Snapshot" {
		t.Fatalf("kind = %q, want Snapshot", f.Kind)
	}
	msg := decodeWire(t, f)
	snap := msg.(IpcMessageSnapshot).Value
	vocab := agentDocTypeTagVocabulary(t)

	actualTags := map[string]bool{}
	for _, n := range snap.Nodes {
		actualTags[n.TypeTag] = true
	}

	for key, expected := range f.Assertions {
		switch key {
		case "epoch":
			assertValueEqualsRaw(t, snap.Epoch, expected, "agentdoc-snapshot:epoch")
		case "node_count":
			assertValueEqualsRaw(t, len(snap.Nodes), expected, "agentdoc-snapshot:node_count")
		case "edge_count":
			assertValueEqualsRaw(t, len(snap.Edges), expected, "agentdoc-snapshot:edge_count")
		case "root_count":
			assertValueEqualsRaw(t, len(snap.Roots), expected, "agentdoc-snapshot:root_count")
		case "type_tags":
			want := rawStringSet(t, expected)
			if !reflect.DeepEqual(want, actualTags) {
				t.Fatalf("type_tags mismatch: got %v, want %v", actualTags, want)
			}
		case "all_type_tags_in_vocabulary":
			all := true
			for tag := range actualTags {
				if !vocab[tag] {
					all = false
					break
				}
			}
			assertValueEqualsRaw(t, all, expected, "agentdoc-snapshot:all_type_tags_in_vocabulary")
		case "cycle_phase":
			node := findNodeByTag(t, snap, "agent_doc.closeout.cycle")
			phase := decodePayloadPhase(t, payloadBytesFromState(t, node.State))
			if phase != rawString(t, expected) {
				t.Fatalf("cycle_phase: got %q, want %s", phase, string(expected))
			}
		case "queue_head_phase":
			node := findNodeByTag(t, snap, "agent_doc.queue.head")
			phase := decodePayloadPhase(t, payloadBytesFromState(t, node.State))
			if phase != rawString(t, expected) {
				t.Fatalf("queue_head_phase: got %q, want %s", phase, string(expected))
			}
		default:
			t.Fatalf("unknown agent-doc snapshot assertion key: %q", key)
		}
	}
}

func TestIPCConformanceAgentDocDelta(t *testing.T) {
	f := loadFixture(t, filepath.Join("agent-doc", "delta_agent_doc_state.json"))
	if f.Kind != "Delta" {
		t.Fatalf("kind = %q, want Delta", f.Kind)
	}
	msg := decodeWire(t, f)
	d := msg.(IpcMessageDelta).Value
	vocab := agentDocTypeTagVocabulary(t)

	addedTags := map[string]bool{}
	for _, op := range d.Ops {
		if add, ok := op.(DeltaOpNodeAdd); ok {
			addedTags[add.TypeTag] = true
		}
	}

	for key, expected := range f.Assertions {
		switch key {
		case "base_epoch":
			assertValueEqualsRaw(t, d.BaseEpoch, expected, "agentdoc-delta:base_epoch")
		case "epoch":
			assertValueEqualsRaw(t, d.Epoch, expected, "agentdoc-delta:epoch")
		case "op_count":
			assertValueEqualsRaw(t, len(d.Ops), expected, "agentdoc-delta:op_count")
		case "added_type_tags":
			want := rawStringSet(t, expected)
			if !reflect.DeepEqual(want, addedTags) {
				t.Fatalf("added_type_tags mismatch: got %v, want %v", addedTags, want)
			}
		case "all_type_tags_in_vocabulary":
			all := true
			for tag := range addedTags {
				if !vocab[tag] {
					all = false
					break
				}
			}
			assertValueEqualsRaw(t, all, expected, "agentdoc-delta:all_type_tags_in_vocabulary")
		case "cycle_phase_after":
			phase := decodePayloadPhase(t, payloadBytesFromValue(t, cellSetPayload(t, d, 102)))
			if phase != rawString(t, expected) {
				t.Fatalf("cycle_phase_after: got %q, want %s", phase, string(expected))
			}
		case "queue_head_phase_after":
			phase := decodePayloadPhase(t, payloadBytesFromValue(t, cellSetPayload(t, d, 103)))
			if phase != rawString(t, expected) {
				t.Fatalf("queue_head_phase_after: got %q, want %s", phase, string(expected))
			}
		default:
			t.Fatalf("unknown agent-doc delta assertion key: %q", key)
		}
	}

	// The delta is a coalesced jump (base_epoch 3 -> epoch 6), not sequential.
	if d.IsNextAfter(d.BaseEpoch) {
		t.Fatalf("agent-doc delta should not be sequential after its base epoch")
	}
}

func findNodeByTag(t *testing.T, snap Snapshot, tag string) NodeSnapshot {
	t.Helper()
	for _, n := range snap.Nodes {
		if n.TypeTag == tag {
			return n
		}
	}
	t.Fatalf("no node with type_tag %q", tag)
	return NodeSnapshot{}
}

func cellSetPayload(t *testing.T, d Delta, node NodeId) IpcValue {
	t.Helper()
	for _, op := range d.Ops {
		if cs, ok := op.(DeltaOpCellSet); ok && cs.Node == node {
			return cs.Payload
		}
	}
	t.Fatalf("no CellSet op targeting node %d", node)
	return nil
}
