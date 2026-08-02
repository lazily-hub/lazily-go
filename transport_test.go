package lazily

// Zero-copy transport conformance (#lzzcpy) — mirrors lazily-rs/src/transport.rs
// tests. Each test names the backend-agnostic law it exercises (proven in
// lazily-formal/LazilyFormal/ZeroCopyTransport.lean).

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func viewEq(view []byte, ok bool, want []byte) bool {
	return ok && bytes.Equal(view, want)
}

// resolve_write identity: bytes spilled to a backend resolve zero-copy, and the
// descriptor is tagged with the backend's kind.
func TestInProcessResolveWrite(t *testing.T) {
	b := NewInProcessBackend()
	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	desc, err := b.Write(payload)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if desc.Backend != BackendInProcess {
		t.Fatalf("descriptor backend = %q, want in_process", desc.Backend)
	}
	if v, ok := b.ReadView(desc); !viewEq(v, ok, payload) {
		t.Fatalf("read_view = %v ok=%v, want %v", v, ok, payload)
	}
}

func TestArrowResolveWrite(t *testing.T) {
	b := NewArrowBackend()
	payload := []byte{10, 20, 30, 40}
	desc, err := b.Write(payload)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if desc.Backend != BackendArrow {
		t.Fatalf("descriptor backend = %q, want arrow", desc.Backend)
	}
	if v, ok := b.ReadView(desc); !viewEq(v, ok, payload) {
		t.Fatalf("read_view = %v ok=%v, want %v", v, ok, payload)
	}
}

// Zero-copy: the resolved view aliases the backend's own storage (no copy). A
// mutation of the backing arena entry is observable through a previously
// resolved view — proving the view is not a defensive copy.
func TestInProcessReadViewAliasesStorage(t *testing.T) {
	arena := NewShmBlobArena(0)
	b := InProcessBackendFromArena(arena)
	desc, _ := b.Write([]byte{7, 7, 7, 7})
	v1, ok := b.ReadView(desc)
	if !ok {
		t.Fatalf("read_view failed")
	}
	v2, _ := b.ReadView(desc)
	// Both views must share the same backing array (same pointer/len/cap).
	if &v1[0] != &v2[0] {
		t.Fatalf("read_view returned distinct buffers; expected a shared zero-copy view")
	}
}

// Backend isolation (resolve_wrong_backend): an in_process descriptor does not
// resolve in an empty router; a shm-kind descriptor does not resolve in an
// in_process-only router.
func TestBackendIsolation(t *testing.T) {
	inproc := NewInProcessBackend()
	desc, _ := inproc.Write([]byte{9, 9, 9})

	empty := NewBlobRouter() // no backends registered
	if _, ok := empty.ReadView(desc); ok {
		t.Fatalf("empty router resolved a descriptor")
	}

	router := NewBlobRouter().Register(inproc)
	if _, ok := router.ReadView(desc); !ok {
		t.Fatalf("router did not resolve its own in_process descriptor")
	}

	// A shm-kind descriptor with no shm backend registered → not resolved.
	shmDesc := desc.WithBackend(BackendShm)
	if _, ok := router.ReadView(shmDesc); ok {
		t.Fatalf("router resolved a shm descriptor with no shm backend")
	}
}

// ABA generation safety (resolve_stale_generation): a stale generation rejects.
func TestStaleGenerationRejects(t *testing.T) {
	b := NewInProcessBackend()
	desc, _ := b.Write([]byte{1, 2, 3})
	stale := desc
	stale.Generation++
	if _, ok := b.ReadView(stale); ok {
		t.Fatalf("stale generation resolved")
	}
}

// Checksum integrity (resolve_corrupt_checksum): a corrupted checksum rejects.
func TestCorruptChecksumRejects(t *testing.T) {
	b := NewInProcessBackend()
	desc, _ := b.Write([]byte{4, 5, 6})
	corrupt := desc
	corrupt.Checksum++
	if _, ok := b.ReadView(corrupt); ok {
		t.Fatalf("corrupt checksum resolved")
	}
}

// Epoch advance invalidates prior descriptors.
func TestEpochAdvanceInvalidates(t *testing.T) {
	b := NewInProcessBackend()
	desc, _ := b.Write([]byte{7, 8})
	if _, ok := b.ReadView(desc); !ok {
		t.Fatalf("fresh descriptor did not resolve")
	}
	b.AdvanceEpoch()
	if _, ok := b.ReadView(desc); ok {
		t.Fatalf("descriptor resolved after epoch advance")
	}
}

// End-to-end transport round-trip (transport_roundtrip): spill a large Inline
// payload → the message now carries a descriptor; resolve via a BlobRouter
// yields the original bytes.
func TestSpillResolveRoundTrip(t *testing.T) {
	b := NewInProcessBackend()
	big := bytes.Repeat([]byte{0x5A}, 500)
	msg := IpcMessageDelta{Value: Delta{
		BaseEpoch: 1,
		Epoch:     2,
		Ops:       []DeltaOp{DeltaOpSlotValue{Node: 7, Payload: IpcValueInline{Bytes: big}}},
	}}

	spilled, total := SpillMessage(msg, b, 64)
	if total != len(big) {
		t.Fatalf("spilled %d bytes, want %d", total, len(big))
	}

	router := NewBlobRouter().Register(b)
	delta := spilled.(IpcMessageDelta).Value
	sv := delta.Ops[0].(DeltaOpSlotValue)
	if _, ok := sv.Payload.(IpcValueSharedBlob); !ok {
		t.Fatalf("payload was not spilled to a SharedBlob: %T", sv.Payload)
	}
	if v, ok := router.Resolve(sv.Payload); !viewEq(v, ok, big) {
		t.Fatalf("resolved payload mismatch (ok=%v)", ok)
	}
}

// Spill across Snapshot NodeState + CrdtSync op state.
func TestSpillSnapshotAndCrdt(t *testing.T) {
	b := NewInProcessBackend()
	big := bytes.Repeat([]byte{0xAB}, 300)

	snap := IpcMessageSnapshot{Value: Snapshot{
		Epoch: 1,
		Nodes: []NodeSnapshot{{Node: 1, TypeTag: "blob", State: NodeStatePayload{Bytes: big}}},
		Roots: []NodeId{1},
	}}
	spilledSnap, total := SpillMessage(snap, b, 64)
	if total != len(big) {
		t.Fatalf("snapshot spilled %d, want %d", total, len(big))
	}
	if _, ok := spilledSnap.(IpcMessageSnapshot).Value.Nodes[0].State.(NodeStateSharedBlob); !ok {
		t.Fatalf("snapshot node state not spilled")
	}

	stamp := WireStamp{WallTime: 1, Logical: 0, Peer: 1}
	sync := IpcMessageCrdtSync{Value: CrdtSync{
		Frontier: []StampFrontierEntry{{Peer: 1, Stamp: stamp}},
		Ops:      []CrdtOp{{Node: 1, Stamp: stamp, State: IpcValueInline{Bytes: big}}},
	}}
	spilledSync, total := SpillMessage(sync, b, 64)
	if total != len(big) {
		t.Fatalf("crdt spilled %d, want %d", total, len(big))
	}
	if _, ok := spilledSync.(IpcMessageCrdtSync).Value.Ops[0].State.(IpcValueSharedBlob); !ok {
		t.Fatalf("crdt op state not spilled")
	}
}

// Sub-threshold payloads stay inline.
func TestSubThresholdStaysInline(t *testing.T) {
	b := NewInProcessBackend()
	msg := IpcMessageDelta{Value: Delta{
		BaseEpoch: 1,
		Epoch:     2,
		Ops:       []DeltaOp{DeltaOpSlotValue{Node: 1, Payload: IpcValueInline{Bytes: []byte{1, 2, 3}}}},
	}}
	spilled, total := SpillMessage(msg, b, 64)
	if total != 0 {
		t.Fatalf("sub-threshold payload spilled %d bytes", total)
	}
	sv := spilled.(IpcMessageDelta).Value.Ops[0].(DeltaOpSlotValue)
	if _, ok := sv.Payload.(IpcValueInline); !ok {
		t.Fatalf("sub-threshold payload was spilled: %T", sv.Payload)
	}
}

// Multi-backend routing: an arrow descriptor routes to the arrow backend, an
// in_process descriptor to the in_process backend.
func TestMultiBackendRouting(t *testing.T) {
	inproc := NewInProcessBackend()
	arrow := NewArrowBackend()
	inprocDesc, _ := inproc.Write([]byte("inproc bytes"))
	arrowDesc, _ := arrow.Write([]byte("arrow bytes"))

	router := NewBlobRouter().Register(inproc).Register(arrow)
	if v, ok := router.ReadView(inprocDesc); !viewEq(v, ok, []byte("inproc bytes")) {
		t.Fatalf("in_process routing failed")
	}
	if v, ok := router.ReadView(arrowDesc); !viewEq(v, ok, []byte("arrow bytes")) {
		t.Fatalf("arrow routing failed")
	}
}

// Arrow IPC stream composition: the descriptor's bytes are an Arrow IPC stream
// the receiver reads zero-copy (here a stand-in "ARROW1\0\0" magic).
func TestArrowIPCStreamBytes(t *testing.T) {
	arrow := NewArrowBackend()
	ipcStream := []byte{0x41, 0x52, 0x52, 0x4f, 0x57, 0x31, 0x00, 0x00}
	desc, _ := arrow.Write(ipcStream)
	if desc.Backend != BackendArrow {
		t.Fatalf("backend = %q, want arrow", desc.Backend)
	}
	if v, ok := arrow.ReadView(desc); !viewEq(v, ok, ipcStream) {
		t.Fatalf("arrow stream read_view mismatch")
	}
}

// ---------------------------------------------------------------------------
// Wire superset: the optional `backend` discriminator is omitted when default
// (shm) and emitted for non-default backends; deserialization is a strict
// superset of the legacy backend-absent descriptor.
// ---------------------------------------------------------------------------

func TestBackendWireOmittedWhenDefault(t *testing.T) {
	// A default (shm) descriptor omits `backend` — byte-identical to legacy.
	shm := ShmBlobRef{Offset: 8, Len: 4, Generation: 1, Epoch: 0, Checksum: 42}
	out, err := json.Marshal(shm)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bytes.Contains(out, []byte("backend")) {
		t.Fatalf("default descriptor emitted a backend field: %s", out)
	}

	// An explicit-shm descriptor is normalized to the default and still omitted.
	explicitShm := shm.WithBackend(BackendShm)
	out2, _ := json.Marshal(explicitShm)
	if !bytes.Equal(out, out2) {
		t.Fatalf("explicit shm (%s) differs from default (%s)", out2, out)
	}

	// A non-default backend is emitted.
	arrow := shm.WithBackend(BackendArrow)
	out3, _ := json.Marshal(arrow)
	if !bytes.Contains(out3, []byte(`"backend":"arrow"`)) {
		t.Fatalf("arrow descriptor missing backend field: %s", out3)
	}
}

func TestBackendWireRoundTrip(t *testing.T) {
	// An ABSENT backend decodes as shm (the default, and the field's only
	// forward-compat channel); each enum value decodes as itself. An unknown
	// token is REJECTED rather than folded into shm — see
	// TestBlobBackendStrictness and blob_backend_discriminator.json
	// (#lzblobbackendstrict).
	cases := []struct {
		in   string
		want BlobBackendKind
	}{
		{`{"offset":40,"len":17,"generation":2,"epoch":9,"checksum":987654321}`, BackendShm},
		{`{"offset":40,"len":17,"generation":2,"epoch":9,"checksum":987654321,"backend":"shm"}`, BackendShm},
		{`{"offset":40,"len":17,"generation":2,"epoch":9,"checksum":987654321,"backend":"arrow"}`, BackendArrow},
		{`{"offset":40,"len":17,"generation":2,"epoch":9,"checksum":987654321,"backend":"in_process"}`, BackendInProcess},
	}
	for _, c := range cases {
		var ref ShmBlobRef
		if err := json.Unmarshal([]byte(c.in), &ref); err != nil {
			t.Fatalf("unmarshal %s: %v", c.in, err)
		}
		if ref.Backend.Normalized() != c.want {
			t.Fatalf("%s → backend %q, want %q", c.in, ref.Backend, c.want)
		}
	}

	const unknown = `{"offset":40,"len":17,"generation":2,"epoch":9,"checksum":987654321,"backend":"rdma"}`
	var ref ShmBlobRef
	if err := json.Unmarshal([]byte(unknown), &ref); err == nil {
		t.Fatalf("%s decoded to %+v; want a rejection naming `rdma`", unknown, ref)
	} else if !strings.Contains(err.Error(), "rdma") {
		t.Fatalf("error %q does not name the offending token", err.Error())
	}
}
