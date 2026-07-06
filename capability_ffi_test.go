package lazily

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Capability negotiation (ported from lazily-dart test/capability_test.dart),
// FFI boundary (test/ffi_test.dart), and shared-memory blob arena
// (shm_blob_arena.dart semantics).

// --- CapabilityHandshake wire ---------------------------------------------

func TestCapabilityDefaultsMatchCanonical(t *testing.T) {
	h := NewCapabilityHandshake(1, "abc-123")
	if h.ProtocolID != ProtocolID {
		t.Fatalf("protocol_id = %q, want %q", h.ProtocolID, ProtocolID)
	}
	if h.ProtocolMajorVersion != ProtocolMajorVersion {
		t.Fatalf("major = %d, want %d", h.ProtocolMajorVersion, ProtocolMajorVersion)
	}
	if h.Codec != "json" || h.MaxFrameSize != 1<<20 {
		t.Fatalf("codec/frame = %q/%d, want json/%d", h.Codec, h.MaxFrameSize, 1<<20)
	}
	if h.FragmentationSupported || !h.OrderedReliable {
		t.Fatalf("frag=%v ordered=%v, want false/true", h.FragmentationSupported, h.OrderedReliable)
	}
	if h.PeerID != 1 || h.SessionID != "abc-123" || len(h.Features) != 0 {
		t.Fatalf("peer/session/features = %d/%q/%v", h.PeerID, h.SessionID, h.Features)
	}
}

func TestCapabilityRoundTripsThroughJSON(t *testing.T) {
	h := NewCapabilityHandshake(7, "s-1").
		WithCodec("msgpack").
		WithMaxFrameSize(1024).
		WithFragmentation(true).
		WithFeatures([]string{"shared-blob", "signaling-relay"})
	encoded, err := h.EncodeJSON()
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	decoded, err := DecodeCapabilityHandshakeJSON(encoded)
	if err != nil {
		t.Fatalf("DecodeCapabilityHandshakeJSON: %v", err)
	}
	if decoded.Codec != "msgpack" || decoded.MaxFrameSize != 1024 || !decoded.FragmentationSupported {
		t.Fatalf("decoded scalars = %q/%d/%v", decoded.Codec, decoded.MaxFrameSize, decoded.FragmentationSupported)
	}
	if !decoded.OrderedReliable || decoded.PeerID != 7 || decoded.SessionID != "s-1" {
		t.Fatalf("decoded = %+v", decoded)
	}
	if !reflect.DeepEqual(decoded.Features, []string{"shared-blob", "signaling-relay"}) {
		t.Fatalf("features = %v", decoded.Features)
	}
}

func TestCapabilityDecoderAppliesDefaultsWhenAbsent(t *testing.T) {
	decoded, err := DecodeCapabilityHandshakeJSON([]byte(`{
		"protocol_id": "lazily-ipc",
		"protocol_major_version": 1,
		"peer_id": 2,
		"session_id": "x",
		"codec": "json",
		"max_frame_size": 4096
	}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.FragmentationSupported {
		t.Fatal("fragmentation_supported should default false")
	}
	if !decoded.OrderedReliable {
		t.Fatal("ordered_reliable should default true")
	}
	if len(decoded.Features) != 0 {
		t.Fatalf("features should default empty, got %v", decoded.Features)
	}
}

func TestCapabilityEncodeJSONProducesCanonicalObject(t *testing.T) {
	h := NewCapabilityHandshake(1, "abc")
	encoded, err := h.EncodeJSON()
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	wire, err := json.Marshal(h.ToWire())
	if err != nil {
		t.Fatalf("marshal wire: %v", err)
	}
	if string(encoded) != string(wire) {
		t.Fatalf("encoded %s != canonical wire %s", encoded, wire)
	}
}

// --- IsCompatibleWith / CheckCompatible ------------------------------------

func capDefaults(peer PeerId) CapabilityHandshake {
	return NewCapabilityHandshake(peer, "s")
}

func TestCapabilityTwoCompliantPeersCompatible(t *testing.T) {
	if !capDefaults(1).IsCompatibleWith(capDefaults(2)) {
		t.Fatal("two compliant peers should be compatible")
	}
}

func TestCapabilityWrongProtocolIDFailsClosed(t *testing.T) {
	impostor, err := DecodeCapabilityHandshakeJSON([]byte(`{
		"protocol_id": "not-lazily", "protocol_major_version": 1,
		"codec": "json", "max_frame_size": 4096, "peer_id": 2, "session_id": "s"
	}`))
	if err != nil {
		t.Fatalf("decode impostor: %v", err)
	}
	check := capDefaults(1).CheckCompatible(impostor)
	if check.IsOk() || check.Field != "protocol_id" {
		t.Fatalf("check = %+v, want fail on protocol_id", check)
	}
}

func TestCapabilityMajorVersionMismatchFailsClosed(t *testing.T) {
	other, err := DecodeCapabilityHandshakeJSON([]byte(`{
		"protocol_id": "lazily-ipc", "protocol_major_version": 999,
		"codec": "json", "max_frame_size": 4096, "peer_id": 2, "session_id": "s"
	}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	check := capDefaults(1).CheckCompatible(other)
	if check.IsOk() || check.Field != "protocol_major_version" {
		t.Fatalf("check = %+v, want fail on protocol_major_version", check)
	}
}

func TestCapabilityCodecMismatchFailsClosed(t *testing.T) {
	other := capDefaults(2).WithCodec("msgpack")
	check := capDefaults(1).CheckCompatible(other)
	if check.IsOk() || check.Field != "codec" {
		t.Fatalf("check = %+v, want fail on codec", check)
	}
}

func TestCapabilityOrderedReliableFalseFailsClosed(t *testing.T) {
	unreliable := capDefaults(2).WithOrderedReliable(false)
	check := capDefaults(1).CheckCompatible(unreliable)
	if check.IsOk() || check.Field != "ordered_reliable" {
		t.Fatalf("check = %+v, want fail on ordered_reliable", check)
	}
}

func TestCapabilityRequiredFeatureNotOfferedFailsClosed(t *testing.T) {
	offer := capDefaults(2) // no features
	check := capDefaults(1).CheckCompatible(offer, "shared-blob")
	if check.IsOk() || check.Field != "features" {
		t.Fatalf("check = %+v, want fail on features", check)
	}
}

func TestCapabilityRequiredFeatureOfferedSucceeds(t *testing.T) {
	offer := capDefaults(2).WithFeatures([]string{"shared-blob"})
	check := capDefaults(1).CheckCompatible(offer, "shared-blob")
	if !check.IsOk() {
		t.Fatalf("check = %+v, want ok", check)
	}
}

func TestCapabilityBindingCapabilities(t *testing.T) {
	caps := NewBindingCapabilities()
	if caps.Binding != "lazily-go" {
		t.Fatalf("binding = %q, want lazily-go", caps.Binding)
	}
	if caps.Ffi != FfiCapabilityHost {
		t.Fatalf("ffi = %q, want host", caps.Ffi)
	}
	if !(caps.ReactiveCore && caps.Collections && caps.StateMachine && caps.StateCharts &&
		caps.Ipc && caps.Crdt && caps.Permissions && caps.CapabilityNegotiation && caps.Async) {
		t.Fatalf("not every MUST layer is set: %+v", caps)
	}
	// Wire form advertises host ffi and every layer as JSON booleans.
	b, err := json.Marshal(caps)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal wire: %v", err)
	}
	if m["binding"] != "lazily-go" || m["ffi"] != "host" {
		t.Fatalf("wire binding/ffi = %v/%v", m["binding"], m["ffi"])
	}
	for _, k := range []string{"reactive_core", "collections", "state_machine", "state_charts",
		"ipc", "crdt", "permissions", "capability_negotiation", "async"} {
		if m[k] != true {
			t.Fatalf("wire[%s] = %v, want true", k, m[k])
		}
	}
}

// --- FFI status / kind discriminants ---------------------------------------

func TestFFIStatusCodesStableAndExhaustive(t *testing.T) {
	cases := map[LazilyFfiStatus]int{
		LazilyFfiStatusOk:             0,
		LazilyFfiStatusEmpty:          1,
		LazilyFfiStatusNullPointer:    2,
		LazilyFfiStatusInvalidMessage: 3,
		LazilyFfiStatusEncodeFailed:   4,
		LazilyFfiStatusPanic:          5,
	}
	for status, code := range cases {
		if int(status) != code {
			t.Fatalf("status %v = %d, want %d", status, int(status), code)
		}
	}
	for i := 0; i <= 5; i++ {
		if _, ok := LazilyFfiStatusFromCode(i); !ok {
			t.Fatalf("FromCode(%d) not ok", i)
		}
	}
	if _, ok := LazilyFfiStatusFromCode(99); ok {
		t.Fatal("FromCode(99) should not be ok")
	}
}

func TestFFIMessageKindIncludesCrdtSync(t *testing.T) {
	if int(LazilyFfiMessageKindUnknown) != 0 || int(LazilyFfiMessageKindSnapshot) != 1 ||
		int(LazilyFfiMessageKindDelta) != 2 || int(LazilyFfiMessageKindCrdtSync) != 3 {
		t.Fatal("message kind discriminants are not stable")
	}
	if LazilyFfiMessageKindFromCode(99) != LazilyFfiMessageKindUnknown {
		t.Fatal("out-of-range kind should decode to unknown")
	}
}

// --- FFI validate / classify / clone ---------------------------------------

func snapshotFrame(t *testing.T, epoch Epoch) LazilyFfiBytes {
	t.Helper()
	msg := IpcMessageSnapshot{Value: Snapshot{Epoch: epoch}}
	b, err := msg.EncodeJSON()
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	return NewLazilyFfiBytes(b)
}

func crdtSyncMessage() IpcMessageCrdtSync {
	stamp := NewWireStamp(9, 0, 1)
	return IpcMessageCrdtSync{Value: CrdtSync{
		Frontier: []StampFrontierEntry{{Peer: 1, Stamp: stamp}},
		Ops:      []CrdtOp{NewCrdtOp(1, stamp, IpcValueInline{Bytes: []byte{1, 2}})},
	}}
}

func TestFFIValidateJSON(t *testing.T) {
	if s := LazilyFfiValidateJSON(snapshotFrame(t, 1)); s != LazilyFfiStatusOk {
		t.Fatalf("validate well-formed = %v, want ok", s)
	}
	if s := LazilyFfiValidateJSON(NewLazilyFfiBytes([]byte("not json at all"))); s != LazilyFfiStatusInvalidMessage {
		t.Fatalf("validate garbage = %v, want invalidMessage", s)
	}
}

func TestFFIKindJSONClassifies(t *testing.T) {
	// Snapshot
	c := LazilyFfiKindJSON(snapshotFrame(t, 7))
	if !c.IsOk() || c.Kind != LazilyFfiMessageKindSnapshot {
		t.Fatalf("snapshot classify = %+v", c)
	}
	// Delta
	deltaMsg := IpcMessageDelta{Value: Delta{BaseEpoch: 0, Epoch: 1}}
	db, _ := deltaMsg.EncodeJSON()
	if c := LazilyFfiKindJSON(NewLazilyFfiBytes(db)); c.Kind != LazilyFfiMessageKindDelta {
		t.Fatalf("delta classify kind = %v, want delta", c.Kind)
	}
	// CrdtSync
	cb, _ := crdtSyncMessage().EncodeJSON()
	if c := LazilyFfiKindJSON(NewLazilyFfiBytes(cb)); !c.IsOk() || c.Kind != LazilyFfiMessageKindCrdtSync {
		t.Fatalf("crdtsync classify = %+v", c)
	}
}

func TestFFICloneJSONRoundTrips(t *testing.T) {
	msg := IpcMessageSnapshot{Value: Snapshot{Epoch: 3}}
	orig, _ := msg.EncodeJSON()
	result := LazilyFfiCloneJSON(NewLazilyFfiBytes(orig))
	if result.Status != LazilyFfiStatusOk || result.Output == nil {
		t.Fatalf("clone = %+v, want ok with output", result)
	}
	decoded, err := DecodeIpcMessageJSON(result.Output.Bytes)
	if err != nil {
		t.Fatalf("decode cloned: %v", err)
	}
	if _, ok := decoded.(IpcMessageSnapshot); !ok {
		t.Fatalf("decoded type = %T, want IpcMessageSnapshot", decoded)
	}
	// Canonical re-encode is byte-identical to the canonical original.
	reencoded, _ := decoded.EncodeJSON()
	if string(reencoded) != string(orig) {
		t.Fatalf("clone not canonical: %s != %s", reencoded, orig)
	}
}

func TestFFICloneJSONRoundTripsCrdtSync(t *testing.T) {
	stamp := NewWireStamp(5, 1, 2)
	key, err := NewNodeKey("docs/x")
	if err != nil {
		t.Fatalf("NewNodeKey: %v", err)
	}
	msg := IpcMessageCrdtSync{Value: CrdtSync{
		Frontier: []StampFrontierEntry{{Peer: 2, Stamp: stamp}},
		Ops:      []CrdtOp{NewKeyedCrdtOp(7, key, stamp, IpcValueInline{Bytes: []byte{9}})},
	}}
	orig, _ := msg.EncodeJSON()
	result := LazilyFfiCloneJSON(NewLazilyFfiBytes(orig))
	if result.Status != LazilyFfiStatusOk || result.Output == nil {
		t.Fatalf("clone = %+v", result)
	}
	decoded, err := DecodeIpcMessageJSON(result.Output.Bytes)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	reencoded, _ := decoded.EncodeJSON()
	if string(reencoded) != string(orig) {
		t.Fatalf("crdtsync clone not canonical: %s != %s", reencoded, orig)
	}
}

func TestFFICloneJSONMalformed(t *testing.T) {
	result := LazilyFfiCloneJSON(NewLazilyFfiBytes([]byte("{ nope }")))
	if result.Status != LazilyFfiStatusInvalidMessage || result.Output != nil {
		t.Fatalf("clone malformed = %+v, want invalidMessage with nil output", result)
	}
}

// --- LazilyFfiChannel ------------------------------------------------------

func TestFFIChannelSendRecvRoundTrip(t *testing.T) {
	ch := NewLazilyFfiChannel()
	if !ch.IsEmpty() {
		t.Fatal("new channel should be empty")
	}
	msg := IpcMessageDelta{Value: Delta{BaseEpoch: 4, Epoch: 5}}
	if s := ch.Send(msg); s != LazilyFfiStatusOk {
		t.Fatalf("send = %v, want ok", s)
	}
	if ch.IsEmpty() || ch.Len() != 1 {
		t.Fatalf("channel len = %d, want 1", ch.Len())
	}
	recv, s := ch.Recv()
	if s != LazilyFfiStatusOk {
		t.Fatalf("recv status = %v, want ok", s)
	}
	got, _ := recv.EncodeJSON()
	want, _ := msg.EncodeJSON()
	if string(got) != string(want) {
		t.Fatalf("recv %s != sent %s", got, want)
	}
	if !ch.IsEmpty() {
		t.Fatal("channel should be empty after recv")
	}
	// Empty recv → nil message, empty status.
	if m, s := ch.Recv(); m != nil || s != LazilyFfiStatusEmpty {
		t.Fatalf("empty recv = (%v, %v), want (nil, empty)", m, s)
	}
}

func TestFFIChannelSendJSONFrameRejectsMalformed(t *testing.T) {
	ch := NewLazilyFfiChannel()
	if s := ch.SendJSONFrame(NewLazilyFfiBytes([]byte("garbage"))); s != LazilyFfiStatusInvalidMessage {
		t.Fatalf("sendJSONFrame garbage = %v, want invalidMessage", s)
	}
	if !ch.IsEmpty() {
		t.Fatal("channel should stay empty after rejected frame")
	}
}

func TestFFIChannelSendJSONFrameCanonicalizes(t *testing.T) {
	ch := NewLazilyFfiChannel()
	msg := IpcMessageSnapshot{Value: Snapshot{Epoch: 1}}
	frame := snapshotFrame(t, 1)
	if s := ch.SendJSONFrame(frame); s != LazilyFfiStatusOk {
		t.Fatalf("sendJSONFrame = %v, want ok", s)
	}
	recv, s := ch.Recv()
	if s != LazilyFfiStatusOk {
		t.Fatalf("recv status = %v", s)
	}
	got, _ := recv.EncodeJSON()
	want, _ := msg.EncodeJSON()
	if string(got) != string(want) {
		t.Fatalf("canonicalized recv %s != %s", got, want)
	}
}

// --- ShmBlobArena (shm_blob_arena.go, ported from shm_blob_arena.dart) -----

func TestShmArenaWriteReadRoundTrip(t *testing.T) {
	arena := NewShmBlobArena(0)
	if !arena.IsEmpty() {
		t.Fatal("new arena should be empty")
	}
	payload := []byte("hello world")
	ref := arena.Write(payload)
	if arena.Length() != 1 || arena.IsEmpty() {
		t.Fatalf("length = %d, want 1", arena.Length())
	}
	got := arena.Read(ref)
	if string(got) != "hello world" {
		t.Fatalf("read = %q, want hello world", got)
	}
	// Header validation: the descriptor carries the payload length and checksum.
	if ref.Len != int64(len(payload)) {
		t.Fatalf("ref.Len = %d, want %d", ref.Len, len(payload))
	}
	// Read returns a defensive copy.
	got[0] = 'X'
	if again := arena.Read(ref); again[0] != 'h' {
		t.Fatal("Read did not return a defensive copy")
	}
}

func TestShmArenaReadRejectsStaleDescriptor(t *testing.T) {
	arena := NewShmBlobArena(0)
	ref := arena.Write([]byte("abc"))
	// A mismatched generation fails header validation.
	stale := ref
	stale.Generation = ref.Generation + 100
	if got := arena.Read(stale); got != nil {
		t.Fatalf("read with stale generation = %v, want nil", got)
	}
	// Out-of-range offset.
	oob := ref
	oob.Offset = 999
	if got := arena.Read(oob); got != nil {
		t.Fatalf("read out-of-range = %v, want nil", got)
	}
}

func TestShmArenaUpdateInPlace(t *testing.T) {
	arena := NewShmBlobArena(0)
	ref := arena.Write([]byte("aaaa"))
	newRef := arena.Update(ref, []byte("bbbb"))
	if newRef == nil {
		t.Fatal("update returned nil for a live descriptor")
	}
	if newRef.Generation == ref.Generation {
		t.Fatal("update should bump generation")
	}
	if string(arena.Read(*newRef)) != "bbbb" {
		t.Fatalf("read after update = %q, want bbbb", arena.Read(*newRef))
	}
	// The old descriptor is now stale.
	if got := arena.Read(ref); got != nil {
		t.Fatalf("old ref after update = %v, want nil", got)
	}
	// Updating a stale ref fails.
	if arena.Update(ref, []byte("cccc")) != nil {
		t.Fatal("update of stale ref should return nil")
	}
}

func TestShmArenaAdvanceEpochInvalidatesDescriptors(t *testing.T) {
	arena := NewShmBlobArena(0)
	ref := arena.Write([]byte("data"))
	if string(arena.Read(ref)) != "data" {
		t.Fatal("pre-epoch read failed")
	}
	arena.AdvanceEpoch()
	if arena.Epoch() != 1 {
		t.Fatalf("epoch = %d, want 1", arena.Epoch())
	}
	// The descriptor minted at epoch 0 no longer validates.
	if got := arena.Read(ref); got != nil {
		t.Fatalf("read after epoch advance = %v, want nil", got)
	}
}

func TestShmArenaRetainFree(t *testing.T) {
	arena := NewShmBlobArena(0)
	ref := arena.Write([]byte("payload"))
	// Retain adds a reference; two Frees are then needed to reclaim.
	if !arena.Retain(ref) {
		t.Fatal("retain should succeed for a live blob")
	}
	if !arena.Free(ref) {
		t.Fatal("first free should succeed")
	}
	// Still live after the first free (refcount 2 -> 1).
	if string(arena.Read(ref)) != "payload" {
		t.Fatal("blob should still be live after one free of two refs")
	}
	if arena.Length() != 1 {
		t.Fatalf("length = %d, want 1", arena.Length())
	}
	if !arena.Free(ref) {
		t.Fatal("second free should succeed")
	}
	// Now reclaimed.
	if got := arena.Read(ref); got != nil {
		t.Fatalf("read after reclaim = %v, want nil", got)
	}
	if !arena.IsEmpty() {
		t.Fatalf("arena length = %d, want 0 after reclaim", arena.Length())
	}
	// Freeing an already-freed descriptor fails.
	if arena.Free(ref) {
		t.Fatal("free of reclaimed slot should return false")
	}
}

func TestShmArenaValidateBlobRef(t *testing.T) {
	valid := ShmBlobRef{Offset: 0, Len: 10, Generation: 1, Epoch: 0, Checksum: 42}
	if !ValidateBlobRef(valid, nil) {
		t.Fatal("valid ref should pass")
	}
	max := int64(5)
	if ValidateBlobRef(valid, &max) {
		t.Fatal("ref longer than maxLen should fail")
	}
	neg := ShmBlobRef{Offset: -1}
	if ValidateBlobRef(neg, nil) {
		t.Fatal("negative offset should fail")
	}
}
