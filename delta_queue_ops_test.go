package lazily

// #lzdeltaqueueops: QueuePush / QueuePop / QueueClose as ordinary DeltaOp
// variants of the Delta frame (protocol.md § QueueCell op-log delta form).

import (
	"bytes"
	"reflect"
	"testing"
)

func queueOpsDelta() Delta {
	return DeltaNext(7, []DeltaOp{
		DeltaOpQueuePush{Node: 6, Payload: IpcValueInline{Bytes: []byte{97}}},
		DeltaOpQueuePop{Node: 6},
		DeltaOpQueueClose{Node: 6},
	})
}

func TestDeltaQueueOpsJSONWireShape(t *testing.T) {
	raw, err := IpcMessageDelta{Value: queueOpsDelta()}.EncodeJSON()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"Delta":{"base_epoch":7,"epoch":8,"ops":[` +
		`{"QueuePush":{"node":6,"payload":{"Inline":[97]}}},` +
		`{"QueuePop":{"node":6}},` +
		`{"QueueClose":{"node":6}}]}}`
	if string(raw) != want {
		t.Fatalf("wire mismatch\n got %s\nwant %s", raw, want)
	}
}

func TestDeltaQueueOpsRoundTripEveryCodec(t *testing.T) {
	msg := IpcMessageDelta{Value: queueOpsDelta()}
	codecs := map[string]struct {
		enc func(IpcMessage) ([]byte, error)
		dec func([]byte) (IpcMessage, error)
	}{
		"json":    {func(m IpcMessage) ([]byte, error) { return m.EncodeJSON() }, DecodeIpcMessageJSON},
		"msgpack": {EncodeIpcMessageMsgpack, DecodeIpcMessageMsgpack},
	}
	for name, c := range codecs {
		for i, op := range msg.Value.Ops {
			single := IpcMessageDelta{Value: DeltaNext(7, []DeltaOp{op})}
			wire, err := c.enc(single)
			if err != nil {
				t.Fatalf("%s op %d encode: %v", name, i, err)
			}
			got, err := c.dec(wire)
			if err != nil {
				t.Fatalf("%s op %d decode: %v", name, i, err)
			}
			if !reflect.DeepEqual(got, single) {
				t.Fatalf("%s op %d round trip: got %#v want %#v", name, i, got, single)
			}
		}
		wire, err := c.enc(msg)
		if err != nil {
			t.Fatalf("%s encode: %v", name, err)
		}
		got, err := c.dec(wire)
		if err != nil {
			t.Fatalf("%s decode: %v", name, err)
		}
		if !reflect.DeepEqual(got, msg) {
			t.Fatalf("%s round trip: got %#v want %#v", name, got, msg)
		}
	}
}

func TestDeltaQueueOpsDecodeRejectsMalformedBodies(t *testing.T) {
	for _, wire := range []string{
		`{"Delta":{"base_epoch":0,"epoch":1,"ops":[{"QueuePush":{"node":6,"payload":{"Bogus":[1]}}}]}}`,
		`{"Delta":{"base_epoch":0,"epoch":1,"ops":[{"QueuePush":{"node":6}}]}}`,
		`{"Delta":{"base_epoch":0,"epoch":1,"ops":[{"QueuePop":{"node":"six"}}]}}`,
		`{"Delta":{"base_epoch":0,"epoch":1,"ops":[{"QueueClose":7}]}}`,
		`{"Delta":{"base_epoch":0,"epoch":1,"ops":[{"QueuePush":{"node":6,"payload":{"Inline":[1]}},"QueuePop":{"node":6}}]}}`,
		`{"Delta":{"base_epoch":0,"epoch":1,"ops":[{"QueuePeek":{"node":6}}]}}`,
	} {
		if _, err := DecodeIpcMessageJSON([]byte(wire)); err == nil {
			t.Fatalf("decoder accepted malformed queue op: %s", wire)
		}
	}
}

func TestDeltaQueueOpsPermissionFilteringIsNodeScopedRead(t *testing.T) {
	const peer PeerId = 1
	p := NewPeerPermissions()
	p.Allow(peer, ReadOp(6))
	d := DeltaNext(0, []DeltaOp{
		DeltaOpQueuePush{Node: 6, Payload: IpcValueInline{Bytes: []byte{1}}},
		DeltaOpQueuePush{Node: 9, Payload: IpcValueInline{Bytes: []byte{2}}},
		DeltaOpQueuePop{Node: 6},
		DeltaOpQueuePop{Node: 9},
		DeltaOpQueueClose{Node: 6},
		DeltaOpQueueClose{Node: 9},
	})
	got := d.FilterReadable(p, peer)
	want := []DeltaOp{
		DeltaOpQueuePush{Node: 6, Payload: IpcValueInline{Bytes: []byte{1}}},
		DeltaOpQueuePop{Node: 6},
		DeltaOpQueueClose{Node: 6},
	}
	if !reflect.DeepEqual(got.Ops, want) {
		t.Fatalf("filtered ops = %#v, want %#v", got.Ops, want)
	}
	// A write grant is not a read grant.
	w := NewPeerPermissions()
	w.Allow(peer, WriteOp(6))
	if n := len(d.FilterReadable(w, peer).Ops); n != 0 {
		t.Fatalf("write-only peer saw %d queue ops", n)
	}
}

func TestDeltaQueuePushPayloadSpillsAndResolvesLikeCellSet(t *testing.T) {
	big := bytes.Repeat([]byte{0xab}, 64)
	backend := NewInProcessBackend()
	msg := IpcMessageDelta{Value: DeltaNext(0, []DeltaOp{
		DeltaOpQueuePush{Node: 6, Payload: IpcValueInline{Bytes: big}},
		DeltaOpQueuePush{Node: 6, Payload: IpcValueInline{Bytes: []byte{1}}},
		DeltaOpQueuePop{Node: 6},
		DeltaOpQueueClose{Node: 6},
	})}
	spilled, total := SpillMessage(msg, backend, 32)
	if total != len(big) {
		t.Fatalf("spilled %d bytes, want %d", total, len(big))
	}
	ops := spilled.(IpcMessageDelta).Value.Ops
	push := ops[0].(DeltaOpQueuePush)
	if _, ok := push.Payload.(IpcValueSharedBlob); !ok {
		t.Fatalf("large QueuePush payload not spilled: %T", push.Payload)
	}
	if _, ok := ops[1].(DeltaOpQueuePush).Payload.(IpcValueInline); !ok {
		t.Fatal("small QueuePush payload should stay inline")
	}
	if !reflect.DeepEqual(ops[2:], msg.Value.Ops[2:]) {
		t.Fatalf("QueuePop/QueueClose changed by spill: %#v", ops[2:])
	}
	if _, ok := msg.Value.Ops[0].(DeltaOpQueuePush).Payload.(IpcValueInline); !ok {
		t.Fatal("SpillMessage mutated its input")
	}
	// The spilled frame survives the wire and resolves to the original bytes.
	wire, err := EncodeIpcMessageMsgpack(spilled)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeIpcMessageMsgpack(wire)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := ResolveValue(decoded.(IpcMessageDelta).Value.Ops[0].(DeltaOpQueuePush).Payload, backend)
	if !ok || !bytes.Equal(got, big) {
		t.Fatalf("resolve spilled QueuePush payload: ok=%v len=%d", ok, len(got))
	}
	routed, ok := NewBlobRouter().Register(backend).Resolve(push.Payload)
	if !ok || !bytes.Equal(routed, big) {
		t.Fatal("BlobRouter failed to resolve a spilled QueuePush payload")
	}
}
