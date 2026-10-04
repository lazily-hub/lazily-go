package lazily

import (
	"encoding/json"
	"strings"
	"testing"
)

// delta_wire_gen.go is generated from lazily-spec schemas/delta.json
// (#lzwiremodel6). These tests pin what its strict decoder refuses that the
// hand-written one accepted, and the leniency the protocol requires it to keep.

func TestGeneratedDeltaDecoderRefusesOffSchemaFrames(t *testing.T) {
	cases := []struct {
		name, wire, want string
	}{
		{"unknown op key", `{"base_epoch":1,"epoch":2,"ops":[{"Invalidate":{"node":1,"extra":0}}]}`, `unknown field "extra"`},
		{"string node", `{"base_epoch":1,"epoch":2,"ops":[{"Invalidate":{"node":"1"}}]}`, "expected an unsigned integer"},
		{"bool node", `{"base_epoch":1,"epoch":2,"ops":[{"Invalidate":{"node":true}}]}`, "expected an unsigned integer"},
		{"negative node", `{"base_epoch":1,"epoch":2,"ops":[{"Invalidate":{"node":-1}}]}`, "expected an unsigned integer"},
		{"fractional node", `{"base_epoch":1,"epoch":2,"ops":[{"Invalidate":{"node":1.0}}]}`, "expected an unsigned integer"},
		{"node past int64", `{"base_epoch":1,"epoch":2,"ops":[{"Invalidate":{"node":9223372036854775808}}]}`, "exceeds this binding's int64 range"},
		{"node past u64", `{"base_epoch":1,"epoch":2,"ops":[{"Invalidate":{"node":18446744073709551616}}]}`, "0..=2^64-1"},
		{"missing node", `{"base_epoch":1,"epoch":2,"ops":[{"Invalidate":{}}]}`, `missing field "node"`},
		{"two tags", `{"base_epoch":1,"epoch":2,"ops":[{"Invalidate":{"node":1},"EdgeAdd":{}}]}`, "single-key object"},
		{"lowercase tag", `{"base_epoch":1,"epoch":2,"ops":[{"invalidate":{"node":1}}]}`, "unknown DeltaOp variant"},
		{"byte past 255", `{"base_epoch":1,"epoch":2,"ops":[{"CellSet":{"node":1,"payload":{"Inline":[1,256]}}}]}`, "array of bytes"},
		{"base64 bytes", `{"base_epoch":1,"epoch":2,"ops":[{"CellSet":{"node":1,"payload":{"Inline":"AQ=="}}}]}`, "expected an array"},
		{"lowercase unit", `{"base_epoch":1,"epoch":2,"ops":[{"NodeAdd":{"node":1,"type_tag":"t","state":"opaque"}}]}`, "unknown NodeState unit variant"},
		{"unit as object", `{"base_epoch":1,"epoch":2,"ops":[{"NodeAdd":{"node":1,"type_tag":"t","state":{"Opaque":null}}}]}`, "unknown NodeState variant"},
		{"missing ops", `{"base_epoch":1,"epoch":2}`, `missing field "ops"`},
		{"null ops", `{"base_epoch":1,"epoch":2,"ops":null}`, "expected an array"},
		{"unknown frame key", `{"base_epoch":1,"epoch":2,"ops":[],"x":0}`, `unknown field "x"`},
	}
	for _, c := range cases {
		var d Delta
		err := json.Unmarshal([]byte(c.wire), &d)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want an error containing %q", c.name, err, c.want)
		}
	}
}

func TestGeneratedDeltaNodeAddKeyAbsentAndNullBothReadAsAbsent(t *testing.T) {
	for _, wire := range []string{
		`{"NodeAdd":{"node":4,"type_tag":"u64","state":"Opaque"}}`,
		`{"NodeAdd":{"node":4,"type_tag":"u64","state":"Opaque","key":null}}`,
	} {
		op, err := unmarshalDeltaOp(json.RawMessage(wire))
		if err != nil {
			t.Fatalf("%s: %v", wire, err)
		}
		add, ok := op.(DeltaOpNodeAdd)
		if !ok || add.Key != nil {
			t.Fatalf("%s: decoded %#v", wire, op)
		}
		out, err := json.Marshal(op)
		if err != nil {
			t.Fatal(err)
		}
		if string(out) != `{"NodeAdd":{"node":4,"type_tag":"u64","state":"Opaque"}}` {
			t.Fatalf("%s re-encoded as %s; an absent key is omitted", wire, out)
		}
	}
}

func TestGeneratedDeltaRoundTripsEveryVariantByteForByte(t *testing.T) {
	wire := `{"base_epoch":40,"epoch":41,"ops":[` +
		`{"CellSet":{"node":1,"payload":{"Inline":[10]}}},` +
		`{"SlotValue":{"node":2,"payload":{"SharedBlob":{"offset":0,"len":1,"generation":1,"epoch":1,"checksum":9,"backend":"arrow"}}}},` +
		`{"Invalidate":{"node":3}},` +
		`{"NodeAdd":{"node":4,"type_tag":"u64","state":{"Payload":[64]},"key":"scores/alice"}},` +
		`{"NodeRemove":{"node":5}},` +
		`{"EdgeAdd":{"dependent":2,"dependency":1}},` +
		`{"EdgeRemove":{"dependent":3,"dependency":1}},` +
		`{"QueuePush":{"node":6,"payload":{"Inline":[]}}},` +
		`{"QueuePop":{"node":6}},` +
		`{"QueueClose":{"node":9223372036854775807}}]}`
	var d Delta
	if err := json.Unmarshal([]byte(wire), &d); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != wire {
		t.Fatalf("re-encoded\n%s\nwant\n%s", out, wire)
	}
}

func TestGeneratedDeltaRefusesToEncodeANegativeId(t *testing.T) {
	if _, err := json.Marshal(DeltaOpInvalidate{Node: -1}); err == nil {
		t.Fatal("a negative NodeId encoded as a u64")
	}
	if _, err := json.Marshal(Delta{BaseEpoch: -1, Epoch: 0}); err == nil {
		t.Fatal("a negative base_epoch encoded as a u64")
	}
	out, err := json.Marshal(Delta{BaseEpoch: 1, Epoch: 2})
	if err != nil || string(out) != `{"base_epoch":1,"epoch":2,"ops":[]}` {
		t.Fatalf("empty delta encoded as %s (%v); ops must be [] not null", out, err)
	}
}

func TestGeneratedDeltaKeepsBlobBackendNullLenient(t *testing.T) {
	// ShmBlobRef keeps its hand-written codec: an explicit `backend: null` is the
	// absent form (#lzblobbackendstrict), which the schema alone rejects.
	v, err := unmarshalIpcValue(json.RawMessage(`{"SharedBlob":{"offset":0,"len":1,"generation":1,"epoch":1,"checksum":9,"backend":null}}`))
	if err != nil {
		t.Fatal(err)
	}
	if blob, ok := v.(IpcValueSharedBlob); !ok || !blob.Blob.Backend.IsDefault() {
		t.Fatalf("decoded %#v", v)
	}
}
