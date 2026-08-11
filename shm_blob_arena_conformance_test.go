package lazily

import (
	"bytes"
	"testing"
)

type arenaFixtureDescriptor struct {
	Offset     int64  `json:"offset"`
	Len        int64  `json:"len"`
	Generation int64  `json:"generation"`
	Epoch      int64  `json:"epoch"`
	Checksum   uint64 `json:"checksum"`
}

type arenaFixture struct {
	conformanceDoc
	ProtocolVersion int            `json:"protocol_version"`
	Kind            string         `json:"kind"`
	Assertions      map[string]any `json:"assertions"`
	Input           struct {
		Capacity int    `json:"capacity"`
		Epoch    int64  `json:"epoch"`
		Payload  []byte `json:"payload"`
	} `json:"input"`
	Expected struct {
		Descriptor    arenaFixtureDescriptor `json:"descriptor"`
		HeaderBytes   []byte                 `json:"header_bytes"`
		PayloadRegion []byte                 `json:"payload_region"`
	} `json:"expected"`
}

func TestShmBlobArenaConformance(t *testing.T) {
	raw := loadConformanceFixture(t, "arena_blob.json")
	var fixture arenaFixture
	mustStrictJSON(t, "arena_blob.json", raw, &fixture)
	if fixture.ProtocolVersion != 1 {
		t.Fatalf("protocol_version = %d, want 1", fixture.ProtocolVersion)
	}
	if fixture.Kind != "Arena" {
		t.Fatalf("kind = %q, want Arena", fixture.Kind)
	}

	arena, err := NewShmBlobArenaWithCapacity(fixture.Input.Capacity)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := arena.WriteBlob(fixture.Input.Epoch, fixture.Input.Payload)
	if err != nil {
		t.Fatal(err)
	}

	if descriptor.Offset != fixture.Expected.Descriptor.Offset ||
		descriptor.Len != fixture.Expected.Descriptor.Len ||
		descriptor.Generation != fixture.Expected.Descriptor.Generation ||
		descriptor.Epoch != fixture.Expected.Descriptor.Epoch ||
		uint64(descriptor.Checksum) != fixture.Expected.Descriptor.Checksum {
		t.Fatalf("descriptor = %+v, want %+v", descriptor, fixture.Expected.Descriptor)
	}

	assertions := consumeKeys(t, "arena_blob.json assertions", fixture.Assertions,
		"capacity", "epoch", "header_len", "magic", "payload_len", "descriptor")
	assertKey(t, assertions, "capacity", arena.Capacity())
	assertKey(t, assertions, "epoch", descriptor.Epoch)
	assertKey(t, assertions, "header_len", ShmBlobHeaderLen)
	header := arena.Bytes()[:ShmBlobHeaderLen]
	assertKey(t, assertions, "magic", string([]byte{header[3], header[2], header[1], header[0]}))
	assertKey(t, assertions, "payload_len", len(fixture.Input.Payload))
	descriptorAssertions := assertKeySub(t, assertions, "descriptor",
		"offset", "len", "generation", "epoch", "checksum")
	assertKey(t, descriptorAssertions, "offset", descriptor.Offset)
	assertKey(t, descriptorAssertions, "len", descriptor.Len)
	assertKey(t, descriptorAssertions, "generation", descriptor.Generation)
	assertKey(t, descriptorAssertions, "epoch", descriptor.Epoch)
	// The `assertions` copy of the checksum cannot be compared exactly here: both
	// sides arrive as float64, and at 1.3e19 the double spacing is 2048, so a ±1
	// perturbation is invisible (#lzgoassertkeygaps). The value IS exactly pinned —
	// the typed `expected.descriptor.checksum` above is a uint64 decoded into a
	// struct field and compared with ==, which ±1 reddens. This excuse records that
	// the assertions copy is redundant rather than unchecked.
	excuseKey(t, descriptorAssertions, "checksum",
		"u64 beyond 2^53 cannot round-trip through float64; pinned exactly by the typed expected.descriptor.checksum comparison above")

	if !bytes.Equal(header, fixture.Expected.HeaderBytes) {
		t.Fatalf("header bytes = %v, want %v", header, fixture.Expected.HeaderBytes)
	}
	payloadRegion := arena.Bytes()[ShmBlobHeaderLen : ShmBlobHeaderLen+len(fixture.Input.Payload)]
	if !bytes.Equal(payloadRegion, fixture.Expected.PayloadRegion) {
		t.Fatalf("payload region = %v, want %v", payloadRegion, fixture.Expected.PayloadRegion)
	}
	roundTrip, err := arena.ReadBlob(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundTrip, fixture.Input.Payload) {
		t.Fatalf("round trip = %v, want %v", roundTrip, fixture.Input.Payload)
	}
}
