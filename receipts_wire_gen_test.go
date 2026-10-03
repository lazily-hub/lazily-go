package lazily

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// The receipt wire types are generated from lazily-spec `schemas/receipts.json`
// (#lzwiremodel). These tests pin what the generated codec must keep doing for
// the hand-written semantics in causal_receipts.go.

// TestGeneratedReceiptsRoundTripFixtureWire proves the generated codec
// re-encodes the canonical fixture's wire body to the same JSON value: a
// generated file replacing the hand-written one must not move a wire byte.
func TestGeneratedReceiptsRoundTripFixtureWire(t *testing.T) {
	raw := loadConformanceFixture(t, "receipts", "causal_receipts.json")
	var fixture struct {
		Wire struct {
			CausalReceipts json.RawMessage `json:"CausalReceipts"`
		} `json:"wire"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	receipts, err := CausalReceiptsFromWire(fixture.Wire.CausalReceipts)
	if err != nil {
		t.Fatalf("CausalReceiptsFromWire: %v", err)
	}
	encoded, err := json.Marshal(receipts)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var want, got bytes.Buffer
	if err := json.Compact(&want, fixture.Wire.CausalReceipts); err != nil {
		t.Fatalf("compact fixture: %v", err)
	}
	if err := json.Compact(&got, encoded); err != nil {
		t.Fatalf("compact encoded: %v", err)
	}
	if got.String() != want.String() {
		t.Fatalf("re-encoded wire differs\n got: %s\nwant: %s", got.String(), want.String())
	}
}

// TestGeneratedReceiptRejectsNegativeGeneration: the schema pins `generation`
// to `minimum: 0`, so the model lowers it to uint64 and a negative wire value
// fails to decode instead of entering the projection.
func TestGeneratedReceiptRejectsNegativeGeneration(t *testing.T) {
	_, err := CausalReceiptFromWire([]byte(`{"receipt_id":"r","causation_id":"c","observer":"o","generation":-1,"outcome":"applied","reason":null,"payload_hash":null}`))
	if err == nil {
		t.Fatal("negative generation decoded; want an error")
	}
}

// TestGeneratedReceiptRejectsUnknownOutcome: the outcome enum is closed.
func TestGeneratedReceiptRejectsUnknownOutcome(t *testing.T) {
	_, err := CausalReceiptFromWire([]byte(`{"receipt_id":"r","causation_id":"c","observer":"o","generation":1,"outcome":"done","reason":null,"payload_hash":null}`))
	if err == nil || !strings.Contains(err.Error(), "unknown ReceiptOutcome") {
		t.Fatalf("unknown outcome error = %v, want unknown ReceiptOutcome", err)
	}
}

// TestGeneratedReceiptsEmitEmptyListNotNull: `receipts` is a required array.
func TestGeneratedReceiptsEmitEmptyListNotNull(t *testing.T) {
	encoded, err := json.Marshal(CausalReceipts{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(encoded) != `{"receipts":[]}` {
		t.Fatalf("empty batch = %s, want {\"receipts\":[]}", encoded)
	}
}

// TestReceiptGenerationMatchesDoesNotWrap: the command plane keeps int64
// generations; comparing against the unsigned receipt generation must neither
// accept a negative entry nor wrap a large receipt generation.
func TestReceiptGenerationMatchesDoesNotWrap(t *testing.T) {
	if !receiptGenerationMatches(7, 7) {
		t.Fatal("7 == 7 must match")
	}
	if receiptGenerationMatches(-1, ^uint64(0)) {
		t.Fatal("entry -1 must not match receipt MaxUint64")
	}
	if got := receiptGenerationInt64(^uint64(0)); got <= 0 {
		t.Fatalf("saturating conversion wrapped to %d", got)
	}
}
