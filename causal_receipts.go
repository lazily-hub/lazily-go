package lazily

// Causal receipts — the generic outcome-tracking primitive.
//
// A receipt records the lifecycle of a causally-linked command or effect
// request keyed by a stable causation id. This is deliberately NOT a transport
// ACK plane: `observed` and `accepted` are non-terminal progress observations;
// `applied` and `rejected` are terminal outcomes.
//
// A Go port of lazily-dart's `lib/src/causal_receipts.dart`, aligned with the
// canonical Rust reference (lazily-rs `src/receipt.rs`) and the JS binding, and
// conformant with lazily-spec `schemas/receipts.json` +
// `conformance/receipts/causal_receipts.json`.
//
// Wire conventions (NORMATIVE, from receipts.json):
//   - snake_case field names: receipt_id, causation_id, observer, generation,
//     outcome, reason, payload_hash.
//   - `reason` and `payload_hash` are REQUIRED fields: they are emitted as
//     JSON null when absent (nullable strings), NOT omitted. Modeled as
//     *string with no `omitempty`.
//   - `outcome` is one of the ReceiptOutcome enum wire strings.
//   - The CausalReceipts envelope always emits `receipts` as an array (never
//     null).
//
// Semantic note (deviation from the Dart source, intentional): the Dart file is
// a simplified stub — its `staleReceiptIds()` always returns [] and its stale
// check compares a receipt's generation against the latest-per-causation
// generation. The canonical Rust reference, the JS binding, and the conformance
// fixture instead define staleness as a generation mismatch against the
// caller-supplied current generation, tracked in a dedicated stale-id set. This
// port follows the canonical/conformance behavior (the conformance fixture
// asserts `stale_receipt_ids == ["receipt-stale"]`, which only that algorithm
// produces), while retaining the Dart-shaped constructor and method surface.

import (
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// ReceiptOutcome (receipts.json#/$defs/ReceiptOutcome)
// ---------------------------------------------------------------------------

// ReceiptOutcome is the lifecycle outcome of a receipt. Serialized as its bare
// wire string; `observed`/`accepted` are non-terminal, `applied`/`rejected`
// are terminal.
type ReceiptOutcome string

const (
	// ReceiptOutcomeObserved: a peer/process observed the causation request.
	ReceiptOutcomeObserved ReceiptOutcome = "observed"
	// ReceiptOutcomeAccepted: a peer/process accepted or queued the request.
	ReceiptOutcomeAccepted ReceiptOutcome = "accepted"
	// ReceiptOutcomeApplied: the requested effect/state change was applied
	// (terminal).
	ReceiptOutcomeApplied ReceiptOutcome = "applied"
	// ReceiptOutcomeRejected: the requested effect/state change was rejected
	// (terminal).
	ReceiptOutcomeRejected ReceiptOutcome = "rejected"
)

// Wire returns the bare wire string of this outcome.
func (o ReceiptOutcome) Wire() string { return string(o) }

// IsTerminal reports whether this outcome is terminal (no further transitions
// expected).
func (o ReceiptOutcome) IsTerminal() bool {
	return o == ReceiptOutcomeApplied || o == ReceiptOutcomeRejected
}

// ReceiptOutcomeFromWire parses a wire string into a ReceiptOutcome, rejecting
// unknown values.
func ReceiptOutcomeFromWire(v string) (ReceiptOutcome, error) {
	switch ReceiptOutcome(v) {
	case ReceiptOutcomeObserved, ReceiptOutcomeAccepted, ReceiptOutcomeApplied, ReceiptOutcomeRejected:
		return ReceiptOutcome(v), nil
	default:
		return "", fmt.Errorf("unknown ReceiptOutcome: %q", v)
	}
}

// ---------------------------------------------------------------------------
// CausalReceipt (receipts.json#/$defs/CausalReceipt)
// ---------------------------------------------------------------------------

// CausalReceipt is one receipt event for a command/effect causation id.
//
// Reason and PayloadHash are nullable wire fields: they marshal to JSON null
// when nil (the schema lists both as required), so they carry no `omitempty`.
type CausalReceipt struct {
	// ReceiptId is the idempotency key for this receipt event.
	ReceiptId string `json:"receipt_id"`
	// CausationId is the stable id of the command/effect this receipt observes.
	CausationId string `json:"causation_id"`
	// Observer is the peer, process, or subsystem that produced the receipt.
	Observer string `json:"observer"`
	// Generation is the producer/editor generation. Consumers discard receipts
	// outside the current generation for the causation id.
	Generation int64 `json:"generation"`
	// Outcome is the receipt lifecycle outcome.
	Outcome ReceiptOutcome `json:"outcome"`
	// Reason is an optional human/debug rejection reason (null when absent).
	Reason *string `json:"reason"`
	// PayloadHash is an optional hash of the observed state/payload (null when
	// absent).
	PayloadHash *string `json:"payload_hash"`
}

// IsTerminal reports whether this receipt's outcome is terminal.
func (r CausalReceipt) IsTerminal() bool { return r.Outcome.IsTerminal() }

// NewCausalReceipt constructs a receipt with the given outcome and no reason or
// payload hash.
func NewCausalReceipt(receiptId, causationId, observer string, generation int64, outcome ReceiptOutcome) CausalReceipt {
	return CausalReceipt{
		ReceiptId:   receiptId,
		CausationId: causationId,
		Observer:    observer,
		Generation:  generation,
		Outcome:     outcome,
	}
}

// ObservedReceipt constructs an `observed` receipt.
func ObservedReceipt(receiptId, causationId, observer string, generation int64) CausalReceipt {
	return NewCausalReceipt(receiptId, causationId, observer, generation, ReceiptOutcomeObserved)
}

// AcceptedReceipt constructs an `accepted` receipt.
func AcceptedReceipt(receiptId, causationId, observer string, generation int64) CausalReceipt {
	return NewCausalReceipt(receiptId, causationId, observer, generation, ReceiptOutcomeAccepted)
}

// AppliedReceipt constructs an `applied` (terminal) receipt.
func AppliedReceipt(receiptId, causationId, observer string, generation int64) CausalReceipt {
	return NewCausalReceipt(receiptId, causationId, observer, generation, ReceiptOutcomeApplied)
}

// RejectedReceipt constructs a `rejected` (terminal) receipt.
func RejectedReceipt(receiptId, causationId, observer string, generation int64) CausalReceipt {
	return NewCausalReceipt(receiptId, causationId, observer, generation, ReceiptOutcomeRejected)
}

// WithReason returns a copy of the receipt carrying a debug/rejection reason.
func (r CausalReceipt) WithReason(reason string) CausalReceipt {
	r.Reason = &reason
	return r
}

// WithPayloadHash returns a copy of the receipt carrying a payload hash.
func (r CausalReceipt) WithPayloadHash(hash string) CausalReceipt {
	r.PayloadHash = &hash
	return r
}

// UnmarshalJSON decodes a receipt and validates the outcome enum.
func (r *CausalReceipt) UnmarshalJSON(b []byte) error {
	type raw CausalReceipt
	var x raw
	if err := json.Unmarshal(b, &x); err != nil {
		return err
	}
	if _, err := ReceiptOutcomeFromWire(string(x.Outcome)); err != nil {
		return err
	}
	*r = CausalReceipt(x)
	return nil
}

// CausalReceiptFromWire decodes a single receipt from JSON bytes.
func CausalReceiptFromWire(data []byte) (CausalReceipt, error) {
	var r CausalReceipt
	if err := json.Unmarshal(data, &r); err != nil {
		return CausalReceipt{}, err
	}
	return r, nil
}

// ---------------------------------------------------------------------------
// CausalReceipts (receipts.json#/properties/CausalReceipts)
// ---------------------------------------------------------------------------

// CausalReceipts is the wire body for a batch of receipts.
type CausalReceipts struct {
	// Receipts is the receipt batch.
	Receipts []CausalReceipt `json:"receipts"`
}

// NewCausalReceipts constructs a receipt batch, copying the input slice.
func NewCausalReceipts(receipts []CausalReceipt) CausalReceipts {
	cp := make([]CausalReceipt, len(receipts))
	copy(cp, receipts)
	return CausalReceipts{Receipts: cp}
}

// MarshalJSON emits { receipts } with receipts always an array (never null).
func (c CausalReceipts) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Receipts []CausalReceipt `json:"receipts"`
	}{nonNilSlice(c.Receipts)})
}

// CausalReceiptsFromWire decodes a receipt batch from JSON bytes.
func CausalReceiptsFromWire(data []byte) (CausalReceipts, error) {
	var c CausalReceipts
	if err := json.Unmarshal(data, &c); err != nil {
		return CausalReceipts{}, err
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// ReceiptApplyStatus (sealed result hierarchy)
// ---------------------------------------------------------------------------

// ReceiptApplyStatus is the result of observing a receipt into a
// ReceiptProjection. It is a sealed union realized as an interface with the
// concrete variants ReceiptRecorded / ReceiptDuplicate / ReceiptStaleGeneration
// / ReceiptTerminalConflict.
type ReceiptApplyStatus interface {
	isReceiptApplyStatus()
}

// ReceiptRecorded means the receipt was recorded into the projection.
type ReceiptRecorded struct{}

func (ReceiptRecorded) isReceiptApplyStatus() {}

// ReceiptDuplicate means the receipt id was already seen (idempotent no-op).
type ReceiptDuplicate struct{}

func (ReceiptDuplicate) isReceiptApplyStatus() {}

// ReceiptStaleGeneration means the receipt's generation did not match the
// current authority generation; the receipt is retained only as a stale id and
// does not update the projection.
type ReceiptStaleGeneration struct {
	// Expected is the current authority generation.
	Expected int64
	// Actual is the generation carried by the receipt.
	Actual int64
}

func (ReceiptStaleGeneration) isReceiptApplyStatus() {}

// ReceiptTerminalConflict means a different terminal outcome already exists for
// this causation id (fail-closed).
type ReceiptTerminalConflict struct {
	// CausationId is the causation id with conflicting terminal receipts.
	CausationId string
	// Existing is the already-recorded terminal outcome.
	Existing ReceiptOutcome
	// Incoming is the conflicting incoming terminal outcome.
	Incoming ReceiptOutcome
}

func (ReceiptTerminalConflict) isReceiptApplyStatus() {}

// ---------------------------------------------------------------------------
// ReceiptProjection (folded, monotonic ledger view)
// ---------------------------------------------------------------------------

// ReceiptProjection is the folded receipt ledger: it tracks the latest and
// terminal receipt per causation id, deduplicates by receipt id, and retains
// stale (out-of-generation) receipt ids separately.
//
// Like the sibling bindings, ReceiptProjection is not safe for concurrent use.
type ReceiptProjection struct {
	receiptsById        map[string]CausalReceipt
	latestByCausation   map[string]CausalReceipt
	terminalByCausation map[string]CausalReceipt
	staleReceiptIds     map[string]struct{}
	currentGeneration   int64
}

// NewReceiptProjection creates an empty projection.
func NewReceiptProjection() *ReceiptProjection {
	return &ReceiptProjection{
		receiptsById:        map[string]CausalReceipt{},
		latestByCausation:   map[string]CausalReceipt{},
		terminalByCausation: map[string]CausalReceipt{},
		staleReceiptIds:     map[string]struct{}{},
	}
}

func (p *ReceiptProjection) ensure() {
	if p.receiptsById == nil {
		p.receiptsById = map[string]CausalReceipt{}
	}
	if p.latestByCausation == nil {
		p.latestByCausation = map[string]CausalReceipt{}
	}
	if p.terminalByCausation == nil {
		p.terminalByCausation = map[string]CausalReceipt{}
	}
	if p.staleReceiptIds == nil {
		p.staleReceiptIds = map[string]struct{}{}
	}
}

// CurrentGeneration is the highest current generation observed so far.
func (p *ReceiptProjection) CurrentGeneration() int64 { return p.currentGeneration }

// ReceiptCount is the number of tracked receipts (recorded plus stale).
func (p *ReceiptProjection) ReceiptCount() int {
	return len(p.receiptsById) + len(p.staleReceiptIds)
}

// Observe applies one receipt and returns its ReceiptApplyStatus.
//
// When currentGeneration is non-nil, a receipt whose generation differs from it
// is retained only as a stale id and does not update the projection. When nil,
// the generation check is skipped (mirrors the canonical Option<u64> semantics).
//
// Ordering (mirrors the Rust/JS reference):
//  1. Duplicate: a receipt id already recorded or already stale is a no-op.
//  2. StaleGeneration: generation mismatch -> record the id as stale.
//  3. TerminalConflict: a differing terminal outcome for the same causation id
//     fails closed and is not recorded.
//  4. Otherwise: set terminal (first terminal wins), set latest, record by id.
func (p *ReceiptProjection) Observe(currentGeneration *int64, receipt CausalReceipt) ReceiptApplyStatus {
	p.ensure()

	if _, dup := p.receiptsById[receipt.ReceiptId]; dup {
		return ReceiptDuplicate{}
	}
	if _, dup := p.staleReceiptIds[receipt.ReceiptId]; dup {
		return ReceiptDuplicate{}
	}

	if currentGeneration != nil {
		if *currentGeneration > p.currentGeneration {
			p.currentGeneration = *currentGeneration
		}
		if receipt.Generation != *currentGeneration {
			p.staleReceiptIds[receipt.ReceiptId] = struct{}{}
			return ReceiptStaleGeneration{Expected: *currentGeneration, Actual: receipt.Generation}
		}
	}

	if receipt.IsTerminal() {
		if existing, ok := p.terminalByCausation[receipt.CausationId]; ok {
			if existing.Outcome != receipt.Outcome {
				return ReceiptTerminalConflict{
					CausationId: receipt.CausationId,
					Existing:    existing.Outcome,
					Incoming:    receipt.Outcome,
				}
			}
		} else {
			p.terminalByCausation[receipt.CausationId] = receipt
		}
	}

	p.latestByCausation[receipt.CausationId] = receipt
	p.receiptsById[receipt.ReceiptId] = receipt
	return ReceiptRecorded{}
}

// LatestFor returns the latest recorded receipt for causationId, terminal or
// not, and whether one exists.
func (p *ReceiptProjection) LatestFor(causationId string) (CausalReceipt, bool) {
	r, ok := p.latestByCausation[causationId]
	return r, ok
}

// TerminalFor returns the terminal receipt for causationId and whether one
// exists.
func (p *ReceiptProjection) TerminalFor(causationId string) (CausalReceipt, bool) {
	r, ok := p.terminalByCausation[causationId]
	return r, ok
}

// ContainsReceipt reports whether receiptId has been observed (recorded or
// stale).
func (p *ReceiptProjection) ContainsReceipt(receiptId string) bool {
	if _, ok := p.receiptsById[receiptId]; ok {
		return true
	}
	_, ok := p.staleReceiptIds[receiptId]
	return ok
}

// StaleReceiptIds returns the receipt ids observed outside the current
// generation.
func (p *ReceiptProjection) StaleReceiptIds() []string {
	out := make([]string, 0, len(p.staleReceiptIds))
	for id := range p.staleReceiptIds {
		out = append(out, id)
	}
	return out
}
