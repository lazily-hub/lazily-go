package lazily

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// DurableProtocolVersion is the durable client wire protocol version.
const DurableProtocolVersion uint64 = 1

// DurableEnvelopeError reports the stable fail-closed reason used by the
// cross-language durable-client corpus.
type DurableEnvelopeError struct{ Reason string }

func (e *DurableEnvelopeError) Error() string { return "lazily: durable envelope: " + e.Reason }

// DurableProjectionCompleteness describes the source history represented by a
// projection. The Client tier accepts only complete history; latest-only state
// remains the separate LatestDurableProjection API.
type DurableProjectionCompleteness string

const DurableProjectionCompleteHistory DurableProjectionCompleteness = "complete_history"

// DurableProjectionHealth is an advisory projection's reconciliation state.
type DurableProjectionHealth string

const (
	DurableProjectionHealthy DurableProjectionHealth = "healthy"
	DurableProjectionLagging DurableProjectionHealth = "lagging"
	DurableProjectionDrifted DurableProjectionHealth = "drifted"
)

// DurableIngressEnvelope is the typed command/event submitted to a durable
// host. Broker acknowledgement of this envelope is not a host commit receipt.
type DurableIngressEnvelope struct {
	ProtocolVersion uint64 `json:"protocol_version"`
	MessageID       string `json:"message_id"`
	SchemaVersion   uint64 `json:"schema_version"`
	CodecVersion    uint64 `json:"codec_version"`
	Payload         []byte `json:"payload"`
}

type durableIngressEnvelopeWire struct {
	ProtocolVersion uint64 `json:"protocol_version"`
	MessageID       string `json:"message_id"`
	SchemaVersion   uint64 `json:"schema_version"`
	CodecVersion    uint64 `json:"codec_version"`
	Payload         []int  `json:"payload"`
}

type durableIngressEnvelopeRawWire struct {
	ProtocolVersion uint64          `json:"protocol_version"`
	MessageID       string          `json:"message_id"`
	SchemaVersion   uint64          `json:"schema_version"`
	CodecVersion    uint64          `json:"codec_version"`
	Payload         json.RawMessage `json:"payload"`
}

// MarshalJSON preserves the cross-language byte-array wire shape rather than
// encoding []byte as Go's default base64 JSON string.
func (e DurableIngressEnvelope) MarshalJSON() ([]byte, error) {
	return json.Marshal(durableIngressEnvelopeWire{
		ProtocolVersion: e.ProtocolVersion, MessageID: e.MessageID,
		SchemaVersion: e.SchemaVersion, CodecVersion: e.CodecVersion,
		Payload: bytesWire(e.Payload),
	})
}

// UnmarshalJSON decodes and validates the canonical v1 envelope before making
// payload bytes available to the caller.
func (e *DurableIngressEnvelope) UnmarshalJSON(data []byte) error {
	var wire durableIngressEnvelopeRawWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if err := validateDurableIngress(wire.ProtocolVersion, wire.MessageID, wire.SchemaVersion, wire.CodecVersion); err != nil {
		return err
	}
	var payloadWire []int
	if err := json.Unmarshal(wire.Payload, &payloadWire); err != nil {
		return fmt.Errorf("lazily: durable payload is not a byte array: %w", err)
	}
	payload := make([]byte, len(payloadWire))
	for index, value := range payloadWire {
		if value < 0 || value > 255 {
			return fmt.Errorf("lazily: durable payload[%d] is outside byte range", index)
		}
		payload[index] = byte(value)
	}
	*e = DurableIngressEnvelope{
		ProtocolVersion: wire.ProtocolVersion, MessageID: wire.MessageID,
		SchemaVersion: wire.SchemaVersion, CodecVersion: wire.CodecVersion, Payload: payload,
	}
	return nil
}

// DurableBrokerPubAck proves only that the broker stored a publication. It
// deliberately cannot represent a host commit or duplicate decision.
type DurableBrokerPubAck struct {
	Stream    string `json:"stream"`
	Sequence  uint64 `json:"sequence"`
	Duplicate bool   `json:"duplicate"`
}

// DurableHostOutcome is the durable host's idempotency decision.
type DurableHostOutcome string

const (
	DurableHostCommitted DurableHostOutcome = "committed"
	DurableHostDuplicate DurableHostOutcome = "duplicate"
	DurableHostConflict  DurableHostOutcome = "conflict"
	DurableHostRejected  DurableHostOutcome = "rejected"
)

// DurableHostReceipt is emitted by the host after its transaction completes.
// It is observed separately from DurableBrokerPubAck.
type DurableHostReceipt struct {
	ProtocolVersion uint64             `json:"protocol_version"`
	ReceiptID       string             `json:"receipt_id"`
	MessageID       string             `json:"message_id"`
	Outcome         DurableHostOutcome `json:"outcome"`
	OwnerPosition   uint64             `json:"owner_position"`
}

// DurableProjectionFingerprint is the portable projection equality class.
type DurableProjectionFingerprint struct {
	ProjectionID           string `json:"projection_id"`
	SourcePosition         uint64 `json:"source_position"`
	Fingerprint            string `json:"fingerprint"`
	Completeness           string `json:"completeness"`
	MayAuthorizeTransition bool   `json:"may_authorize_transition"`
}

// DurableProjectionFingerprintComparison compares source identity separately
// from binding-specific digest bytes.
type DurableProjectionFingerprintComparison struct {
	SameSource       bool `json:"same_source"`
	SameFingerprint  bool `json:"same_fingerprint"`
	SameCompleteness bool `json:"same_completeness"`
	Equivalent       bool `json:"equivalent"`
}

func CompareDurableProjectionFingerprints(left, right DurableProjectionFingerprint) DurableProjectionFingerprintComparison {
	sameSource := left.ProjectionID == right.ProjectionID && left.SourcePosition == right.SourcePosition
	sameFingerprint := left.Fingerprint == right.Fingerprint
	sameCompleteness := left.Completeness == right.Completeness
	return DurableProjectionFingerprintComparison{SameSource: sameSource, SameFingerprint: sameFingerprint, SameCompleteness: sameCompleteness, Equivalent: sameSource && sameFingerprint && sameCompleteness}
}

// DurableIngressClassification describes stable message-identity replay.
type DurableIngressClassification string

const (
	DurableIngressFirst     DurableIngressClassification = "first"
	DurableIngressDuplicate DurableIngressClassification = "duplicate"
	DurableIngressConflict  DurableIngressClassification = "conflict"
)

// DurableProjectionEvent is an advisory complete-history projection update.
// MayAuthorizeTransition must always be false: only the durable owner may
// authorize a state transition.
type DurableProjectionEvent[T any] struct {
	ProtocolVersion        uint64                        `json:"protocol_version"`
	OwnerID                string                        `json:"owner_id"`
	Generation             uint64                        `json:"generation"`
	SourcePosition         uint64                        `json:"source_position"`
	ProjectionVersion      uint64                        `json:"projection_version"`
	SchemaVersion          uint64                        `json:"schema_version"`
	CodecVersion           uint64                        `json:"codec_version"`
	Completeness           DurableProjectionCompleteness `json:"completeness"`
	Entries                T                             `json:"entries"`
	SourceFingerprint      string                        `json:"source_fingerprint"`
	ProjectionFingerprint  string                        `json:"projection_fingerprint"`
	Health                 DurableProjectionHealth       `json:"health"`
	MayAuthorizeTransition bool                          `json:"may_authorize_transition"`
}

// DurableSubscription is the minimal NATS-compatible subscription seam.
type DurableSubscription interface {
	Next(context.Context) ([]byte, error)
	Close() error
}

// DurableNATSTransport is injected by applications. The package intentionally
// does not select a NATS implementation or own broker lifecycle.
type DurableNATSTransport interface {
	Publish(context.Context, string, []byte) (DurableBrokerPubAck, error)
	Subscribe(context.Context, string) (DurableSubscription, error)
}

// DurableClient implements the Client tier only. It publishes typed ingress,
// observes host receipts, and orders/deduplicates advisory projections through
// the existing IngressCore. It does not implement a durable host.
type DurableClient[P any] struct {
	transport                  DurableNATSTransport
	projections                *IngressCore[string, DurableProjectionEvent[P]]
	latest                     map[string]DurableProjectionEvent[P]
	receipts                   map[string]DurableHostReceipt
	messages                   map[string][]byte
	deliveryOrder              []string
	projectionIdentities       map[string]string
	appliedProjectionPositions map[string][]uint64
}

// NewDurableClient constructs a Client-tier adapter over an injected transport.
func NewDurableClient[P any](transport DurableNATSTransport) (*DurableClient[P], error) {
	if transport == nil {
		return nil, errors.New("lazily: durable client transport is nil")
	}
	core, err := NewIngressCore[string, DurableProjectionEvent[P]](DefaultIngressPolicy(), KeepLatest[DurableProjectionEvent[P]]())
	if err != nil {
		return nil, err
	}
	return &DurableClient[P]{
		transport: transport, projections: core,
		latest:   make(map[string]DurableProjectionEvent[P]),
		receipts: make(map[string]DurableHostReceipt),
		messages: make(map[string][]byte), projectionIdentities: make(map[string]string),
		appliedProjectionPositions: make(map[string][]uint64),
	}, nil
}

// ObserveIngress preserves broker observation order while classifying stable
// message-identity repeats. It does not infer durable-owner order.
func (c *DurableClient[P]) ObserveIngress(envelope DurableIngressEnvelope) (DurableIngressClassification, error) {
	if err := validateDurableIngress(envelope.ProtocolVersion, envelope.MessageID, envelope.SchemaVersion, envelope.CodecVersion); err != nil {
		return "", err
	}
	wire, err := json.Marshal(envelope)
	if err != nil {
		return "", err
	}
	c.deliveryOrder = append(c.deliveryOrder, envelope.MessageID)
	prior, ok := c.messages[envelope.MessageID]
	if !ok {
		c.messages[envelope.MessageID] = wire
		return DurableIngressFirst, nil
	}
	if bytes.Equal(prior, wire) {
		return DurableIngressDuplicate, nil
	}
	return DurableIngressConflict, nil
}

// ObservedMessageIDs returns broker observation order without owner-order inference.
func (c *DurableClient[P]) ObservedMessageIDs() []string {
	return append([]string(nil), c.deliveryOrder...)
}

// PublishIngress validates and publishes an envelope. Its result is a broker
// PubAck only; callers must separately observe a DurableHostReceipt.
func (c *DurableClient[P]) PublishIngress(ctx context.Context, subject string, envelope DurableIngressEnvelope) (DurableBrokerPubAck, error) {
	if err := validateDurableIngress(envelope.ProtocolVersion, envelope.MessageID, envelope.SchemaVersion, envelope.CodecVersion); err != nil {
		return DurableBrokerPubAck{}, err
	}
	wire, err := json.Marshal(envelope)
	if err != nil {
		return DurableBrokerPubAck{}, fmt.Errorf("lazily: encode durable ingress: %w", err)
	}
	return c.transport.Publish(ctx, subject, wire)
}

// Subscribe exposes the injected NATS-compatible subscription seam.
func (c *DurableClient[P]) Subscribe(ctx context.Context, subject string) (DurableSubscription, error) {
	if subject == "" {
		return nil, errors.New("lazily: durable subscription subject is empty")
	}
	return c.transport.Subscribe(ctx, subject)
}

// ObserveHostReceipt validates and deduplicates a host receipt by message ID.
// Replaying the same receipt is harmless; conflicting receipts fail closed.
func (c *DurableClient[P]) ObserveHostReceipt(receipt DurableHostReceipt) error {
	if err := validateDurableHostReceipt(receipt); err != nil {
		return err
	}
	if prior, ok := c.receipts[receipt.ReceiptID]; ok {
		if prior != receipt {
			return fmt.Errorf("lazily: conflicting durable receipt %q", receipt.ReceiptID)
		}
		return nil
	}
	c.receipts[receipt.ReceiptID] = receipt
	return nil
}

// HostReceipt returns an observed host receipt without conflating it with a
// broker acknowledgement.
func (c *DurableClient[P]) HostReceipt(receiptID string) (DurableHostReceipt, bool) {
	receipt, ok := c.receipts[receiptID]
	return receipt, ok
}

// ObserveProjection validates, orders, and deduplicates a complete-history
// projection update. SourcePosition is one-based on the durable wire and maps
// to the zero-based sequence expected by IngressCore.
func (c *DurableClient[P]) ObserveProjection(event DurableProjectionEvent[P]) (IngressAdmission, error) {
	if err := validateDurableProjection(event); err != nil {
		return IngressAdmission{}, err
	}
	identity := fmt.Sprintf("%s\x00%d\x00%d", event.OwnerID, event.Generation, event.SourcePosition)
	fingerprint := fmt.Sprintf("%s\x00%s\x00%d\x00%d", event.SourceFingerprint, event.ProjectionFingerprint, event.SchemaVersion, event.CodecVersion)
	if prior, ok := c.projectionIdentities[identity]; ok && prior != fingerprint {
		return IngressAdmission{}, fmt.Errorf("lazily: conflicting durable projection at %s/%d", event.OwnerID, event.SourcePosition)
	}
	c.projectionIdentities[identity] = fingerprint
	before := int64(-1)
	if authority, ok := c.projections.Authority(event.OwnerID); ok && authority.DeliveredThrough.Present {
		before = int64(authority.DeliveredThrough.Value)
	}
	envelope := NewIngressEnvelope(event.OwnerID, event.Generation, event.SourcePosition-1, event.SourcePosition, event)
	_, admission := c.projections.Admit(envelope)
	after := before
	if authority, ok := c.projections.Authority(event.OwnerID); ok && authority.DeliveredThrough.Present {
		after = int64(authority.DeliveredThrough.Value)
	}
	for sequence := before + 1; sequence <= after; sequence++ {
		c.appliedProjectionPositions[event.OwnerID] = append(c.appliedProjectionPositions[event.OwnerID], uint64(sequence+1))
	}
	if _, projected, ok := c.projections.Drain(event.OwnerID); ok {
		c.latest[event.OwnerID] = projected
	}
	return admission, nil
}

// AppliedSourcePositions returns the complete-history positions made visible in
// authoritative source order, never broker arrival order.
func (c *DurableClient[P]) AppliedSourcePositions(ownerID string) []uint64 {
	return append([]uint64(nil), c.appliedProjectionPositions[ownerID]...)
}

// Projection returns the newest in-order advisory projection for ownerID.
func (c *DurableClient[P]) Projection(ownerID string) (DurableProjectionEvent[P], bool) {
	projection, ok := c.latest[ownerID]
	return projection, ok
}

func validateDurableIngress(protocolVersion uint64, messageID string, schemaVersion, codecVersion uint64) error {
	if protocolVersion != DurableProtocolVersion {
		return &DurableEnvelopeError{Reason: "unsupported_protocol_version"}
	}
	if messageID == "" {
		return &DurableEnvelopeError{Reason: "invalid_message_id"}
	}
	if schemaVersion == 0 || schemaVersion > 0xffffffff {
		return &DurableEnvelopeError{Reason: "invalid_schema_version"}
	}
	if codecVersion == 0 || codecVersion > 0xffffffff {
		return &DurableEnvelopeError{Reason: "invalid_codec_version"}
	}
	return nil
}

func validateDurableHostReceipt(receipt DurableHostReceipt) error {
	if err := validateDurableIngress(receipt.ProtocolVersion, receipt.MessageID, 1, 1); err != nil {
		return err
	}
	if receipt.ReceiptID == "" {
		return errors.New("lazily: durable receipt_id is required")
	}
	if receipt.Outcome != DurableHostCommitted && receipt.Outcome != DurableHostDuplicate && receipt.Outcome != DurableHostConflict && receipt.Outcome != DurableHostRejected {
		return fmt.Errorf("lazily: invalid durable host outcome %q", receipt.Outcome)
	}
	return nil
}

func validateDurableProjection[T any](event DurableProjectionEvent[T]) error {
	if event.ProtocolVersion != DurableProtocolVersion || event.OwnerID == "" || event.Generation == 0 || event.SourcePosition == 0 || event.ProjectionVersion == 0 || event.SchemaVersion == 0 || event.CodecVersion == 0 {
		return errors.New("lazily: invalid durable projection identity or version")
	}
	if event.Completeness != DurableProjectionCompleteHistory {
		return errors.New("lazily: durable projection client requires complete_history")
	}
	if event.MayAuthorizeTransition {
		return errors.New("lazily: advisory projection may not authorize transitions")
	}
	if event.SourceFingerprint == "" || event.ProjectionFingerprint == "" {
		return errors.New("lazily: durable projection fingerprints are required")
	}
	if event.Health != DurableProjectionHealthy && event.Health != DurableProjectionLagging && event.Health != DurableProjectionDrifted {
		return fmt.Errorf("lazily: invalid durable projection health %q", event.Health)
	}
	return nil
}

// EqualDurablePayload compares canonical encoded payloads in tests and
// adapters without assuming that a generic payload is Go-comparable.
func EqualDurablePayload(a, b any) bool {
	left, leftErr := json.Marshal(a)
	right, rightErr := json.Marshal(b)
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}
