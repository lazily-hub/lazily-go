package lazily

import (
	"context"
	"errors"
	"testing"
)

type testDurableTransport struct {
	subject string
	payload []byte
	ack     DurableBrokerPubAck
}

func (t *testDurableTransport) Publish(_ context.Context, subject string, payload []byte) (DurableBrokerPubAck, error) {
	t.subject, t.payload = subject, append([]byte(nil), payload...)
	return t.ack, nil
}

func (t *testDurableTransport) Subscribe(context.Context, string) (DurableSubscription, error) {
	return nil, errors.New("not used")
}

func testProjection(position uint64, entries []string) DurableProjectionEvent[[]string] {
	return DurableProjectionEvent[[]string]{
		ProtocolVersion: DurableProtocolVersion, OwnerID: "sample-owner", Generation: 1,
		SourcePosition: position, ProjectionVersion: position, SchemaVersion: 1, CodecVersion: 1,
		Completeness: DurableProjectionCompleteHistory, Entries: entries,
		SourceFingerprint: "source-fingerprint", ProjectionFingerprint: "projection-fingerprint",
		Health: DurableProjectionHealthy, MayAuthorizeTransition: false,
	}
}

func TestDurableClientKeepsPubAckDistinctFromHostReceipt(t *testing.T) {
	transport := &testDurableTransport{ack: DurableBrokerPubAck{Stream: "INGRESS", Sequence: 7}}
	client, err := NewDurableClient[[]string](transport)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := client.PublishIngress(context.Background(), "sample.ingress", DurableIngressEnvelope{
		ProtocolVersion: 1, MessageID: "message-1", SchemaVersion: 1, CodecVersion: 1,
		Payload: []byte{0, 1, 127, 128, 255},
	})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Sequence != 7 || transport.subject != "sample.ingress" {
		t.Fatalf("ack/subject = %+v/%q", ack, transport.subject)
	}
	if _, ok := client.HostReceipt("receipt-1"); ok {
		t.Fatal("broker ack must not synthesize host receipt")
	}
	receipt := DurableHostReceipt{ProtocolVersion: 1, ReceiptID: "receipt-1", MessageID: "message-1", Outcome: DurableHostCommitted, OwnerPosition: 9}
	if err := client.ObserveHostReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	if got, ok := client.HostReceipt("receipt-1"); !ok || got != receipt {
		t.Fatalf("receipt = %+v/%v", got, ok)
	}
	if err := client.ObserveHostReceipt(DurableHostReceipt{ProtocolVersion: 1, ReceiptID: "receipt-1", MessageID: "message-1", Outcome: DurableHostDuplicate, OwnerPosition: 9}); err == nil {
		t.Fatal("conflicting receipt accepted")
	}
}

func TestDurableClientOrdersAndDeduplicatesCompleteHistoryProjection(t *testing.T) {
	client, err := NewDurableClient[[]string](&testDurableTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if admission, err := client.ObserveProjection(testProjection(2, []string{"a", "b"})); err != nil || admission.Kind != IngressAdmissionBuffered {
		t.Fatalf("position 2 = %+v, %v", admission, err)
	}
	if _, ok := client.Projection("sample-owner"); ok {
		t.Fatal("gap must not publish projection")
	}
	if _, err := client.ObserveProjection(testProjection(1, []string{"a"})); err != nil {
		t.Fatal(err)
	}
	got, ok := client.Projection("sample-owner")
	if !ok || got.SourcePosition != 2 || !EqualDurablePayload(got.Entries, []string{"a", "b"}) {
		t.Fatalf("projection = %+v/%v", got, ok)
	}
	duplicate, err := client.ObserveProjection(testProjection(2, []string{"a", "b"}))
	if err != nil || duplicate.Kind != IngressAdmissionDropped || duplicate.Reason != IngressDropDuplicateSequence {
		t.Fatalf("duplicate = %+v, %v", duplicate, err)
	}
	conflict := testProjection(2, []string{"changed"})
	conflict.ProjectionFingerprint = "changed-fingerprint"
	if _, err := client.ObserveProjection(conflict); err == nil {
		t.Fatal("same-position fingerprint conflict accepted")
	}
}

func TestDurableProjectionIsAdvisoryCompleteHistoryOnly(t *testing.T) {
	client, err := NewDurableClient[[]string](&testDurableTransport{})
	if err != nil {
		t.Fatal(err)
	}
	latest := testProjection(1, []string{"a"})
	latest.Completeness = "latest_durable"
	if _, err := client.ObserveProjection(latest); err == nil {
		t.Fatal("latest-only projection accepted")
	}
	authoritative := testProjection(1, []string{"a"})
	authoritative.MayAuthorizeTransition = true
	if _, err := client.ObserveProjection(authoritative); err == nil {
		t.Fatal("authoritative projection accepted")
	}
}

func TestDurableCapabilityTierIsClientOnly(t *testing.T) {
	caps := NewBindingCapabilities()
	if !caps.CoreTier || !caps.ClientTier || caps.DurableHostTier || caps.DistributedHostTier || caps.AcceleratedHostTier {
		t.Fatalf("durable tiers = %+v", caps)
	}
}
