package lazily

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

type durableClientFixture struct {
	EnvelopeVectors []struct {
		Envelope json.RawMessage `json:"envelope"`
		Expected struct {
			Accepted bool `json:"accepted"`
		} `json:"expected"`
	} `json:"envelope_vectors"`
	OrderingVectors []struct {
		Observed           []string `json:"observed_message_ids"`
		Expected           []string `json:"expected_delivery_order"`
		OwnerOrderInferred bool     `json:"owner_order_inferred"`
	} `json:"ordering_vectors"`
	ProjectionOrderingVectors []struct {
		Observed                 []uint64 `json:"observed_source_positions"`
		ExpectedPositions        []uint64 `json:"expected_applied_positions"`
		ExpectedClassifications  []string `json:"expected_delivery_classification"`
		BrokerOrderAuthoritative bool     `json:"broker_order_authoritative"`
		MayAuthorizeTransition   bool     `json:"may_authorize_transition"`
	} `json:"projection_ordering_vectors"`
	DedupVectors []struct {
		Deliveries []json.RawMessage              `json:"deliveries"`
		Expected   []DurableIngressClassification `json:"expected_classification"`
	} `json:"dedup_vectors"`
	ReceiptVectors []struct {
		Receipt                DurableHostReceipt `json:"receipt"`
		Expected               DurableHostReceipt `json:"expected_round_trip"`
		TransportAckEquivalent bool               `json:"transport_ack_equivalent"`
	} `json:"receipt_vectors"`
	FingerprintVectors []struct {
		Left     DurableProjectionFingerprint           `json:"left"`
		Right    DurableProjectionFingerprint           `json:"right"`
		Expected DurableProjectionFingerprintComparison `json:"expected"`
	} `json:"projection_fingerprint_vectors"`
}

func loadDurableClientFixture(t *testing.T) (durableClientFixture, map[string]any) {
	t.Helper()
	data, err := specReadFile(specPath("durable-client", "envelope_v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture durableClientFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	var tracked map[string]any
	mustStrictJSON(t, "durable-client/envelope_v1.json", data, &tracked)
	return fixture, tracked
}

func TestDurableClientReplaysCanonicalFixture(t *testing.T) {
	fixture, tracked := loadDurableClientFixture(t)
	transport := &testDurableTransport{ack: DurableBrokerPubAck{Stream: "INGRESS", Sequence: 7}}
	client, err := NewDurableClient[[]uint64](transport)
	if err != nil {
		t.Fatal(err)
	}

	trackedEnvelopeVectors := jsList(tracked["envelope_vectors"])
	for index, vector := range fixture.EnvelopeVectors {
		var envelope DurableIngressEnvelope
		decodeErr := json.Unmarshal(vector.Envelope, &envelope)
		actualAccepted := decodeErr == nil
		actualReason := "accepted"
		if decodeErr != nil {
			var envelopeError *DurableEnvelopeError
			if !errors.As(decodeErr, &envelopeError) {
				t.Fatal(decodeErr)
			}
			actualReason = envelopeError.Reason
		}
		expectedBlock := consumeKeys(t, "durable client envelope expected", jsMap(jsMap(trackedEnvelopeVectors[index])["expected"]), "accepted", "reason", "payload_decoded")
		assertKey(t, expectedBlock, "accepted", actualAccepted)
		assertKey(t, expectedBlock, "reason", actualReason)
		assertKey(t, expectedBlock, "payload_decoded", actualAccepted)
		if vector.Expected.Accepted {
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if _, err := client.PublishIngress(context.Background(), "sample.ingress", envelope); err != nil {
				t.Fatal(err)
			}
			var roundTrip DurableIngressEnvelope
			if err := json.Unmarshal(transport.payload, &roundTrip); err != nil {
				t.Fatal(err)
			}
			if roundTrip.MessageID != envelope.MessageID || !bytesEqual(roundTrip.Payload, envelope.Payload) {
				t.Fatalf("round trip = %+v", roundTrip)
			}
		} else if decodeErr == nil {
			t.Fatalf("invalid envelope accepted: %s", vector.Envelope)
		}
	}

	for _, vector := range fixture.OrderingVectors {
		ordered, _ := NewDurableClient[[]uint64](transport)
		for index, messageID := range vector.Observed {
			_, err := ordered.ObserveIngress(DurableIngressEnvelope{ProtocolVersion: 1, MessageID: messageID, SchemaVersion: 7, CodecVersion: 11, Payload: []byte{byte(index)}})
			if err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(ordered.ObservedMessageIDs(), vector.Expected) || vector.OwnerOrderInferred {
			t.Fatalf("order = %v", ordered.ObservedMessageIDs())
		}
	}

	for _, vector := range fixture.ProjectionOrderingVectors {
		projected, _ := NewDurableClient[[]uint64](transport)
		classifications := make([]string, 0, len(vector.Observed))
		for _, position := range vector.Observed {
			event := DurableProjectionEvent[[]uint64]{ProtocolVersion: 1, OwnerID: "sample-owner", Generation: 1, SourcePosition: position, ProjectionVersion: position, SchemaVersion: 7, CodecVersion: 11, Completeness: DurableProjectionCompleteHistory, Entries: []uint64{position}, SourceFingerprint: "source", ProjectionFingerprint: "projection", Health: DurableProjectionHealthy}
			admission, err := projected.ObserveProjection(event)
			if err != nil {
				t.Fatal(err)
			}
			switch admission.Kind {
			case IngressAdmissionBuffered:
				classifications = append(classifications, "buffered")
			case IngressAdmissionDropped:
				classifications = append(classifications, "duplicate")
			default:
				classifications = append(classifications, "applied")
			}
		}
		if !reflect.DeepEqual(classifications, vector.ExpectedClassifications) || !reflect.DeepEqual(projected.AppliedSourcePositions("sample-owner"), vector.ExpectedPositions) || vector.BrokerOrderAuthoritative || vector.MayAuthorizeTransition {
			t.Fatalf("projection fixture mismatch: %v/%v", classifications, projected.AppliedSourcePositions("sample-owner"))
		}
	}

	for _, vector := range fixture.DedupVectors {
		dedup, _ := NewDurableClient[[]uint64](transport)
		actual := make([]DurableIngressClassification, 0, len(vector.Deliveries))
		for _, raw := range vector.Deliveries {
			var envelope DurableIngressEnvelope
			if err := json.Unmarshal(raw, &envelope); err != nil {
				t.Fatal(err)
			}
			classification, err := dedup.ObserveIngress(envelope)
			if err != nil {
				t.Fatal(err)
			}
			actual = append(actual, classification)
		}
		if !reflect.DeepEqual(actual, vector.Expected) {
			t.Fatalf("classifications = %v, want %v", actual, vector.Expected)
		}
	}

	for _, vector := range fixture.ReceiptVectors {
		if vector.Receipt != vector.Expected || vector.TransportAckEquivalent {
			t.Fatalf("receipt vector = %+v", vector)
		}
		if err := client.ObserveHostReceipt(vector.Receipt); err != nil {
			t.Fatal(err)
		}
		if err := client.ObserveHostReceipt(vector.Receipt); err != nil {
			t.Fatal(err)
		}
	}
	trackedFingerprints := jsList(tracked["projection_fingerprint_vectors"])
	for index, vector := range fixture.FingerprintVectors {
		if actual := CompareDurableProjectionFingerprints(vector.Left, vector.Right); actual != vector.Expected {
			t.Fatalf("fingerprint = %+v, want %+v", actual, vector.Expected)
		} else {
			expectedBlock := consumeKeys(t, "durable client fingerprint expected", jsMap(jsMap(trackedFingerprints[index])["expected"]), "same_source", "same_fingerprint", "same_completeness", "equivalent")
			assertKey(t, expectedBlock, "same_source", actual.SameSource)
			assertKey(t, expectedBlock, "same_fingerprint", actual.SameFingerprint)
			assertKey(t, expectedBlock, "same_completeness", actual.SameCompleteness)
			assertKey(t, expectedBlock, "equivalent", actual.Equivalent)
		}
	}
}

func bytesEqual(left, right []byte) bool { return reflect.DeepEqual(left, right) }
