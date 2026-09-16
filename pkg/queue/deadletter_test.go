package queue

import (
	"bytes"
	"testing"
	"time"
)

func TestDeadLetterEnvelopeRoundTripPreservesMalformedPayload(t *testing.T) {
	payload := []byte{0xff, 0x00, '{'}
	timestamp := time.Date(2026, 9, 16, 9, 48, 15, 0, time.UTC)
	encoded, err := encodeDeadLetter(payload, DeadLetterMetadata{
		Source:      "events",
		Destination: "deadletter",
		Reason:      DeadLetterReasonMalformed,
		Timestamp:   timestamp,
	})
	if err != nil {
		t.Fatalf("encodeDeadLetter: %v", err)
	}
	message, err := decodeDeadLetter("message-1", encoded)
	if err != nil {
		t.Fatalf("decodeDeadLetter: %v", err)
	}
	if message.Legacy || !bytes.Equal(message.Payload, payload) {
		t.Fatalf("decoded message = %+v", message)
	}
	if message.DeadLetter.Source != "events" || message.DeadLetter.Timestamp != timestamp {
		t.Fatalf("decoded metadata = %+v", message.DeadLetter)
	}
}

func TestDeadLetterEnvelopeAlwaysWrapsEnvelopeShapedRuntimePayload(t *testing.T) {
	inner, err := encodeDeadLetter([]byte("payload"), DeadLetterMetadata{
		Source:      "attacker-selected-queue",
		Destination: "deadletter",
	})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := encodeDeadLetter(inner, DeadLetterMetadata{
		Source:      "actual-source",
		Destination: "deadletter",
	})
	if err != nil {
		t.Fatal(err)
	}

	message, err := decodeDeadLetter("message-1", outer)
	if err != nil {
		t.Fatal(err)
	}
	if message.DeadLetter.Source != "actual-source" || !bytes.Equal(message.Payload, inner) {
		t.Fatalf("decoded message = %+v", message)
	}
}

func TestDecodeDeadLetterTreatsRawPayloadAsLegacy(t *testing.T) {
	message, err := decodeDeadLetter("legacy-1", []byte(`{"event":"capture"}`))
	if err != nil {
		t.Fatalf("decodeDeadLetter: %v", err)
	}

	if !message.Legacy || deadLetterSource(message) != UnknownSourceQueue {
		t.Fatalf("decoded message = %+v", message)
	}
}

func TestDecodeDeadLetterRejectsUnknownEnvelopeVersion(t *testing.T) {
	_, err := decodeDeadLetter("future-1", []byte(`{"schema":"uug.ai/dead-letter/v2"}`))
	if err == nil {
		t.Fatal("expected unsupported envelope version error")
	}
}

func TestDeadLetterInspectionGroupsBySource(t *testing.T) {
	var result DeadLetterInspectResult
	addDeadLetterInspection(&result, DeadLetterMessage{
		DeadLetter: DeadLetterMetadata{Source: "monitor", Timestamp: time.Unix(10, 0)},
	}, "")
	addDeadLetterInspection(&result, DeadLetterMessage{
		DeadLetter: DeadLetterMetadata{Source: "monitor", Timestamp: time.Unix(20, 0)},
	}, "")
	addDeadLetterInspection(&result, DeadLetterMessage{Legacy: true}, "")

	if result.Scanned != 3 || result.Matched != 3 || result.Legacy != 1 {
		t.Fatalf("inspection result = %+v", result)
	}
	if result.Groups["monitor"].Count != 2 || result.Groups[UnknownSourceQueue].Count != 1 {
		t.Fatalf("inspection groups = %+v", result.Groups)
	}
}

func TestReplayPlanRequiresDestinationForLegacyMessage(t *testing.T) {
	var result DeadLetterReplayResult
	plan, err := planDeadLetterReplay(&result, DeadLetterMessage{ID: "legacy", Legacy: true}, DeadLetterReplayRequest{
		Limit:   1,
		Execute: true,
	}, "deadletter")
	if err != nil {
		t.Fatalf("planDeadLetterReplay: %v", err)
	}
	if plan.destination != "" || result.Unroutable != 1 || result.Retained != 1 {
		t.Fatalf("plan = %+v, result = %+v", plan, result)
	}
}
