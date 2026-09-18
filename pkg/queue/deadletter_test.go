package queue

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestDeadLetterEnvelopeRoundTripPreservesMalformedPayload(t *testing.T) {
	payload := []byte{0xff, 0x00, '{'}
	timestamp := time.Date(2026, 9, 16, 9, 48, 15, 0, time.UTC)
	encoded, err := encodeDeadLetter(payload, DeadLetterMetadata{
		Source:            "events",
		Destination:       "deadletter",
		ReplayDestination: "router",
		Reason:            DeadLetterReasonMalformed,
		Timestamp:         timestamp,
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
	if message.DeadLetter.Source != "events" ||
		message.DeadLetter.ReplayDestination != "router" ||
		message.DeadLetter.Timestamp != timestamp {
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

func TestReplayPlanDestinationPrecedence(t *testing.T) {
	message := DeadLetterMessage{
		ID: "message-1",
		DeadLetter: DeadLetterMetadata{
			Source:            "sequence",
			ReplayDestination: "event",
		},
	}
	tests := []struct {
		name        string
		request     DeadLetterReplayRequest
		destination string
	}{
		{
			name:        "recorded router",
			request:     DeadLetterReplayRequest{Limit: 1},
			destination: "event",
		},
		{
			name:        "explicit override",
			request:     DeadLetterReplayRequest{Limit: 1, Destination: "manual"},
			destination: "manual",
		},
		{
			name:        "older envelope fallback",
			request:     DeadLetterReplayRequest{Limit: 1},
			destination: "sequence",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			replayMessage := message
			if test.name == "older envelope fallback" {
				replayMessage.DeadLetter.ReplayDestination = ""
			}
			var result DeadLetterReplayResult
			plan, err := planDeadLetterReplay(&result, replayMessage, test.request, "deadletter")
			if err != nil {
				t.Fatalf("planDeadLetterReplay: %v", err)
			}
			if plan.destination != test.destination ||
				result.Planned != 1 ||
				result.Destinations[test.destination] != 1 {
				t.Fatalf("plan = %+v, result = %+v", plan, result)
			}
		})
	}
}

func TestRuntimeDeadLetterMetadataFallsBackToSourceWithoutRouter(t *testing.T) {
	metadata := runtimeDeadLetterMetadata("sequence", "deadletter", "", DeadLetterReasonHandlerError, 2)
	if metadata.ReplayDestination != "sequence" {
		t.Fatalf("replay destination = %q, want sequence", metadata.ReplayDestination)
	}
}

func TestTransformDeadLetterReplayMessages(t *testing.T) {
	messages := []DeadLetterMessage{
		{ID: "one", Payload: []byte("first")},
		{ID: "two", Payload: []byte("second")},
	}
	transformations, err := transformDeadLetterReplayMessages(context.Background(), func(_ context.Context, got []DeadLetterMessage) ([]DeadLetterReplayTransformation, error) {
		if len(got) != 2 || got[0].ID != "one" || got[1].ID != "two" {
			t.Fatalf("messages = %+v", got)
		}
		return []DeadLetterReplayTransformation{
			{Payload: []byte("transformed-first")},
			{Payload: []byte("transformed-second")},
		}, nil
	}, messages)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(transformations[0].Payload, []byte("transformed-first")) ||
		!bytes.Equal(transformations[1].Payload, []byte("transformed-second")) {
		t.Fatalf("transformations = %+v", transformations)
	}
}

func TestTransformDeadLetterReplayMessagesRequiresMatchingResultCount(t *testing.T) {
	_, err := transformDeadLetterReplayMessages(context.Background(), func(context.Context, []DeadLetterMessage) ([]DeadLetterReplayTransformation, error) {
		return nil, nil
	}, []DeadLetterMessage{{ID: "one"}})
	if err == nil {
		t.Fatal("expected transform result count error")
	}
}

func TestTransformDeadLetterReplayMessagesRejectsSkipAndDiscard(t *testing.T) {
	_, err := transformDeadLetterReplayMessages(context.Background(), func(context.Context, []DeadLetterMessage) ([]DeadLetterReplayTransformation, error) {
		return []DeadLetterReplayTransformation{{Skip: true, Discard: true}}, nil
	}, []DeadLetterMessage{{ID: "one"}})
	if err == nil || !strings.Contains(err.Error(), "cannot both skip and discard") {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateDeadLetterReplayRequestAllowsLargeBatchedLimit(t *testing.T) {
	if err := validateDeadLetterReplayRequest(DeadLetterReplayRequest{
		Limit:        30000,
		BatchSize:    100,
		BatchDelay:   time.Second,
		BatchTimeout: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	if err := validateDeadLetterReplayRequest(DeadLetterReplayRequest{
		Limit:     30000,
		BatchSize: 0,
	}); err == nil {
		t.Fatal("expected the legacy unbatched limit to remain capped")
	}
	if err := validateDeadLetterReplayRequest(DeadLetterReplayRequest{
		Limit:        1,
		BatchSize:    1,
		BatchTimeout: -time.Second,
	}); err == nil {
		t.Fatal("expected a negative batch timeout to be rejected")
	}
}
