package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	DeadLetterSchemaV1 = "uug.ai/dead-letter/v1"
	UnknownSourceQueue = "unknown"
)

type DeadLetterReason string

const (
	DeadLetterReasonUnspecified    DeadLetterReason = "unspecified"
	DeadLetterReasonMalformed      DeadLetterReason = "malformed"
	DeadLetterReasonHandlerError   DeadLetterReason = "handler_error"
	DeadLetterReasonRetryExhausted DeadLetterReason = "retry_exhausted"
	DeadLetterReasonPublishFailed  DeadLetterReason = "publish_failed"
)

// DeadLetterMetadata describes where and why a message was parked.
type DeadLetterMetadata struct {
	Source            string            `json:"source,omitempty"`
	Destination       string            `json:"destination"`
	ReplayDestination string            `json:"replayDestination,omitempty"`
	Service           string            `json:"service,omitempty"`
	Reason            DeadLetterReason  `json:"reason"`
	Attempts          int               `json:"attempts,omitempty"`
	Timestamp         time.Time         `json:"timestamp"`
	Attributes        map[string]string `json:"attributes,omitempty"`
}

// DeadLetterEnvelope is the provider-independent format stored on dead-letter
// destinations. Payload is base64 encoded by encoding/json so malformed or
// non-JSON messages can be preserved without alteration.
type DeadLetterEnvelope struct {
	Schema     string             `json:"schema"`
	Payload    []byte             `json:"payload"`
	DeadLetter DeadLetterMetadata `json:"deadLetter"`
}

type DeadLetterMessage struct {
	ID         string
	Payload    []byte
	DeadLetter DeadLetterMetadata
	Legacy     bool
}

type DeadLetterInspectRequest struct {
	Limit       int
	Source      string
	IdleTimeout time.Duration
}

type DeadLetterGroup struct {
	Source string
	Count  int
	Oldest time.Time
	Newest time.Time
}

type DeadLetterInspectResult struct {
	Scanned int
	Matched int
	Legacy  int
	Groups  map[string]DeadLetterGroup
}

type DeadLetterReplayRequest struct {
	Limit        int
	BatchSize    int
	BatchDelay   time.Duration
	BatchTimeout time.Duration
	Source       string
	Destination  string
	Execute      bool
	IdleTimeout  time.Duration
	Transform    DeadLetterReplayTransformer
}

type DeadLetterReplayTransformation struct {
	Payload []byte
	Skip    bool
	Discard bool
}

// DeadLetterReplayTransformer replaces, skips, or discards payloads for a planned replay
// batch. Implementations must return one result for each input message, in the
// same order. A skipped message remains on the dead-letter destination. A
// discarded message is settled without being published when execution is enabled.
type DeadLetterReplayTransformer func(context.Context, []DeadLetterMessage) ([]DeadLetterReplayTransformation, error)

type DeadLetterReplayResult struct {
	Scanned      int
	Matched      int
	Planned      int
	Replayed     int
	DropPlanned  int
	Dropped      int
	Retained     int
	Legacy       int
	Unroutable   int
	Skipped      int
	Destinations map[string]int
}

// DeadLetterAdmin is intentionally separate from QueueInterface: applications
// that only publish and consume messages do not need administrative operations.
type DeadLetterAdmin interface {
	InspectDeadLetters(context.Context, DeadLetterInspectRequest) (DeadLetterInspectResult, error)
	ReplayDeadLetters(context.Context, DeadLetterReplayRequest) (DeadLetterReplayResult, error)
	PublishDeadLetter(context.Context, []byte, DeadLetterMetadata) error
}

func encodeDeadLetter(payload []byte, metadata DeadLetterMetadata) ([]byte, error) {
	if metadata.Destination == "" {
		return nil, fmt.Errorf("dead-letter destination is required")
	}
	if metadata.Reason == "" {
		metadata.Reason = DeadLetterReasonUnspecified
	}
	if metadata.Timestamp.IsZero() {
		metadata.Timestamp = time.Now().UTC()
	}
	return json.Marshal(DeadLetterEnvelope{
		Schema:     DeadLetterSchemaV1,
		Payload:    append([]byte(nil), payload...),
		DeadLetter: metadata,
	})
}

func runtimeDeadLetterMetadata(source, deadLetterDestination, routerDestination string, reason DeadLetterReason, attempts int) DeadLetterMetadata {
	replayDestination := strings.TrimSpace(routerDestination)
	if replayDestination == "" {
		replayDestination = strings.TrimSpace(source)
	}
	return DeadLetterMetadata{
		Source:            source,
		Destination:       deadLetterDestination,
		ReplayDestination: replayDestination,
		Reason:            reason,
		Attempts:          attempts,
	}
}

func transformDeadLetterReplayMessages(ctx context.Context, transform DeadLetterReplayTransformer, messages []DeadLetterMessage) ([]DeadLetterReplayTransformation, error) {
	transformations := make([]DeadLetterReplayTransformation, len(messages))
	if len(messages) == 0 {
		return transformations, nil
	}
	if transform == nil {
		for index := range messages {
			transformations[index].Payload = append([]byte(nil), messages[index].Payload...)
		}
		return transformations, nil
	}
	transformed, err := transform(ctx, messages)
	if err != nil {
		return nil, fmt.Errorf("transform dead-letter replay batch: %w", err)
	}
	if len(transformed) != len(messages) {
		return nil, fmt.Errorf("transform dead-letter replay batch returned %d payloads for %d messages", len(transformed), len(messages))
	}
	for index := range transformed {
		if transformed[index].Skip && transformed[index].Discard {
			return nil, fmt.Errorf("dead-letter replay transformation %d cannot both skip and discard", index)
		}
		transformations[index] = DeadLetterReplayTransformation{
			Payload: append([]byte(nil), transformed[index].Payload...),
			Skip:    transformed[index].Skip,
			Discard: transformed[index].Discard,
		}
	}
	return transformations, nil
}

func encodeDeadLetterForDestination(payload []byte, metadata DeadLetterMetadata, destination string) ([]byte, error) {
	if metadata.Source == "" {
		return nil, fmt.Errorf("dead-letter source is required")
	}
	if metadata.Destination != destination {
		return nil, fmt.Errorf("dead-letter destination %q does not match configured destination %q", metadata.Destination, destination)
	}
	return encodeDeadLetter(payload, metadata)
}

// EncodeDeadLetter creates a portable dead-letter envelope for administrative
// tooling and tests. Runtime clients normally create envelopes automatically.
func EncodeDeadLetter(payload []byte, metadata DeadLetterMetadata) ([]byte, error) {
	return encodeDeadLetter(payload, metadata)
}

func decodeDeadLetter(id string, payload []byte) (DeadLetterMessage, error) {
	envelopePayload, recognized, err := decodeDeadLetterEnvelope(payload)
	if err != nil {
		return DeadLetterMessage{}, err
	}
	if !recognized {
		return DeadLetterMessage{
			ID:      id,
			Payload: append([]byte(nil), payload...),
			Legacy:  true,
		}, nil
	}

	var envelope DeadLetterEnvelope
	if err := json.Unmarshal(envelopePayload, &envelope); err != nil {
		return DeadLetterMessage{}, fmt.Errorf("decode dead-letter envelope: %w", err)
	}
	return DeadLetterMessage{
		ID:         id,
		Payload:    append([]byte(nil), envelope.Payload...),
		DeadLetter: envelope.DeadLetter,
	}, nil
}

func decodeDeadLetterEnvelope(payload []byte) ([]byte, bool, error) {
	var header struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(payload, &header); err != nil || header.Schema == "" {
		return nil, false, nil
	}
	if header.Schema != DeadLetterSchemaV1 {
		if strings.HasPrefix(header.Schema, "uug.ai/dead-letter/") {
			return nil, true, fmt.Errorf("unsupported dead-letter schema %q", header.Schema)
		}
		return nil, false, nil
	}

	var envelope DeadLetterEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, true, fmt.Errorf("decode %s envelope: %w", DeadLetterSchemaV1, err)
	}
	if envelope.DeadLetter.Destination == "" {
		return nil, true, fmt.Errorf("%s envelope has no dead-letter destination", DeadLetterSchemaV1)
	}
	if envelope.DeadLetter.Timestamp.IsZero() {
		return nil, true, fmt.Errorf("%s envelope has no timestamp", DeadLetterSchemaV1)
	}
	return append([]byte(nil), payload...), true, nil
}

func validateDeadLetterLimit(limit int) error {
	if limit < 1 {
		return fmt.Errorf("dead-letter message limit must be at least 1")
	}
	if limit > 10000 {
		return fmt.Errorf("dead-letter message limit cannot exceed 10000")
	}
	return nil
}

func validateDeadLetterReplayRequest(request DeadLetterReplayRequest) error {
	if request.BatchDelay < 0 {
		return fmt.Errorf("dead-letter batch delay cannot be negative")
	}
	if request.BatchTimeout < 0 {
		return fmt.Errorf("dead-letter batch timeout cannot be negative")
	}
	if request.BatchSize == 0 {
		return validateDeadLetterLimit(request.Limit)
	}
	if request.Limit < 1 || request.Limit > 1000000 {
		return fmt.Errorf("dead-letter message limit must be between 1 and 1000000 for batched replay")
	}
	if request.BatchSize < 1 || request.BatchSize > 10000 {
		return fmt.Errorf("dead-letter batch size must be between 1 and 10000")
	}
	return nil
}

func deadLetterReplayBatchSize(request DeadLetterReplayRequest) int {
	if request.BatchSize > 0 {
		return request.BatchSize
	}
	return request.Limit
}

func waitForDeadLetterReplayBatch(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func deadLetterReplayBatchContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func addDeadLetterInspection(result *DeadLetterInspectResult, message DeadLetterMessage, sourceFilter string) {
	result.Scanned++
	if message.Legacy {
		result.Legacy++
	}
	source := deadLetterSource(message)
	if sourceFilter != "" && source != sourceFilter {
		return
	}
	result.Matched++
	if result.Groups == nil {
		result.Groups = make(map[string]DeadLetterGroup)
	}
	group := result.Groups[source]
	group.Source = source
	group.Count++
	timestamp := message.DeadLetter.Timestamp
	if !timestamp.IsZero() {
		if group.Oldest.IsZero() || timestamp.Before(group.Oldest) {
			group.Oldest = timestamp
		}
		if group.Newest.IsZero() || timestamp.After(group.Newest) {
			group.Newest = timestamp
		}
	}
	result.Groups[source] = group
}

type deadLetterReplayPlan struct {
	matched     bool
	destination string
}

func planDeadLetterReplay(result *DeadLetterReplayResult, message DeadLetterMessage, request DeadLetterReplayRequest, deadLetterDestination string) (deadLetterReplayPlan, error) {
	result.Scanned++
	if message.Legacy {
		result.Legacy++
	}
	source := deadLetterSource(message)
	if request.Source != "" && source != request.Source {
		result.Retained++
		return deadLetterReplayPlan{}, nil
	}

	result.Matched++
	destination := strings.TrimSpace(request.Destination)
	if destination == "" && !message.Legacy {
		destination = strings.TrimSpace(message.DeadLetter.ReplayDestination)
		if destination == "" {
			destination = strings.TrimSpace(message.DeadLetter.Source)
		}
	}
	if destination == "" {
		result.Unroutable++
		result.Retained++
		return deadLetterReplayPlan{matched: true}, nil
	}
	if destination == deadLetterDestination {
		return deadLetterReplayPlan{}, fmt.Errorf("refusing to replay dead-letter message %q back to %q", message.ID, deadLetterDestination)
	}
	result.Planned++
	if result.Destinations == nil {
		result.Destinations = make(map[string]int)
	}
	result.Destinations[destination]++
	if !request.Execute {
		result.Retained++
	}
	return deadLetterReplayPlan{matched: true, destination: destination}, nil
}

func deadLetterSource(message DeadLetterMessage) string {
	if message.Legacy || strings.TrimSpace(message.DeadLetter.Source) == "" {
		return UnknownSourceQueue
	}
	return message.DeadLetter.Source
}

func skipDeadLetterReplay(result *DeadLetterReplayResult, destination string, execute bool) {
	result.Skipped++
	retainPlannedDeadLetterReplay(result, destination, execute)
}

func retainPlannedDeadLetterReplay(result *DeadLetterReplayResult, destination string, execute bool) {
	result.Planned--
	if count := result.Destinations[destination]; count <= 1 {
		delete(result.Destinations, destination)
	} else {
		result.Destinations[destination] = count - 1
	}
	if execute {
		result.Retained++
	}
}

func planDeadLetterDiscard(result *DeadLetterReplayResult, destination string) {
	result.Planned--
	if count := result.Destinations[destination]; count <= 1 {
		delete(result.Destinations, destination)
	} else {
		result.Destinations[destination] = count - 1
	}
	result.DropPlanned++
}
