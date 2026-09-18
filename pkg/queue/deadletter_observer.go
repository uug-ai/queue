package queue

import "encoding/json"

// DeadLetterPublishEvent reports one logical RabbitMQ DLQ publish after its final
// outcome, including any reconnect attempt, but before delivery settlement or
// disaster recovery. It contains no message body.
type DeadLetterPublishEvent struct {
	Source        string
	Destination   string // Actual configured DLQ, not an envelope's replay destination.
	Reason        DeadLetterReason
	Attempts      int
	TraceID       string
	MediaFileName string
	Confirmed     bool // True only for a broker-confirmed publish with Err == nil.
	Err           error
}

func (r *RabbitMQ) publishAndObserveDeadLetter(destination string, body []byte, confirmed bool, publish func() error) error {
	observer := r.options.DeadLetterObserver
	if observer == nil || destination != r.options.DeadletterQueue {
		return publish()
	}

	err := publish()
	event := DeadLetterPublishEvent{
		Source:      r.options.ConsumerQueue,
		Destination: destination,
		Reason:      DeadLetterReasonUnspecified,
		Confirmed:   confirmed && err == nil,
		Err:         err,
	}
	// Project only routing metadata and correlation fields, never the complete
	// application model (which may contain credentials or other sensitive data).
	var envelope struct {
		Schema     string `json:"schema"`
		Payload    []byte `json:"payload"`
		DeadLetter struct {
			Source   string           `json:"source"`
			Reason   DeadLetterReason `json:"reason"`
			Attempts int              `json:"attempts"`
		} `json:"deadLetter"`
	}
	if json.Unmarshal(body, &envelope) == nil && envelope.Schema == DeadLetterSchemaV1 {
		event.Source = envelope.DeadLetter.Source
		event.Reason = envelope.DeadLetter.Reason
		event.Attempts = envelope.DeadLetter.Attempts
		var correlation struct {
			TraceID  string `json:"traceId"`
			FileName string `json:"fileName"`
			Payload  struct {
				Key string `json:"key"`
			} `json:"payload"`
		}
		if json.Unmarshal(envelope.Payload, &correlation) == nil {
			event.TraceID = correlation.TraceID
			event.MediaFileName = correlation.Payload.Key
			if event.MediaFileName == "" {
				event.MediaFileName = correlation.FileName
			}
		}
	}
	observer(event)
	return err
}
