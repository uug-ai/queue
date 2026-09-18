package queue

import (
	"github.com/uug-ai/models/pkg/api"
	"github.com/uug-ai/models/pkg/models"
)

func (r *RabbitMQ) logDeadLetterRequest(reason DeadLetterReason, attempts int, events ...*models.PipelineEvent) {
	r.logDeadLetterRequestFromSource(r.options.ConsumerQueue, reason, attempts, events...)
}

func (r *RabbitMQ) logDeadLetterRequestFromSource(source string, reason DeadLetterReason, attempts int, events ...*models.PipelineEvent) {
	logger := r.options.Logger
	if logger == nil {
		return
	}
	if reason == "" {
		reason = DeadLetterReasonUnspecified
	}
	metadata := api.Metadata{
		Data: map[string]any{
			"sourceQueue":     source,
			"deadLetterQueue": r.options.DeadletterQueue,
			"reason":          reason,
			"attempts":        attempts,
		},
	}
	if len(events) > 0 && events[0] != nil {
		event := events[0]
		metadata.TraceId = event.TraceId
		metadata.MediaFileName = event.Payload.FileName
		if metadata.MediaFileName == "" {
			metadata.MediaFileName = event.FileName
		}
	}
	warning := api.CreateWarning(api.HttpNoStatus, api.PipelineWarning, api.PipelineStatus("dead_letter_requested"), metadata)
	logger.WithFields(api.CreateWarningLog(logger, warning)).Warn("Dead-letter publication requested")
}
