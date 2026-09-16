//go:build !cgo

package queue

import "context"

func (k *Kafka) PublishDeadLetter(context.Context, []byte, DeadLetterMetadata) error {
	return kafkaUnavailableError()
}

func (k *Kafka) InspectDeadLetters(context.Context, DeadLetterInspectRequest) (DeadLetterInspectResult, error) {
	return DeadLetterInspectResult{}, kafkaUnavailableError()
}

func (k *Kafka) ReplayDeadLetters(context.Context, DeadLetterReplayRequest) (DeadLetterReplayResult, error) {
	return DeadLetterReplayResult{}, kafkaUnavailableError()
}
