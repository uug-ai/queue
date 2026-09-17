//go:build cgo

package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var _ DeadLetterAdmin = (*Kafka)(nil)
var _ DeadLetterAdmin = (*AzureEventHub)(nil)

func (k *Kafka) PublishDeadLetter(ctx context.Context, payload []byte, metadata DeadLetterMetadata) error {
	envelope, err := encodeDeadLetterForDestination(payload, metadata, k.options.DeadletterTopic)
	if err != nil {
		return err
	}
	if err := k.ensureConnected(); err != nil {
		return err
	}
	_, producer := k.clients()
	return k.publishContext(ctx, producer, k.options.DeadletterTopic, envelope, nil)
}

func (k *Kafka) InspectDeadLetters(ctx context.Context, request DeadLetterInspectRequest) (DeadLetterInspectResult, error) {
	var result DeadLetterInspectResult
	if err := validateDeadLetterLimit(request.Limit); err != nil {
		return result, err
	}
	if err := k.ensureConnected(); err != nil {
		return result, err
	}
	consumer, _ := k.clients()
	if err := consumer.SubscribeTopics([]string{k.options.DeadletterTopic}, nil); err != nil {
		return result, err
	}

	idleTimeout := request.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 2 * time.Second
	}
	assigned := false
	for result.Scanned < request.Limit {
		message, done, err := readKafkaDeadLetter(ctx, consumer, idleTimeout, &assigned)
		if err != nil {
			return result, err
		}
		if done {
			break
		}
		decoded, err := decodeDeadLetter(kafkaDeadLetterID(message), message.Value)
		if err != nil {
			return result, err
		}
		addDeadLetterInspection(&result, decoded, request.Source)
	}
	return result, nil
}

func (k *Kafka) ReplayDeadLetters(ctx context.Context, request DeadLetterReplayRequest) (DeadLetterReplayResult, error) {
	var result DeadLetterReplayResult
	if err := validateDeadLetterLimit(request.Limit); err != nil {
		return result, err
	}
	if request.Execute && request.Source != "" {
		return result, fmt.Errorf("Kafka cannot safely commit a selective source replay; omit --source and route each envelope to its recorded source")
	}
	if err := k.ensureConnected(); err != nil {
		return result, err
	}
	consumer, _ := k.clients()
	if err := consumer.SubscribeTopics([]string{k.options.DeadletterTopic}, nil); err != nil {
		return result, err
	}

	idleTimeout := request.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 2 * time.Second
	}
	assigned := false
	type replayItem struct {
		sourceMessage *kafka.Message
		message       DeadLetterMessage
		plan          deadLetterReplayPlan
	}
	items := make([]replayItem, 0, request.Limit)
	for result.Scanned < request.Limit {
		message, done, err := readKafkaDeadLetter(ctx, consumer, idleTimeout, &assigned)
		if err != nil {
			return result, err
		}
		if done {
			break
		}
		decoded, err := decodeDeadLetter(kafkaDeadLetterID(message), message.Value)
		if err != nil {
			return result, err
		}
		plan, err := planDeadLetterReplay(&result, decoded, request, k.options.DeadletterTopic)
		if err != nil {
			return result, err
		}
		if plan.destination == "" {
			if request.Execute && plan.matched {
				// Kafka commits are contiguous per partition. Do not process a
				// later message after retaining this one, because committing the
				// later offset could make the retained message unreplayable.
				break
			}
			continue
		}
		items = append(items, replayItem{sourceMessage: message, message: decoded, plan: plan})
	}

	decodedMessages := make([]DeadLetterMessage, len(items))
	for index := range items {
		decodedMessages[index] = items[index].message
	}
	payloads, err := transformDeadLetterReplayMessages(ctx, request.Transform, decodedMessages)
	if err != nil {
		return result, err
	}
	if request.Execute && len(items) > 0 {
		if !k.options.DisableAutoTopicCreation {
			return result, fmt.Errorf("Kafka replay requires automatic topic creation to be disabled")
		}
		_, producer := k.clients()
		validatedDestinations := make(map[string]struct{})
		for index, item := range items {
			if _, exists := validatedDestinations[item.plan.destination]; !exists {
				if err := ensureKafkaTopicExists(producer, item.plan.destination, k.deliveryTimeout()); err != nil {
					return result, fmt.Errorf("validate Kafka replay destination for message %q: %w", item.message.ID, err)
				}
				validatedDestinations[item.plan.destination] = struct{}{}
			}
			if err := k.publishContext(ctx, producer, item.plan.destination, payloads[index], nil); err != nil {
				return result, fmt.Errorf("replay Kafka dead-letter message %q: %w", item.message.ID, err)
			}
			if _, err := consumer.CommitMessage(item.sourceMessage); err != nil {
				return result, fmt.Errorf("commit replayed Kafka dead-letter message %q: %w", item.message.ID, err)
			}
			result.Replayed++
		}
	}
	return result, nil
}

func readKafkaDeadLetter(ctx context.Context, consumer KafkaConsumer, idleTimeout time.Duration, assigned *bool) (*kafka.Message, bool, error) {
	startupDeadline := time.Now().Add(30 * time.Second)
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		timeout := idleTimeout
		if deadline, ok := ctx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return nil, false, ctx.Err()
			}
			if remaining < timeout {
				timeout = remaining
			}
		}
		message, err := consumer.ReadMessage(timeout)
		if err == nil {
			if message == nil {
				return nil, false, fmt.Errorf("Kafka returned an empty dead-letter message")
			}
			*assigned = true
			return message, false, nil
		}
		var kafkaError kafka.Error
		if !errors.As(err, &kafkaError) || kafkaError.Code() != kafka.ErrTimedOut {
			return nil, false, err
		}
		if *assigned {
			return nil, true, nil
		}
		assignmentReader, ok := consumer.(kafkaAssignmentReader)
		if !ok {
			return nil, false, fmt.Errorf("Kafka consumer does not expose partition assignment")
		}
		assignment, assignmentErr := assignmentReader.Assignment()
		if assignmentErr != nil {
			return nil, false, fmt.Errorf("read Kafka partition assignment: %w", assignmentErr)
		}
		if len(assignment) > 0 {
			*assigned = true
			continue
		}
		if time.Now().After(startupDeadline) {
			return nil, false, fmt.Errorf("timed out waiting for Kafka dead-letter partition assignment")
		}
	}
}

func ensureKafkaTopicExists(producer KafkaProducer, topic string, timeout time.Duration) error {
	if producer == nil {
		return fmt.Errorf("Kafka producer is not initialized")
	}
	metadata, err := producer.GetMetadata(&topic, false, int(timeout/time.Millisecond))
	if err != nil {
		return err
	}
	if metadata == nil {
		return fmt.Errorf("Kafka returned no metadata for topic %q", topic)
	}
	topicMetadata, ok := metadata.Topics[topic]
	if !ok {
		return fmt.Errorf("Kafka topic %q does not exist", topic)
	}
	if topicMetadata.Error.Code() != kafka.ErrNoError {
		return fmt.Errorf("Kafka topic %q is unavailable: %w", topic, topicMetadata.Error)
	}
	return nil
}

func kafkaDeadLetterID(message *kafka.Message) string {
	topic := ""
	if message.TopicPartition.Topic != nil {
		topic = *message.TopicPartition.Topic
	}
	return fmt.Sprintf("%s/%d/%d", topic, message.TopicPartition.Partition, message.TopicPartition.Offset)
}
