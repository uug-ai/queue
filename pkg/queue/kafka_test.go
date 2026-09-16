//go:build cgo

package queue

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/uug-ai/models/pkg/models"
)

var errStopKafkaConsumer = errors.New("stop Kafka consumer after commit")

type fakeKafkaConsumer struct {
	message       *kafka.Message
	subscribed    []string
	commitCount   int
	commitMessage *kafka.Message
	commitError   error
}

func (f *fakeKafkaConsumer) SubscribeTopics(topics []string, _ kafka.RebalanceCb) error {
	f.subscribed = append([]string(nil), topics...)
	return nil
}

func (f *fakeKafkaConsumer) ReadMessage(time.Duration) (*kafka.Message, error) {
	return f.message, nil
}

func (f *fakeKafkaConsumer) CommitMessage(message *kafka.Message) ([]kafka.TopicPartition, error) {
	f.commitCount++
	f.commitMessage = message
	return nil, f.commitError
}

func (f *fakeKafkaConsumer) Assignment() ([]kafka.TopicPartition, error) {
	return []kafka.TopicPartition{{Partition: 0}}, nil
}

func (f *fakeKafkaConsumer) Close() error { return nil }

type fakeKafkaProducer struct {
	messages    []kafka.Message
	metadata    *kafka.Metadata
	metadataErr error
}

func (f *fakeKafkaProducer) Produce(message *kafka.Message, deliveryChan chan kafka.Event) error {
	copyMessage := *message
	copyMessage.Value = append([]byte(nil), message.Value...)
	copyMessage.Headers = append([]kafka.Header(nil), message.Headers...)
	f.messages = append(f.messages, copyMessage)
	deliveryChan <- &kafka.Message{TopicPartition: message.TopicPartition}
	return nil
}

func (f *fakeKafkaProducer) GetMetadata(*string, bool, int) (*kafka.Metadata, error) {
	return f.metadata, f.metadataErr
}

func (f *fakeKafkaProducer) Flush(int) int { return 0 }
func (f *fakeKafkaProducer) Close()        {}

func newTestKafka(t *testing.T, message *kafka.Message) (*Kafka, *fakeKafkaConsumer, *fakeKafkaProducer) {
	t.Helper()
	opts := NewKafkaOptions().
		SetConsumerTopic("events").
		SetRouterTopic("router").
		SetDeadletterTopic("deadletter").
		SetBroker("kafka:9092").
		SetGroupID("pipeline").
		SetMaxRetries(1).
		Build()
	client, err := NewKafka(opts)
	if err != nil {
		t.Fatalf("NewKafka: %v", err)
	}
	consumer := &fakeKafkaConsumer{message: message, commitError: errStopKafkaConsumer}
	producer := &fakeKafkaProducer{
		metadata: &kafka.Metadata{
			Topics: map[string]kafka.TopicMetadata{
				"events": {Topic: "events"},
			},
		},
	}
	client.Consumer = consumer
	client.Producer = producer
	return client, consumer, producer
}

func TestKafkaDeadLetterReplayPublishesBeforeCommit(t *testing.T) {
	envelope, err := encodeDeadLetter([]byte("payload"), DeadLetterMetadata{
		Source:      "events",
		Destination: "deadletter",
	})
	if err != nil {
		t.Fatal(err)
	}
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     stringPointer("deadletter"),
			Partition: 1,
			Offset:    42,
		},
		Value: envelope,
	}
	client, consumer, producer := newTestKafka(t, message)
	consumer.commitError = nil
	client.options.DisableAutoTopicCreation = true

	result, err := client.ReplayDeadLetters(context.Background(), DeadLetterReplayRequest{
		Limit:   1,
		Execute: true,
	})
	if err != nil {
		t.Fatalf("ReplayDeadLetters: %v", err)
	}
	if result.Replayed != 1 || consumer.commitCount != 1 || len(producer.messages) != 1 {
		t.Fatalf("result=%+v commits=%d messages=%+v", result, consumer.commitCount, producer.messages)
	}
	if *producer.messages[0].TopicPartition.Topic != "events" || string(producer.messages[0].Value) != "payload" {
		t.Fatalf("replayed message = %+v", producer.messages[0])
	}
}

func TestKafkaDeadLetterReplayRejectsMissingDestinationBeforePublishOrCommit(t *testing.T) {
	envelope, err := encodeDeadLetter([]byte("payload"), DeadLetterMetadata{
		Source:      "missing-events",
		Destination: "deadletter",
	})
	if err != nil {
		t.Fatal(err)
	}
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     stringPointer("deadletter"),
			Partition: 1,
			Offset:    42,
		},
		Value: envelope,
	}
	client, consumer, producer := newTestKafka(t, message)
	client.options.DisableAutoTopicCreation = true

	_, err = client.ReplayDeadLetters(context.Background(), DeadLetterReplayRequest{
		Limit:   1,
		Execute: true,
	})
	if err == nil || !strings.Contains(err.Error(), `Kafka topic "missing-events" does not exist`) {
		t.Fatalf("ReplayDeadLetters error = %v, want missing topic error", err)
	}
	if consumer.commitCount != 0 || len(producer.messages) != 0 {
		t.Fatalf("commits=%d messages=%d, want no commit or publish", consumer.commitCount, len(producer.messages))
	}
}

func TestKafkaDeadLetterReplayRequiresAutoTopicCreationDisabled(t *testing.T) {
	envelope, err := encodeDeadLetter([]byte("payload"), DeadLetterMetadata{
		Source:      "events",
		Destination: "deadletter",
	})
	if err != nil {
		t.Fatal(err)
	}
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: stringPointer("deadletter")},
		Value:          envelope,
	}
	client, consumer, producer := newTestKafka(t, message)

	_, err = client.ReplayDeadLetters(context.Background(), DeadLetterReplayRequest{
		Limit:   1,
		Execute: true,
	})
	if err == nil || !strings.Contains(err.Error(), "automatic topic creation") {
		t.Fatalf("ReplayDeadLetters error = %v, want safe configuration error", err)
	}
	if consumer.commitCount != 0 || len(producer.messages) != 0 {
		t.Fatalf("commits=%d messages=%d, want no commit or publish", consumer.commitCount, len(producer.messages))
	}
}

func TestKafkaDeadLetterInspectDoesNotCommit(t *testing.T) {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: stringPointer("deadletter")},
		Value:          []byte(`{"legacy":true}`),
	}
	client, consumer, _ := newTestKafka(t, message)

	result, err := client.InspectDeadLetters(context.Background(), DeadLetterInspectRequest{Limit: 1})
	if err != nil {
		t.Fatalf("InspectDeadLetters: %v", err)
	}
	if result.Groups[UnknownSourceQueue].Count != 1 || consumer.commitCount != 0 {
		t.Fatalf("result=%+v commits=%d", result, consumer.commitCount)
	}
}

func stringPointer(value string) *string {
	return &value
}

func TestNewSelectsKafkaClient(t *testing.T) {
	opts := NewKafkaOptions().
		SetConsumerTopic("events").
		SetDeadletterTopic("deadletter").
		SetBroker("kafka:9092").
		SetGroupID("pipeline").
		Build()
	client, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := client.Client.(*Kafka); !ok {
		t.Fatalf("expected *Kafka, got %T", client.Client)
	}
}

func TestKafkaConfigMapDisablesAutoCommit(t *testing.T) {
	client, _, _ := newTestKafka(t, nil)
	config := client.configMap()
	if config["enable.auto.commit"] != false || config["enable.auto.offset.store"] != false {
		t.Fatalf("expected manual offset management, got %v", config)
	}
	if config["session.timeout.ms"] != 10000 || config["auto.offset.reset"] != "earliest" {
		t.Fatalf("expected historical Kafka defaults, got %v", config)
	}
	if _, exists := config["allow.auto.create.topics"]; exists {
		t.Fatalf("library clients must preserve the provider default, got %v", config)
	}
}

func TestKafkaConfigMapCanDisableAutoTopicCreation(t *testing.T) {
	client, _, _ := newTestKafka(t, nil)
	client.options.DisableAutoTopicCreation = true

	if got := client.configMap()["allow.auto.create.topics"]; got != false {
		t.Fatalf("allow.auto.create.topics = %v, want false", got)
	}
}

func TestKafkaPublishWaitsForDelivery(t *testing.T) {
	client, _, producer := newTestKafka(t, nil)
	if err := client.Publish("target", []byte("payload")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if len(producer.messages) != 1 {
		t.Fatalf("published messages = %d, want 1", len(producer.messages))
	}
	if topic := *producer.messages[0].TopicPartition.Topic; topic != "target" {
		t.Fatalf("published topic = %q, want target", topic)
	}
}

func TestKafkaPublishToDeadLetterTopicAddsEnvelope(t *testing.T) {
	client, _, producer := newTestKafka(t, nil)
	if err := client.Publish("deadletter", []byte("payload")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	message, err := decodeDeadLetter("deadletter", producer.messages[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if message.Legacy || message.DeadLetter.Source != "events" || string(message.Payload) != "payload" {
		t.Fatalf("dead-letter message = %+v", message)
	}
}

func TestKafkaReadMessagesForwardsThenCommits(t *testing.T) {
	payload, err := json.Marshal(models.PipelineEvent{Stages: []string{"analysis", "classify"}})
	if err != nil {
		t.Fatal(err)
	}
	message := &kafka.Message{Value: payload}
	client, consumer, producer := newTestKafka(t, message)
	metricsCount := 0
	err = client.ReadMessages(
		func(event models.PipelineEvent, _ ...any) (models.PipelineAction, models.PipelineEvent, int) {
			return models.PipelineForward, event, 0
		},
		func(models.PipelineMetrics) { metricsCount++ },
	)
	if !errors.Is(err, errStopKafkaConsumer) {
		t.Fatalf("ReadMessages error = %v, want stop error", err)
	}
	if consumer.commitCount != 1 || consumer.commitMessage != message {
		t.Fatalf("message was not committed after forwarding")
	}
	if len(consumer.subscribed) != 1 || consumer.subscribed[0] != "events" {
		t.Fatalf("subscribed topics = %v, want [events]", consumer.subscribed)
	}
	if len(producer.messages) != 1 || *producer.messages[0].TopicPartition.Topic != "router" {
		t.Fatalf("forwarded messages = %+v, want one router message", producer.messages)
	}
	var forwarded models.PipelineEvent
	if err := json.Unmarshal(producer.messages[0].Value, &forwarded); err != nil {
		t.Fatal(err)
	}
	if len(forwarded.Stages) != 1 || forwarded.Stages[0] != "classify" {
		t.Fatalf("forwarded stages = %v, want [classify]", forwarded.Stages)
	}
	if metricsCount != 1 {
		t.Fatalf("metrics calls = %d, want 1", metricsCount)
	}
}

func TestKafkaMalformedMessageDeadlettersThenCommits(t *testing.T) {
	message := &kafka.Message{Value: []byte("not-json")}
	client, consumer, producer := newTestKafka(t, message)
	err := client.ReadMessages(
		func(event models.PipelineEvent, _ ...any) (models.PipelineAction, models.PipelineEvent, int) {
			return models.PipelineCancel, event, 0
		},
		func(models.PipelineMetrics) {},
	)
	if !errors.Is(err, errStopKafkaConsumer) {
		t.Fatalf("ReadMessages error = %v, want stop error", err)
	}
	if consumer.commitCount != 1 {
		t.Fatalf("commit count = %d, want 1", consumer.commitCount)
	}
	if len(producer.messages) != 1 || *producer.messages[0].TopicPartition.Topic != "deadletter" {
		t.Fatalf("deadletter messages = %+v, want one deadletter message", producer.messages)
	}
	deadLetter, err := decodeDeadLetter("deadletter", producer.messages[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if deadLetter.DeadLetter.Source != "events" || deadLetter.DeadLetter.Reason != DeadLetterReasonMalformed {
		t.Fatalf("dead-letter metadata = %+v", deadLetter.DeadLetter)
	}
}

func TestKafkaRetryExhaustionDeadlettersThenCommits(t *testing.T) {
	payload, err := json.Marshal(models.PipelineEvent{})
	if err != nil {
		t.Fatal(err)
	}
	message := &kafka.Message{
		Value:   payload,
		Headers: []kafka.Header{{Key: retryCountHeader, Value: []byte("1")}},
	}
	client, consumer, producer := newTestKafka(t, message)
	err = client.ReadMessages(
		func(event models.PipelineEvent, _ ...any) (models.PipelineAction, models.PipelineEvent, int) {
			return models.PipelineRetry, event, 0
		},
		func(models.PipelineMetrics) {},
	)
	if !errors.Is(err, errStopKafkaConsumer) {
		t.Fatalf("ReadMessages error = %v, want stop error", err)
	}
	if consumer.commitCount != 1 {
		t.Fatalf("commit count = %d, want 1", consumer.commitCount)
	}
	if len(producer.messages) != 1 || *producer.messages[0].TopicPartition.Topic != "deadletter" {
		t.Fatalf("retry exhaustion messages = %+v, want one deadletter message", producer.messages)
	}
}

func TestKafkaRetryHeadersReplaceExistingCount(t *testing.T) {
	headers := kafkaRetryHeaders([]kafka.Header{
		{Key: "trace", Value: []byte("abc")},
		{Key: retryCountHeader, Value: []byte("1")},
	}, 2)
	if len(headers) != 2 || kafkaRetryCount(headers) != 2 {
		t.Fatalf("retry headers = %+v, want trace header and retry count 2", headers)
	}
}
