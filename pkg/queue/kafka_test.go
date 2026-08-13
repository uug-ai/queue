//go:build cgo

package queue

import (
	"encoding/json"
	"errors"
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
	return nil, errStopKafkaConsumer
}

func (f *fakeKafkaConsumer) Close() error { return nil }

type fakeKafkaProducer struct {
	messages []kafka.Message
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
	return &kafka.Metadata{}, nil
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
	consumer := &fakeKafkaConsumer{message: message}
	producer := &fakeKafkaProducer{}
	client.Consumer = consumer
	client.Producer = producer
	return client, consumer, producer
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
