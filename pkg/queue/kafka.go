//go:build cgo

package queue

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/uug-ai/models/pkg/models"
)

const kafkaDeliveryTimeout = 5 * time.Second

type KafkaConsumer interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	CommitMessage(message *kafka.Message) ([]kafka.TopicPartition, error)
	Close() error
}

type KafkaProducer interface {
	Produce(message *kafka.Message, deliveryChan chan kafka.Event) error
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
	Flush(timeoutMs int) int
	Close()
}

// Kafka implements QueueInterface using Confluent's librdkafka client.
type Kafka struct {
	options                 *KafkaOptions
	Consumer                KafkaConsumer
	Producer                KafkaProducer
	disasterRecoveryHandler DisasterRecoveryHandler
	mu                      sync.Mutex
}

func NewKafka(options *KafkaOptions) (*Kafka, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return &Kafka{options: options}, nil
}

func (k *Kafka) configMap() kafka.ConfigMap {
	sessionTimeout := k.options.SessionTimeout
	if sessionTimeout <= 0 {
		sessionTimeout = 10000
	}
	autoOffsetReset := k.options.AutoOffsetReset
	if autoOffsetReset == "" {
		autoOffsetReset = "earliest"
	}

	config := kafka.ConfigMap{
		"bootstrap.servers":        k.options.Broker,
		"group.id":                 k.options.GroupID,
		"session.timeout.ms":       sessionTimeout,
		"auto.offset.reset":        autoOffsetReset,
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
	}
	if k.options.Mechanism != "" {
		config["sasl.mechanisms"] = k.options.Mechanism
	}
	if k.options.Security != "" {
		config["security.protocol"] = k.options.Security
	}
	if k.options.Username != "" {
		config["sasl.username"] = k.options.Username
	}
	if k.options.Password != "" {
		config["sasl.password"] = k.options.Password
	}
	return config
}

func (k *Kafka) Connect() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	config := k.configMap()
	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		_ = consumer.Close()
		return err
	}
	if _, err := producer.GetMetadata(nil, false, int(kafkaDeliveryTimeout/time.Millisecond)); err != nil {
		producer.Close()
		_ = consumer.Close()
		return err
	}

	oldConsumer := k.Consumer
	oldProducer := k.Producer
	k.Consumer = consumer
	k.Producer = producer
	k.closeClients(oldConsumer, oldProducer)
	return nil
}

func (k *Kafka) Reconnect() error {
	return k.Connect()
}

func (k *Kafka) Close() {
	k.mu.Lock()
	consumer := k.Consumer
	producer := k.Producer
	k.Consumer = nil
	k.Producer = nil
	k.mu.Unlock()
	k.closeClients(consumer, producer)
}

func (k *Kafka) closeClients(consumer KafkaConsumer, producer KafkaProducer) {
	if producer != nil {
		producer.Flush(int(kafkaDeliveryTimeout / time.Millisecond))
		producer.Close()
	}
	if consumer != nil {
		_ = consumer.Close()
	}
}

func (k *Kafka) clients() (KafkaConsumer, KafkaProducer) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.Consumer, k.Producer
}

func (k *Kafka) ensureConnected() error {
	consumer, producer := k.clients()
	if consumer != nil && producer != nil {
		return nil
	}
	return k.Connect()
}

func (k *Kafka) Publish(topic string, payload []byte) error {
	return k.publishWithReconnect(topic, payload, nil)
}

func (k *Kafka) publishWithReconnect(topic string, payload []byte, headers []kafka.Header) error {
	if err := k.ensureConnected(); err != nil {
		_ = k.DisasterRecovery(payload)
		return err
	}
	_, producer := k.clients()
	err := k.publish(producer, topic, payload, headers)
	if err != nil {
		if reconnectErr := k.Reconnect(); reconnectErr == nil {
			_, producer = k.clients()
			err = k.publish(producer, topic, payload, headers)
		}
	}
	if err != nil {
		_ = k.DisasterRecovery(payload)
	}
	return err
}

func (k *Kafka) publish(producer KafkaProducer, topic string, payload []byte, headers []kafka.Header) error {
	if producer == nil {
		return fmt.Errorf("Kafka producer is not initialized")
	}
	delivery := make(chan kafka.Event, 1)
	if err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Headers:        headers,
	}, delivery); err != nil {
		return err
	}

	select {
	case event := <-delivery:
		switch result := event.(type) {
		case *kafka.Message:
			return result.TopicPartition.Error
		case kafka.Error:
			return result
		default:
			return fmt.Errorf("unexpected Kafka delivery event %T", event)
		}
	case <-time.After(kafkaDeliveryTimeout):
		return fmt.Errorf("Kafka delivery timed out for topic %q", topic)
	}
}

func (k *Kafka) PublishWithDelay(topic string, payload []byte, backoff int) {
	go func() {
		if backoff > 0 {
			time.Sleep(time.Duration(backoff) * time.Second)
		}
		_ = k.Publish(topic, payload)
	}()
}

func (k *Kafka) ReadMessages(handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	return k.consume(func(message *kafka.Message) error {
		started := time.Now()
		payload := message.Value
		var pipelineEvent models.PipelineEvent
		if err := json.Unmarshal(payload, &pipelineEvent); err != nil {
			k.preserve(payload)
			return nil
		}

		action, pipelineEvent, backoff := handleMessage(pipelineEvent, args...)
		switch action {
		case models.PipelineForward:
			if len(pipelineEvent.Stages) < 2 {
				break
			}
			pipelineEvent.Stages = pipelineEvent.Stages[1:]
			forwardPayload, err := json.Marshal(pipelineEvent)
			if err != nil {
				k.preserve(payload)
				break
			}
			if err := k.Publish(k.options.RouterTopic, forwardPayload); err != nil {
				k.preserve(payload)
			}
		case models.PipelineError:
			deadletterPayload, err := json.Marshal(pipelineEvent)
			if err != nil {
				deadletterPayload = payload
			}
			if err := k.Publish(k.options.DeadletterTopic, deadletterPayload); err != nil {
				k.preserve(payload)
			}
		case models.PipelineRetry:
			if err := k.retryOrDeadletter(message.Headers, payload, backoff); err != nil {
				return err
			}
		}

		handlePrometheus(models.PipelineMetrics{ProcessingTime: time.Since(started).Seconds()})
		return nil
	})
}

func (k *Kafka) RouteMessages(_ models.MessageHandler, handlePrometheus models.PrometheusHandler, _ ...any) error {
	return k.consume(func(message *kafka.Message) error {
		started := time.Now()
		var pipelineEvent models.PipelineEvent
		if err := json.Unmarshal(message.Value, &pipelineEvent); err != nil {
			k.preserve(message.Value)
			return nil
		}
		if len(pipelineEvent.Stages) > 0 {
			topic := "kcloud-" + pipelineEvent.Stages[0] + "-queue"
			if err := k.Publish(topic, message.Value); err != nil {
				k.preserve(message.Value)
			}
		}
		handlePrometheus(models.PipelineMetrics{ProcessingTime: time.Since(started).Seconds()})
		return nil
	})
}

func (k *Kafka) ReadRawMessages(handleMessage RawMessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	return k.consume(func(message *kafka.Message) error {
		started := time.Now()
		action, output, backoff := handleMessage(message.Value, args...)
		switch action {
		case models.PipelineForward:
			if k.options.RouterTopic == "" {
				k.preserve(message.Value)
				break
			}
			if output == nil {
				output = message.Value
			}
			if err := k.Publish(k.options.RouterTopic, output); err != nil {
				k.preserve(message.Value)
			}
		case models.PipelineError:
			k.preserve(message.Value)
		case models.PipelineRetry:
			if err := k.retryOrDeadletter(message.Headers, message.Value, backoff); err != nil {
				return err
			}
		}
		handlePrometheus(models.PipelineMetrics{ProcessingTime: time.Since(started).Seconds()})
		return nil
	})
}

func (k *Kafka) consume(process func(*kafka.Message) error) error {
	if err := k.ensureConnected(); err != nil {
		return err
	}
	consumer, _ := k.clients()
	if err := consumer.SubscribeTopics([]string{k.options.ConsumerTopic}, nil); err != nil {
		return err
	}

	for {
		message, err := consumer.ReadMessage(-1)
		if err != nil {
			if reconnectErr := k.Reconnect(); reconnectErr != nil {
				return err
			}
			consumer, _ = k.clients()
			if subscribeErr := consumer.SubscribeTopics([]string{k.options.ConsumerTopic}, nil); subscribeErr != nil {
				return subscribeErr
			}
			continue
		}
		if err := process(message); err != nil {
			return err
		}
		if _, err := consumer.CommitMessage(message); err != nil {
			return err
		}
	}
}

func (k *Kafka) maxRetries() int {
	if k.options.MaxRetries > 0 {
		return k.options.MaxRetries
	}
	return defaultMaxRetries
}

func kafkaRetryCount(headers []kafka.Header) int {
	for _, header := range headers {
		if header.Key == retryCountHeader {
			count, err := strconv.Atoi(string(header.Value))
			if err == nil {
				return count
			}
		}
	}
	return 0
}

func kafkaRetryHeaders(headers []kafka.Header, count int) []kafka.Header {
	result := make([]kafka.Header, 0, len(headers)+1)
	for _, header := range headers {
		if header.Key != retryCountHeader {
			result = append(result, header)
		}
	}
	return append(result, kafka.Header{Key: retryCountHeader, Value: []byte(strconv.Itoa(count))})
}

func (k *Kafka) retryOrDeadletter(headers []kafka.Header, payload []byte, backoff int) error {
	attempts := kafkaRetryCount(headers)
	if attempts >= k.maxRetries() {
		return k.AddToDeadletter(payload)
	}
	if backoff <= 0 {
		backoff = 5
	}
	time.Sleep(time.Duration(backoff) * time.Second)
	return k.publishWithReconnect(k.options.ConsumerTopic, payload, kafkaRetryHeaders(headers, attempts+1))
}

func (k *Kafka) AddToDeadletter(payload []byte) error {
	return k.Publish(k.options.DeadletterTopic, payload)
}

func (k *Kafka) preserve(payload []byte) {
	if err := k.AddToDeadletter(payload); err != nil {
		_ = k.DisasterRecovery(payload)
	}
}

func (k *Kafka) SetDisasterRecoveryHandler(handler DisasterRecoveryHandler) {
	k.disasterRecoveryHandler = handler
}

func (k *Kafka) DisasterRecovery(payload []byte) error {
	if k.disasterRecoveryHandler != nil {
		return k.disasterRecoveryHandler(payload)
	}
	return nil
}

func (k *Kafka) LoadMessages(filename string) error {
	file, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	var events []models.PipelineEvent
	if err := json.Unmarshal(file, &events); err != nil {
		return err
	}
	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if err := k.Publish(k.options.ConsumerTopic, payload); err != nil {
			return err
		}
	}
	return nil
}
