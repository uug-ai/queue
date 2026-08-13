//go:build !cgo

package queue

import (
	"fmt"

	"github.com/uug-ai/models/pkg/models"
)

type Kafka struct{}

func kafkaUnavailableError() error {
	return fmt.Errorf("Kafka support requires CGO_ENABLED=1 for github.com/confluentinc/confluent-kafka-go/v2")
}

func NewKafka(options *KafkaOptions) (*Kafka, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return nil, kafkaUnavailableError()
}

func (k *Kafka) Connect() error                       { return kafkaUnavailableError() }
func (k *Kafka) Reconnect() error                     { return kafkaUnavailableError() }
func (k *Kafka) Close()                               {}
func (k *Kafka) Publish(string, []byte) error         { return kafkaUnavailableError() }
func (k *Kafka) PublishWithDelay(string, []byte, int) {}
func (k *Kafka) ReadMessages(models.MessageHandler, models.PrometheusHandler, ...any) error {
	return kafkaUnavailableError()
}
func (k *Kafka) RouteMessages(models.MessageHandler, models.PrometheusHandler, ...any) error {
	return kafkaUnavailableError()
}
func (k *Kafka) ReadRawMessages(RawMessageHandler, models.PrometheusHandler, ...any) error {
	return kafkaUnavailableError()
}
func (k *Kafka) AddToDeadletter([]byte) error                       { return kafkaUnavailableError() }
func (k *Kafka) DisasterRecovery([]byte) error                      { return kafkaUnavailableError() }
func (k *Kafka) SetDisasterRecoveryHandler(DisasterRecoveryHandler) {}
func (k *Kafka) LoadMessages(string) error                          { return kafkaUnavailableError() }
