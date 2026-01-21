package queue

import (
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/uug-ai/models/pkg/models"
)

type QueueInterface interface {
	Connect() error
	Reconnect() error
	Close()
	Publish(queueName string, payload []byte) error
	PublishWithDelay(queueName string, payload []byte, backoff int)
	ReadMessages(handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error
	RouteMessages(handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error
	AddToDeadletter(payload []byte) error
	DisasterRecovery(payload []byte) error
	SetDisasterRecoveryHandler(handler DisasterRecoveryHandler)
	LoadMessages(filename string) error
}

type QueueOptions interface {
	Validate() error
}

// Queue represents a queue client instance
type Queue struct {
	Options QueueOptions
	Client  QueueInterface
}

func New(opts QueueOptions, client ...QueueInterface) (*Queue, error) {
	// Validate Database configuration
	validate := validator.New()
	err := validate.Struct(opts)
	if err != nil {
		return nil, err
	}

	// If no client provided, create default production client
	var q QueueInterface
	if len(client) == 0 {
		// Type assert to RabbitOptions for creating RabbitMQ client
		if rabbitOpts, ok := opts.(*RabbitOptions); ok {
			q, err = NewRabbitMQ(rabbitOpts)
		} else {
			return nil, fmt.Errorf("unsupported queue options type")
		}
	} else {
		q, err = client[0], nil
	}

	return &Queue{
		Options: opts,
		Client:  q,
	}, err
}
