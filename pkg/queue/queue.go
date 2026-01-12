package queue

import (
	"fmt"
)

type QueueInterface interface {
	Connect() error
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
	// If no client provided, create default production client
	var err error
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
