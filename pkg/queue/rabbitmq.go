package queue

import (
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitOptions holds the configuration for RabbitMQ
type RabbitOptions struct {
	QueueName     string `validate:"required"`
	Uri           string `validate:"required"`
	Host          string `validate:"required"`
	Username      string `validate:"required"`
	Password      string `validate:"required"`
	PrefetchCount int
	Exchange      string
}

// RabbitOptionsBuilder provides a fluent interface for building Rabbit options
type RabbitOptionsBuilder struct {
	options *RabbitOptions
}

// RabbitOptions creates a new Rabbit options builder
func NewRabbitOptions() *RabbitOptionsBuilder {
	return &RabbitOptionsBuilder{
		options: &RabbitOptions{},
	}
}

// SetQueueName sets the queue name
func (b *RabbitOptionsBuilder) SetQueueName(queueName string) *RabbitOptionsBuilder {
	b.options.QueueName = queueName
	return b
}

// SetUri sets the URI
func (b *RabbitOptionsBuilder) SetUri(uri string) *RabbitOptionsBuilder {
	b.options.Uri = uri
	return b
}

// SetHost sets the host
func (b *RabbitOptionsBuilder) SetHost(host string) *RabbitOptionsBuilder {
	b.options.Host = host
	return b
}

// SetUsername sets the username
func (b *RabbitOptionsBuilder) SetUsername(username string) *RabbitOptionsBuilder {
	b.options.Username = username
	return b
}

// SetPassword sets the password
func (b *RabbitOptionsBuilder) SetPassword(password string) *RabbitOptionsBuilder {
	b.options.Password = password
	return b
}

// SetExchange sets the exchange
func (b *RabbitOptionsBuilder) SetExchange(exchange string) *RabbitOptionsBuilder {
	b.options.Exchange = exchange
	return b
}

// SetPrefetchCount sets the prefetch count
func (b *RabbitOptionsBuilder) SetPrefetchCount(count int) *RabbitOptionsBuilder {
	b.options.PrefetchCount = count
	return b
}

// Build builds the Rabbit options
func (b *RabbitOptionsBuilder) Build() *RabbitOptions {
	return b.options
}

// RabbitMQ wraps rabbitmq.Client to implement the Queue interface
type RabbitMQ struct {
	options          *RabbitOptions
	connectionString string
	Connection       *amqp.Connection
	Consumer         *amqp.Channel
	Producer         *amqp.Channel
}

// NewRabbitMQ creates a new RabbitMQ with the provided RabbitMQ settings
func NewRabbitMQ(options *RabbitOptions) (*RabbitMQ, error) {
	// Validate RabbitMQ configuration
	validate := validator.New()
	err := validate.Struct(options)
	if err != nil {
		return nil, err
	}

	// Extract protocol from host if present, otherwise default to amqp://
	protocol := "amqp://"
	host := options.Host
	if strings.HasPrefix(host, "amqps://") {
		protocol = "amqps://"
		host = strings.TrimPrefix(host, "amqps://")
	} else if strings.HasPrefix(host, "amqp://") {
		host = strings.TrimPrefix(host, "amqp://")
	}

	connectionString := protocol + options.Username + ":" + options.Password + "@" + host + "/"

	return &RabbitMQ{
		options:          options,
		connectionString: connectionString,
	}, nil
}

func (r *RabbitMQ) Connect() error {

	prefetchCount := 5
	if r.options.PrefetchCount > 0 {
		prefetchCount = r.options.PrefetchCount
	}

	// Establish connection, with tweaked
	connection, err := amqp.DialConfig(r.connectionString, amqp.Config{
		Heartbeat:       time.Duration(10) * time.Second, // Set the default heartbeat interval
		TLSClientConfig: nil,                             // No TLS configuration
	})
	if err != nil {
		return err
	}
	r.Connection = connection

	// Create channel for producing, publishing messages.
	r.Producer, err = r.Connection.Channel()
	if err != nil {
		return err
	}

	// Create channel for consuming, receiving messages.
	r.Consumer, err = r.Connection.Channel()
	if err != nil {
		return err
	}
	// prefetch count - max unacked messages per consumer
	err = r.Consumer.Qos(prefetchCount, 0, false)
	if err != nil {
		return err
	}

	// Declare the queue
	err = r.declareQueue()
	if err != nil {
		return err
	}

	return nil
}

// declareQueue declares a quorum queue with the configured queue name
func (r *RabbitMQ) declareQueue() error {
	// Declare quorum queue (idempotent - succeeds if queue exists with same parameters)
	_, err := r.Consumer.QueueDeclare(
		r.options.QueueName, // name
		true,                // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		amqp.Table{
			"x-queue-type": "quorum",
		}, // arguments
	)
	if err != nil {
		return err
	}
	return nil
}
