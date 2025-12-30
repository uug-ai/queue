package queue

import (
	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitOptions holds the configuration for RabbitMQ
type RabbitOptions struct {
	Uri      string `validate:"required"`
	Host     string `validate:"required"`
	Username string `validate:"required"`
	Password string `validate:"required"`
	Exchange string
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

// Build builds the Rabbit options
func (b *RabbitOptionsBuilder) Build() *RabbitOptions {
	return b.options
}

// RabbitMQ wraps rabbitmq.Client to implement the Queue interface
type RabbitMQ struct {
	options          *RabbitOptions
	connectionString string
	Connection       *amqp.Connection
}

// NewRabbitMQ creates a new RabbitMQ with the provided RabbitMQ settings
func NewRabbitMQ(options *RabbitOptions) (*RabbitMQ, error) {
	// Validate Database configuration
	validate := validator.New()
	err := validate.Struct(options)
	if err != nil {
		return nil, err
	}

	// @ TODO we will need to rework this, as it's not optimal and dirty coding style.
	// Check if host containers aqmps:// or amqp://
	connectionString := ""
	hasProtocol := false
	protocol := ""
	if len(options.Host) > 7 {
		if options.Host[:8] == "amqps://" {
			hasProtocol = true
			protocol = "amqps://"
			options.Host = options.Host[8:]
		} else if options.Host[:7] == "amqp://" {
			hasProtocol = true
			protocol = "amqp://"
			options.Host = options.Host[7:]
		}
	}

	if options.Host != "" && options.Username != "" && options.Password != "" {
		if hasProtocol {
			connectionString = protocol + options.Username + ":" + options.Password + "@" + options.Host + "/"
		} else {
			connectionString = "amqp://" + options.Username + ":" + options.Password + "@" + options.Host + "/"
		}
	}

	return &RabbitMQ{
		options:          options,
		connectionString: connectionString,
	}, nil
}

func (r *RabbitMQ) Connect() error {
	return nil
}
