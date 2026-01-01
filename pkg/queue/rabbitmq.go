package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/uug-ai/models/pkg/models"
)

// RabbitOptions holds the configuration for RabbitMQ
type RabbitOptions struct {
	ConsumerQueue   string `validate:"required"` // Queue from which to consume messages, one consumer per queue
	DeadletterQueue string `validate:"required"` // When something goes wrong, messages are sent here
	RouterQueue     string `validate:"required"` // Router queue for routing messages, the message will be send to this queue if Forward action reached.
	Uri             string
	Host            string `validate:"required"`
	Username        string `validate:"required"`
	Password        string `validate:"required"`
	PrefetchCount   int
	Exchange        string
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

// SetConsumerQueue sets the consumer queue name
func (b *RabbitOptionsBuilder) SetConsumerQueue(queueName string) *RabbitOptionsBuilder {
	b.options.ConsumerQueue = queueName
	return b
}

// SetDeadletterQueue sets the deadletter queue name
func (b *RabbitOptionsBuilder) SetDeadletterQueue(queueName string) *RabbitOptionsBuilder {
	b.options.DeadletterQueue = queueName
	return b
}

// SetRouterQueue sets the router queue name
func (b *RabbitOptionsBuilder) SetRouterQueue(queueName string) *RabbitOptionsBuilder {
	b.options.RouterQueue = queueName
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
	connectionString string           // e.g., amqp://user:pass@host:port/
	Connection       *amqp.Connection // The underlying RabbitMQ connection
	Consumer         *amqp.Channel    // Channel for consuming messages
	Producer         *amqp.Channel    // Channel for producing messages
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
	// Build connection string
	return &RabbitMQ{
		options:          options,
		connectionString: protocol + options.Username + ":" + options.Password + "@" + host + "/",
	}, nil
}

// Connect establishes the RabbitMQ connection and channels
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

// Reconnect attempts to re-establish the RabbitMQ connection
// Basic implementation just calls Connect again
func (r *RabbitMQ) Reconnect() error {
	err := r.Connect()
	if err != nil {
		return err
	}
	return nil
}

// declareQueue declares a quorum queue with the configured queue name
func (r *RabbitMQ) declareQueue() error {
	// Declare quorum queue (idempotent - succeeds if queue exists with same parameters)
	_, err := r.Consumer.QueueDeclare(
		r.options.ConsumerQueue, // name
		true,                    // durable
		false,                   // delete when unused
		false,                   // exclusive
		false,                   // no-wait
		amqp.Table{
			"x-queue-type": "quorum",
		}, // arguments
	)
	if err != nil {
		return err
	}
	return nil
}

// PipelineMetrics represents processing metrics for a pipeline event
type PipelineMetrics struct {
	ProcessingTime float64 `json:"processingTime,omitempty"`
}

// HandlerResult represents the result of message handling
type PipelineAction string

const (
	// Forward indicates the message should be forwarded to the next stage
	Forward PipelineAction = "forward"
	// Cancel indicates the message processing should be cancelled
	Cancel PipelineAction = "cancel"
	// Retry indicates the message should be retried
	Retry PipelineAction = "retry"
	// Error indicates an error occurred during message processing
	Error PipelineAction = "error"
)

// MessageHandler is a function type for handling pipeline events
type MessageHandler func(models.PipelineEvent, ...any) (PipelineAction, models.PipelineEvent, int)
type PrometheusHandler func(PipelineMetrics)

// ReadMessages reads messages from the RabbitMQ queue, processes them using the provided handler,
// and reports metrics using the provided Prometheus handler. It will then take action based on the handler's result.
// Forwards, cancels, retries or sends to deadletter as needed.
//
// Parameters:
// - handleMessage: function to process each message
// - handlePrometheus: function to handle metrics reporting
// - args: additional arguments to pass to the message handler
func (r *RabbitMQ) ReadMessages(handleMessage MessageHandler, handlePrometheus PrometheusHandler, args ...any) error {

	// Subscribe to a queue
	if r.Consumer == nil {
		return fmt.Errorf("RabbitMQ channel is not initialized")
	}

	msgs, err := r.Consumer.Consume(
		r.options.ConsumerQueue, // queue
		"",                      // consumer
		false,                   // auto-ack
		false,                   // exclusive
		false,                   // no-local
		false,                   // no-wait
		nil,
	)
	if err != nil {
		return err
	}

	for d := range msgs {

		// Chrono start, we will measure processing time (start to end)
		startTime := time.Now()

		// Extract message
		payload := d.Body

		// Unmarshal message body into PipelineEvent
		var pipelineEvent models.PipelineEvent
		err = json.Unmarshal(payload, &pipelineEvent)
		if err != nil {
			r.AddToDeadletter(payload)
			return err
		}

		// We will override payload with the new payload
		// some consumers might provide additional information, that can be leveraged
		// later (stateful messaging).
		pipelineAction, pipelineEvent, _ := handleMessage(pipelineEvent, args...)

		// Depending on action, we either forward, cancel or retry
		switch pipelineAction {
		case Forward:
			// Bring event to the next stage
			pipelineEvent.Stages = pipelineEvent.Stages[1:]
			if len(pipelineEvent.Stages) == 0 {
				// No more stages, nothing to do
				break
			}
			// Marshal updated event
			pipelineEventPayload, err := json.Marshal(pipelineEvent)
			if err != nil {
				r.AddToDeadletter(payload)
				return err
			}
			topic := r.options.RouterQueue
			err = r.Publish(topic, pipelineEventPayload)
			if err != nil {
				r.AddToDeadletter(payload)
				return err
			}
		case Error:
			// Send to deadletter queue
			pipelineEventPayload, err := json.Marshal(pipelineEvent)
			if err != nil {
				r.AddToDeadletter(payload)
				return err
			}
			topic := r.options.DeadletterQueue
			err = r.Publish(topic, pipelineEventPayload)
			if err != nil {
				r.AddToDeadletter(payload)
				return err
			}
		case Cancel:
			// Nothing to do, just acknowledge, message will be removed from the queue.
		case Retry:
			// Re-publish the same message to the same queue for retry
			backoff := 5
			r.PublishWithDelay(r.options.ConsumerQueue, payload, backoff)
		}

		// Always acknowledge messages regardless of sync mode
		err = d.Ack(false)
		if err != nil {
			r.AddToDeadletter(payload)
			return err
		}

		// Chrono end
		endTime := time.Now()
		processingTime := endTime.Sub(startTime)
		e := PipelineMetrics{
			ProcessingTime: processingTime.Seconds(),
		}
		handlePrometheus(e)
	}
	r.Close()
	return nil
}

func (r *RabbitMQ) RouteMessages(handleMessage MessageHandler, handlePrometheus PrometheusHandler, args ...any) error {

	// Subscribe to a queue
	if r.Consumer == nil {
		return fmt.Errorf("RabbitMQ channel is not initialized")
	}

	msgs, err := r.Consumer.Consume(
		r.options.ConsumerQueue, // queue
		"",                      // consumer
		false,                   // auto-ack
		false,                   // exclusive
		false,                   // no-local
		false,                   // no-wait
		nil,
	)
	if err != nil {
		return err
	}

	for d := range msgs {

		// Chrono start, we will measure processing time (start to end)
		startTime := time.Now()

		// Extract message
		payload := d.Body

		// Unmarshal message body into PipelineEvent
		var pipelineEvent models.PipelineEvent
		err = json.Unmarshal(payload, &pipelineEvent)
		if err != nil {
			r.AddToDeadletter(payload)
			return err
		}

		if len(pipelineEvent.Stages) > 0 {
			nextQueue := pipelineEvent.Stages[0]
			nextQueue = r.formatQueueName(nextQueue) // Apply legacy naming convention, we will remove this later
			err = r.Publish(nextQueue, payload)
			if err != nil {
				r.AddToDeadletter(payload)
				return err
			}
		}

		// Always acknowledge messages regardless of sync mode
		err = d.Ack(false)
		if err != nil {
			r.AddToDeadletter(payload)
			return err
		}

		// Chrono end
		endTime := time.Now()
		processingTime := endTime.Sub(startTime)
		e := PipelineMetrics{
			ProcessingTime: processingTime.Seconds(),
		}
		handlePrometheus(e)
	}
	r.Close()
	return nil
}

func (r *RabbitMQ) Close() {
	if r.Consumer != nil {
		r.Consumer.Close()
	}
	if r.Producer != nil {
		r.Producer.Close()
	}
	if r.Connection != nil {
		r.Connection.Close()
	}
}

// formatQueueName applies legacy naming convention to queue names
// TODO: Remove this once legacy naming convention is deprecated
func (r *RabbitMQ) formatQueueName(queueName string) string {
	return "kcloud-" + queueName + "-queue"
}

// Publish sends a message immediately to the specified RabbitMQ queue
func (r *RabbitMQ) Publish(queueName string, payload []byte) error {
	if r.Producer == nil {
		r.DisasterRecovery(payload)
		return fmt.Errorf("RabbitMQ producer channel is not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := r.Producer.PublishWithContext(ctx,
		"",        // exchange
		queueName, // routing key (queue name)
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        payload,
		},
	)
	if err != nil {
		r.DisasterRecovery(payload)
	}
	return err
}

// PublishWithDelay sends a message to the specified RabbitMQ queue after a delay
// This is non-blocking and runs in a goroutine. Errors are not returned to the caller.
// For production use, consider adding logging or an error channel.
func (r *RabbitMQ) PublishWithDelay(queueName string, payload []byte, backoff int) {
	go func() {
		time.Sleep(time.Duration(backoff) * time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Note: errors from delayed publishes are not returned to caller
		// Consider adding logging or error channel if needed
		err := r.Producer.PublishWithContext(ctx,
			"",        // exchange
			queueName, // routing key (queue name)
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        payload,
			},
		)
		if err != nil {
			r.DisasterRecovery(payload)
		}
	}()
}

// AddToDeadletter adds a message to the deadletter queue
func (r RabbitMQ) AddToDeadletter(payload []byte) error {
	topic := r.options.DeadletterQueue
	return r.Publish(topic, payload)
}

func (r RabbitMQ) DisasterRecovery(payload []byte) error {
	// We should persist the message on disk or take other actions
	// @TODO: implement persistent storage or logging
	return nil
}

func (r RabbitMQ) LoadMessages(filename string) error {
	// This method is only meaningful for MockQueue
	return nil
}
