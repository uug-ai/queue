package queue

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/uug-ai/models/pkg/models"
)

// DisasterRecoveryHandler is a function type for handling messages that failed to publish
type DisasterRecoveryHandler func([]byte) error

// ReturnHandler is invoked for every message the broker returns as unroutable
// (published mandatory with no queue bound to its routing key). With the default
// exchange the routing key is the target queue name, so a return means the
// producer is publishing to a queue that does not exist — typically a
// producer/consumer queue-name drift that is otherwise completely silent. The
// handler runs asynchronously for both legacy and confirmed publishes.
type ReturnHandler func(amqp.Return)

// RabbitOptions holds the configuration for RabbitMQ
type RabbitOptions struct {
	ConsumerQueue     string `validate:"required"` // Queue from which to consume messages, one consumer per queue
	RouterQueue       string // Router queue; only used — and only needs to be set — when a handler returns the Forward action. Stage workers that never forward may leave it unset.
	DeadletterQueue   string `validate:"required"` // When something goes wrong, messages are sent here
	AnalysisQueue     string // Queue for analysis messages
	Uri               string
	Host              string `validate:"required"`
	Username          string `validate:"required"`
	Password          string `validate:"required"`
	PrefetchCount     int
	Exchange          string
	ConfirmedDelivery bool

	// MaxRetries caps how many times a PipelineRetry re-queues a message before
	// it is parked on the deadletter queue; zero selects defaultMaxRetries. It is
	// the hard stop that keeps a permanently failing payload from looping the
	// consumer queue forever (see retryOrDeadletter).
	MaxRetries int

	// TLS configuration for secure connections (e.g., AWS Amazon MQ)
	TLS                   bool   // Enable TLS (auto-enabled when host starts with amqps://)
	TLSInsecureSkipVerify bool   // Skip TLS certificate verification (development only)
	TLSCACertFile         string // Path to custom CA certificate file (PEM format)
}

// Validate validates the RabbitOptions configuration
func (r *RabbitOptions) Validate() error {
	validate := validator.New()
	return validate.Struct(r)
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

// SetWorkflowsQueue sets the queue the workflows engine consumes from
// (WORKFLOWS_QUEUE, e.g. "hub-workflows-queue"). It is a workflow-oriented alias
// for SetConsumerQueue — the engine's consumer queue *is* the workflows queue,
// and stage workers publish their finished run back to it. Use this when wiring
// the hub-workflows engine; a stage worker uses SetWorkflowsStageQueue for the
// queue it consumes.
func (b *RabbitOptionsBuilder) SetWorkflowsQueue(queueName string) *RabbitOptionsBuilder {
	return b.SetConsumerQueue(queueName)
}

// SetWorkflowsStageQueue sets the queue a workflow stage worker consumes its
// dispatched runs from (its <STAGE>_QUEUE, e.g. "hub-workflows-loitering"). It is
// a workflow-oriented alias for SetConsumerQueue. The worker handles each run and
// publishes the result back to the workflows engine queue (see SetWorkflowsQueue).
func (b *RabbitOptionsBuilder) SetWorkflowsStageQueue(queueName string) *RabbitOptionsBuilder {
	return b.SetConsumerQueue(queueName)
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

// SetAnalysisQueue sets the analysis queue name
func (b *RabbitOptionsBuilder) SetAnalysisQueue(queueName string) *RabbitOptionsBuilder {
	b.options.AnalysisQueue = queueName
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

// SetMaxRetries caps how many times a PipelineRetry re-queues a message before
// it is dead-lettered. Zero (the default) selects defaultMaxRetries. Set it to
// bound how long a transient sink failure is retried before the message is
// parked for inspection instead of being requeued forever.
func (b *RabbitOptionsBuilder) SetMaxRetries(maxRetries int) *RabbitOptionsBuilder {
	b.options.MaxRetries = maxRetries
	return b
}

// SetConfirmedDelivery provisions the dedicated publisher-confirm channel used
// by PublishConfirmed and the confirmed consumer methods.
func (b *RabbitOptionsBuilder) SetConfirmedDelivery(enabled bool) *RabbitOptionsBuilder {
	b.options.ConfirmedDelivery = enabled
	return b
}

// SetTLS enables TLS for the connection
func (b *RabbitOptionsBuilder) SetTLS(enabled bool) *RabbitOptionsBuilder {
	b.options.TLS = enabled
	return b
}

// SetTLSInsecureSkipVerify sets whether to skip TLS certificate verification (development only)
func (b *RabbitOptionsBuilder) SetTLSInsecureSkipVerify(skip bool) *RabbitOptionsBuilder {
	b.options.TLSInsecureSkipVerify = skip
	return b
}

// SetTLSCACertFile sets the path to a custom CA certificate file (PEM format)
func (b *RabbitOptionsBuilder) SetTLSCACertFile(path string) *RabbitOptionsBuilder {
	b.options.TLSCACertFile = path
	return b
}

// Build builds the Rabbit options
func (b *RabbitOptionsBuilder) Build() *RabbitOptions {
	return b.options
}

// RabbitMQ wraps rabbitmq.Client to implement the Queue interface
type RabbitMQ struct {
	options                 *RabbitOptions
	connectionString        string           // e.g., amqp://user:pass@host:port/
	Connection              *amqp.Connection // The underlying RabbitMQ connection
	Consumer                *amqp.Channel    // Channel for consuming messages
	Producer                *amqp.Channel    // Channel for producing messages
	confirmedProducer       *amqp.Channel
	confirmedReturns        <-chan amqp.Return
	disasterRecoveryHandler DisasterRecoveryHandler // Optional handler for failed messages
	returnHandler           ReturnHandler           // Optional handler for unroutable (mandatory-returned) messages
	mu                      sync.Mutex
	publishMu               sync.Mutex
}

// NewRabbitMQ creates a new RabbitMQ with the provided RabbitMQ settings
func NewRabbitMQ(options *RabbitOptions) (*RabbitMQ, error) {

	// Validate RabbitMQ configuration
	if err := options.Validate(); err != nil {
		return nil, err
	}
	// Extract protocol from host if present, otherwise default to amqp://
	protocol := "amqp://"
	host := options.Host
	if strings.HasPrefix(host, "amqps://") {
		protocol = "amqps://"
		host = strings.TrimPrefix(host, "amqps://")
		// Auto-enable TLS when amqps:// protocol is detected
		options.TLS = true
	} else if strings.HasPrefix(host, "amqp://") {
		host = strings.TrimPrefix(host, "amqp://")
	}

	// If TLS is explicitly enabled, ensure we use amqps:// protocol
	if options.TLS {
		protocol = "amqps://"
	}

	// Build connection string
	return &RabbitMQ{
		options:          options,
		connectionString: protocol + options.Username + ":" + options.Password + "@" + host + "/",
	}, nil
}

// SetDisasterRecoveryHandler sets a custom disaster recovery handler for failed message publishes
func (r *RabbitMQ) SetDisasterRecoveryHandler(handler DisasterRecoveryHandler) {
	r.disasterRecoveryHandler = handler
}

// SetReturnHandler sets a handler invoked for every mandatory message the broker
// returns as unroutable (no queue bound to the routing key, i.e. the target queue
// does not exist). Use it to log or increment a metric so a producer/consumer
// queue-name drift is visible instead of silently dropping messages. When unset,
// returns are logged via the standard logger so the condition is never silent.
func (r *RabbitMQ) SetReturnHandler(handler ReturnHandler) {
	r.returnHandler = handler
}

// Connect establishes the RabbitMQ connection and channels
func (r *RabbitMQ) Connect() error {
	r.publishMu.Lock()
	defer r.publishMu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.connectLocked()
}

func (r *RabbitMQ) connectLocked() error {

	prefetchCount := 5
	if r.options.PrefetchCount > 0 {
		prefetchCount = r.options.PrefetchCount
	}

	tlsConfig, err := r.buildTLSConfig()
	if err != nil {
		return err
	}

	// Establish connection, with tweaked config
	connection, err := amqp.DialConfig(r.connectionString, amqp.Config{
		Heartbeat:       time.Duration(10) * time.Second, // Set the default heartbeat interval
		TLSClientConfig: tlsConfig,                       // TLS configuration (nil when TLS is disabled)
	})
	if err != nil {
		return err
	}

	// Create channel for producing, publishing messages.
	producer, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return err
	}
	returns := producer.NotifyReturn(make(chan amqp.Return, 16))
	go r.watchReturns(returns)

	var confirmedProducer *amqp.Channel
	var confirmedReturns <-chan amqp.Return
	if r.options.ConfirmedDelivery {
		confirmedProducer, err = connection.Channel()
		if err != nil {
			_ = producer.Close()
			_ = connection.Close()
			return err
		}
		confirmedReturns = confirmedProducer.NotifyReturn(make(chan amqp.Return, 1))
		if err := confirmedProducer.Confirm(false); err != nil {
			_ = confirmedProducer.Close()
			_ = producer.Close()
			_ = connection.Close()
			return err
		}
	}

	// Create channel for consuming, receiving messages.
	consumer, err := connection.Channel()
	if err != nil {
		if confirmedProducer != nil {
			_ = confirmedProducer.Close()
		}
		_ = producer.Close()
		_ = connection.Close()
		return err
	}
	// prefetch count - max unacked messages per consumer
	err = consumer.Qos(prefetchCount, 0, false)
	if err != nil {
		_ = consumer.Close()
		if confirmedProducer != nil {
			_ = confirmedProducer.Close()
		}
		_ = producer.Close()
		_ = connection.Close()
		return err
	}

	// Declare the queue
	err = r.declareQueue(consumer)
	if err != nil {
		_ = consumer.Close()
		if confirmedProducer != nil {
			_ = confirmedProducer.Close()
		}
		_ = producer.Close()
		_ = connection.Close()
		return err
	}

	oldConnection := r.Connection
	oldConsumer := r.Consumer
	oldProducer := r.Producer
	oldConfirmedProducer := r.confirmedProducer

	r.Connection = connection
	r.Consumer = consumer
	r.Producer = producer
	r.confirmedProducer = confirmedProducer
	r.confirmedReturns = confirmedReturns

	r.closeResources(oldConsumer, oldProducer, oldConfirmedProducer, oldConnection)

	return nil
}

// Reconnect attempts to re-establish the RabbitMQ connection
// Basic implementation just calls Connect again
func (r *RabbitMQ) Reconnect() error {
	return r.Connect()
}

// declareQueue declares quorum queues for the consumer and deadletter queues
func (r *RabbitMQ) declareQueue(consumer *amqp.Channel) error {
	// Declare consumer quorum queue (idempotent - succeeds if queue exists with same parameters)
	_, err := consumer.QueueDeclare(
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

	// Declare deadletter quorum queue
	_, err = consumer.QueueDeclare(
		r.options.DeadletterQueue, // name
		true,                      // durable
		false,                     // delete when unused
		false,                     // exclusive
		false,                     // no-wait
		amqp.Table{
			"x-queue-type": "quorum",
		}, // arguments
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQ) buildTLSConfig() (*tls.Config, error) {
	if !r.options.TLS {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: r.options.TLSInsecureSkipVerify,
	}

	if r.options.TLSCACertFile == "" {
		return tlsConfig, nil
	}

	caCert, err := os.ReadFile(r.options.TLSCACertFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate file: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate from %s", r.options.TLSCACertFile)
	}

	tlsConfig.RootCAs = caCertPool
	return tlsConfig, nil
}

func (r *RabbitMQ) ensureConnected() error {
	r.mu.Lock()
	if !r.needsReconnectLocked() {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	r.publishMu.Lock()
	defer r.publishMu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.needsReconnectLocked() {
		return nil
	}

	return r.connectLocked()
}

func (r *RabbitMQ) needsReconnect() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.needsReconnectLocked()
}

func (r *RabbitMQ) needsReconnectLocked() bool {
	if r.Connection == nil || r.Connection.IsClosed() {
		return true
	}

	if r.Consumer == nil || r.Consumer.IsClosed() {
		return true
	}

	if r.Producer == nil || r.Producer.IsClosed() {
		return true
	}
	if r.options.ConfirmedDelivery && (r.confirmedProducer == nil || r.confirmedProducer.IsClosed()) {
		return true
	}

	return false
}

func (r *RabbitMQ) currentConsumer() *amqp.Channel {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.Consumer
}

func (r *RabbitMQ) currentProducer() *amqp.Channel {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.Producer
}

func (r *RabbitMQ) currentConfirmedPublisher() (*amqp.Channel, <-chan amqp.Return) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.confirmedProducer, r.confirmedReturns
}

func (r *RabbitMQ) closeResources(consumer, producer, confirmedProducer *amqp.Channel, connection *amqp.Connection) {
	if consumer != nil {
		_ = consumer.Close()
	}
	if producer != nil {
		_ = producer.Close()
	}
	if confirmedProducer != nil {
		_ = confirmedProducer.Close()
	}
	if connection != nil {
		_ = connection.Close()
	}
}

func (r *RabbitMQ) reportReturn(ret amqp.Return) {
	if r.returnHandler != nil {
		r.returnHandler(ret)
		return
	}
	log.Printf("queue: message returned as unroutable (no queue bound to routing key %q on exchange %q): replyCode=%d replyText=%q — likely a producer/consumer queue-name drift",
		ret.RoutingKey, ret.Exchange, ret.ReplyCode, ret.ReplyText)
}

func (r *RabbitMQ) watchReturns(returns <-chan amqp.Return) {
	for ret := range returns {
		r.reportReturn(ret)
	}
}

func isClosedError(err error) bool {
	if err == nil {
		return false
	}

	message := err.Error()
	return strings.Contains(message, "channel/connection is not open") || strings.Contains(message, "connection is not open")
}

func (r *RabbitMQ) settleTransferredDelivery(delivery amqp.Delivery, transfer func() error) error {
	if err := transfer(); err != nil {
		nackErr := delivery.Nack(false, true)
		r.Close()
		if nackErr != nil {
			return fmt.Errorf("transfer failed: %w; requeue failed: %v", err, nackErr)
		}
		return err
	}

	if err := delivery.Ack(false); err != nil {
		r.Close()
		return err
	}

	return nil
}

func (r *RabbitMQ) requireConfirmedDelivery() error {
	if !r.options.ConfirmedDelivery {
		return fmt.Errorf("confirmed RabbitMQ delivery is not enabled")
	}
	return nil
}

func (r *RabbitMQ) deadletterAndSettle(delivery amqp.Delivery, payload []byte, confirmed bool) error {
	if confirmed {
		return r.settleTransferredDelivery(delivery, func() error {
			return r.deadletterOrRecover(payload)
		})
	}

	if err := r.AddToDeadletter(payload); err != nil {
		_ = r.DisasterRecovery(payload)
	}
	_ = delivery.Ack(false)
	return nil
}

// ReadMessages reads messages from the RabbitMQ queue, processes them using the provided handler,
// and reports metrics using the provided Prometheus handler. It will then take action based on the handler's result.
// Forwards, cancels, retries or sends to deadletter as needed.
//
// Parameters:
// - handleMessage: function to process each message
// - handlePrometheus: function to handle metrics reporting
// - args: additional arguments to pass to the message handler
func (r *RabbitMQ) ReadMessages(handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	return r.readMessages(false, handleMessage, handlePrometheus, args...)
}

// ReadMessagesConfirmed processes PipelineEvents with confirmed forwarding and
// only acknowledges each source delivery after its transfer is durable.
func (r *RabbitMQ) ReadMessagesConfirmed(handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	if err := r.requireConfirmedDelivery(); err != nil {
		return err
	}
	return r.readMessages(true, handleMessage, handlePrometheus, args...)
}

func (r *RabbitMQ) readMessages(confirmed bool, handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	publish := r.publishLegacyWithRecovery
	if confirmed {
		publish = r.publishConfirmedWithReconnect
	}

	for {
		if err := r.ensureConnected(); err != nil {
			return err
		}

		consumer := r.currentConsumer()
		if consumer == nil {
			return fmt.Errorf("RabbitMQ channel is not initialized")
		}

		msgs, err := consumer.Consume(
			r.options.ConsumerQueue, // queue
			"",                      // consumer
			false,                   // auto-ack
			false,                   // exclusive
			false,                   // no-local
			false,                   // no-wait
			nil,
		)
		if err != nil {
			if (r.needsReconnect() || isClosedError(err)) && r.Reconnect() == nil {
				continue
			}
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
				if err := r.deadletterAndSettle(d, payload, confirmed); err != nil {
					return err
				}
				continue
			}

			// We will override payload with the new payload
			// some consumers might provide additional information, that can be leveraged
			// later (stateful messaging).
			pipelineAction, pipelineEvent, _ := handleMessage(pipelineEvent, args...)

			// Depending on action, we either forward, cancel or retry
			switch pipelineAction {
			case models.PipelineForward:
				// Bring event to the next stage
				if len(pipelineEvent.Stages) == 0 {
					// No stages to advance, nothing to do
					break
				}
				pipelineEvent.Stages = pipelineEvent.Stages[1:]
				if len(pipelineEvent.Stages) == 0 {
					// No more stages, nothing to do
					break
				}
				// Marshal updated event
				pipelineEventPayload, err := json.Marshal(pipelineEvent)
				if err != nil {
					if err := r.deadletterAndSettle(d, payload, confirmed); err != nil {
						return err
					}
					continue
				}
				topic := r.options.RouterQueue
				err = publish(topic, pipelineEventPayload, nil)
				if err != nil {
					if err := r.deadletterAndSettle(d, payload, confirmed); err != nil {
						return err
					}
					continue
				}
			case models.PipelineError:
				// Send to deadletter queue
				pipelineEventPayload, err := json.Marshal(pipelineEvent)
				if err != nil {
					if err := r.deadletterAndSettle(d, payload, confirmed); err != nil {
						return err
					}
					continue
				}
				if confirmed {
					if err := r.deadletterAndSettle(d, pipelineEventPayload, true); err != nil {
						return err
					}
					continue
				}
				if err := publish(r.options.DeadletterQueue, pipelineEventPayload, nil); err != nil {
					if err := r.deadletterAndSettle(d, payload, false); err != nil {
						return err
					}
					continue
				}
			case models.PipelineCancel:
				// Nothing to do, just acknowledge, message will be removed from the queue.
			case models.PipelineRetry:
				// Re-queue for another attempt, but capped: retryOrDeadletter tracks
				// the attempt count on the message and dead-letters it once the cap is
				// hit, so a permanently failing payload can never loop the consumer
				// queue forever.
				if confirmed {
					if err := r.settleTransferredDelivery(d, func() error {
						return r.retryOrDeadletter(d.Headers, payload, 5*time.Second, publish)
					}); err != nil {
						return err
					}
					continue
				}
				if err := r.retryOrDeadletter(d.Headers, payload, 5*time.Second, publish); err != nil {
					return err
				}
			}

			// Always acknowledge messages regardless of sync mode
			err = d.Ack(false)
			if err != nil {
				if confirmed {
					r.Close()
					return err
				}
				_ = r.DisasterRecovery(payload)
				continue
			}

			// Chrono end
			endTime := time.Now()
			processingTime := endTime.Sub(startTime)
			e := models.PipelineMetrics{
				ProcessingTime: processingTime.Seconds(),
			}
			handlePrometheus(e)
		}

		if r.needsReconnect() {
			if err := r.Reconnect(); err != nil {
				return err
			}
			continue
		}

		r.Close()
		return nil
	}
}

// ReadOneRaw pulls a single message from the consumer queue, hands its raw body
// to handler, and acts on the returned action exactly once before returning —
// the bounded, one-shot counterpart to ReadRawMessages. It is built for verify
// or drain tooling that wants to inspect a single message without joining the
// long-lived consume loop. The returned bool is false (no error) when the queue
// is empty. Action mapping: Cancel acks (removes), Retry/Forward requeue (the
// message is left on the queue, untouched), Error dead-letters. A nil-ack/nack
// failure is reported. The handler's replacement payload is ignored; this never
// publishes onward.
func (r *RabbitMQ) ReadOneRaw(handleMessage RawMessageHandler, args ...any) (bool, error) {
	return r.readOneRaw(false, handleMessage, args...)
}

// ReadOneRawConfirmed is ReadOneRaw with confirmed dead-letter settlement.
func (r *RabbitMQ) ReadOneRawConfirmed(handleMessage RawMessageHandler, args ...any) (bool, error) {
	if err := r.requireConfirmedDelivery(); err != nil {
		return false, err
	}
	return r.readOneRaw(true, handleMessage, args...)
}

func (r *RabbitMQ) readOneRaw(confirmed bool, handleMessage RawMessageHandler, args ...any) (bool, error) {
	if err := r.ensureConnected(); err != nil {
		return false, err
	}
	consumer := r.currentConsumer()
	if consumer == nil {
		return false, fmt.Errorf("RabbitMQ channel is not initialized")
	}

	d, ok, err := consumer.Get(r.options.ConsumerQueue, false)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	action, _, _ := handleMessage(d.Body, args...)
	switch action {
	case models.PipelineCancel:
		return true, d.Ack(false)
	case models.PipelineError:
		if confirmed {
			if err := r.deadletterAndSettle(d, d.Body, true); err != nil {
				return true, err
			}
			return true, nil
		}
		if err := r.Publish(r.options.DeadletterQueue, d.Body); err != nil {
			if dlErr := r.AddToDeadletter(d.Body); dlErr != nil {
				_ = r.DisasterRecovery(d.Body)
			}
		}
		return true, d.Ack(false)
	default:
		// Retry/Forward/unknown: requeue, leaving the queue intact.
		return true, d.Nack(false, true)
	}
}

// RawMessageHandler processes a raw queue message body and returns the action
// the queue layer should take, an optional replacement payload to forward, and
// a retry backoff (seconds). It is the message-shape-agnostic counterpart to
// models.MessageHandler: consumers whose queue does not carry a PipelineEvent
// (e.g. the workflow subsystem, which exchanges models.WorkflowRun) unmarshal
// the body themselves rather than having the library decode it as a
// PipelineEvent. The returned payload is only used for the Forward action.
type RawMessageHandler func(payload []byte, args ...any) (models.PipelineAction, []byte, int)

// ReadRawMessages is ReadMessages without the PipelineEvent codec: it hands the
// raw message body to the handler and acts on the returned action, so a service
// can consume a queue carrying any JSON shape. It deliberately does not touch a
// PipelineEvent's Stages on Forward — Forward simply re-publishes the handler's
// returned payload to the router queue — because raw consumers manage their own
// fan-out (the workflow engine publishes to stage queues itself and otherwise
// cancels). The connection, ack, deadletter and reconnect handling mirror
// ReadMessages so behaviour is identical apart from the message shape.
func (r *RabbitMQ) ReadRawMessages(handleMessage RawMessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	return r.readRawMessages(false, handleMessage, handlePrometheus, args...)
}

// ReadRawMessagesConfirmed processes raw deliveries with confirmed forwarding
// and only acknowledges each source delivery after its transfer is durable.
func (r *RabbitMQ) ReadRawMessagesConfirmed(handleMessage RawMessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	if err := r.requireConfirmedDelivery(); err != nil {
		return err
	}
	return r.readRawMessages(true, handleMessage, handlePrometheus, args...)
}

func (r *RabbitMQ) readRawMessages(confirmed bool, handleMessage RawMessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	publish := r.publishLegacyWithRecovery
	if confirmed {
		publish = r.publishConfirmedWithReconnect
	}

	for {
		if err := r.ensureConnected(); err != nil {
			return err
		}

		consumer := r.currentConsumer()
		if consumer == nil {
			return fmt.Errorf("RabbitMQ channel is not initialized")
		}

		msgs, err := consumer.Consume(
			r.options.ConsumerQueue, // queue
			"",                      // consumer
			false,                   // auto-ack
			false,                   // exclusive
			false,                   // no-local
			false,                   // no-wait
			nil,
		)
		if err != nil {
			if (r.needsReconnect() || isClosedError(err)) && r.Reconnect() == nil {
				continue
			}
			return err
		}

		for d := range msgs {
			startTime := time.Now()
			payload := d.Body

			action, outPayload, backoff := handleMessage(payload, args...)

			switch action {
			case models.PipelineForward:
				// Forwarding needs a router queue. Workers that never forward
				// (e.g. workflow stage workers) leave it unset; treat a forward
				// with no router queue as a misconfiguration and dead-letter the
				// message rather than publishing to an empty queue name.
				if r.options.RouterQueue == "" {
					if !confirmed {
						if err := r.Publish(r.options.DeadletterQueue, payload); err != nil {
							if dlErr := r.AddToDeadletter(payload); dlErr != nil {
								_ = r.DisasterRecovery(payload)
							}
						}
						_ = d.Ack(false)
						continue
					}
					if err := r.deadletterAndSettle(d, payload, confirmed); err != nil {
						return err
					}
					continue
				}
				forwardPayload := outPayload
				if forwardPayload == nil {
					forwardPayload = payload
				}
				if err := publish(r.options.RouterQueue, forwardPayload, nil); err != nil {
					if err := r.deadletterAndSettle(d, payload, confirmed); err != nil {
						return err
					}
					continue
				}
			case models.PipelineError:
				if !confirmed {
					if err := r.Publish(r.options.DeadletterQueue, payload); err != nil {
						if dlErr := r.AddToDeadletter(payload); dlErr != nil {
							_ = r.DisasterRecovery(payload)
						}
						_ = d.Ack(false)
						continue
					}
					break
				}
				if err := r.deadletterAndSettle(d, payload, confirmed); err != nil {
					return err
				}
				continue
			case models.PipelineCancel:
				// Nothing to do, just acknowledge below.
			case models.PipelineRetry:
				// Re-queue for another attempt, but capped: retryOrDeadletter tracks
				// the attempt count on the message and dead-letters it once the cap is
				// hit, so a permanently failing payload can never loop the consumer
				// queue forever (the workflow engine's marker-ingest retry that backed
				// this queue up).
				if confirmed {
					if err := r.settleTransferredDelivery(d, func() error {
						return r.retryOrDeadletter(d.Headers, payload, time.Duration(backoff)*time.Second, publish)
					}); err != nil {
						return err
					}
					continue
				}
				if err := r.retryOrDeadletter(d.Headers, payload, time.Duration(backoff)*time.Second, publish); err != nil {
					return err
				}
			}

			if err := d.Ack(false); err != nil {
				if confirmed {
					r.Close()
					return err
				}
				_ = r.DisasterRecovery(payload)
				continue
			}

			processingTime := time.Since(startTime)
			handlePrometheus(models.PipelineMetrics{ProcessingTime: processingTime.Seconds()})
		}

		if r.needsReconnect() {
			if err := r.Reconnect(); err != nil {
				return err
			}
			continue
		}

		r.Close()
		return nil
	}
}

func (r *RabbitMQ) RouteMessages(handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	return r.routeMessages(false, handleMessage, handlePrometheus, args...)
}

// RouteMessagesConfirmed routes PipelineEvents with confirmed publishing and
// only acknowledges each source delivery after its transfer is durable.
func (r *RabbitMQ) RouteMessagesConfirmed(handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	if err := r.requireConfirmedDelivery(); err != nil {
		return err
	}
	return r.routeMessages(true, handleMessage, handlePrometheus, args...)
}

func (r *RabbitMQ) routeMessages(confirmed bool, handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	publish := r.publishLegacyWithRecovery
	if confirmed {
		publish = r.publishConfirmedWithReconnect
	}

	for {
		if err := r.ensureConnected(); err != nil {
			return err
		}

		consumer := r.currentConsumer()
		if consumer == nil {
			return fmt.Errorf("RabbitMQ channel is not initialized")
		}

		msgs, err := consumer.Consume(
			r.options.ConsumerQueue, // queue
			"",                      // consumer
			false,                   // auto-ack
			false,                   // exclusive
			false,                   // no-local
			false,                   // no-wait
			nil,
		)
		if err != nil {
			if (r.needsReconnect() || isClosedError(err)) && r.Reconnect() == nil {
				continue
			}
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
				if err := r.deadletterAndSettle(d, payload, confirmed); err != nil {
					return err
				}
				continue
			}

			if len(pipelineEvent.Stages) > 0 {
				nextQueue := pipelineEvent.Stages[0]
				nextQueue = r.formatQueueName(nextQueue) // Apply legacy naming convention, we will remove this later
				err = publish(nextQueue, payload, nil)
				if err != nil {
					if err := r.deadletterAndSettle(d, payload, confirmed); err != nil {
						return err
					}
					continue
				}
			}

			// Always acknowledge messages regardless of sync mode
			err = d.Ack(false)
			if err != nil {
				if confirmed {
					r.Close()
					return err
				}
				_ = r.DisasterRecovery(payload)
				continue
			}

			// Chrono end
			endTime := time.Now()
			processingTime := endTime.Sub(startTime)
			e := models.PipelineMetrics{
				ProcessingTime: processingTime.Seconds(),
			}
			handlePrometheus(e)
		}

		if r.needsReconnect() {
			if err := r.Reconnect(); err != nil {
				return err
			}
			continue
		}

		r.Close()
		return nil
	}
}

func (r *RabbitMQ) Close() {
	r.publishMu.Lock()
	defer r.publishMu.Unlock()

	r.mu.Lock()
	consumer := r.Consumer
	producer := r.Producer
	confirmedProducer := r.confirmedProducer
	connection := r.Connection
	r.Consumer = nil
	r.Producer = nil
	r.confirmedProducer = nil
	r.confirmedReturns = nil
	r.Connection = nil
	r.mu.Unlock()

	r.closeResources(consumer, producer, confirmedProducer, connection)
}

// formatQueueName applies legacy naming convention to queue names
// TODO: Remove this once legacy naming convention is deprecated
func (r *RabbitMQ) formatQueueName(queueName string) string {
	return "kcloud-" + queueName + "-queue"
}

// Publish sends a message immediately to the specified RabbitMQ queue
func (r *RabbitMQ) Publish(queueName string, payload []byte) error {
	return r.publishLegacyWithRecovery(queueName, payload, nil)
}

// PublishConfirmed publishes a persistent message and waits for RabbitMQ to
// confirm that it accepted the message into the destination queue.
func (r *RabbitMQ) PublishConfirmed(queueName string, payload []byte) error {
	if err := r.requireConfirmedDelivery(); err != nil {
		return err
	}
	err := r.publishConfirmedWithReconnect(queueName, payload, nil)
	if err == nil {
		return nil
	}
	if recoveryErr := r.DisasterRecovery(payload); recoveryErr != nil {
		return fmt.Errorf("publish failed: %w; disaster recovery failed: %v", err, recoveryErr)
	}
	return err
}

func (r *RabbitMQ) publishLegacyWithReconnect(queueName string, payload []byte, headers amqp.Table) error {
	if err := r.ensureConnected(); err != nil {
		return err
	}

	err := r.publishLegacy(queueName, payload, headers)
	if err != nil && (r.needsReconnect() || isClosedError(err)) {
		if reconnectErr := r.Reconnect(); reconnectErr == nil {
			err = r.publishLegacy(queueName, payload, headers)
		}
	}
	return err
}

func (r *RabbitMQ) publishLegacyWithRecovery(queueName string, payload []byte, headers amqp.Table) error {
	err := r.publishLegacyWithReconnect(queueName, payload, headers)
	if err != nil {
		_ = r.DisasterRecovery(payload)
	}
	return err
}

func (r *RabbitMQ) publishLegacy(queueName string, payload []byte, headers amqp.Table) error {
	producer := r.currentProducer()
	if producer == nil {
		return fmt.Errorf("RabbitMQ producer channel is not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return producer.PublishWithContext(ctx, "", queueName, true, false, amqp.Publishing{
		ContentType: "application/json",
		Headers:     headers,
		Body:        payload,
	})
}

// publishConfirmedWithReconnect publishes payload (optionally carrying headers) and, on a
// closed/stale channel, reconnects once and retries. It is the shared body
// behind confirmed publishing and confirmed consumer transfers.
func (r *RabbitMQ) publishConfirmedWithReconnect(queueName string, payload []byte, headers amqp.Table) error {
	if err := r.ensureConnected(); err != nil {
		return err
	}

	err := r.publishConfirmed(queueName, payload, headers)
	if err != nil && (r.needsReconnect() || isClosedError(err)) {
		if reconnectErr := r.Reconnect(); reconnectErr == nil {
			err = r.publishConfirmed(queueName, payload, headers)
		}
	}
	return err
}

func (r *RabbitMQ) publishConfirmed(queueName string, payload []byte, headers amqp.Table) error {
	r.publishMu.Lock()
	defer r.publishMu.Unlock()

	producer, returns := r.currentConfirmedPublisher()
	if producer == nil {
		return fmt.Errorf("RabbitMQ producer channel is not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	confirmation, err := producer.PublishWithDeferredConfirmWithContext(ctx,
		"",        // exchange
		queueName, // routing key (queue name)
		true,      // mandatory: return (don't silently drop) a message no queue is bound to
		false,     // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Headers:      headers,
			Body:         payload,
		},
	)
	if err != nil {
		return err
	}
	if confirmation == nil {
		return fmt.Errorf("RabbitMQ producer channel is not in confirm mode")
	}

	acknowledged, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("waiting for RabbitMQ publish confirmation: %w", err)
	}
	if !acknowledged {
		return fmt.Errorf("RabbitMQ broker negatively acknowledged publish to %q", queueName)
	}

	select {
	case ret, ok := <-returns:
		if !ok {
			return fmt.Errorf("RabbitMQ return channel closed while publishing to %q", queueName)
		}
		go r.reportReturn(ret)
		return fmt.Errorf("RabbitMQ returned publish to %q as unroutable: %s", ret.RoutingKey, ret.ReplyText)
	default:
		return nil
	}
}

// PublishWithDelay sends a message to the specified RabbitMQ queue after a delay
// This is non-blocking and runs in a goroutine. Errors are not returned to the caller.
// For production use, consider adding logging or an error channel.
func (r *RabbitMQ) PublishWithDelay(queueName string, payload []byte, backoff int) {
	r.publishWithDelayHeaders(queueName, payload, backoff, nil)
}

// publishWithDelayHeaders is PublishWithDelay with explicit message headers, used
// by the capped retry path to carry the incremented retry counter onto the
// re-queued message. Non-blocking; errors are swallowed exactly as PublishWithDelay.
func (r *RabbitMQ) publishWithDelayHeaders(queueName string, payload []byte, backoff int, headers amqp.Table) {
	go func() {
		time.Sleep(time.Duration(backoff) * time.Second)
		_ = r.publishLegacyWithRecovery(queueName, payload, headers)
	}()
}

// retryCountHeader is the AMQP header the queue layer uses to count how many
// times a message has been re-queued for a PipelineRetry. It is absent on a
// first delivery (count 0) and set explicitly on every re-queue.
const retryCountHeader = "x-retry-count"

// defaultMaxRetries bounds PipelineRetry re-queues when RabbitOptions.MaxRetries
// is unset. With the default 5s backoff this rides out a brief sink outage
// (~50s) before the message is dead-lettered instead of looping forever.
const defaultMaxRetries = 10

// maxRetries returns the configured PipelineRetry cap, or defaultMaxRetries when
// unset.
func (r *RabbitMQ) maxRetries() int {
	if r.options.MaxRetries > 0 {
		return r.options.MaxRetries
	}
	return defaultMaxRetries
}

// retryCount reads the x-retry-count header off a delivery, tolerating the
// several integer types AMQP may decode it as. A missing header means this is
// the first attempt (0).
func retryCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	switch v := headers[retryCountHeader].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	case float64:
		return int(v)
	default:
		return 0
	}
}

// retryOrDeadletter handles a PipelineRetry. It synchronously re-queues the
// message after a delay, carrying an incremented x-retry-count, until the count
// reaches the configured cap; past the cap it parks the payload on the
// deadletter queue instead. The caller acknowledges the original delivery only
// after this returns nil, so a failed retry publish cannot silently lose it.
func (r *RabbitMQ) retryOrDeadletter(headers amqp.Table, payload []byte, backoff time.Duration, publish func(string, []byte, amqp.Table) error) error {
	attempts := retryCount(headers)
	if attempts >= r.maxRetries() {
		return publish(r.options.DeadletterQueue, payload, nil)
	}
	if backoff <= 0 {
		backoff = 5 * time.Second
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	<-timer.C
	return publish(r.options.ConsumerQueue, payload, amqp.Table{
		retryCountHeader: int32(attempts + 1),
	})
}

// AddToDeadletter adds a message to the deadletter queue
func (r *RabbitMQ) AddToDeadletter(payload []byte) error {
	topic := r.options.DeadletterQueue
	return r.Publish(topic, payload)
}

func (r *RabbitMQ) addToDeadletterConfirmed(payload []byte) error {
	return r.publishConfirmedWithReconnect(r.options.DeadletterQueue, payload, nil)
}

func (r *RabbitMQ) deadletterOrRecover(payload []byte) error {
	if err := r.addToDeadletterConfirmed(payload); err != nil {
		if r.disasterRecoveryHandler == nil {
			return fmt.Errorf("dead-letter publish failed and no disaster recovery handler is configured: %w", err)
		}
		if recoveryErr := r.DisasterRecovery(payload); recoveryErr != nil {
			return fmt.Errorf("dead-letter publish failed: %w; disaster recovery failed: %v", err, recoveryErr)
		}
	}

	return nil
}

// DisasterRecovery handles messages that failed to publish
// Uses the injected disaster recovery handler if provided, otherwise does nothing
func (r *RabbitMQ) DisasterRecovery(payload []byte) error {
	if r.disasterRecoveryHandler != nil {
		return r.disasterRecoveryHandler(payload)
	}
	// No handler configured - message will be lost
	// Consider logging this in production
	return nil
}

func (r *RabbitMQ) LoadMessages(filename string) error {
	// This method is only meaningful for MockQueue
	return nil
}
