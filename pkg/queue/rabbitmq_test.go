package queue

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type recordingAcknowledger struct {
	acks      int
	nacks     int
	requeue   bool
	ackError  error
	nackError error
}

func (a *recordingAcknowledger) Ack(_ uint64, _ bool) error {
	a.acks++
	return a.ackError
}

func (a *recordingAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a.nacks++
	a.requeue = requeue
	return a.nackError
}

func (a *recordingAcknowledger) Reject(_ uint64, _ bool) error {
	return nil
}

// TestRabbitOptionsValidation tests the validation of RabbitMQ options
func TestRabbitOptionsValidation(t *testing.T) {
	tests := []struct {
		name        string
		buildOpts   func() *RabbitOptions
		expectError bool
	}{
		{
			name: "ValidOptionsComplete",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetUri("amqp://user:pass@localhost:5672/").
					SetHost("localhost:5672").
					SetUsername("user").
					SetPassword("pass").
					SetExchange("my-exchange").
					Build()
			},
			expectError: false,
		},
		{
			name: "ValidOptionsMinimal",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetUri("amqp://localhost").
					SetHost("localhost").
					SetUsername("guest").
					SetPassword("guest").
					SetExchange("default").
					Build()
			},
			expectError: false,
		},
		{
			// A workflow stage worker only consumes and dead-letters; it never
			// forwards, so RouterQueue (and AnalysisQueue) may be omitted.
			name: "ValidOptionsStageWorkerNoRouterQueue",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("hub-workflows-loitering").
					SetDeadletterQueue("dead-letter-queue").
					SetHost("localhost").
					SetUsername("user").
					SetPassword("pass").
					Build()
			},
			expectError: false,
		},
		{
			name: "MissingConsumerQueue",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetUri("amqp://localhost").
					SetHost("localhost").
					SetUsername("user").
					SetPassword("pass").
					SetExchange("my-exchange").
					Build()
			},
			expectError: true,
		},
		{
			name: "MissingHost",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetUri("amqp://localhost").
					SetUsername("user").
					SetPassword("pass").
					SetExchange("my-exchange").
					Build()
			},
			expectError: true,
		},
		{
			name: "MissingUsername",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetUri("amqp://localhost").
					SetHost("localhost").
					SetPassword("pass").
					SetExchange("my-exchange").
					Build()
			},
			expectError: true,
		},
		{
			name: "MissingPassword",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetUri("amqp://localhost").
					SetHost("localhost").
					SetUsername("user").
					SetExchange("my-exchange").
					Build()
			},
			expectError: true,
		},
		{
			name: "EmptyOptions",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().Build()
			},
			expectError: true,
		},
		{
			name: "ValidOptionsWithAmqps",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("secure-queue").
					SetDeadletterQueue("secure-queue-dlq").
					SetRouterQueue("secure-queue-router").
					SetUri("amqps://user:pass@localhost:5671/").
					SetHost("amqps://localhost:5671").
					SetUsername("user").
					SetPassword("pass").
					SetExchange("secure-exchange").
					Build()
			},
			expectError: false,
		},
		{
			name: "ValidOptionsWithExplicitTLS",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("tls-queue").
					SetDeadletterQueue("tls-queue-dlq").
					SetRouterQueue("tls-queue-router").
					SetHost("localhost:5671").
					SetUsername("user").
					SetPassword("pass").
					SetTLS(true).
					Build()
			},
			expectError: false,
		},
		{
			name: "ValidOptionsWithTLSAndCACert",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("tls-queue").
					SetDeadletterQueue("tls-queue-dlq").
					SetRouterQueue("tls-queue-router").
					SetHost("localhost:5671").
					SetUsername("user").
					SetPassword("pass").
					SetTLS(true).
					SetTLSCACertFile("/path/to/ca.pem").
					Build()
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.buildOpts()

			_, err := NewRabbitMQ(opts)

			if tt.expectError && err == nil {
				t.Errorf("expected validation error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

// TestRabbitOptionsBuilder tests the fluent builder pattern for RabbitMQ options
func TestRabbitOptionsBuilder(t *testing.T) {
	t.Run("BuilderSettersChaining", func(t *testing.T) {
		opts := NewRabbitOptions().
			SetConsumerQueue("test-queue").
			SetDeadletterQueue("test-queue-dlq").
			SetRouterQueue("test-queue-router").
			SetUri("amqp://testuser:testpass@localhost:5672/testvhost").
			SetHost("localhost:5672").
			SetUsername("testuser").
			SetPassword("testpass").
			SetExchange("test-exchange").
			Build()

		if opts.ConsumerQueue != "test-queue" {
			t.Errorf("expected ConsumerQueue to be 'test-queue', got '%s'", opts.ConsumerQueue)
		}
		if opts.DeadletterQueue != "test-queue-dlq" {
			t.Errorf("expected DeadletterQueue to be 'test-queue-dlq', got '%s'", opts.DeadletterQueue)
		}
		if opts.RouterQueue != "test-queue-router" {
			t.Errorf("expected RouterQueue to be 'test-queue-router', got '%s'", opts.RouterQueue)
		}
		if opts.Uri != "amqp://testuser:testpass@localhost:5672/testvhost" {
			t.Errorf("expected Uri to be 'amqp://testuser:testpass@localhost:5672/testvhost', got '%s'", opts.Uri)
		}
		if opts.Host != "localhost:5672" {
			t.Errorf("expected Host to be 'localhost:5672', got '%s'", opts.Host)
		}
		if opts.Username != "testuser" {
			t.Errorf("expected Username to be 'testuser', got '%s'", opts.Username)
		}
		if opts.Password != "testpass" {
			t.Errorf("expected Password to be 'testpass', got '%s'", opts.Password)
		}
		if opts.Exchange != "test-exchange" {
			t.Errorf("expected Exchange to be 'test-exchange', got '%s'", opts.Exchange)
		}
	})

	t.Run("PartialBuilder", func(t *testing.T) {
		opts := NewRabbitOptions().
			SetUri("amqp://localhost").
			SetHost("localhost").
			Build()

		if opts.ConsumerQueue != "" {
			t.Errorf("expected ConsumerQueue to be empty by default, got '%s'", opts.ConsumerQueue)
		}
		if opts.DeadletterQueue != "" {
			t.Errorf("expected DeadletterQueue to be empty by default, got '%s'", opts.DeadletterQueue)
		}
		if opts.RouterQueue != "" {
			t.Errorf("expected RouterQueue to be empty by default, got '%s'", opts.RouterQueue)
		}
		if opts.Uri != "amqp://localhost" {
			t.Errorf("expected Uri to be set, got '%s'", opts.Uri)
		}
		if opts.Host != "localhost" {
			t.Errorf("expected Host to be set, got '%s'", opts.Host)
		}
		if opts.Username != "" {
			t.Errorf("expected Username to be empty by default, got '%s'", opts.Username)
		}
		if opts.Password != "" {
			t.Errorf("expected Password to be empty by default, got '%s'", opts.Password)
		}
		if opts.Exchange != "" {
			t.Errorf("expected Exchange to be empty by default, got '%s'", opts.Exchange)
		}
	})

	t.Run("EmptyBuilder", func(t *testing.T) {
		opts := NewRabbitOptions().Build()

		if opts.ConsumerQueue != "" {
			t.Errorf("expected ConsumerQueue to be empty, got '%s'", opts.ConsumerQueue)
		}
		if opts.DeadletterQueue != "" {
			t.Errorf("expected DeadletterQueue to be empty, got '%s'", opts.DeadletterQueue)
		}
		if opts.RouterQueue != "" {
			t.Errorf("expected RouterQueue to be empty, got '%s'", opts.RouterQueue)
		}
		if opts.Uri != "" {
			t.Errorf("expected Uri to be empty, got '%s'", opts.Uri)
		}
		if opts.Host != "" {
			t.Errorf("expected Host to be empty, got '%s'", opts.Host)
		}
		if opts.Username != "" {
			t.Errorf("expected Username to be empty, got '%s'", opts.Username)
		}
		if opts.Password != "" {
			t.Errorf("expected Password to be empty, got '%s'", opts.Password)
		}
		if opts.Exchange != "" {
			t.Errorf("expected Exchange to be empty, got '%s'", opts.Exchange)
		}
	})

	t.Run("ConfirmedDeliveryIsOptIn", func(t *testing.T) {
		disabled := NewRabbitOptions().Build()
		if disabled.ConfirmedDelivery {
			t.Fatal("expected confirmed delivery to be disabled by default")
		}

		enabled := NewRabbitOptions().SetConfirmedDelivery(true).Build()
		if !enabled.ConfirmedDelivery {
			t.Fatal("expected SetConfirmedDelivery to enable confirmed delivery")
		}
	})
}

// TestWorkflowsQueueAliases verifies the workflow-oriented builder aliases set
// the same consumer queue as SetConsumerQueue, so workflow services can use the
// clearer names without changing behaviour.
func TestWorkflowsQueueAliases(t *testing.T) {
	t.Run("SetWorkflowsQueue", func(t *testing.T) {
		opts := NewRabbitOptions().
			SetWorkflowsQueue("hub-workflows-queue").
			SetDeadletterQueue("dead-letter-queue").
			SetRouterQueue("kcloud-event-queue").
			SetHost("localhost").
			SetUsername("user").
			SetPassword("pass").
			Build()

		if opts.ConsumerQueue != "hub-workflows-queue" {
			t.Errorf("expected ConsumerQueue to be 'hub-workflows-queue', got '%s'", opts.ConsumerQueue)
		}
		if err := opts.Validate(); err != nil {
			t.Errorf("expected options to validate, got: %v", err)
		}
	})

	t.Run("SetWorkflowsStageQueue", func(t *testing.T) {
		opts := NewRabbitOptions().
			SetWorkflowsStageQueue("hub-workflows-loitering").
			SetDeadletterQueue("dead-letter-queue").
			SetRouterQueue("kcloud-event-queue").
			SetHost("localhost").
			SetUsername("user").
			SetPassword("pass").
			Build()

		if opts.ConsumerQueue != "hub-workflows-loitering" {
			t.Errorf("expected ConsumerQueue to be 'hub-workflows-loitering', got '%s'", opts.ConsumerQueue)
		}
		if err := opts.Validate(); err != nil {
			t.Errorf("expected options to validate, got: %v", err)
		}
	})
}

// TestRabbitConnectionStringGeneration tests the connection string generation logic
func TestRabbitConnectionStringGeneration(t *testing.T) {
	tests := []struct {
		name            string
		buildOpts       func() *RabbitOptions
		expectedConnStr string
	}{
		{
			name: "BasicAmqpProtocol",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetUri("amqp://localhost").
					SetHost("localhost:5672").
					SetUsername("user").
					SetPassword("pass").
					SetExchange("exchange").
					Build()
			},
			expectedConnStr: "amqp://user:pass@localhost:5672/",
		},
		{
			name: "AmqpsProtocol",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetUri("amqps://localhost").
					SetHost("amqps://localhost:5671").
					SetUsername("user").
					SetPassword("pass").
					SetExchange("exchange").
					Build()
			},
			expectedConnStr: "amqps://user:pass@localhost:5671/",
		},
		{
			name: "AmqpProtocol",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetUri("amqp://localhost").
					SetHost("amqp://localhost:5672").
					SetUsername("user").
					SetPassword("pass").
					SetExchange("exchange").
					Build()
			},
			expectedConnStr: "amqp://user:pass@localhost:5672/",
		},
		{
			name: "NoProtocolInHost",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetUri("amqp://localhost").
					SetHost("localhost:5672").
					SetUsername("guest").
					SetPassword("guest").
					SetExchange("default").
					Build()
			},
			expectedConnStr: "amqp://guest:guest@localhost:5672/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.buildOpts()
			queueClient, err := New(opts)
			if err != nil {
				t.Fatalf("failed to create RabbitMQ instance: %v", err)
			}

			rabbit := queueClient.Client.(*RabbitMQ)

			if rabbit.connectionString != tt.expectedConnStr {
				t.Errorf("expected connection string '%s', got '%s'", tt.expectedConnStr, rabbit.connectionString)
			}
		})
	}
}

func TestRabbitMQIntegration(t *testing.T) {

	// Read environment variables
	uri := os.Getenv("RABBITMQ_URI")
	host := os.Getenv("RABBITMQ_HOST")
	username := os.Getenv("RABBITMQ_USERNAME")
	password := os.Getenv("RABBITMQ_PASSWORD")
	queueName := os.Getenv("RABBITMQ_QUEUE_NAME")
	exchange := os.Getenv("RABBITMQ_EXCHANGE")

	// Skip integration tests if required environment variables are not set
	if host == "" || username == "" || password == "" {
		t.Skip("Skipping integration tests: RABBITMQ_HOST, RABBITMQ_USERNAME, and RABBITMQ_PASSWORD must be set")
	}

	// Set defaults for optional values
	if queueName == "" {
		queueName = "test-integration-queue"
	}

	t.Run("ConfirmedPublishRejectsUnroutableMessage", func(t *testing.T) {
		testQueue := fmt.Sprintf("%s-confirm-%d", queueName, time.Now().UnixNano())
		opts := NewRabbitOptions().
			SetConsumerQueue(testQueue).
			SetDeadletterQueue(testQueue + "-dlq").
			SetHost(host).
			SetUsername(username).
			SetPassword(password).
			SetConfirmedDelivery(true).
			Build()

		rabbit, err := NewRabbitMQ(opts)
		if err != nil {
			t.Fatalf("create RabbitMQ client: %v", err)
		}
		if err := rabbit.Connect(); err != nil {
			t.Fatalf("connect to RabbitMQ: %v", err)
		}
		defer rabbit.Close()

		returned := make(chan amqp.Return, 1)
		rabbit.SetReturnHandler(func(ret amqp.Return) {
			returned <- ret
		})

		missingQueue := testQueue + "-missing"
		if err := rabbit.PublishConfirmed(missingQueue, []byte(`{"message":"unroutable"}`)); err == nil {
			t.Fatal("expected an unroutable confirmed publish to fail")
		}
		select {
		case ret := <-returned:
			if ret.RoutingKey != missingQueue {
				t.Fatalf("returned routing key = %q, want %q", ret.RoutingKey, missingQueue)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for the mandatory return handler")
		}
	})

	t.Run("ConnectionInterruptionReconnectsAndRedelivers", func(t *testing.T) {
		testQueue := fmt.Sprintf("%s-reconnect-%d", queueName, time.Now().UnixNano())
		opts := NewRabbitOptions().
			SetConsumerQueue(testQueue).
			SetDeadletterQueue(testQueue + "-dlq").
			SetHost(host).
			SetUsername(username).
			SetPassword(password).
			SetConfirmedDelivery(true).
			Build()

		rabbit, err := NewRabbitMQ(opts)
		if err != nil {
			t.Fatalf("create RabbitMQ client: %v", err)
		}
		if err := rabbit.Connect(); err != nil {
			t.Fatalf("connect to RabbitMQ: %v", err)
		}
		defer rabbit.Close()

		payload := []byte(`{"message":"redeliver"}`)
		if err := rabbit.PublishConfirmed(testQueue, payload); err != nil {
			t.Fatalf("publish test message: %v", err)
		}

		delivery, ok, err := rabbit.currentConsumer().Get(testQueue, false)
		if err != nil {
			t.Fatalf("get test message: %v", err)
		}
		if !ok {
			t.Fatal("expected the published message")
		}
		if delivery.DeliveryMode != amqp.Persistent {
			t.Fatalf("delivery mode = %d, want persistent (%d)", delivery.DeliveryMode, amqp.Persistent)
		}

		oldConnection := rabbit.Connection
		if err := oldConnection.Close(); err != nil {
			t.Fatalf("interrupt RabbitMQ connection: %v", err)
		}
		if err := rabbit.ensureConnected(); err != nil {
			t.Fatalf("reconnect after interruption: %v", err)
		}
		if rabbit.Connection == oldConnection {
			t.Fatal("expected reconnect to replace the interrupted connection")
		}

		deadline := time.Now().Add(5 * time.Second)
		for {
			delivery, ok, err = rabbit.currentConsumer().Get(testQueue, false)
			if err != nil {
				t.Fatalf("get redelivered message: %v", err)
			}
			if ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for unacked message redelivery")
			}
			time.Sleep(25 * time.Millisecond)
		}

		if string(delivery.Body) != string(payload) {
			t.Fatalf("redelivered body = %q, want %q", delivery.Body, payload)
		}
		if !delivery.Redelivered {
			t.Fatal("expected interrupted unacked delivery to be marked redelivered")
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("ack redelivered message: %v", err)
		}
	})

	t.Run("ConnectToRealRabbitMQ", func(t *testing.T) {
		// Build RabbitMQ options from environment variables
		opts := NewRabbitOptions().
			SetConsumerQueue(queueName).
			SetDeadletterQueue(queueName + "-dlq").
			SetRouterQueue(queueName + "-router").
			SetUri(uri).
			SetHost(host).
			SetUsername(username).
			SetPassword(password).
			SetExchange(exchange).
			SetPrefetchCount(5).
			Build()

		// Create RabbitMQ instance
		queueClient, err := New(opts)
		if err != nil {
			t.Fatalf("Failed to create RabbitMQ instance: %v", err)
		}

		// Get the underlying RabbitMQ client
		rabbit, ok := queueClient.Client.(*RabbitMQ)
		if !ok {
			t.Fatal("Failed to assert RabbitMQ client type")
		}

		// Test connection
		err = rabbit.Connect()
		if err != nil {
			t.Fatalf("Failed to connect to RabbitMQ: %v", err)
		}
		defer func() {
			if rabbit.Connection != nil && !rabbit.Connection.IsClosed() {
				rabbit.Connection.Close()
			}
		}()

		// Verify connection is established
		if rabbit.Connection == nil {
			t.Fatal("Connection is nil after Connect()")
		}
		if rabbit.Connection.IsClosed() {
			t.Fatal("Connection is closed after Connect()")
		}

		// Verify channels are created
		if rabbit.Producer == nil {
			t.Fatal("Producer channel is nil after Connect()")
		}
		if rabbit.Consumer == nil {
			t.Fatal("Consumer channel is nil after Connect()")
		}

		t.Logf("Successfully connected to RabbitMQ at %s", host)
		t.Logf("Queue: %s", queueName)
		t.Logf("Exchange: %s", exchange)
	})

	t.Run("ConnectionHealthCheck", func(t *testing.T) {
		opts := NewRabbitOptions().
			SetConsumerQueue(queueName).
			SetDeadletterQueue(queueName + "-dlq").
			SetRouterQueue(queueName + "-router").
			SetUri(uri).
			SetHost(host).
			SetUsername(username).
			SetPassword(password).
			SetExchange(exchange).
			Build()

		queueClient, err := New(opts)
		if err != nil {
			t.Fatalf("Failed to create RabbitMQ instance: %v", err)
		}

		rabbit := queueClient.Client.(*RabbitMQ)

		err = rabbit.Connect()
		if err != nil {
			t.Fatalf("Failed to connect to RabbitMQ: %v", err)
		}
		defer func() {
			if rabbit.Connection != nil && !rabbit.Connection.IsClosed() {
				rabbit.Connection.Close()
			}
		}()

		// Wait a moment to ensure connection stability
		time.Sleep(100 * time.Millisecond)

		// Check that connection remains stable
		if rabbit.Connection.IsClosed() {
			t.Fatal("Connection closed unexpectedly after initial connection")
		}

		t.Log("Connection health check passed")
	})

	t.Run("MultipleConnections", func(t *testing.T) {
		opts := NewRabbitOptions().
			SetConsumerQueue(queueName).
			SetDeadletterQueue(queueName + "-dlq").
			SetRouterQueue(queueName + "-router").
			SetUri(uri).
			SetHost(host).
			SetUsername(username).
			SetPassword(password).
			SetExchange(exchange).
			Build()

		// Create multiple connections
		connections := make([]*RabbitMQ, 3)
		for i := 0; i < 3; i++ {
			queueClient, err := New(opts)
			if err != nil {
				t.Fatalf("Failed to create RabbitMQ instance %d: %v", i, err)
			}

			rabbit := queueClient.Client.(*RabbitMQ)

			err = rabbit.Connect()
			if err != nil {
				t.Fatalf("Failed to connect RabbitMQ instance %d: %v", i, err)
			}

			connections[i] = rabbit
		}

		// Close all connections
		for i, rabbit := range connections {
			if rabbit.Connection != nil && !rabbit.Connection.IsClosed() {
				err := rabbit.Connection.Close()
				if err != nil {
					t.Errorf("Failed to close connection %d: %v", i, err)
				}
			}
		}

		t.Log("Successfully created and closed multiple connections")
	})
}

// TestFormatQueueName tests the formatQueueName function that applies legacy naming convention
func TestFormatQueueName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple queue name",
			input:    "test",
			expected: "kcloud-test-queue",
		},
		{
			name:     "Queue name with dashes",
			input:    "test-service",
			expected: "kcloud-test-service-queue",
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "kcloud--queue",
		},
		{
			name:     "Queue name with underscores",
			input:    "test_service",
			expected: "kcloud-test_service-queue",
		},
		{
			name:     "Queue name with numbers",
			input:    "test123",
			expected: "kcloud-test123-queue",
		},
	}

	// Create a minimal RabbitMQ instance for testing
	rabbit := &RabbitMQ{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rabbit.formatQueueName(tt.input)
			if result != tt.expected {
				t.Errorf("formatQueueName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsClosedError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "NilError",
			err:      nil,
			expected: false,
		},
		{
			name:     "ClosedChannelError",
			err:      errors.New("Exception (504) Reason: \"channel/connection is not open\""),
			expected: true,
		},
		{
			name:     "ClosedConnectionError",
			err:      errors.New("connection is not open"),
			expected: true,
		},
		{
			name:     "UnrelatedError",
			err:      errors.New("permission denied"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isClosedError(tt.err)
			if result != tt.expected {
				t.Errorf("isClosedError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

func TestRabbitMQNeedsReconnectWithoutResources(t *testing.T) {
	rabbit := &RabbitMQ{}

	if !rabbit.needsReconnect() {
		t.Fatal("expected reconnect to be required when connection resources are missing")
	}
}

func TestConfirmedDeliveryRequiresOptIn(t *testing.T) {
	rabbit := &RabbitMQ{options: &RabbitOptions{}}

	if err := rabbit.PublishConfirmed("target", []byte("payload")); err == nil {
		t.Fatal("expected PublishConfirmed to require confirmed delivery")
	}
	if err := rabbit.ReadMessagesConfirmed(nil, nil); err == nil {
		t.Fatal("expected ReadMessagesConfirmed to require confirmed delivery")
	}
	if err := rabbit.ReadRawMessagesConfirmed(nil, nil); err == nil {
		t.Fatal("expected ReadRawMessagesConfirmed to require confirmed delivery")
	}
	if err := rabbit.RouteMessagesConfirmed(nil, nil); err == nil {
		t.Fatal("expected RouteMessagesConfirmed to require confirmed delivery")
	}
	if _, err := rabbit.ReadOneRawConfirmed(nil); err == nil {
		t.Fatal("expected ReadOneRawConfirmed to require confirmed delivery")
	}
}

func TestSettleTransferredDelivery(t *testing.T) {
	t.Run("AcknowledgesAfterSuccessfulTransfer", func(t *testing.T) {
		acknowledger := &recordingAcknowledger{}
		delivery := amqp.Delivery{Acknowledger: acknowledger, DeliveryTag: 1}

		err := (&RabbitMQ{}).settleTransferredDelivery(delivery, func() error { return nil })
		if err != nil {
			t.Fatalf("settle successful transfer: %v", err)
		}
		if acknowledger.acks != 1 || acknowledger.nacks != 0 {
			t.Fatalf("acks = %d, nacks = %d; want one ack and no nacks", acknowledger.acks, acknowledger.nacks)
		}
	})

	t.Run("RequeuesAfterFailedTransfer", func(t *testing.T) {
		acknowledger := &recordingAcknowledger{}
		delivery := amqp.Delivery{Acknowledger: acknowledger, DeliveryTag: 1}
		transferError := errors.New("destination unavailable")

		err := (&RabbitMQ{}).settleTransferredDelivery(delivery, func() error { return transferError })
		if !errors.Is(err, transferError) {
			t.Fatalf("settle error = %v, want %v", err, transferError)
		}
		if acknowledger.acks != 0 || acknowledger.nacks != 1 || !acknowledger.requeue {
			t.Fatalf("acks = %d, nacks = %d, requeue = %t; want no ack and one requeue nack", acknowledger.acks, acknowledger.nacks, acknowledger.requeue)
		}
	})

	t.Run("ReturnsAcknowledgementFailure", func(t *testing.T) {
		ackError := errors.New("consumer channel closed")
		acknowledger := &recordingAcknowledger{ackError: ackError}
		delivery := amqp.Delivery{Acknowledger: acknowledger, DeliveryTag: 1}

		err := (&RabbitMQ{}).settleTransferredDelivery(delivery, func() error { return nil })
		if !errors.Is(err, ackError) {
			t.Fatalf("settle error = %v, want %v", err, ackError)
		}
		if acknowledger.acks != 1 || acknowledger.nacks != 0 {
			t.Fatalf("acks = %d, nacks = %d; want one attempted ack and no nacks", acknowledger.acks, acknowledger.nacks)
		}
	})
}

// TestDisasterRecovery tests the DisasterRecovery function
func TestDisasterRecovery(t *testing.T) {
	t.Run("WithHandler", func(t *testing.T) {
		// Create a RabbitMQ instance using the builder pattern
		opts := NewRabbitOptions().
			SetConsumerQueue("test-queue").
			SetDeadletterQueue("test-queue-dlq").
			SetRouterQueue("test-queue-router").
			SetHost("localhost").
			SetUsername("guest").
			SetPassword("guest").
			Build()

		queueClient, err := New(opts)
		if err != nil {
			t.Fatalf("failed to create RabbitMQ instance: %v", err)
		}

		rabbit := queueClient.Client.(*RabbitMQ)

		// Track whether handler was called and with what payload
		var handlerCalled bool
		var receivedPayload []byte
		var handlerError error

		// Set a custom disaster recovery handler
		rabbit.SetDisasterRecoveryHandler(func(payload []byte) error {
			handlerCalled = true
			receivedPayload = payload
			return handlerError
		})

		// Test payload
		testPayload := []byte(`{"test": "data"}`)

		// Call DisasterRecovery
		err = rabbit.DisasterRecovery(testPayload)

		// Verify handler was called
		if !handlerCalled {
			t.Error("expected disaster recovery handler to be called, but it wasn't")
		}

		// Verify correct payload was passed
		if string(receivedPayload) != string(testPayload) {
			t.Errorf("expected payload %q, got %q", testPayload, receivedPayload)
		}

		// Verify no error was returned
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("WithHandlerReturningError", func(t *testing.T) {
		// Create a RabbitMQ instance using the builder pattern
		opts := NewRabbitOptions().
			SetConsumerQueue("test-queue").
			SetDeadletterQueue("test-queue-dlq").
			SetRouterQueue("test-queue-router").
			SetHost("localhost").
			SetUsername("guest").
			SetPassword("guest").
			Build()

		queueClient, err := New(opts)
		if err != nil {
			t.Fatalf("failed to create RabbitMQ instance: %v", err)
		}

		rabbit := queueClient.Client.(*RabbitMQ)

		// Set a handler that returns an error
		expectedErr := fmt.Errorf("handler error")
		rabbit.SetDisasterRecoveryHandler(func(payload []byte) error {
			return expectedErr
		})

		// Test payload
		testPayload := []byte(`{"test": "data"}`)

		// Call DisasterRecovery
		err = rabbit.DisasterRecovery(testPayload)

		// Verify error was returned
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("WithoutHandler", func(t *testing.T) {
		// Create a RabbitMQ instance without setting a handler
		opts := NewRabbitOptions().
			SetConsumerQueue("test-queue").
			SetDeadletterQueue("test-queue-dlq").
			SetRouterQueue("test-queue-router").
			SetHost("localhost").
			SetUsername("guest").
			SetPassword("guest").
			Build()

		queueClient, err := New(opts)
		if err != nil {
			t.Fatalf("failed to create RabbitMQ instance: %v", err)
		}

		rabbit := queueClient.Client.(*RabbitMQ)

		// Test payload
		testPayload := []byte(`{"test": "data"}`)

		// Call DisasterRecovery
		err = rabbit.DisasterRecovery(testPayload)

		// Verify no error was returned (default behavior)
		if err != nil {
			t.Errorf("expected no error when no handler is set, got %v", err)
		}
	})

	t.Run("WithEmptyPayload", func(t *testing.T) {
		// Create a RabbitMQ instance using the builder pattern
		opts := NewRabbitOptions().
			SetConsumerQueue("test-queue").
			SetDeadletterQueue("test-queue-dlq").
			SetRouterQueue("test-queue-router").
			SetHost("localhost").
			SetUsername("guest").
			SetPassword("guest").
			Build()

		queueClient, err := New(opts)
		if err != nil {
			t.Fatalf("failed to create RabbitMQ instance: %v", err)
		}

		rabbit := queueClient.Client.(*RabbitMQ)

		// Track handler call
		var handlerCalled bool
		var receivedPayload []byte

		rabbit.SetDisasterRecoveryHandler(func(payload []byte) error {
			handlerCalled = true
			receivedPayload = payload
			return nil
		})

		// Empty payload
		testPayload := []byte{}

		// Call DisasterRecovery
		err = rabbit.DisasterRecovery(testPayload)

		// Verify handler was called
		if !handlerCalled {
			t.Error("expected disaster recovery handler to be called")
		}

		// Verify empty payload was passed
		if len(receivedPayload) != 0 {
			t.Errorf("expected empty payload, got %v", receivedPayload)
		}

		// Verify no error
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("WithNilPayload", func(t *testing.T) {
		// Create a RabbitMQ instance using the builder pattern
		opts := NewRabbitOptions().
			SetConsumerQueue("test-queue").
			SetDeadletterQueue("test-queue-dlq").
			SetRouterQueue("test-queue-router").
			SetHost("localhost").
			SetUsername("guest").
			SetPassword("guest").
			Build()

		queueClient, err := New(opts)
		if err != nil {
			t.Fatalf("failed to create RabbitMQ instance: %v", err)
		}

		rabbit := queueClient.Client.(*RabbitMQ)

		// Track handler call
		var handlerCalled bool
		var receivedPayload []byte

		rabbit.SetDisasterRecoveryHandler(func(payload []byte) error {
			handlerCalled = true
			receivedPayload = payload
			return nil
		})

		// Call DisasterRecovery with nil
		err = rabbit.DisasterRecovery(nil)

		// Verify handler was called
		if !handlerCalled {
			t.Error("expected disaster recovery handler to be called")
		}

		// Verify nil payload was passed
		if receivedPayload != nil {
			t.Errorf("expected nil payload, got %v", receivedPayload)
		}

		// Verify no error
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})
}

// TestTLSOptionsBuilder tests the TLS-related builder methods
func TestTLSOptionsBuilder(t *testing.T) {
	t.Run("SetTLSEnabled", func(t *testing.T) {
		opts := NewRabbitOptions().
			SetTLS(true).
			Build()

		if !opts.TLS {
			t.Error("expected TLS to be true")
		}
	})

	t.Run("SetTLSInsecureSkipVerify", func(t *testing.T) {
		opts := NewRabbitOptions().
			SetTLS(true).
			SetTLSInsecureSkipVerify(true).
			Build()

		if !opts.TLSInsecureSkipVerify {
			t.Error("expected TLSInsecureSkipVerify to be true")
		}
	})

	t.Run("SetTLSCACertFile", func(t *testing.T) {
		opts := NewRabbitOptions().
			SetTLS(true).
			SetTLSCACertFile("/path/to/ca.pem").
			Build()

		if opts.TLSCACertFile != "/path/to/ca.pem" {
			t.Errorf("expected TLSCACertFile to be '/path/to/ca.pem', got '%s'", opts.TLSCACertFile)
		}
	})

	t.Run("TLSDefaultsFalse", func(t *testing.T) {
		opts := NewRabbitOptions().Build()

		if opts.TLS {
			t.Error("expected TLS to be false by default")
		}
		if opts.TLSInsecureSkipVerify {
			t.Error("expected TLSInsecureSkipVerify to be false by default")
		}
		if opts.TLSCACertFile != "" {
			t.Errorf("expected TLSCACertFile to be empty by default, got '%s'", opts.TLSCACertFile)
		}
	})
}

// TestTLSConnectionString tests that TLS options correctly affect the connection string
func TestTLSConnectionString(t *testing.T) {
	tests := []struct {
		name            string
		buildOpts       func() *RabbitOptions
		expectedConnStr string
		expectedTLS     bool
	}{
		{
			name: "ExplicitTLSForcesAmqps",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetHost("localhost:5671").
					SetUsername("user").
					SetPassword("pass").
					SetTLS(true).
					Build()
			},
			expectedConnStr: "amqps://user:pass@localhost:5671/",
			expectedTLS:     true,
		},
		{
			name: "AmqpsHostAutoEnablesTLS",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetHost("amqps://broker.amazonaws.com:5671").
					SetUsername("user").
					SetPassword("pass").
					Build()
			},
			expectedConnStr: "amqps://user:pass@broker.amazonaws.com:5671/",
			expectedTLS:     true,
		},
		{
			name: "NoTLSUsesAmqp",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("test-queue").
					SetDeadletterQueue("test-queue-dlq").
					SetRouterQueue("test-queue-router").
					SetHost("localhost:5672").
					SetUsername("user").
					SetPassword("pass").
					Build()
			},
			expectedConnStr: "amqp://user:pass@localhost:5672/",
			expectedTLS:     false,
		},
		{
			name: "AWSAmazonMQStyle",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
					SetConsumerQueue("my-queue").
					SetDeadletterQueue("my-queue-dlq").
					SetRouterQueue("my-queue-router").
					SetHost("amqps://b-xxxx-xxxx.mq.us-east-1.amazonaws.com:5671").
					SetUsername("admin").
					SetPassword("secret").
					SetTLSInsecureSkipVerify(false).
					Build()
			},
			expectedConnStr: "amqps://admin:secret@b-xxxx-xxxx.mq.us-east-1.amazonaws.com:5671/",
			expectedTLS:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.buildOpts()
			rabbit, err := NewRabbitMQ(opts)
			if err != nil {
				t.Fatalf("failed to create RabbitMQ instance: %v", err)
			}

			if rabbit.connectionString != tt.expectedConnStr {
				t.Errorf("expected connection string '%s', got '%s'", tt.expectedConnStr, rabbit.connectionString)
			}

			if rabbit.options.TLS != tt.expectedTLS {
				t.Errorf("expected TLS to be %v, got %v", tt.expectedTLS, rabbit.options.TLS)
			}
		})
	}
}

// TestTLSConnectWithInvalidCACert tests that Connect fails gracefully with invalid CA cert
func TestTLSConnectWithInvalidCACert(t *testing.T) {
	t.Run("NonExistentCACertFile", func(t *testing.T) {
		opts := NewRabbitOptions().
			SetConsumerQueue("test-queue").
			SetDeadletterQueue("test-queue-dlq").
			SetRouterQueue("test-queue-router").
			SetHost("localhost:5671").
			SetUsername("user").
			SetPassword("pass").
			SetTLS(true).
			SetTLSCACertFile("/nonexistent/ca.pem").
			Build()

		rabbit, err := NewRabbitMQ(opts)
		if err != nil {
			t.Fatalf("failed to create RabbitMQ instance: %v", err)
		}

		err = rabbit.Connect()
		if err == nil {
			t.Fatal("expected error for non-existent CA cert file, got nil")
		}
	})

	t.Run("InvalidCACertContent", func(t *testing.T) {
		// Create a temp file with invalid PEM content
		tmpFile, err := os.CreateTemp("", "invalid-ca-*.pem")
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())

		_, err = tmpFile.WriteString("this is not a valid PEM certificate")
		if err != nil {
			t.Fatalf("failed to write to temp file: %v", err)
		}
		tmpFile.Close()

		opts := NewRabbitOptions().
			SetConsumerQueue("test-queue").
			SetDeadletterQueue("test-queue-dlq").
			SetRouterQueue("test-queue-router").
			SetHost("localhost:5671").
			SetUsername("user").
			SetPassword("pass").
			SetTLS(true).
			SetTLSCACertFile(tmpFile.Name()).
			Build()

		rabbit, err := NewRabbitMQ(opts)
		if err != nil {
			t.Fatalf("failed to create RabbitMQ instance: %v", err)
		}

		err = rabbit.Connect()
		if err == nil {
			t.Fatal("expected error for invalid CA cert content, got nil")
		}
	})
}
