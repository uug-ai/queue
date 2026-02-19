package queue

import (
	"fmt"
	"os"
	"testing"
	"time"
)

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
