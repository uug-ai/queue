package queue

import (
	"testing"
)

// MockQueueInterface is a mock implementation of Queue interface for testing
type MockQueueInterface struct {
	ConnectCalled bool
	ConnectError  error
}

func (m *MockQueueInterface) Connect() error {
	m.ConnectCalled = true
	return m.ConnectError
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
			name: "MissingUri",
			buildOpts: func() *RabbitOptions {
				return NewRabbitOptions().
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
					SetUri("amqps://user:pass@localhost:5671/").
					SetHost("amqps://localhost:5671").
					SetUsername("user").
					SetPassword("pass").
					SetExchange("secure-exchange").
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
			SetUri("amqp://testuser:testpass@localhost:5672/testvhost").
			SetHost("localhost:5672").
			SetUsername("testuser").
			SetPassword("testpass").
			SetExchange("test-exchange").
			Build()

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
			rabbit, err := NewRabbitMQ(opts)

			if err != nil {
				t.Fatalf("failed to create RabbitMQ instance: %v", err)
			}

			if rabbit.connectionString != tt.expectedConnStr {
				t.Errorf("expected connection string '%s', got '%s'", tt.expectedConnStr, rabbit.connectionString)
			}
		})
	}
}
