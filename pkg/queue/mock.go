package queue

import (
	"github.com/go-playground/validator/v10"
	"github.com/uug-ai/models/pkg/models"
)

// MockOptions holds the configuration for MockQueue
type MockOptions struct {
	QueueName string `validate:"required"`
}

// RabbitOptionsBuilder provides a fluent interface for building Rabbit options
type MockOptionsBuilder struct {
	options *MockOptions
}

// MockOptions creates a new Mock options builder
func NewMockOptions() *MockOptionsBuilder {
	return &MockOptionsBuilder{
		options: &MockOptions{},
	}
}

// SetQueueName sets the queue name
func (b *MockOptionsBuilder) SetQueueName(queueName string) *MockOptionsBuilder {
	b.options.QueueName = queueName
	return b
}

// Build builds the MockOptions instance
func (b *MockOptionsBuilder) Build() *MockOptions {
	return b.options
}

// MockQueue is a mock implementation of Queue interface for testing
type MockQueue struct {
	options       *MockOptions           // Configuration options
	sentMessages  []string               // Store messages sent during testing
	messageQueue  []models.PipelineEvent // In-memory queue to store messages
	running       bool                   // Running flag for controlling the message loop
	ConnectCalled bool                   // Flag to indicate if Connect was called
	ConnectError  error                  // Error to return on Connect
}

// NewMock creates a new MockQueue instance
func NewMock(options *MockOptions) (*MockQueue, error) {
	// Validate Database configuration
	validate := validator.New()
	err := validate.Struct(options)
	if err != nil {
		return nil, err
	}

	return &MockQueue{
		options:      options,
		sentMessages: make([]string, 0),
		messageQueue: make([]models.PipelineEvent, 0),
		running:      false,
	}, nil
}

func (m *MockQueue) Connect() error {
	m.ConnectCalled = true
	return m.ConnectError
}
