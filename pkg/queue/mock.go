package queue

import (
	"encoding/json"
	"os"
	"time"

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
	CloseCalled   bool                   // Flag to indicate if Close was called
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

// Close stops the queue operations and cleans up resources
func (m *MockQueue) Close() error {
	m.CloseCalled = true

	// Stop the message processing loop
	m.running = false

	// Clear internal queues
	m.messageQueue = make([]models.PipelineEvent, 0)
	m.sentMessages = make([]string, 0)
	return nil
}

func (m *MockQueue) ReadMessages(handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {

	m.running = true
	// Process messages from internal queue
	for m.running {
		if len(m.messageQueue) > 0 {
			// Get the first message from the queue
			pipelineEvent := m.messageQueue[0]
			m.messageQueue = m.messageQueue[1:] // Remove processed message

			// Simulate processing time
			startTime := time.Now()

			// Call the message handler
			pipelineAction, pipelineEvent, _ := handleMessage(pipelineEvent, args...)

			switch pipelineAction {
			case models.PipelineForward:
				_ = pipelineEvent // Suppress unused variable warning

			case models.PipelineCancel:
				_ = pipelineEvent
			case models.PipelineRetry:
				// Put the message back at the end of the queue
				m.messageQueue = append(m.messageQueue, pipelineEvent)
				_ = pipelineEvent
			}

			// Simulate processing time event
			endTime := time.Now()
			processingTime := endTime.Sub(startTime)
			e := models.PipelineMetrics{
				ProcessingTime: processingTime.Seconds(),
			}
			handlePrometheus(e)
		} else {
			// No messages, wait a bit
			time.Sleep(100 * time.Millisecond)
		}
	}

	return nil
}

// RouteMessages simulates routing messages to other queues
func (m *MockQueue) RouteMessages(handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {

	m.running = true

	for m.running {
		if len(m.messageQueue) > 0 {
			// Get the first message from the queue
			pipelineEvent := m.messageQueue[0]
			m.messageQueue = m.messageQueue[1:] // Remove processed message

			// Simulate processing time
			startTime := time.Now()

			// Simulate forwarding logic from SQS implementation
			if len(pipelineEvent.Stages) > 0 {
				// In a real implementation, this would send to the next queue
				// For mock, we just log the action
			}

			// Simulate processing time event
			endTime := time.Now()
			processingTime := endTime.Sub(startTime)
			e := models.PipelineMetrics{
				ProcessingTime: processingTime.Seconds(),
			}
			handlePrometheus(e)
		} else {
			// No messages, wait a bit
			time.Sleep(100 * time.Millisecond)
		}
	}

	return nil
}

// Publish sends a message immediately to the specified RabbitMQ queue
func (m *MockQueue) Publish(queueName string, payload []byte) error {

	// Parse the payload as a PipelineEvent
	var pipelineEvent models.PipelineEvent
	if err := json.Unmarshal([]byte(payload), &pipelineEvent); err != nil {
		return err
	}

	// Add the message to internal queue
	m.messageQueue = append(m.messageQueue, pipelineEvent)

	// Store the message payload for testing purposes
	m.sentMessages = append(m.sentMessages, string(payload))
	return nil
}

// SendMessageFifo simulates sending a FIFO message with deduplication
func (m *MockQueue) SendMessageFifo(queueName string, bytes []byte, deduplicationId string) error {

	// Parse the payload as a PipelineEvent
	var pipelineEvent models.PipelineEvent
	if err := json.Unmarshal(bytes, &pipelineEvent); err != nil {
		return err
	}

	// Check for duplicate messages (simple deduplication)
	for _, msg := range m.sentMessages {
		if msg == string(bytes) {
			return nil
		}
	}

	// Add the message to internal queue
	m.messageQueue = append(m.messageQueue, pipelineEvent)

	// Store the message payload for testing purposes
	m.sentMessages = append(m.sentMessages, string(bytes))
	return nil
}

// Helper methods for testing
// AddMessageToQueue adds a message directly to the internal queue for testing
func (m *MockQueue) AddMessageToQueue(event models.PipelineEvent) {
	m.messageQueue = append(m.messageQueue, event)
}

// GetQueueSize returns the current number of messages in the internal queue
func (m *MockQueue) GetQueueSize() int {
	return len(m.messageQueue)
}

// GetSentMessages returns a copy of all sent messages for testing verification
func (m *MockQueue) GetSentMessages() []string {
	messages := make([]string, len(m.sentMessages))
	copy(messages, m.sentMessages)
	return messages
}

// ClearQueues clears both internal message queue and sent messages
func (m *MockQueue) ClearQueues() {
	m.messageQueue = make([]models.PipelineEvent, 0)
	m.sentMessages = make([]string, 0)
}

// IsRunning returns the current running state of the queue
func (m *MockQueue) IsRunning() bool {
	return m.running
}

// Stop stops the queue operations without clearing the messages
func (m *MockQueue) Stop() {
	m.running = false
}

// LoadMessages loads pipeline events from a JSON file and sends them to the queue
func (m *MockQueue) LoadMessages(filename string) error {
	// Read the mock.json which is an array with models.PipelineEvent
	file, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	var events []models.PipelineEvent
	err = json.Unmarshal(file, &events)
	if err != nil {
		return err
	}
	for _, event := range events {
		// convert event to a string
		eventBytes, err := json.Marshal(event)
		if err != nil {
			return err
		}
		err = m.Publish(m.options.QueueName, eventBytes)
		if err != nil {
			return err
		}
	}
	return nil
}
