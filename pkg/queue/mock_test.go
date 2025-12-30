package queue

import (
	"testing"
)

// TestMockOptionsValidation tests the validation of MockQueue options
func TestMockOptionsValidation(t *testing.T) {
	tests := []struct {
		name        string
		buildOpts   func() *MockOptions
		expectError bool
	}{
		{
			name: "ValidOptionsWithQueueName",
			buildOpts: func() *MockOptions {
				return NewMockOptions().
					SetQueueName("test-queue").
					Build()
			},
			expectError: false,
		},
		{
			name: "ValidOptionsWithDifferentQueueName",
			buildOpts: func() *MockOptions {
				return NewMockOptions().
					SetQueueName("my-test-queue").
					Build()
			},
			expectError: false,
		},
		{
			name: "MissingQueueName",
			buildOpts: func() *MockOptions {
				return NewMockOptions().
					Build()
			},
			expectError: true, // QueueName is required
		},
		{
			name: "EmptyQueueName",
			buildOpts: func() *MockOptions {
				return NewMockOptions().
					SetQueueName("").
					Build()
			},
			expectError: true, // QueueName is required
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.buildOpts()

			_, err := NewMock(opts)

			if tt.expectError && err == nil {
				t.Errorf("expected validation error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

// TestMockOptionsBuilder tests the fluent builder pattern for MockQueue options
func TestMockOptionsBuilder(t *testing.T) {
	t.Run("BuilderSettersChaining", func(t *testing.T) {
		opts := NewMockOptions().
			SetQueueName("test-queue").
			Build()

		if opts.QueueName != "test-queue" {
			t.Errorf("expected QueueName to be 'test-queue', got '%s'", opts.QueueName)
		}
	})

	t.Run("PartialBuilder", func(t *testing.T) {
		opts := NewMockOptions().
			Build()

		if opts.QueueName != "" {
			t.Errorf("expected QueueName to be empty by default, got '%s'", opts.QueueName)
		}
	})

	t.Run("EmptyBuilder", func(t *testing.T) {
		opts := NewMockOptions().Build()

		if opts.QueueName != "" {
			t.Errorf("expected QueueName to be empty, got '%s'", opts.QueueName)
		}
	})

	t.Run("MultipleSetters", func(t *testing.T) {
		opts := NewMockOptions().
			SetQueueName("first-queue").
			SetQueueName("second-queue").
			Build()

		if opts.QueueName != "second-queue" {
			t.Errorf("expected QueueName to be 'second-queue' (last set value), got '%s'", opts.QueueName)
		}
	})
}

// TestMockQueueCreation tests the creation and initialization of MockQueue
func TestMockQueueCreation(t *testing.T) {
	tests := []struct {
		name      string
		buildOpts func() *MockOptions
	}{
		{
			name: "CreateWithValidOptions",
			buildOpts: func() *MockOptions {
				return NewMockOptions().
					SetQueueName("test-queue").
					Build()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.buildOpts()
			mock, err := NewMock(opts)

			if err != nil {
				t.Fatalf("failed to create MockQueue instance: %v", err)
			}

			if mock == nil {
				t.Fatal("expected MockQueue instance but got nil")
			}

			if mock.options == nil {
				t.Error("expected options to be set")
			}

			if mock.options.QueueName != opts.QueueName {
				t.Errorf("expected QueueName '%s', got '%s'", opts.QueueName, mock.options.QueueName)
			}

			if mock.sentMessages == nil {
				t.Error("expected sentMessages to be initialized")
			}

			if len(mock.sentMessages) != 0 {
				t.Errorf("expected sentMessages to be empty initially, got %d items", len(mock.sentMessages))
			}

			if mock.messageQueue == nil {
				t.Error("expected messageQueue to be initialized")
			}

			if len(mock.messageQueue) != 0 {
				t.Errorf("expected messageQueue to be empty initially, got %d items", len(mock.messageQueue))
			}

			if mock.running {
				t.Error("expected running to be false initially")
			}

			if mock.ConnectCalled {
				t.Error("expected ConnectCalled to be false initially")
			}

			if mock.ConnectError != nil {
				t.Errorf("expected ConnectError to be nil initially, got %v", mock.ConnectError)
			}
		})
	}
}

// TestMockQueueConnect tests the Connect method of MockQueue
func TestMockQueueConnect(t *testing.T) {
	t.Run("ConnectSuccess", func(t *testing.T) {
		opts := NewMockOptions().SetQueueName("test-queue").Build()
		mock, err := NewMock(opts)
		if err != nil {
			t.Fatalf("failed to create MockQueue: %v", err)
		}

		if mock.ConnectCalled {
			t.Error("expected ConnectCalled to be false before calling Connect")
		}

		err = mock.Connect()

		if err != nil {
			t.Errorf("expected no error from Connect, got: %v", err)
		}

		if !mock.ConnectCalled {
			t.Error("expected ConnectCalled to be true after calling Connect")
		}
	})

	t.Run("ConnectWithPredefinedError", func(t *testing.T) {
		opts := NewMockOptions().SetQueueName("test-queue").Build()
		mock, err := NewMock(opts)
		if err != nil {
			t.Fatalf("failed to create MockQueue: %v", err)
		}

		// Set a predefined error
		expectedError := &MockError{Message: "connection failed"}
		mock.ConnectError = expectedError

		err = mock.Connect()

		if err != expectedError {
			t.Errorf("expected error %v, got %v", expectedError, err)
		}

		if !mock.ConnectCalled {
			t.Error("expected ConnectCalled to be true even when error is returned")
		}
	})

	t.Run("ConnectMultipleTimes", func(t *testing.T) {
		opts := NewMockOptions().SetQueueName("test-queue").Build()
		mock, err := NewMock(opts)
		if err != nil {
			t.Fatalf("failed to create MockQueue: %v", err)
		}

		// Call Connect multiple times
		for i := 0; i < 3; i++ {
			err = mock.Connect()
			if err != nil {
				t.Errorf("expected no error from Connect call %d, got: %v", i+1, err)
			}
		}

		if !mock.ConnectCalled {
			t.Error("expected ConnectCalled to be true after multiple Connect calls")
		}
	})
}

// MockError is a simple error type for testing
type MockError struct {
	Message string
}

func (e *MockError) Error() string {
	return e.Message
}
