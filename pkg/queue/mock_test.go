package queue

import (
	"os"
	"testing"
)

// TestMockQueueCreation tests the creation and initialization of MockQueue
func TestMockQueueCreation(t *testing.T) {
	t.Run("CreateMockQueue", func(t *testing.T) {
		mock, err := NewMockQueue()

		if err != nil {
			t.Fatalf("failed to create MockQueue instance: %v", err)
		}

		if mock == nil {
			t.Fatal("expected MockQueue instance but got nil")
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

// TestMockQueueConnect tests the Connect method of MockQueue
func TestMockQueueConnect(t *testing.T) {
	t.Run("ConnectSuccess", func(t *testing.T) {
		mock, err := NewMockQueue()
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
		mock, err := NewMockQueue()
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
		mock, err := NewMockQueue()
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

// TestLoadMessages tests the LoadMessages method of MockQueue
func TestLoadMessages(t *testing.T) {
	t.Run("LoadMessagesFromValidFile", func(t *testing.T) {
		// Create a temporary test file with valid JSON
		testFile := t.TempDir() + "/test_events.json"
		validJSON := `[
			{
				"id": "event-1",
				"stages": []
			},
			{
				"id": "event-2",
				"stages": []
			}
		]`

		if err := os.WriteFile(testFile, []byte(validJSON), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		mock, err := NewMockQueue()
		if err != nil {
			t.Fatalf("failed to create MockQueue: %v", err)
		}

		// Load messages from file
		err = mock.LoadMessages(testFile)
		if err != nil {
			t.Errorf("expected no error loading messages, got: %v", err)
		}

		// Verify messages were loaded
		if mock.GetQueueSize() != 2 {
			t.Errorf("expected 2 messages in queue, got %d", mock.GetQueueSize())
		}

		// Verify messages were also recorded as sent
		sentMessages := mock.GetSentMessages()
		if len(sentMessages) != 2 {
			t.Errorf("expected 2 sent messages, got %d", len(sentMessages))
		}
	})

	t.Run("LoadMessagesFromEmptyArray", func(t *testing.T) {
		// Create a test file with empty array
		testFile := t.TempDir() + "/empty_events.json"
		emptyJSON := `[]`

		if err := os.WriteFile(testFile, []byte(emptyJSON), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		mock, err := NewMockQueue()
		if err != nil {
			t.Fatalf("failed to create MockQueue: %v", err)
		}

		err = mock.LoadMessages(testFile)
		if err != nil {
			t.Errorf("expected no error loading empty array, got: %v", err)
		}

		if mock.GetQueueSize() != 0 {
			t.Errorf("expected 0 messages in queue, got %d", mock.GetQueueSize())
		}
	})

	t.Run("LoadMessagesFromNonExistentFile", func(t *testing.T) {
		mock, err := NewMockQueue()
		if err != nil {
			t.Fatalf("failed to create MockQueue: %v", err)
		}

		// Attempt to load from non-existent file
		err = mock.LoadMessages("/non/existent/file.json")
		if err == nil {
			t.Error("expected error loading from non-existent file, got nil")
		}

		// Queue should remain empty
		if mock.GetQueueSize() != 0 {
			t.Errorf("expected 0 messages in queue after failed load, got %d", mock.GetQueueSize())
		}
	})

	t.Run("LoadMessagesFromInvalidJSON", func(t *testing.T) {
		// Create a test file with invalid JSON
		testFile := t.TempDir() + "/invalid_events.json"
		invalidJSON := `{this is not valid json}`

		if err := os.WriteFile(testFile, []byte(invalidJSON), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		mock, err := NewMockQueue()
		if err != nil {
			t.Fatalf("failed to create MockQueue: %v", err)
		}

		err = mock.LoadMessages(testFile)
		if err == nil {
			t.Error("expected error loading invalid JSON, got nil")
		}

		// Queue should remain empty
		if mock.GetQueueSize() != 0 {
			t.Errorf("expected 0 messages in queue after failed load, got %d", mock.GetQueueSize())
		}
	})

	t.Run("LoadMessagesFromWrongJSONStructure", func(t *testing.T) {
		// Create a test file with wrong structure (object instead of array)
		testFile := t.TempDir() + "/wrong_structure.json"
		wrongJSON := `{"id": "event-1", "stages": []}`

		if err := os.WriteFile(testFile, []byte(wrongJSON), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		mock, err := NewMockQueue()
		if err != nil {
			t.Fatalf("failed to create MockQueue: %v", err)
		}

		err = mock.LoadMessages(testFile)
		if err == nil {
			t.Error("expected error loading wrong JSON structure, got nil")
		}
	})

	t.Run("LoadMessagesMultipleTimes", func(t *testing.T) {
		// Create test files
		testFile1 := t.TempDir() + "/events1.json"
		testFile2 := t.TempDir() + "/events2.json"

		json1 := `[{"id": "event-1", "stages": []}]`
		json2 := `[{"id": "event-2", "stages": []}, {"id": "event-3", "stages": []}]`

		if err := os.WriteFile(testFile1, []byte(json1), 0644); err != nil {
			t.Fatalf("failed to create test file 1: %v", err)
		}
		if err := os.WriteFile(testFile2, []byte(json2), 0644); err != nil {
			t.Fatalf("failed to create test file 2: %v", err)
		}

		mock, err := NewMockQueue()
		if err != nil {
			t.Fatalf("failed to create MockQueue: %v", err)
		}

		// Load first file
		err = mock.LoadMessages(testFile1)
		if err != nil {
			t.Errorf("expected no error loading first file, got: %v", err)
		}

		if mock.GetQueueSize() != 1 {
			t.Errorf("expected 1 message after first load, got %d", mock.GetQueueSize())
		}

		// Load second file
		err = mock.LoadMessages(testFile2)
		if err != nil {
			t.Errorf("expected no error loading second file, got: %v", err)
		}

		// Should accumulate messages
		if mock.GetQueueSize() != 3 {
			t.Errorf("expected 3 messages after second load, got %d", mock.GetQueueSize())
		}

		// Verify sent messages count
		sentMessages := mock.GetSentMessages()
		if len(sentMessages) != 3 {
			t.Errorf("expected 3 sent messages, got %d", len(sentMessages))
		}
	})

	t.Run("LoadMessagesWithComplexEvents", func(t *testing.T) {
		// Create a test file with complex event structure
		testFile := t.TempDir() + "/complex_events.json"
		complexJSON := `[
			{
				"id": "event-1",
				"stages": [
					{"name": "stage1", "status": "pending"},
					{"name": "stage2", "status": "pending"}
				],
				"metadata": {
					"key": "value"
				}
			}
		]`

		if err := os.WriteFile(testFile, []byte(complexJSON), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		mock, err := NewMockQueue()
		if err != nil {
			t.Fatalf("failed to create MockQueue: %v", err)
		}

		err = mock.LoadMessages(testFile)
		if err != nil {
			t.Errorf("expected no error loading complex events, got: %v", err)
		}

		if mock.GetQueueSize() != 1 {
			t.Errorf("expected 1 message in queue, got %d", mock.GetQueueSize())
		}
	})

	t.Run("LoadMessagesWithEmptyFile", func(t *testing.T) {
		// Create an empty file
		testFile := t.TempDir() + "/empty.json"

		if err := os.WriteFile(testFile, []byte(""), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}

		mock, err := NewMockQueue()
		if err != nil {
			t.Fatalf("failed to create MockQueue: %v", err)
		}

		err = mock.LoadMessages(testFile)
		if err == nil {
			t.Error("expected error loading empty file, got nil")
		}
	})
}
