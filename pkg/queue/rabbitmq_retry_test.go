package queue

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/uug-ai/models/pkg/models"
)

func TestRetryDelaysValidation(t *testing.T) {
	for _, test := range []struct {
		delays []time.Duration
		valid  bool
	}{
		{nil, true},
		{[]time.Duration{5 * time.Second, time.Minute}, true},
		{[]time.Duration{500 * time.Millisecond}, false},
		{[]time.Duration{1500 * time.Millisecond}, false},
		{[]time.Duration{0}, false},
	} {
		opts := NewRabbitOptions().
			SetConsumerQueue("notification").
			SetDeadletterQueue("notification-dlq").
			SetHost("localhost:5672").
			SetUsername("user").
			SetPassword("password").
			SetRetryDelays(test.delays...).
			Build()
		if err := opts.Validate(); (err == nil) != test.valid {
			t.Errorf("Validate() with delays %v = %v, want valid %t", test.delays, err, test.valid)
		}
	}
}

type recordedPublish struct {
	queue   string
	headers amqp.Table
}

// With RetryDelays a retry is handed to its delay queue straight away: the
// consumer no longer waits out the backoff itself.
func TestRetryOrDeadletterUsesDelayQueues(t *testing.T) {
	r := &RabbitMQ{options: &RabbitOptions{
		ConsumerQueue:   "notification",
		DeadletterQueue: "notification-dlq",
		MaxRetries:      4,
		RetryDelays:     []time.Duration{5 * time.Second, 30 * time.Second},
	}}
	var published []recordedPublish
	publish := func(queue string, _ []byte, headers amqp.Table) error {
		published = append(published, recordedPublish{queue, headers})
		return nil
	}

	start := time.Now()
	for attempts := 0; attempts <= 4; attempts++ {
		var headers amqp.Table
		if attempts > 0 {
			headers = amqp.Table{retryCountHeader: int32(attempts)}
		}
		if err := r.retryOrDeadletter(headers, []byte(`{}`), 5*time.Second, publish); err != nil {
			t.Fatalf("retry %d: %v", attempts, err)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("retries took %s, want no in-process wait", elapsed)
	}

	want := []string{"notification.retry.5s", "notification.retry.30s", "notification.retry.30s", "notification.retry.30s", "notification-dlq"}
	if len(published) != len(want) {
		t.Fatalf("published %d messages, want %d", len(published), len(want))
	}
	for i, queue := range want {
		if published[i].queue != queue {
			t.Errorf("publish %d went to %q, want %q", i, published[i].queue, queue)
		}
		if i < len(want)-1 && retryCount(published[i].headers) != i+1 {
			t.Errorf("publish %d carries retry count %d, want %d", i, retryCount(published[i].headers), i+1)
		}
	}
}

func TestRetryQueueTopology(t *testing.T) {
	r := &RabbitMQ{options: &RabbitOptions{
		ConsumerQueue: "notification",
		RetryDelays:   []time.Duration{5 * time.Second, 30 * time.Second, 30 * time.Second},
	}}
	if got, want := fmt.Sprint(r.retryQueueNames()), "[notification.retry.5s notification.retry.30s]"; got != want {
		t.Fatalf("retry queues = %s, want %s", got, want)
	}
	args := retryQueueArguments("notification", 30*time.Second)
	if args["x-message-ttl"] != int64(30000) || args["x-dead-letter-exchange"] != "" ||
		args["x-dead-letter-routing-key"] != "notification" || args["x-dead-letter-strategy"] != "at-least-once" ||
		args["x-overflow"] != "reject-publish" || args["x-queue-type"] != "quorum" {
		t.Fatalf("retry queue arguments = %v", args)
	}
}

// Against a real broker: a retried message leaves the consumer at once, waits
// in its delay queue and comes back with its retry count, while a message
// published after it is processed during the wait.
func TestRabbitMQDelayedRetryIntegration(t *testing.T) {
	host := os.Getenv("RABBITMQ_HOST")
	username := os.Getenv("RABBITMQ_USERNAME")
	password := os.Getenv("RABBITMQ_PASSWORD")
	if host == "" || username == "" || password == "" {
		t.Skip("Skipping integration tests: RABBITMQ_HOST, RABBITMQ_USERNAME, and RABBITMQ_PASSWORD must be set")
	}

	testQueue := fmt.Sprintf("test-delayed-retry-%d", time.Now().UnixNano())
	rabbit, err := NewRabbitMQ(NewRabbitOptions().
		SetConsumerQueue(testQueue).
		SetDeadletterQueue(testQueue + "-dlq").
		SetHost(host).
		SetUsername(username).
		SetPassword(password).
		SetPrefetchCount(1).
		SetRetryDelays(2 * time.Second).
		Build())
	if err != nil {
		t.Fatalf("create RabbitMQ client: %v", err)
	}
	if err := rabbit.Connect(); err != nil {
		t.Fatalf("connect to RabbitMQ: %v", err)
	}
	t.Cleanup(func() {
		if channel, err := rabbit.Connection.Channel(); err == nil {
			for _, name := range []string{testQueue, testQueue + "-dlq", testQueue + ".retry.2s"} {
				_, _ = channel.QueueDelete(name, false, false, false)
			}
			_ = channel.Close()
		}
		rabbit.Close()
	})

	type seen struct {
		id      string
		attempt int
		at      time.Time
	}
	var mu sync.Mutex
	var deliveries []seen
	done := make(chan struct{})
	handler := func(event models.PipelineEvent, _ ...any) (models.PipelineAction, models.PipelineEvent, int) {
		mu.Lock()
		defer mu.Unlock()
		retried := 0
		for _, d := range deliveries {
			if d.id == event.Operation {
				retried++
			}
		}
		deliveries = append(deliveries, seen{event.Operation, retried, time.Now()})
		if event.Operation == "failing" && retried == 0 {
			return models.PipelineRetry, event, 0
		}
		if event.Operation == "failing" {
			close(done)
		}
		return models.PipelineCancel, event, 0
	}
	go func() { _ = rabbit.ReadMessages(handler, func(models.PipelineMetrics) {}) }()

	start := time.Now()
	for _, operation := range []string{"failing", "healthy"} {
		payload, _ := json.Marshal(models.PipelineEvent{Operation: operation})
		if err := rabbit.Publish(testQueue, payload); err != nil {
			t.Fatalf("publish %s: %v", operation, err)
		}
	}

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for the retried message to come back")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(deliveries) != 3 || deliveries[1].id != "healthy" || deliveries[2].id != "failing" {
		t.Fatalf("deliveries = %+v, want failing, healthy, failing", deliveries)
	}
	if wait := deliveries[1].at.Sub(start); wait > time.Second {
		t.Fatalf("healthy message waited %s behind the retry, want it processed during the delay", wait)
	}
	if wait := deliveries[2].at.Sub(deliveries[0].at); wait < 2*time.Second {
		t.Fatalf("retry came back after %s, want at least the 2s delay", wait)
	}
}
