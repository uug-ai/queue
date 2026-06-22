package queue

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// newTestRabbit builds a RabbitMQ with the minimum valid options so the
// retry-cap helpers can be exercised without a broker.
func newTestRabbit(t *testing.T, maxRetries int) *RabbitMQ {
	t.Helper()
	opts := NewRabbitOptions().
		SetConsumerQueue("c").
		SetDeadletterQueue("c-dlq").
		SetHost("localhost").
		SetUsername("u").
		SetPassword("p").
		SetMaxRetries(maxRetries).
		Build()
	r, err := NewRabbitMQ(opts)
	if err != nil {
		t.Fatalf("NewRabbitMQ: %v", err)
	}
	return r
}

func TestSetMaxRetriesBuilder(t *testing.T) {
	if opts := NewRabbitOptions().SetMaxRetries(7).Build(); opts.MaxRetries != 7 {
		t.Errorf("MaxRetries = %d, want 7", opts.MaxRetries)
	}
	// Unset leaves the zero value so the client falls back to defaultMaxRetries.
	if def := NewRabbitOptions().Build(); def.MaxRetries != 0 {
		t.Errorf("default MaxRetries = %d, want 0 (selects defaultMaxRetries at runtime)", def.MaxRetries)
	}
}

func TestMaxRetriesDefault(t *testing.T) {
	if got := newTestRabbit(t, 0).maxRetries(); got != defaultMaxRetries {
		t.Errorf("maxRetries() with unset option = %d, want default %d", got, defaultMaxRetries)
	}
	if got := newTestRabbit(t, 3).maxRetries(); got != 3 {
		t.Errorf("maxRetries() = %d, want 3", got)
	}
}

// TestRetryCount covers the header decode that drives the retry cap: a missing
// header means a first attempt (0), and the several integer types AMQP may
// decode the counter as are all accepted.
func TestRetryCount(t *testing.T) {
	cases := []struct {
		name    string
		headers amqp.Table
		want    int
	}{
		{"nil headers", nil, 0},
		{"missing key", amqp.Table{"other": "x"}, 0},
		{"int32", amqp.Table{retryCountHeader: int32(4)}, 4},
		{"int64", amqp.Table{retryCountHeader: int64(5)}, 5},
		{"int", amqp.Table{retryCountHeader: 6}, 6},
		{"float64", amqp.Table{retryCountHeader: float64(7)}, 7},
		{"wrong type", amqp.Table{retryCountHeader: "nope"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryCount(tc.headers); got != tc.want {
				t.Errorf("retryCount(%v) = %d, want %d", tc.headers, got, tc.want)
			}
		})
	}
}
