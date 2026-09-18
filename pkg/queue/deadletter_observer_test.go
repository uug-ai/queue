package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitDeadLetterObserverOutcomes(t *testing.T) {
	failed := errors.New("publish failed")
	closed := errors.New("channel/connection is not open")
	for _, test := range []struct {
		name         string
		confirmed    bool
		firstErr     error
		finalErr     error
		connectErr   error
		reconnectErr error
		wantAttempts int
		wantErr      error
	}{
		{name: "confirmed", confirmed: true, wantAttempts: 1},
		{name: "unconfirmed", wantAttempts: 1},
		{name: "failure", confirmed: true, firstErr: failed, wantAttempts: 1, wantErr: failed},
		{name: "connection failure", confirmed: true, connectErr: failed, wantErr: failed},
		{name: "reconnect success", confirmed: true, firstErr: closed, wantAttempts: 2},
		{name: "legacy reconnect success", firstErr: closed, wantAttempts: 2},
		{name: "reconnect publish failure", confirmed: true, firstErr: closed, finalErr: failed, wantAttempts: 2, wantErr: failed},
		{name: "reconnect failure preserves original error", confirmed: true, firstErr: closed, reconnectErr: failed, wantAttempts: 1, wantErr: closed},
	} {
		t.Run(test.name, func(t *testing.T) {
			rabbit := newTestRabbit(t, 2)
			envelope, err := rabbit.deadLetterEnvelope([]byte(`{"traceId":"trace","payload":{"key":"clip.mp4"}}`), DeadLetterReasonHandlerError, 2)
			if err != nil {
				t.Fatal(err)
			}
			calls, attempts, reconnects := 0, 0, 0
			rabbit.options.DeadLetterObserver = func(event DeadLetterPublishEvent) {
				calls++
				if !rabbit.publishMu.TryLock() {
					t.Fatal("observer called under publishing lock")
				}
				rabbit.publishMu.Unlock()
				if !rabbit.mu.TryLock() {
					t.Fatal("observer called under state lock")
				}
				rabbit.mu.Unlock()
				if attempts != test.wantAttempts {
					t.Fatalf("observer before final attempt: %d", attempts)
				}
				want := DeadLetterPublishEvent{
					Source: "c", Destination: "c-dlq", Reason: DeadLetterReasonHandlerError,
					Attempts: 2, TraceID: "trace", MediaFileName: "clip.mp4",
					Confirmed: test.confirmed && test.wantErr == nil, Err: test.wantErr,
				}
				if event != want {
					t.Fatalf("event = %+v, want %+v", event, want)
				}
			}
			err = rabbit.publishAndObserveDeadLetter("c-dlq", envelope, test.confirmed, func() error {
				return rabbitPublishWithReconnect(func() error {
					return test.connectErr
				}, func() error {
					rabbit.publishMu.Lock()
					defer rabbit.publishMu.Unlock()
					if calls != 0 {
						t.Fatal("observer called before publish finished")
					}
					attempts++
					if attempts == 1 {
						return test.firstErr
					}
					return test.finalErr
				}, func() bool { return false }, func() error {
					reconnects++
					return test.reconnectErr
				})
			})
			if !errors.Is(err, test.wantErr) || calls != 1 || attempts != test.wantAttempts {
				t.Fatalf("err=%v calls=%d attempts=%d", err, calls, attempts)
			}
			if (reconnects == 1) != (test.firstErr == closed) {
				t.Fatalf("reconnects=%d", reconnects)
			}
		})
	}
}

func TestRabbitDeadLetterObserverCorrelation(t *testing.T) {
	for _, test := range []struct {
		name, body, trace, file string
	}{
		{"pipeline key wins", `{"traceId":"trace","fileName":"other","payload":{"key":"clip.mp4","password":"SECRET"},"token":"SECRET"}`, "trace", "clip.mp4"},
		{"fileName fallback", `{"traceId":"trace","fileName":"clip.mp4"}`, "trace", "clip.mp4"},
		{"unrelated fields", `{"traceId":"trace","unknown":{"password":"SECRET"},"payload":{"key":"clip.mp4","nested":"SECRET"}}`, "trace", "clip.mp4"},
		{"raw malformed", `{"traceId":"trace","payload":{"key":"clip.mp4"}`, "", ""},
		{"invalid correlation type", `{"traceId":"trace","fileName":7}`, "", ""},
		{"raw binary", "\xff\x00", "", ""},
		{"null body", `null`, "", ""},
		{"null payload", `{"payload":null}`, "", ""},
		{"unrelated JSON", `{"password":"SECRET"}`, "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			rabbit := newTestRabbit(t, 2)
			// Historical envelopes can record a replay queue as Destination.
			envelope, err := encodeDeadLetter([]byte(test.body), DeadLetterMetadata{
				Source: "original-source", Destination: "replay-queue",
				Reason: DeadLetterReasonMalformed, Attempts: 7,
			})
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			rabbit.options.DeadLetterObserver = func(event DeadLetterPublishEvent) {
				calls++
				if event.Source != "original-source" || event.Destination != "c-dlq" ||
					event.Reason != DeadLetterReasonMalformed || event.Attempts != 7 ||
					event.TraceID != test.trace || event.MediaFileName != test.file {
					t.Fatalf("event = %+v", event)
				}
				if strings.Contains(fmt.Sprintf("%+v", event), "SECRET") {
					t.Fatal("observer leaked unrelated payload fields")
				}
			}
			if err := rabbit.publishAndObserveDeadLetter("c-dlq", envelope, true, func() error { return nil }); err != nil || calls != 1 {
				t.Fatalf("err=%v calls=%d", err, calls)
			}
		})
	}
}

func TestRabbitDeadLetterObserverEntryPoints(t *testing.T) {
	for _, method := range []string{"AddToDeadletter", "Publish", "PublishDeadLetter"} {
		t.Run(method, func(t *testing.T) {
			rabbit := newTestRabbit(t, 2)
			published, calls := false, 0
			rabbit.options = NewRabbitOptions().
				SetConsumerQueue("c").
				SetDeadletterQueue("c-dlq").
				SetDeadLetterObserver(func(event DeadLetterPublishEvent) {
					calls++
					if !published || !event.Confirmed || event.Err != nil || event.TraceID != "trace" {
						t.Fatalf("event = %+v, published=%v", event, published)
					}
				}).Build()
			rabbit.deadLetterReplayPublish = func(context.Context, string, []byte) error {
				published = true
				return nil
			}
			payload := []byte(`{"traceId":"trace"}`)
			var err error
			switch method {
			case "AddToDeadletter":
				err = rabbit.AddToDeadletter(payload)
			case "Publish":
				err = rabbit.Publish("c-dlq", payload)
			case "PublishDeadLetter":
				err = rabbit.PublishDeadLetter(context.Background(), payload, DeadLetterMetadata{Source: "c", Destination: "c-dlq"})
			}
			if err != nil || calls != 1 {
				t.Fatalf("err=%v calls=%d", err, calls)
			}
		})
	}
}

func TestRabbitDeadLetterObserverRuntimeFailures(t *testing.T) {
	for _, method := range []string{"AddToDeadletter", "Publish", "PublishConfirmed", "PublishWithDelay", "forced transfer", "PublishDeadLetter"} {
		t.Run(method, func(t *testing.T) {
			rabbit := newTestRabbit(t, 2)
			rabbit.options.ConfirmedDelivery = true
			// Invalid URI fails before dialing, exercising the real public paths.
			rabbit.connectionString = "://"
			events := make(chan DeadLetterPublishEvent, 2)
			rabbit.options.DeadLetterObserver = func(event DeadLetterPublishEvent) { events <- event }
			payload := []byte("malformed")
			var err error
			switch method {
			case "AddToDeadletter":
				err = rabbit.AddToDeadletter(payload)
			case "Publish":
				err = rabbit.Publish("c-dlq", payload)
			case "PublishConfirmed":
				err = rabbit.PublishConfirmed("c-dlq", payload)
			case "PublishWithDelay":
				rabbit.PublishWithDelay("c-dlq", payload, 0)
			case "forced transfer":
				err = rabbit.addToDeadletterConfirmed(payload, DeadLetterReasonMalformed)
			case "PublishDeadLetter":
				err = rabbit.PublishDeadLetter(context.Background(), payload, DeadLetterMetadata{Source: "c", Destination: "c-dlq"})
			}
			select {
			case event := <-events:
				if event.Err == nil || event.Confirmed || event.TraceID != "" || event.MediaFileName != "" {
					t.Fatalf("event = %+v", event)
				}
				if method != "PublishWithDelay" && !errors.Is(err, event.Err) {
					t.Fatalf("returned error=%v, event error=%v", err, event.Err)
				}
			case <-time.After(time.Second):
				t.Fatal("missing publish outcome")
			}
			if len(events) != 0 {
				t.Fatal("duplicate publish outcome")
			}
		})
	}
}

func TestRabbitDeadLetterObserverRetryExhaustion(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		rabbit := newTestRabbit(t, 2)
		calls := 0
		rabbit.options.DeadLetterObserver = func(event DeadLetterPublishEvent) {
			calls++
			if event.Reason != DeadLetterReasonRetryExhausted || event.Attempts != 2 || event.Confirmed != confirmed {
				t.Fatalf("event = %+v", event)
			}
		}
		publish := func(destination string, body []byte, _ amqp.Table) error {
			return rabbit.publishAndObserveDeadLetter(destination, body, confirmed, func() error { return nil })
		}
		if err := rabbit.retryOrDeadletter(nil, []byte("raw"), time.Nanosecond, publish); err != nil || calls != 0 {
			t.Fatalf("ordinary retry err=%v calls=%d", err, calls)
		}
		if err := rabbit.retryOrDeadletter(amqp.Table{retryCountHeader: int32(2)}, []byte("raw"), time.Nanosecond, publish); err != nil || calls != 1 {
			t.Fatalf("exhausted retry err=%v calls=%d", err, calls)
		}
	}
}

func TestRabbitDeadLetterObserverSettlement(t *testing.T) {
	for _, reason := range []DeadLetterReason{DeadLetterReasonMalformed, DeadLetterReasonHandlerError, DeadLetterReasonPublishFailed} {
		t.Run(string(reason), func(t *testing.T) {
			rabbit := newTestRabbit(t, 2)
			ack := &recordingAcknowledger{}
			calls := 0
			rabbit.options.DeadLetterObserver = func(event DeadLetterPublishEvent) {
				calls++
				if event.Reason != reason || ack.acks != 0 {
					t.Fatalf("event=%+v acks=%d", event, ack.acks)
				}
			}
			rabbit.deadLetterReplayPublish = func(context.Context, string, []byte) error { return nil }
			if err := rabbit.deadletterAndSettle(amqp.Delivery{Acknowledger: ack}, []byte("raw"), reason, false); err != nil || calls != 1 || ack.acks != 1 {
				t.Fatalf("err=%v calls=%d acks=%d", err, calls, ack.acks)
			}
		})
	}
}

func TestRabbitDeadLetterObserverFailureBeforeRecovery(t *testing.T) {
	rabbit := newTestRabbit(t, 2)
	rabbit.options.ConfirmedDelivery = true
	rabbit.connectionString = "://"
	ack := &recordingAcknowledger{}
	calls, recovered := 0, false
	rabbit.options.DeadLetterObserver = func(event DeadLetterPublishEvent) {
		calls++
		if event.Err == nil || event.Confirmed || recovered || ack.acks != 0 {
			t.Fatalf("event=%+v recovered=%v acks=%d", event, recovered, ack.acks)
		}
	}
	rabbit.SetDisasterRecoveryHandler(func([]byte) error {
		if calls != 1 {
			t.Fatal("missing publish failure before recovery")
		}
		recovered = true
		return nil
	})
	err := rabbit.deadletterAndSettle(amqp.Delivery{Acknowledger: ack}, []byte("raw"), DeadLetterReasonHandlerError, true)
	if err != nil || !recovered || calls != 1 || ack.acks != 1 || ack.nacks != 0 {
		t.Fatalf("err=%v recovered=%v calls=%d ack=%+v", err, recovered, calls, ack)
	}
}

func TestRabbitDeadLetterObserverIgnoresAdministrativeRestoration(t *testing.T) {
	for _, operation := range []string{"inspect", "scan", "skip", "replay", "discard", "transform failure", "restore failure"} {
		t.Run(operation, func(t *testing.T) {
			rabbit := newTestRabbit(t, 2)
			rabbit.options.DeadLetterObserver = func(DeadLetterPublishEvent) { t.Fatal("administration emitted a new entry") }
			body, err := rabbit.deadLetterEnvelope([]byte("raw"), DeadLetterReasonHandlerError, 0)
			if err != nil {
				t.Fatal(err)
			}
			ack := &fakeRabbitAcknowledger{}
			read := false
			rabbit.deadLetterGet = func() (amqp.Delivery, bool, error) {
				if read {
					return amqp.Delivery{}, false, nil
				}
				read = true
				return amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, Body: body}, true, nil
			}
			publishes := 0
			rabbit.deadLetterReplayPublish = func(context.Context, string, []byte) error {
				publishes++
				if operation == "restore failure" {
					return errors.New("restore failed")
				}
				return nil
			}
			if operation == "inspect" || operation == "restore failure" {
				_, err = rabbit.InspectDeadLetters(context.Background(), DeadLetterInspectRequest{Limit: 1})
			} else {
				request := DeadLetterReplayRequest{Limit: 1, Execute: operation != "scan"}
				switch operation {
				case "skip", "discard", "transform failure":
					request.Transform = func(context.Context, []DeadLetterMessage) ([]DeadLetterReplayTransformation, error) {
						if operation == "transform failure" {
							return nil, errors.New("transform failed")
						}
						return []DeadLetterReplayTransformation{{Skip: operation == "skip", Discard: operation == "discard"}}, nil
					}
				}
				_, err = rabbit.ReplayDeadLetters(context.Background(), request)
			}
			wantError := operation == "transform failure" || operation == "restore failure"
			if (err != nil) != wantError {
				t.Fatalf("err=%v", err)
			}
			wantPublishes := 1
			if operation == "discard" {
				wantPublishes = 0
			}
			if publishes != wantPublishes {
				t.Fatalf("publishes=%d, want %d", publishes, wantPublishes)
			}
		})
	}
}

func TestRabbitDeadLetterObserverDisabled(t *testing.T) {
	rabbit := newTestRabbit(t, 2)
	if rabbit.options.DeadLetterObserver != nil {
		t.Fatal("observer enabled by default")
	}
	failed := errors.New("publish failed")
	if err := rabbit.publishAndObserveDeadLetter("c-dlq", []byte("invalid envelope"), true, func() error { return failed }); err != failed {
		t.Fatalf("err=%v", err)
	}
}
