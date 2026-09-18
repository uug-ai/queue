package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"github.com/uug-ai/models/pkg/api"
	"github.com/uug-ai/models/pkg/models"
)

func captureDeadLetterLog(rabbit *RabbitMQ) *bytes.Buffer {
	buffer := &bytes.Buffer{}
	logger := logrus.New()
	logger.SetOutput(buffer)
	logger.SetFormatter(&logrus.JSONFormatter{})
	rabbit.options.Logger = logger
	return buffer
}

func assertDeadLetterRequestLog(t *testing.T, buffer *bytes.Buffer, reason DeadLetterReason, attempts int, trace, media string) {
	t.Helper()
	var entry struct {
		api.WarningResponse
		Level string `json:"level"`
	}
	decoder := json.NewDecoder(bytes.NewReader(buffer.Bytes()))
	if err := decoder.Decode(&entry); err != nil {
		t.Fatalf("missing request log: %v; output=%s", err, buffer)
	}
	if entry.Level != "warning" || entry.ApplicationStatusCode != api.PipelineWarning || entry.EntityStatusCode != "dead_letter_requested" {
		t.Fatalf("wrong request envelope: %+v", entry)
	}
	if entry.Metadata.TraceId != trace || entry.Metadata.MediaFileName != media || entry.Metadata.Error != "" {
		t.Fatalf("correlation/error metadata: %+v", entry.Metadata)
	}
	data := entry.Metadata.Data
	if len(data) != 4 || data["sourceQueue"] != "c" || data["deadLetterQueue"] != "c-dlq" ||
		data["reason"] != string(reason) || data["attempts"] != float64(attempts) {
		t.Fatalf("request fields: %+v", data)
	}
	if err := decoder.Decode(&entry); err != io.EOF {
		t.Fatalf("expected exactly one request log: %v; output=%s", err, buffer)
	}
	for _, forbidden := range []string{"SECRET", "signedUrl", `"payload"`, `"confirmed"`, "dead_letter_succeeded"} {
		if strings.Contains(buffer.String(), forbidden) {
			t.Fatalf("unsafe or success-only field %q in log: %s", forbidden, buffer)
		}
	}
}

func TestRabbitDeadLetterLoggerBuilderAndDisabled(t *testing.T) {
	logger := logrus.New()
	if options := NewRabbitOptions().SetLogger(logger).Build(); options.Logger != logger {
		t.Fatal("builder did not preserve injected logger")
	}
	rabbit := newTestRabbit(t, 1)
	if rabbit.options.Logger != nil || rabbit.options.DeadLetterObserver != nil {
		t.Fatal("logging/observation must be opt-in")
	}
	event := &models.PipelineEvent{TraceId: "trace"}
	if allocations := testing.AllocsPerRun(100, func() {
		rabbit.logDeadLetterRequest(DeadLetterReasonHandlerError, 0, event)
	}); allocations != 0 {
		t.Fatalf("disabled logger allocated: %v", allocations)
	}
}

type panicLogJSON struct{}

func (panicLogJSON) MarshalJSON() ([]byte, error) {
	panic("logging must not serialize the event or payload")
}

func TestRabbitDeadLetterLoggerCatalogAndSafeProjection(t *testing.T) {
	for _, reason := range []DeadLetterReason{
		DeadLetterReasonUnspecified, DeadLetterReasonMalformed, DeadLetterReasonHandlerError,
		DeadLetterReasonRetryExhausted, DeadLetterReasonPublishFailed,
	} {
		t.Run(string(reason), func(t *testing.T) {
			rabbit := newTestRabbit(t, 2)
			buffer := captureDeadLetterLog(rabbit)
			event := &models.PipelineEvent{TraceId: "trace", FileName: "fallback.mp4", Data: map[string]any{"SECRET": panicLogJSON{}}}
			event.Payload.FileName = "clip.mp4"
			rabbit.logDeadLetterRequest(reason, 2, event)
			assertDeadLetterRequestLog(t, buffer, reason, 2, "trace", "clip.mp4")
			buffer.Reset()
			event.Payload.FileName = ""
			rabbit.logDeadLetterRequest(reason, 0, event)
			assertDeadLetterRequestLog(t, buffer, reason, 0, "trace", "fallback.mp4")
		})
	}
}

func TestRabbitDeadLetterLoggerPipelineDecisions(t *testing.T) {
	for _, test := range []struct {
		name       string
		action     models.PipelineAction
		malformed  bool
		forwardErr bool
		wantReason DeadLetterReason
	}{
		{"malformed", models.PipelineError, true, false, DeadLetterReasonMalformed},
		{"handler error", models.PipelineError, false, false, DeadLetterReasonHandlerError},
		{"exhausted retry", models.PipelineRetry, false, false, DeadLetterReasonRetryExhausted},
		{"forward failure", models.PipelineForward, false, true, DeadLetterReasonPublishFailed},
		{"forward success", models.PipelineForward, false, false, ""},
		{"cancel", models.PipelineCancel, false, false, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			rabbit := newTestRabbit(t, 2)
			rabbit.options.RouterQueue = "router"
			buffer := captureDeadLetterLog(rabbit)
			payload := []byte(" {\"events\":[\"sequence\",\"next\"],\"traceId\":\"trace\",\"payload\":{\"key\":\"clip.mp4\",\"signedUrl\":\"SECRET\"},\"data\":{\"credential\":\"SECRET\"}} \n")
			trace, media := "trace", "clip.mp4"
			if test.malformed {
				payload = []byte(`{"traceId":"SECRET" malformed`)
				trace, media = "", ""
			}
			ack := &recordingAcknowledger{}
			deliveries := make(chan amqp.Delivery, 1)
			deliveries <- amqp.Delivery{Acknowledger: ack, Body: payload, Headers: amqp.Table{retryCountHeader: int32(2)}}
			close(deliveries)
			dlqPublishes, forwards, handlerCalls := 0, 0, 0
			checkPublish := func(destination string, body []byte) error {
				dlqPublishes++
				attempts := 0
				if test.action == models.PipelineRetry {
					attempts = 2
				}
				assertDeadLetterRequestLog(t, buffer, test.wantReason, attempts, trace, media)
				if ack.acks != 0 || destination != "c-dlq" {
					t.Fatalf("early acknowledgement or wrong destination: %d %s", ack.acks, destination)
				}
				message, err := decodeDeadLetter("", body)
				if err != nil || message.DeadLetter.Attributes != nil {
					t.Fatalf("unexpected envelope metadata: %+v err=%v", message, err)
				}
				if test.action == models.PipelineRetry && !bytes.Equal(message.Payload, payload) {
					t.Fatal("retry exhaustion changed original body")
				}
				return nil
			}
			rabbit.deadLetterReplayPublish = func(_ context.Context, destination string, body []byte) error {
				return checkPublish(destination, body)
			}
			publish := func(destination string, body []byte, headers amqp.Table) error {
				if headers != nil {
					t.Fatal("unexpected diagnostic or retry headers on terminal publish")
				}
				if destination == "c-dlq" {
					return checkPublish(destination, body)
				}
				forwards++
				if destination != "router" || buffer.Len() != 0 || ack.acks != 0 {
					t.Fatal("unexpected forward logging or settlement")
				}
				if test.forwardErr {
					return errors.New("SECRET connection URI")
				}
				return nil
			}
			err := rabbit.readPipelineDeliveries(deliveries, false,
				func(event models.PipelineEvent, args ...any) (models.PipelineAction, models.PipelineEvent, int) {
					handlerCalls++
					if len(args) != 1 || args[0] != "argument" {
						t.Fatalf("handler args=%+v", args)
					}
					return test.action, event, 0
				}, func(models.PipelineMetrics) {}, publish, "argument")
			if err != nil || ack.acks != 1 || ack.nacks != 0 {
				t.Fatalf("err=%v ack=%+v", err, ack)
			}
			if (dlqPublishes == 1) != (test.wantReason != "") ||
				(forwards == 1) != (test.action == models.PipelineForward) || (handlerCalls == 0) != test.malformed {
				t.Fatalf("dlq=%d forwards=%d handlers=%d", dlqPublishes, forwards, handlerCalls)
			}
			if test.wantReason == "" && buffer.Len() != 0 {
				t.Fatalf("successful action logged: %s", buffer)
			}
		})
	}
}

func TestRabbitDeadLetterLoggerRetryOnlyAtExhaustion(t *testing.T) {
	rabbit := newTestRabbit(t, 2)
	buffer := captureDeadLetterLog(rabbit)
	payload := []byte(" {\"traceId\":\"SECRET\",\"unknown\":\"preserve original\"}\n")
	event := &models.PipelineEvent{TraceId: "trace", FileName: "clip.mp4"}
	if err := rabbit.retryOrDeadletter(nil, payload, time.Nanosecond,
		func(destination string, body []byte, headers amqp.Table) error {
			if buffer.Len() != 0 || destination != "c" || !bytes.Equal(body, payload) || len(headers) != 1 || retryCount(headers) != 1 {
				t.Fatalf("nonterminal retry changed or logged: destination=%s headers=%+v logs=%s", destination, headers, buffer)
			}
			return nil
		}, event); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("SECRET broker error")
	err := rabbit.retryOrDeadletter(amqp.Table{retryCountHeader: int32(2)}, payload, time.Nanosecond,
		func(string, []byte, amqp.Table) error {
			assertDeadLetterRequestLog(t, buffer, DeadLetterReasonRetryExhausted, 2, "trace", "clip.mp4")
			return wantErr
		}, event)
	if !errors.Is(err, wantErr) {
		t.Fatalf("publish error changed: %v", err)
	}
	assertDeadLetterRequestLog(t, buffer, DeadLetterReasonRetryExhausted, 2, "trace", "clip.mp4")
}

func TestRabbitDeadLetterLoggerDirectEntryPoints(t *testing.T) {
	for _, method := range []string{"AddToDeadletter", "Publish", "PublishDeadLetter", "raw transfer"} {
		t.Run(method, func(t *testing.T) {
			rabbit := newTestRabbit(t, 2)
			buffer := captureDeadLetterLog(rabbit)
			payload := []byte(`{"traceId":"SECRET","payload":{"key":"SECRET","signedUrl":"SECRET"}}`)
			calls := 0
			ack := &recordingAcknowledger{}
			rabbit.deadLetterReplayPublish = func(context.Context, string, []byte) error {
				calls++
				assertDeadLetterRequestLog(t, buffer, DeadLetterReasonUnspecified, 0, "", "")
				if ack.acks != 0 {
					t.Fatal("acknowledged before request/publish")
				}
				return nil
			}
			var err error
			switch method {
			case "AddToDeadletter":
				err = rabbit.AddToDeadletter(payload)
			case "Publish":
				err = rabbit.Publish("c-dlq", payload)
			case "PublishDeadLetter":
				err = rabbit.PublishDeadLetter(context.Background(), payload, DeadLetterMetadata{Source: "c", Destination: "c-dlq"})
			case "raw transfer":
				err = rabbit.deadletterAndSettle(amqp.Delivery{Acknowledger: ack}, payload, DeadLetterReasonUnspecified, false)
			}
			if err != nil || calls != 1 {
				t.Fatalf("err=%v publishes=%d", err, calls)
			}
			assertDeadLetterRequestLog(t, buffer, DeadLetterReasonUnspecified, 0, "", "")
		})
	}
}

func TestRabbitDeadLetterLoggerConfirmedFailureSettlement(t *testing.T) {
	for _, recover := range []bool{false, true} {
		t.Run(fmt.Sprintf("recover=%v", recover), func(t *testing.T) {
			rabbit := newTestRabbit(t, 2)
			rabbit.options.ConfirmedDelivery = true
			rabbit.connectionString = "://"
			buffer := captureDeadLetterLog(rabbit)
			ack := &recordingAcknowledger{}
			payload := []byte("SECRET original payload")
			if recover {
				rabbit.SetDisasterRecoveryHandler(func(body []byte) error {
					assertDeadLetterRequestLog(t, buffer, DeadLetterReasonHandlerError, 0, "trace", "clip.mp4")
					if ack.acks != 0 || !bytes.Equal(body, payload) {
						t.Fatal("recovery changed settlement or payload")
					}
					return nil
				})
			}
			err := rabbit.deadletterAndSettle(amqp.Delivery{Acknowledger: ack}, payload, DeadLetterReasonHandlerError, true,
				&models.PipelineEvent{TraceId: "trace", FileName: "clip.mp4"})
			if recover {
				if err != nil || ack.acks != 1 || ack.nacks != 0 {
					t.Fatalf("err=%v ack=%+v", err, ack)
				}
			} else if err == nil || ack.acks != 0 || ack.nacks != 1 || !ack.requeue {
				t.Fatalf("err=%v ack=%+v", err, ack)
			}
			assertDeadLetterRequestLog(t, buffer, DeadLetterReasonHandlerError, 0, "trace", "clip.mp4")
		})
	}
}

func TestRabbitDeadLetterLoggerIgnoresAdministration(t *testing.T) {
	for _, operation := range []string{"inspect", "scan", "replay", "discard"} {
		t.Run(operation, func(t *testing.T) {
			rabbit := newTestRabbit(t, 2)
			buffer := captureDeadLetterLog(rabbit)
			body, err := rabbit.deadLetterEnvelope([]byte("SECRET"), DeadLetterReasonHandlerError, 0)
			if err != nil {
				t.Fatal(err)
			}
			read := false
			rabbit.deadLetterGet = func() (amqp.Delivery, bool, error) {
				if read {
					return amqp.Delivery{}, false, nil
				}
				read = true
				return amqp.Delivery{Acknowledger: &recordingAcknowledger{}, DeliveryTag: 1, Body: body}, true, nil
			}
			rabbit.deadLetterReplayPublish = func(context.Context, string, []byte) error { return nil }
			if operation == "inspect" {
				_, err = rabbit.InspectDeadLetters(context.Background(), DeadLetterInspectRequest{Limit: 1})
			} else {
				request := DeadLetterReplayRequest{Limit: 1, Execute: operation != "scan"}
				if operation == "discard" {
					request.Transform = func(context.Context, []DeadLetterMessage) ([]DeadLetterReplayTransformation, error) {
						return []DeadLetterReplayTransformation{{Discard: true}}, nil
					}
				}
				_, err = rabbit.ReplayDeadLetters(context.Background(), request)
			}
			if err != nil || buffer.Len() != 0 {
				t.Fatalf("err=%v unexpected request log=%s", err, buffer)
			}
		})
	}
}
