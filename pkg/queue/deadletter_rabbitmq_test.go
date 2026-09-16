package queue

import (
	"bytes"
	"context"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeRabbitAcknowledger struct {
	acked    []uint64
	requeued []uint64
}

func (f *fakeRabbitAcknowledger) Ack(tag uint64, _ bool) error {
	f.acked = append(f.acked, tag)
	return nil
}

func (f *fakeRabbitAcknowledger) Nack(tag uint64, _ bool, requeue bool) error {
	if requeue {
		f.requeued = append(f.requeued, tag)
	}
	return nil
}

func (f *fakeRabbitAcknowledger) Reject(tag uint64, requeue bool) error {
	return f.Nack(tag, false, requeue)
}

func TestRabbitDeadLetterReplayPublishesBeforeAck(t *testing.T) {
	client, err := NewRabbitMQ(NewRabbitOptions().
		SetConsumerQueue("events").
		SetDeadletterQueue("deadletter").
		SetHost("rabbitmq:5672").
		SetUsername("guest").
		SetPassword("guest").
		Build())
	if err != nil {
		t.Fatalf("NewRabbitMQ: %v", err)
	}

	envelope, err := encodeDeadLetter([]byte("payload"), DeadLetterMetadata{
		Source:      "events",
		Destination: "deadletter",
	})
	if err != nil {
		t.Fatal(err)
	}
	acknowledger := &fakeRabbitAcknowledger{}
	deliveries := []amqp.Delivery{{
		Acknowledger: acknowledger,
		DeliveryTag:  1,
		Body:         envelope,
	}}
	client.deadLetterGet = func() (amqp.Delivery, bool, error) {
		if len(deliveries) == 0 {
			return amqp.Delivery{}, false, nil
		}
		delivery := deliveries[0]
		deliveries = deliveries[1:]
		return delivery, true, nil
	}
	published := false
	client.deadLetterReplayPublish = func(_ context.Context, destination string, payload []byte) error {
		if destination != "events" || !bytes.Equal(payload, []byte("payload")) {
			t.Fatalf("publish destination=%q payload=%q", destination, payload)
		}
		if len(acknowledger.acked) != 0 {
			t.Fatal("delivery acknowledged before replay publish")
		}
		published = true
		return nil
	}

	result, err := client.ReplayDeadLetters(context.Background(), DeadLetterReplayRequest{
		Limit:   1,
		Execute: true,
	})
	if err != nil {
		t.Fatalf("ReplayDeadLetters: %v", err)
	}
	if !published || result.Replayed != 1 || len(acknowledger.acked) != 1 || len(acknowledger.requeued) != 0 {
		t.Fatalf("result=%+v acknowledger=%+v", result, acknowledger)
	}
}

func TestRabbitPublishToDeadLetterQueueAddsEnvelope(t *testing.T) {
	client, err := NewRabbitMQ(NewRabbitOptions().
		SetConsumerQueue("events").
		SetDeadletterQueue("deadletter").
		SetHost("rabbitmq:5672").
		SetUsername("guest").
		SetPassword("guest").
		Build())
	if err != nil {
		t.Fatalf("NewRabbitMQ: %v", err)
	}
	var published []byte
	client.deadLetterReplayPublish = func(_ context.Context, destination string, payload []byte) error {
		if destination != "deadletter" {
			t.Fatalf("destination = %q", destination)
		}
		published = append([]byte(nil), payload...)
		return nil
	}

	if err := client.Publish("deadletter", []byte("payload")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	message, err := decodeDeadLetter("deadletter", published)
	if err != nil {
		t.Fatal(err)
	}
	if message.Legacy || message.DeadLetter.Source != "events" || string(message.Payload) != "payload" {
		t.Fatalf("dead-letter message = %+v", message)
	}
}

func TestRabbitPublishDeadLetterUsesTrustedAdministrativeMetadata(t *testing.T) {
	client, err := NewRabbitMQ(NewRabbitOptions().
		SetConsumerQueue("deadletter").
		SetDeadletterQueue("deadletter").
		SetHost("rabbitmq:5672").
		SetUsername("guest").
		SetPassword("guest").
		Build())
	if err != nil {
		t.Fatalf("NewRabbitMQ: %v", err)
	}
	var published []byte
	client.deadLetterReplayPublish = func(_ context.Context, destination string, payload []byte) error {
		if destination != "deadletter" {
			t.Fatalf("destination = %q", destination)
		}
		published = append([]byte(nil), payload...)
		return nil
	}

	err = client.PublishDeadLetter(context.Background(), []byte("payload"), DeadLetterMetadata{
		Source:      "events",
		Destination: "deadletter",
		Reason:      DeadLetterReasonHandlerError,
	})
	if err != nil {
		t.Fatalf("PublishDeadLetter: %v", err)
	}
	message, err := decodeDeadLetter("deadletter", published)
	if err != nil {
		t.Fatal(err)
	}
	if message.DeadLetter.Source != "events" || string(message.Payload) != "payload" {
		t.Fatalf("dead-letter message = %+v", message)
	}
}

func TestRabbitReadMessagesToDeadletterRequiresConfirmedDelivery(t *testing.T) {
	client, err := NewRabbitMQ(NewRabbitOptions().
		SetConsumerQueue("events").
		SetDeadletterQueue("deadletter").
		SetHost("rabbitmq:5672").
		SetUsername("guest").
		SetPassword("guest").
		Build())
	if err != nil {
		t.Fatalf("NewRabbitMQ: %v", err)
	}

	err = client.ReadMessagesToDeadletter(DeadLetterReasonHandlerError)
	if err == nil || !strings.Contains(err.Error(), "confirmed RabbitMQ delivery is not enabled") {
		t.Fatalf("error = %v, want confirmed delivery requirement", err)
	}
}

func TestRabbitDeadLetterEnvelopePreservesFailureReason(t *testing.T) {
	client, err := NewRabbitMQ(NewRabbitOptions().
		SetConsumerQueue("events").
		SetDeadletterQueue("deadletter").
		SetHost("rabbitmq:5672").
		SetUsername("guest").
		SetPassword("guest").
		Build())
	if err != nil {
		t.Fatalf("NewRabbitMQ: %v", err)
	}

	for _, reason := range []DeadLetterReason{
		DeadLetterReasonMalformed,
		DeadLetterReasonHandlerError,
		DeadLetterReasonPublishFailed,
	} {
		t.Run(string(reason), func(t *testing.T) {
			payload, err := client.deadLetterEnvelope([]byte("payload"), reason, 0)
			if err != nil {
				t.Fatal(err)
			}
			message, err := decodeDeadLetter("deadletter", payload)
			if err != nil {
				t.Fatal(err)
			}
			if message.DeadLetter.Reason != reason {
				t.Fatalf("reason = %q, want %q", message.DeadLetter.Reason, reason)
			}
		})
	}
}

func TestRabbitDeadLetterInspectRequeuesMessages(t *testing.T) {
	client, err := NewRabbitMQ(NewRabbitOptions().
		SetConsumerQueue("events").
		SetDeadletterQueue("deadletter").
		SetHost("rabbitmq:5672").
		SetUsername("guest").
		SetPassword("guest").
		Build())
	if err != nil {
		t.Fatalf("NewRabbitMQ: %v", err)
	}
	acknowledger := &fakeRabbitAcknowledger{}
	deliveries := []amqp.Delivery{{
		Acknowledger: acknowledger,
		DeliveryTag:  1,
		Body:         []byte(`{"legacy":true}`),
	}}
	client.deadLetterGet = func() (amqp.Delivery, bool, error) {
		if len(deliveries) == 0 {
			return amqp.Delivery{}, false, nil
		}
		delivery := deliveries[0]
		deliveries = deliveries[1:]
		return delivery, true, nil
	}
	var restored [][]byte
	client.deadLetterReplayPublish = func(_ context.Context, destination string, payload []byte) error {
		if destination != "deadletter" {
			t.Fatalf("restore destination = %q", destination)
		}
		restored = append(restored, append([]byte(nil), payload...))
		return nil
	}

	result, err := client.InspectDeadLetters(context.Background(), DeadLetterInspectRequest{Limit: 1})
	if err != nil {
		t.Fatalf("InspectDeadLetters: %v", err)
	}
	if result.Groups[UnknownSourceQueue].Count != 1 || len(restored) != 1 ||
		len(acknowledger.acked) != 1 || len(acknowledger.requeued) != 0 {
		t.Fatalf("result=%+v acknowledger=%+v", result, acknowledger)
	}
}
