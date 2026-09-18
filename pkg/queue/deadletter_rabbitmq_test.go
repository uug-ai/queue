package queue

import (
	"bytes"
	"context"
	"errors"
	"slices"
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
		if destination != "events" || !bytes.Equal(payload, []byte("transformed")) {
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
		Transform: func(_ context.Context, messages []DeadLetterMessage) ([]DeadLetterReplayTransformation, error) {
			if len(messages) != 1 || string(messages[0].Payload) != "payload" {
				t.Fatalf("transform messages = %+v", messages)
			}
			return []DeadLetterReplayTransformation{{Payload: []byte("transformed")}}, nil
		},
	})
	if err != nil {
		t.Fatalf("ReplayDeadLetters: %v", err)
	}
	if !published || result.Replayed != 1 || len(acknowledger.acked) != 1 || len(acknowledger.requeued) != 0 {
		t.Fatalf("result=%+v acknowledger=%+v", result, acknowledger)
	}
}

func TestRabbitDeadLetterReplayDiscardsWithoutPublishing(t *testing.T) {
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
	client.deadLetterReplayPublish = func(context.Context, string, []byte) error {
		t.Fatal("discard must not publish")
		return nil
	}

	result, err := client.ReplayDeadLetters(context.Background(), DeadLetterReplayRequest{
		Limit:   1,
		Execute: true,
		Transform: func(context.Context, []DeadLetterMessage) ([]DeadLetterReplayTransformation, error) {
			return []DeadLetterReplayTransformation{{Discard: true}}, nil
		},
	})
	if err != nil {
		t.Fatalf("ReplayDeadLetters: %v", err)
	}
	if result.Planned != 0 || result.DropPlanned != 1 || result.Dropped != 1 ||
		result.Replayed != 0 || result.Retained != 0 || len(acknowledger.acked) != 1 {
		t.Fatalf("result=%+v acknowledger=%+v", result, acknowledger)
	}
}

func TestRabbitDeadLetterReplayTransformFailureRestoresBatch(t *testing.T) {
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
	eventPublishes := 0
	restored := 0
	client.deadLetterReplayPublish = func(_ context.Context, destination string, _ []byte) error {
		if destination == "events" {
			eventPublishes++
		} else if destination == "deadletter" {
			restored++
		}
		return nil
	}

	_, err = client.ReplayDeadLetters(context.Background(), DeadLetterReplayRequest{
		Limit:   1,
		Execute: true,
		Transform: func(context.Context, []DeadLetterMessage) ([]DeadLetterReplayTransformation, error) {
			return nil, errors.New("refresh failed")
		},
	})
	if err == nil || !strings.Contains(err.Error(), "refresh failed") {
		t.Fatalf("error = %v", err)
	}
	if eventPublishes != 0 || restored != 1 || len(acknowledger.acked) != 1 {
		t.Fatalf("event publishes=%d restored=%d acknowledgements=%v", eventPublishes, restored, acknowledger.acked)
	}
}

func TestRabbitDeadLetterReplayBatchesAndRetainsSkippedMessages(t *testing.T) {
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
	deliveries := make([]amqp.Delivery, 0, 5)
	for index, payload := range []string{"one", "poison", "three", "four", "five"} {
		envelope, encodeErr := encodeDeadLetter([]byte(payload), DeadLetterMetadata{
			Source:      "events",
			Destination: "deadletter",
		})
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		deliveries = append(deliveries, amqp.Delivery{
			Acknowledger: acknowledger,
			DeliveryTag:  uint64(index + 1),
			Body:         envelope,
		})
	}
	client.deadLetterGet = func() (amqp.Delivery, bool, error) {
		if len(deliveries) == 0 {
			return amqp.Delivery{}, false, nil
		}
		delivery := deliveries[0]
		deliveries = deliveries[1:]
		return delivery, true, nil
	}
	var batches []int
	var replayed, restored []string
	client.deadLetterReplayPublish = func(_ context.Context, destination string, payload []byte) error {
		if destination == "events" {
			replayed = append(replayed, string(payload))
		} else if destination == "deadletter" {
			message, decodeErr := decodeDeadLetter("restored", payload)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			restored = append(restored, string(message.Payload))
		}
		return nil
	}

	result, err := client.ReplayDeadLetters(context.Background(), DeadLetterReplayRequest{
		Limit:     5,
		BatchSize: 2,
		Execute:   true,
		Transform: func(_ context.Context, messages []DeadLetterMessage) ([]DeadLetterReplayTransformation, error) {
			batches = append(batches, len(messages))
			transformed := make([]DeadLetterReplayTransformation, len(messages))
			for index, message := range messages {
				if string(message.Payload) == "poison" {
					transformed[index].Skip = true
				} else {
					transformed[index].Payload = append([]byte("fresh-"), message.Payload...)
				}
			}
			return transformed, nil
		},
	})
	if err != nil {
		t.Fatalf("ReplayDeadLetters: %v", err)
	}
	if !slices.Equal(batches, []int{2, 2, 1}) {
		t.Fatalf("transform batch sizes = %v", batches)
	}
	if !slices.Equal(replayed, []string{"fresh-one", "fresh-three", "fresh-four", "fresh-five"}) ||
		!slices.Equal(restored, []string{"poison"}) {
		t.Fatalf("replayed=%v restored=%v", replayed, restored)
	}
	if result.Scanned != 5 || result.Matched != 5 || result.Planned != 4 ||
		result.Replayed != 4 || result.Retained != 1 || result.Skipped != 1 ||
		result.Destinations["events"] != 4 {
		t.Fatalf("result = %+v", result)
	}
}

func TestRestoreRabbitDeadLettersStopsPublishingAfterFailure(t *testing.T) {
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
	deliveries := []amqp.Delivery{
		{Acknowledger: acknowledger, DeliveryTag: 1, Body: []byte("one")},
		{Acknowledger: acknowledger, DeliveryTag: 2, Body: []byte("two")},
		{Acknowledger: acknowledger, DeliveryTag: 3, Body: []byte("three")},
	}
	publishes := 0
	client.deadLetterReplayPublish = func(context.Context, string, []byte) error {
		publishes++
		if publishes == 2 {
			return errors.New("connection closed")
		}
		return nil
	}

	err = client.restoreRabbitDeadLetters(deliveries)
	if err == nil || !strings.Contains(err.Error(), "connection closed") {
		t.Fatalf("error = %v", err)
	}
	if publishes != 2 || !slices.Equal(acknowledger.acked, []uint64{1}) ||
		!slices.Equal(acknowledger.requeued, []uint64{2, 3}) {
		t.Fatalf("publishes=%d acknowledger=%+v", publishes, acknowledger)
	}
}

func TestRabbitPublishToDeadLetterQueueAddsEnvelope(t *testing.T) {
	client, err := NewRabbitMQ(NewRabbitOptions().
		SetConsumerQueue("events").
		SetRouterQueue("router").
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
	if message.Legacy ||
		message.DeadLetter.Source != "events" ||
		message.DeadLetter.ReplayDestination != "router" ||
		string(message.Payload) != "payload" {
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

func TestRabbitReadMessagesToDeadletterRejectsSameQueue(t *testing.T) {
	client, err := NewRabbitMQ(NewRabbitOptions().
		SetConsumerQueue("deadletter").
		SetDeadletterQueue("deadletter").
		SetHost("rabbitmq:5672").
		SetUsername("guest").
		SetPassword("guest").
		SetConfirmedDelivery(true).
		Build())
	if err != nil {
		t.Fatalf("NewRabbitMQ: %v", err)
	}

	err = client.ReadMessagesToDeadletter(DeadLetterReasonHandlerError)
	if err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("error = %v, want same-queue rejection", err)
	}
}

func TestRabbitDeadLetterEnvelopePreservesFailureReason(t *testing.T) {
	client, err := NewRabbitMQ(NewRabbitOptions().
		SetConsumerQueue("events").
		SetRouterQueue("router").
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
			if message.DeadLetter.ReplayDestination != "router" {
				t.Fatalf("replay destination = %q, want router", message.DeadLetter.ReplayDestination)
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
