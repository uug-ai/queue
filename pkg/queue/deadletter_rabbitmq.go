package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var _ DeadLetterAdmin = (*RabbitMQ)(nil)

func (r *RabbitMQ) PublishDeadLetter(ctx context.Context, payload []byte, metadata DeadLetterMetadata) error {
	envelope, err := encodeDeadLetterForDestination(payload, metadata, r.options.DeadletterQueue)
	if err != nil {
		return err
	}
	if r.deadLetterReplayPublish == nil {
		if err := r.ensureConnected(); err != nil {
			return err
		}
	}
	return r.publishDeadLetterConfirmed(ctx, r.options.DeadletterQueue, envelope)
}

func (r *RabbitMQ) InspectDeadLetters(ctx context.Context, request DeadLetterInspectRequest) (DeadLetterInspectResult, error) {
	var result DeadLetterInspectResult
	if err := validateDeadLetterLimit(request.Limit); err != nil {
		return result, err
	}
	if r.deadLetterGet == nil {
		if err := r.ensureConnected(); err != nil {
			return result, err
		}
	}

	deliveries := make([]amqp.Delivery, 0, request.Limit)
	for len(deliveries) < request.Limit {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(err, r.restoreRabbitDeadLetters(deliveries))
		}
		delivery, ok, err := r.getDeadLetter()
		if err != nil {
			return result, errors.Join(err, r.restoreRabbitDeadLetters(deliveries))
		}
		if !ok {
			break
		}
		deliveries = append(deliveries, delivery)
		decoded, err := decodeDeadLetter(rabbitDeadLetterID(delivery), delivery.Body)
		if err != nil {
			return result, errors.Join(err, r.restoreRabbitDeadLetters(deliveries))
		}
		addDeadLetterInspection(&result, decoded, request.Source)
	}
	if err := r.restoreRabbitDeadLetters(deliveries); err != nil {
		return result, err
	}
	return result, nil
}

func (r *RabbitMQ) ReplayDeadLetters(ctx context.Context, request DeadLetterReplayRequest) (DeadLetterReplayResult, error) {
	var result DeadLetterReplayResult
	if err := validateDeadLetterLimit(request.Limit); err != nil {
		return result, err
	}
	if r.deadLetterGet == nil || r.deadLetterReplayPublish == nil {
		if err := r.ensureConnected(); err != nil {
			return result, err
		}
	}

	retained := make([]amqp.Delivery, 0, request.Limit)
	requeueRetained := func(operationErr error) error {
		return errors.Join(operationErr, r.restoreRabbitDeadLetters(retained))
	}
	for result.Scanned < request.Limit {
		if err := ctx.Err(); err != nil {
			return result, requeueRetained(err)
		}
		delivery, ok, err := r.getDeadLetter()
		if err != nil {
			return result, requeueRetained(err)
		}
		if !ok {
			break
		}
		retained = append(retained, delivery)
		decoded, err := decodeDeadLetter(rabbitDeadLetterID(delivery), delivery.Body)
		if err != nil {
			return result, requeueRetained(err)
		}
		plan, err := planDeadLetterReplay(&result, decoded, request, r.options.DeadletterQueue)
		if err != nil {
			return result, requeueRetained(err)
		}
		if plan.destination == "" || !request.Execute {
			continue
		}
		if err := r.publishDeadLetterConfirmed(ctx, plan.destination, decoded.Payload); err != nil {
			return result, requeueRetained(fmt.Errorf("replay RabbitMQ dead-letter message %q: %w", decoded.ID, err))
		}
		if err := delivery.Ack(false); err != nil {
			return result, requeueRetained(fmt.Errorf("settle replayed RabbitMQ dead-letter message %q: %w", decoded.ID, err))
		}
		retained = retained[:len(retained)-1]
		result.Replayed++
	}
	if err := r.restoreRabbitDeadLetters(retained); err != nil {
		return result, err
	}
	return result, nil
}

func (r *RabbitMQ) getDeadLetter() (amqp.Delivery, bool, error) {
	if r.deadLetterGet != nil {
		return r.deadLetterGet()
	}
	consumer := r.currentConsumer()
	if consumer == nil {
		return amqp.Delivery{}, false, fmt.Errorf("RabbitMQ consumer channel is not initialized")
	}
	return consumer.Get(r.options.DeadletterQueue, false)
}

func (r *RabbitMQ) publishDeadLetterConfirmed(ctx context.Context, destination string, payload []byte) error {
	if r.deadLetterReplayPublish != nil {
		return r.deadLetterReplayPublish(ctx, destination, payload)
	}

	r.mu.Lock()
	connection := r.Connection
	r.mu.Unlock()
	if connection == nil {
		return fmt.Errorf("RabbitMQ connection is not initialized")
	}
	channel, err := connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	if _, err := channel.QueueInspect(destination); err != nil {
		return fmt.Errorf("inspect RabbitMQ replay destination %q: %w", destination, err)
	}
	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := channel.NotifyReturn(make(chan amqp.Return, 1))
	if err := channel.PublishWithContext(ctx, "", destination, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         payload,
	}); err != nil {
		return err
	}

	var returned *amqp.Return
	for {
		select {
		case message := <-returns:
			returned = &message
		case confirmation := <-confirms:
			if !confirmation.Ack {
				return fmt.Errorf("RabbitMQ rejected replay publish to %q", destination)
			}
			if returned == nil {
				select {
				case message := <-returns:
					returned = &message
				default:
				}
			}
			if returned != nil {
				return fmt.Errorf("RabbitMQ replay destination %q returned message: %s", destination, returned.ReplyText)
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *RabbitMQ) restoreRabbitDeadLetters(deliveries []amqp.Delivery) error {
	var result error
	for _, delivery := range deliveries {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := r.publishDeadLetterConfirmed(ctx, r.options.DeadletterQueue, delivery.Body)
		cancel()
		if err != nil {
			nackErr := delivery.Nack(false, true)
			result = errors.Join(result, fmt.Errorf("republish retained RabbitMQ dead-letter message: %w", err))
			if nackErr != nil {
				result = errors.Join(result, fmt.Errorf("requeue retained RabbitMQ dead-letter message: %w", nackErr))
			}
			continue
		}
		if err := delivery.Ack(false); err != nil {
			result = errors.Join(result, fmt.Errorf("settle retained RabbitMQ dead-letter message: %w", err))
		}
	}
	return result
}

func rabbitDeadLetterID(delivery amqp.Delivery) string {
	if delivery.MessageId != "" {
		return delivery.MessageId
	}
	return strconv.FormatUint(delivery.DeliveryTag, 10)
}
