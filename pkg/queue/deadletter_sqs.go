package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var _ DeadLetterAdmin = (*SQS)(nil)

func (s *SQS) PublishDeadLetter(ctx context.Context, payload []byte, metadata DeadLetterMetadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	envelope, err := encodeDeadLetterForDestination(payload, metadata, s.options.DeadletterQueue)
	if err != nil {
		return err
	}
	return s.publish(s.options.DeadletterQueue, envelope, 0)
}

func (s *SQS) InspectDeadLetters(ctx context.Context, request DeadLetterInspectRequest) (DeadLetterInspectResult, error) {
	var result DeadLetterInspectResult
	if err := validateDeadLetterLimit(request.Limit); err != nil {
		return result, err
	}
	if err := s.ensureConnected(); err != nil {
		return result, err
	}
	deadLetterQueueURL, err := s.queueURL(ctx, s.options.DeadletterQueue)
	if err != nil {
		return result, err
	}

	held := make([]types.Message, 0, request.Limit)
	for result.Scanned < request.Limit {
		messages, receiveErr := s.receiveDeadLetterBatch(ctx, deadLetterQueueURL, request.Limit-result.Scanned)
		if receiveErr != nil {
			return result, errors.Join(receiveErr, s.releaseSQSDeadLetters(deadLetterQueueURL, held))
		}
		if len(messages) == 0 {
			break
		}
		held = append(held, messages...)
		for _, message := range messages {
			id := aws.ToString(message.MessageId)
			decoded, decodeErr := decodeDeadLetter(id, []byte(aws.ToString(message.Body)))
			if decodeErr != nil {
				return result, errors.Join(decodeErr, s.releaseSQSDeadLetters(deadLetterQueueURL, held))
			}
			addDeadLetterInspection(&result, decoded, request.Source)
		}
	}
	if err := s.releaseSQSDeadLetters(deadLetterQueueURL, held); err != nil {
		return result, err
	}
	return result, nil
}

func (s *SQS) ReplayDeadLetters(ctx context.Context, request DeadLetterReplayRequest) (DeadLetterReplayResult, error) {
	var result DeadLetterReplayResult
	if err := validateDeadLetterLimit(request.Limit); err != nil {
		return result, err
	}
	if err := s.ensureConnected(); err != nil {
		return result, err
	}
	deadLetterQueueURL, err := s.queueURL(ctx, s.options.DeadletterQueue)
	if err != nil {
		return result, err
	}
	retained := make([]types.Message, 0, request.Limit)
	releaseRetained := func(operationErr error) error {
		return errors.Join(operationErr, s.releaseSQSDeadLetters(deadLetterQueueURL, retained))
	}
	for result.Scanned < request.Limit {
		messages, receiveErr := s.receiveDeadLetterBatch(ctx, deadLetterQueueURL, request.Limit-result.Scanned)
		if receiveErr != nil {
			return result, releaseRetained(receiveErr)
		}
		if len(messages) == 0 {
			break
		}
		retained = append(retained, messages...)
		for _, message := range messages {
			id := aws.ToString(message.MessageId)
			decoded, decodeErr := decodeDeadLetter(id, []byte(aws.ToString(message.Body)))
			if decodeErr != nil {
				return result, releaseRetained(decodeErr)
			}
			plan, planErr := planDeadLetterReplay(&result, decoded, request, s.options.DeadletterQueue)
			if planErr != nil {
				return result, releaseRetained(planErr)
			}
			if plan.destination == "" || !request.Execute {
				continue
			}
			destinationURL, resolveErr := s.queueURL(ctx, plan.destination)
			if resolveErr != nil {
				return result, releaseRetained(resolveErr)
			}
			if destinationURL == deadLetterQueueURL {
				return result, releaseRetained(fmt.Errorf("refusing to replay SQS dead-letter message %q back to %q", decoded.ID, s.options.DeadletterQueue))
			}
			if err := s.publishReplay(ctx, plan.destination, destinationURL, decoded.Payload); err != nil {
				return result, releaseRetained(fmt.Errorf("replay SQS dead-letter message %q: %w", decoded.ID, err))
			}

			if err := s.deleteSQSDeadLetter(ctx, deadLetterQueueURL, message); err != nil {
				return result, releaseRetained(fmt.Errorf("settle replayed SQS dead-letter message %q: %w", decoded.ID, err))
			}
			retained = removeSQSDeadLetter(retained, aws.ToString(message.ReceiptHandle))
			result.Replayed++
		}
	}
	if err := s.releaseSQSDeadLetters(deadLetterQueueURL, retained); err != nil {
		return result, err
	}
	return result, nil
}

func (s *SQS) publishReplay(ctx context.Context, queueName, queueURL string, payload []byte) error {
	client, _ := s.clientAndConsumerURL()
	input := &awssqs.SendMessageInput{
		MessageBody: aws.String(string(payload)),
		QueueUrl:    aws.String(queueURL),
	}
	if isSQSFiFoQueue(queueName, queueURL) {
		groupID := s.options.MessageGroupID
		if groupID == "" {
			groupID = "queue"
		}
		deduplicationID, err := newSQSDeduplicationID()
		if err != nil {
			return err
		}
		input.MessageGroupId = aws.String(groupID)
		input.MessageDeduplicationId = aws.String(deduplicationID)
	}
	if _, err := client.SendMessage(ctx, input); err != nil {
		return err
	}
	return nil
}

func (s *SQS) receiveDeadLetterBatch(ctx context.Context, queueURL string, limit int) ([]types.Message, error) {
	client, _ := s.clientAndConsumerURL()
	batchSize := limit
	if batchSize > int(defaultSQSMaxNumberOfMessages) {
		batchSize = int(defaultSQSMaxNumberOfMessages)
	}
	output, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: int32(batchSize),
		VisibilityTimeout:   deadLetterVisibilityTimeout(ctx, s.options.visibilityTimeout()),
		WaitTimeSeconds:     s.options.waitTimeSeconds(),
	})
	if err != nil {
		return nil, fmt.Errorf("receive SQS dead-letter messages: %w", err)
	}
	return output.Messages, nil
}

func (s *SQS) releaseSQSDeadLetters(queueURL string, messages []types.Message) error {
	if len(messages) == 0 {
		return nil
	}
	client, _ := s.clientAndConsumerURL()
	if batchClient, ok := client.(sqsVisibilityBatchClient); ok {
		for start := 0; start < len(messages); start += int(defaultSQSMaxNumberOfMessages) {
			end := start + int(defaultSQSMaxNumberOfMessages)
			if end > len(messages) {
				end = len(messages)
			}
			entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, 0, end-start)
			for index, message := range messages[start:end] {
				if message.ReceiptHandle == nil || *message.ReceiptHandle == "" {
					return fmt.Errorf("SQS dead-letter message has no receipt handle")
				}
				entries = append(entries, types.ChangeMessageVisibilityBatchRequestEntry{
					Id:                aws.String(fmt.Sprintf("%d", index)),
					ReceiptHandle:     message.ReceiptHandle,
					VisibilityTimeout: 0,
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), sqsOperationTimeout)
			output, err := batchClient.ChangeMessageVisibilityBatch(ctx, &awssqs.ChangeMessageVisibilityBatchInput{
				QueueUrl: aws.String(queueURL),
				Entries:  entries,
			})
			cancel()
			if err != nil {
				return fmt.Errorf("release SQS dead-letter message batch: %w", err)
			}
			if output == nil {
				return fmt.Errorf("release SQS dead-letter message batch: empty response")
			}
			if len(output.Failed) > 0 {
				return fmt.Errorf("release SQS dead-letter message batch: %s", aws.ToString(output.Failed[0].Message))
			}
		}
		return nil
	}
	for _, message := range messages {
		if message.ReceiptHandle == nil || *message.ReceiptHandle == "" {
			return fmt.Errorf("SQS dead-letter message has no receipt handle")
		}
		ctx, cancel := context.WithTimeout(context.Background(), sqsOperationTimeout)
		_, err := client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(queueURL),
			ReceiptHandle:     message.ReceiptHandle,
			VisibilityTimeout: 0,
		})
		cancel()
		if err != nil {
			return fmt.Errorf("release SQS dead-letter message: %w", err)
		}
	}

	return nil
}

func deadLetterVisibilityTimeout(ctx context.Context, configured int32) int32 {
	timeout := configured
	if timeout < 300 {
		timeout = 300
	}
	if deadline, ok := ctx.Deadline(); ok {
		seconds := int32(time.Until(deadline)/time.Second) + 30
		if seconds > timeout {
			timeout = seconds
		}
	}
	if timeout > 43200 {
		return 43200
	}
	return timeout
}

func (s *SQS) deleteSQSDeadLetter(ctx context.Context, queueURL string, message types.Message) error {
	if message.ReceiptHandle == nil || *message.ReceiptHandle == "" {
		return fmt.Errorf("SQS dead-letter message has no receipt handle")
	}
	client, _ := s.clientAndConsumerURL()
	_, err := client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: message.ReceiptHandle,
	})
	return err
}

func removeSQSDeadLetter(messages []types.Message, receiptHandle string) []types.Message {
	for index := range messages {
		if aws.ToString(messages[index].ReceiptHandle) == receiptHandle {
			return append(messages[:index], messages[index+1:]...)
		}
	}
	return messages
}
