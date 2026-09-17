package queue

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/uug-ai/models/pkg/models"
)

var errStopSQSConsumer = errors.New("stop SQS consumer")

type fakeSQSClient struct {
	queueURLs         map[string]string
	receiveMessages   []types.Message
	sent              []*awssqs.SendMessageInput
	deleted           []*awssqs.DeleteMessageInput
	visibilityChanges []*awssqs.ChangeMessageVisibilityInput
	deleteError       error
	visibilityError   error
	operations        []string
}

func (f *fakeSQSClient) GetQueueUrl(_ context.Context, input *awssqs.GetQueueUrlInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	queueURL := f.queueURLs[aws.ToString(input.QueueName)]
	if queueURL == "" {
		return nil, errors.New("queue not found")
	}
	return &awssqs.GetQueueUrlOutput{QueueUrl: aws.String(queueURL)}, nil
}

func (f *fakeSQSClient) ReceiveMessage(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	return &awssqs.ReceiveMessageOutput{Messages: f.receiveMessages}, nil
}

func (f *fakeSQSClient) SendMessage(_ context.Context, input *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.sent = append(f.sent, input)
	f.operations = append(f.operations, "send")
	return &awssqs.SendMessageOutput{}, nil
}

func (f *fakeSQSClient) DeleteMessage(_ context.Context, input *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	f.deleted = append(f.deleted, input)
	f.operations = append(f.operations, "delete")
	return &awssqs.DeleteMessageOutput{}, f.deleteError
}

func (f *fakeSQSClient) ChangeMessageVisibility(_ context.Context, input *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.visibilityChanges = append(f.visibilityChanges, input)
	f.operations = append(f.operations, "visibility")
	return &awssqs.ChangeMessageVisibilityOutput{}, f.visibilityError
}

func newTestSQS(t *testing.T, message types.Message) (*SQS, *fakeSQSClient) {
	t.Helper()
	options := NewSQSOptions().
		SetConsumerQueue("events").
		SetRouterQueue("router").
		SetDeadletterQueue("deadletter").
		SetRegion("eu-west-1").
		SetMaxRetries(2).
		Build()
	client, err := NewSQS(options)
	if err != nil {
		t.Fatalf("NewSQS: %v", err)
	}
	fake := &fakeSQSClient{
		queueURLs: map[string]string{
			"events":     "https://sqs.eu-west-1.amazonaws.com/123/events",
			"router":     "https://sqs.eu-west-1.amazonaws.com/123/router",
			"deadletter": "https://sqs.eu-west-1.amazonaws.com/123/deadletter",
		},
		receiveMessages: []types.Message{message},
		deleteError:     errStopSQSConsumer,
		visibilityError: errStopSQSConsumer,
	}

	client.Client = fake
	client.consumerQueueURL = fake.queueURLs["events"]
	client.queueURLs = map[string]string{
		"events":     fake.queueURLs["events"],
		"router":     fake.queueURLs["router"],
		"deadletter": fake.queueURLs["deadletter"],
	}
	return client, fake
}

func TestSQSDeadLetterReplaySendsBeforeDelete(t *testing.T) {
	envelope, err := encodeDeadLetter([]byte("payload"), DeadLetterMetadata{
		Source:      "events",
		Destination: "deadletter",
	})
	if err != nil {
		t.Fatal(err)
	}
	message := types.Message{
		MessageId:     aws.String("message-1"),
		Body:          aws.String(string(envelope)),
		ReceiptHandle: aws.String("receipt"),
	}
	client, fake := newTestSQS(t, message)
	fake.deleteError = nil
	fake.visibilityError = nil

	result, err := client.ReplayDeadLetters(context.Background(), DeadLetterReplayRequest{
		Limit:   1,
		Execute: true,
		Transform: func(_ context.Context, messages []DeadLetterMessage) ([][]byte, error) {
			if len(messages) != 1 || string(messages[0].Payload) != "payload" {
				t.Fatalf("transform messages = %+v", messages)
			}
			return [][]byte{[]byte("transformed")}, nil
		},
	})
	if err != nil {
		t.Fatalf("ReplayDeadLetters: %v", err)
	}
	if result.Replayed != 1 || len(fake.operations) != 2 || fake.operations[0] != "send" || fake.operations[1] != "delete" {
		t.Fatalf("result=%+v operations=%v", result, fake.operations)
	}
	if aws.ToString(fake.sent[0].QueueUrl) != fake.queueURLs["events"] || aws.ToString(fake.sent[0].MessageBody) != "transformed" {
		t.Fatalf("replay send = %+v", fake.sent[0])
	}
}

func TestSQSDeadLetterReplayTransformFailureReleasesBatch(t *testing.T) {
	envelope, err := encodeDeadLetter([]byte("payload"), DeadLetterMetadata{
		Source:      "events",
		Destination: "deadletter",
	})
	if err != nil {
		t.Fatal(err)
	}
	client, fake := newTestSQS(t, types.Message{
		MessageId:     aws.String("message-1"),
		Body:          aws.String(string(envelope)),
		ReceiptHandle: aws.String("receipt"),
	})
	fake.visibilityError = nil

	_, err = client.ReplayDeadLetters(context.Background(), DeadLetterReplayRequest{
		Limit:   1,
		Execute: true,
		Transform: func(context.Context, []DeadLetterMessage) ([][]byte, error) {
			return nil, errors.New("refresh failed")
		},
	})
	if err == nil || !strings.Contains(err.Error(), "refresh failed") {
		t.Fatalf("error = %v", err)
	}
	if len(fake.sent) != 0 || len(fake.deleted) != 0 || len(fake.visibilityChanges) != 1 {
		t.Fatalf("sent=%d deleted=%d visibility=%d", len(fake.sent), len(fake.deleted), len(fake.visibilityChanges))
	}
}

func TestSQSDeadLetterEnvelopeRecordsRouter(t *testing.T) {
	client, _ := newTestSQS(t, types.Message{})
	envelope, err := client.deadLetterEnvelope([]byte("payload"), DeadLetterReasonHandlerError, 0)
	if err != nil {
		t.Fatal(err)
	}
	message, err := decodeDeadLetter("message-1", envelope)
	if err != nil {
		t.Fatal(err)
	}
	if message.DeadLetter.Source != "events" || message.DeadLetter.ReplayDestination != "router" {
		t.Fatalf("dead-letter metadata = %+v", message.DeadLetter)
	}
}

func TestSQSDeadLetterInspectReleasesMessages(t *testing.T) {
	message := types.Message{
		MessageId:     aws.String("legacy-1"),
		Body:          aws.String(`{"legacy":true}`),
		ReceiptHandle: aws.String("receipt"),
	}

	client, fake := newTestSQS(t, message)
	fake.visibilityError = nil

	result, err := client.InspectDeadLetters(context.Background(), DeadLetterInspectRequest{Limit: 1})
	if err != nil {
		t.Fatalf("InspectDeadLetters: %v", err)
	}
	if result.Groups[UnknownSourceQueue].Count != 1 || len(fake.visibilityChanges) != 1 || fake.visibilityChanges[0].VisibilityTimeout != 0 {
		t.Fatalf("result=%+v visibility=%+v", result, fake.visibilityChanges)
	}
}

func TestSQSDeadLetterReplayRejectsDeadLetterURLAlias(t *testing.T) {
	message := types.Message{
		MessageId:     aws.String("legacy-1"),
		Body:          aws.String(`{"legacy":true}`),
		ReceiptHandle: aws.String("receipt"),
	}
	client, fake := newTestSQS(t, message)
	fake.visibilityError = nil

	_, err := client.ReplayDeadLetters(context.Background(), DeadLetterReplayRequest{
		Limit:       1,
		Destination: fake.queueURLs["deadletter"],
		Execute:     true,
	})
	if err == nil {
		t.Fatal("expected dead-letter URL alias to be rejected")
	}
	if len(fake.sent) != 0 || len(fake.deleted) != 0 || len(fake.visibilityChanges) != 1 {
		t.Fatalf("sent=%d deleted=%d visibility=%d", len(fake.sent), len(fake.deleted), len(fake.visibilityChanges))
	}
}

func testSQSMessage(t *testing.T, event models.PipelineEvent, receiveCount string) types.Message {
	t.Helper()
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return types.Message{
		Body:          aws.String(string(payload)),
		ReceiptHandle: aws.String("receipt"),
		Attributes:    map[string]string{"ApproximateReceiveCount": receiveCount},
	}
}

func TestNewSelectsSQSClient(t *testing.T) {
	options := NewSQSOptions().
		SetConsumerQueue("events").
		SetDeadletterQueue("deadletter").
		SetRegion("eu-west-1").
		Build()
	client, err := New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := client.Client.(*SQS); !ok {
		t.Fatalf("expected *SQS, got %T", client.Client)
	}
}

func TestSQSPublish(t *testing.T) {
	client, fake := newTestSQS(t, types.Message{})
	if err := client.Publish("router", []byte("payload")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if len(fake.sent) != 1 || aws.ToString(fake.sent[0].QueueUrl) != fake.queueURLs["router"] || aws.ToString(fake.sent[0].MessageBody) != "payload" {
		t.Fatalf("unexpected send: %+v", fake.sent)
	}
}

func TestSQSPublishToDeadLetterQueueAddsEnvelope(t *testing.T) {
	client, fake := newTestSQS(t, types.Message{})
	if err := client.Publish("deadletter", []byte("payload")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	message, err := decodeDeadLetter("deadletter", []byte(aws.ToString(fake.sent[0].MessageBody)))
	if err != nil {
		t.Fatal(err)
	}
	if message.Legacy || message.DeadLetter.Source != "events" || string(message.Payload) != "payload" {
		t.Fatalf("dead-letter message = %+v", message)
	}
}

func TestSQSPublishWithDelayToDeadLetterQueueAddsEnvelope(t *testing.T) {
	client, fake := newTestSQS(t, types.Message{})
	client.PublishWithDelay("deadletter", []byte("payload"), 5)

	if len(fake.sent) != 1 || fake.sent[0].DelaySeconds != 5 {
		t.Fatalf("unexpected delayed send: %+v", fake.sent)
	}
	message, err := decodeDeadLetter("deadletter", []byte(aws.ToString(fake.sent[0].MessageBody)))
	if err != nil {
		t.Fatal(err)
	}
	if message.Legacy || message.DeadLetter.Source != "events" || string(message.Payload) != "payload" {
		t.Fatalf("dead-letter message = %+v", message)
	}
}

func TestSQSFIFOPublish(t *testing.T) {
	client, fake := newTestSQS(t, types.Message{})
	client.options.MessageGroupID = "pipeline"
	fake.queueURLs["target.fifo"] = "https://sqs.eu-west-1.amazonaws.com/123/target.fifo"
	if err := client.Publish("target.fifo", []byte("payload")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(fake.sent) != 1 || aws.ToString(fake.sent[0].MessageGroupId) != "pipeline" || aws.ToString(fake.sent[0].MessageDeduplicationId) == "" {
		t.Fatalf("FIFO metadata missing: %+v", fake.sent)
	}
	if err := client.publish("target.fifo", []byte("payload"), 1); err == nil {
		t.Fatal("expected FIFO delay error")
	}
}

func TestSQSPublishFailureInvokesDisasterRecoveryOnce(t *testing.T) {
	client, _ := newTestSQS(t, types.Message{})
	recoveryCalls := 0
	client.SetDisasterRecoveryHandler(func([]byte) error {
		recoveryCalls++
		return nil
	})
	client.PublishWithDelay("target.fifo", []byte("payload"), 1)
	if recoveryCalls != 1 {
		t.Fatalf("disaster recovery calls = %d", recoveryCalls)
	}
}

func TestSQSReadMessagesForwardsBeforeDelete(t *testing.T) {
	message := testSQSMessage(t, models.PipelineEvent{Stages: []string{"analysis", "classify"}}, "1")
	client, fake := newTestSQS(t, message)
	err := client.ReadMessages(
		func(event models.PipelineEvent, _ ...any) (models.PipelineAction, models.PipelineEvent, int) {
			if event.ReceiveCount != 1 {
				t.Fatalf("receive count = %d", event.ReceiveCount)
			}
			return models.PipelineForward, event, 0
		},
		func(models.PipelineMetrics) {},
	)
	if !errors.Is(err, errStopSQSConsumer) {
		t.Fatalf("ReadMessages error = %v", err)
	}
	if len(fake.sent) != 1 || aws.ToString(fake.sent[0].QueueUrl) != fake.queueURLs["router"] {
		t.Fatalf("expected router publish before delete: %+v", fake.sent)
	}
	if len(fake.deleted) != 1 {
		t.Fatalf("delete calls = %d", len(fake.deleted))
	}
}

func TestSQSRetryChangesVisibilityWithoutDelete(t *testing.T) {
	message := testSQSMessage(t, models.PipelineEvent{}, "1")
	client, fake := newTestSQS(t, message)
	err := client.ReadMessages(
		func(event models.PipelineEvent, _ ...any) (models.PipelineAction, models.PipelineEvent, int) {
			return models.PipelineRetry, event, 17
		},
		func(models.PipelineMetrics) {},
	)
	if !errors.Is(err, errStopSQSConsumer) {
		t.Fatalf("ReadMessages error = %v", err)
	}
	if len(fake.visibilityChanges) != 1 || fake.visibilityChanges[0].VisibilityTimeout != 17 {
		t.Fatalf("visibility changes = %+v", fake.visibilityChanges)
	}
	if len(fake.deleted) != 0 || len(fake.sent) != 0 {
		t.Fatalf("retry unexpectedly sent or deleted: sent=%d deleted=%d", len(fake.sent), len(fake.deleted))
	}
}

func TestSQSRetryExhaustionDeadlettersBeforeDelete(t *testing.T) {
	message := testSQSMessage(t, models.PipelineEvent{}, "3")
	client, fake := newTestSQS(t, message)
	err := client.ReadMessages(
		func(event models.PipelineEvent, _ ...any) (models.PipelineAction, models.PipelineEvent, int) {
			return models.PipelineRetry, event, 0
		},
		func(models.PipelineMetrics) {},
	)
	if !errors.Is(err, errStopSQSConsumer) {
		t.Fatalf("ReadMessages error = %v", err)
	}
	if len(fake.sent) != 1 || aws.ToString(fake.sent[0].QueueUrl) != fake.queueURLs["deadletter"] {
		t.Fatalf("expected deadletter publish: %+v", fake.sent)
	}
	deadLetter, decodeErr := decodeDeadLetter("deadletter", []byte(aws.ToString(fake.sent[0].MessageBody)))
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if deadLetter.DeadLetter.Source != "events" || deadLetter.DeadLetter.Reason != DeadLetterReasonRetryExhausted || deadLetter.DeadLetter.Attempts != 2 {
		t.Fatalf("dead-letter metadata = %+v", deadLetter.DeadLetter)
	}
	if len(fake.deleted) != 1 || len(fake.visibilityChanges) != 0 {
		t.Fatalf("unexpected exhaustion operations: deleted=%d visibility=%d", len(fake.deleted), len(fake.visibilityChanges))
	}
}

func TestSQSMalformedMessageDeadlettersBeforeDelete(t *testing.T) {
	message := types.Message{
		Body:          aws.String("not-json"),
		ReceiptHandle: aws.String("receipt"),
	}
	client, fake := newTestSQS(t, message)
	err := client.ReadMessages(
		func(event models.PipelineEvent, _ ...any) (models.PipelineAction, models.PipelineEvent, int) {
			return models.PipelineCancel, event, 0
		},
		func(models.PipelineMetrics) {},
	)
	if !errors.Is(err, errStopSQSConsumer) {
		t.Fatalf("ReadMessages error = %v", err)
	}
	if len(fake.sent) != 1 || aws.ToString(fake.sent[0].QueueUrl) != fake.queueURLs["deadletter"] || len(fake.deleted) != 1 {
		t.Fatalf("unexpected malformed-message operations: sent=%d deleted=%d", len(fake.sent), len(fake.deleted))
	}
}
