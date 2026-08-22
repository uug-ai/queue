package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/uug-ai/models/pkg/models"
)

const sqsOperationTimeout = 10 * time.Second

const sqsApproximateReceiveCount types.QueueAttributeName = "ApproximateReceiveCount"

type SQSClient interface {
	GetQueueUrl(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error)
	ReceiveMessage(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	SendMessage(context.Context, *awssqs.SendMessageInput, ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
	DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(context.Context, *awssqs.ChangeMessageVisibilityInput, ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
}

// SQS implements QueueInterface using the AWS SDK for Go v2.
type SQS struct {
	options                 *SQSOptions
	Client                  SQSClient
	disasterRecoveryHandler DisasterRecoveryHandler
	consumerQueueURL        string
	queueURLs               map[string]string
	ctx                     context.Context
	cancel                  context.CancelFunc
	mu                      sync.Mutex
}

var _ QueueInterface = (*SQS)(nil)

func NewSQS(options *SQSOptions) (*SQS, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &SQS{
		options:   options,
		queueURLs: make(map[string]string),
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func (s *SQS) Connect() error {
	loadOptions := []func(*config.LoadOptions) error{config.WithRegion(s.options.Region)}
	if s.options.AccessKeyID != "" {
		provider := credentials.NewStaticCredentialsProvider(
			s.options.AccessKeyID,
			s.options.SecretAccessKey,
			s.options.SessionToken,
		)
		loadOptions = append(loadOptions, config.WithCredentialsProvider(provider))
	}

	awsConfig, err := config.LoadDefaultConfig(s.ctx, loadOptions...)
	if err != nil {
		return err
	}
	client := awssqs.NewFromConfig(awsConfig, func(options *awssqs.Options) {
		if s.options.Endpoint != "" {
			options.BaseEndpoint = aws.String(s.options.Endpoint)
		}
	})

	queueURLs := make(map[string]string)
	for _, queueName := range []string{
		s.options.ConsumerQueue,
		s.options.RouterQueue,
		s.options.DeadletterQueue,
	} {
		if queueName == "" {
			continue
		}
		queueURL, err := resolveSQSQueueURL(s.ctx, client, queueName)
		if err != nil {
			return err
		}
		queueURLs[queueName] = queueURL
	}

	s.mu.Lock()
	s.Client = client
	s.consumerQueueURL = queueURLs[s.options.ConsumerQueue]
	s.queueURLs = queueURLs
	s.mu.Unlock()
	return nil
}

func (s *SQS) Reconnect() error {
	return s.Connect()
}

func (s *SQS) Close() {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.Client = nil
	s.consumerQueueURL = ""
	s.queueURLs = make(map[string]string)
	s.mu.Unlock()
}

func (s *SQS) clientAndConsumerURL() (SQSClient, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Client, s.consumerQueueURL
}

func (s *SQS) ensureConnected() error {
	client, consumerQueueURL := s.clientAndConsumerURL()
	if client != nil && consumerQueueURL != "" {
		return nil
	}
	return s.Connect()
}

func resolveSQSQueueURL(ctx context.Context, client SQSClient, queueName string) (string, error) {
	if strings.HasPrefix(queueName, "https://") || strings.HasPrefix(queueName, "http://") {
		return queueName, nil
	}
	result, err := client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if err != nil {
		return "", fmt.Errorf("resolve SQS queue %q: %w", queueName, err)
	}
	if result.QueueUrl == nil || *result.QueueUrl == "" {
		return "", fmt.Errorf("SQS queue %q returned an empty URL", queueName)
	}
	return *result.QueueUrl, nil
}

func (s *SQS) queueURL(ctx context.Context, queueName string) (string, error) {
	s.mu.Lock()
	if queueURL := s.queueURLs[queueName]; queueURL != "" {
		s.mu.Unlock()
		return queueURL, nil
	}
	client := s.Client
	s.mu.Unlock()
	if client == nil {
		return "", fmt.Errorf("SQS client is not initialized")
	}
	queueURL, err := resolveSQSQueueURL(ctx, client, queueName)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.queueURLs[queueName] = queueURL
	s.mu.Unlock()
	return queueURL, nil
}

func (s *SQS) Publish(queueName string, payload []byte) error {
	err := s.publish(queueName, payload, 0)
	if err != nil {
		_ = s.DisasterRecovery(payload)
	}
	return err
}

func (s *SQS) PublishWithDelay(queueName string, payload []byte, backoff int) {
	if err := s.publish(queueName, payload, int32(backoff)); err != nil {
		_ = s.DisasterRecovery(payload)
	}
}

func (s *SQS) publish(queueName string, payload []byte, delaySeconds int32) error {
	if delaySeconds < 0 || delaySeconds > 900 {
		return fmt.Errorf("SQS delay must be between 0 and 900 seconds")
	}
	if err := s.ensureConnected(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(s.ctx, sqsOperationTimeout)
	defer cancel()
	queueURL, err := s.queueURL(ctx, queueName)
	if err != nil {
		return err
	}
	client, _ := s.clientAndConsumerURL()
	input := &awssqs.SendMessageInput{
		MessageBody:  aws.String(string(payload)),
		QueueUrl:     aws.String(queueURL),
		DelaySeconds: delaySeconds,
	}
	if isSQSFiFoQueue(queueName, queueURL) {
		if delaySeconds > 0 {
			return fmt.Errorf("per-message delays are not supported for SQS FIFO queues")
		}
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
		if reconnectErr := s.Reconnect(); reconnectErr == nil {
			ctx, cancel := context.WithTimeout(s.ctx, sqsOperationTimeout)
			defer cancel()
			queueURL, resolveErr := s.queueURL(ctx, queueName)
			if resolveErr == nil {
				input.QueueUrl = aws.String(queueURL)
				client, _ = s.clientAndConsumerURL()
				_, err = client.SendMessage(ctx, input)
			}
		}
		return err
	}
	return nil
}

func isSQSFiFoQueue(queueName, queueURL string) bool {
	return strings.HasSuffix(strings.ToLower(queueName), ".fifo") ||
		strings.HasSuffix(strings.ToLower(queueURL), ".fifo")
}

func newSQSDeduplicationID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("create SQS FIFO deduplication ID: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func (s *SQS) ReadMessages(handleMessage models.MessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	return s.consume(func(ctx context.Context, message types.Message) error {
		started := time.Now()
		payload := []byte(aws.ToString(message.Body))
		var event models.PipelineEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return s.deadletterAndDelete(ctx, message, payload)
		}

		receiveCount := sqsReceiveCount(message)
		event.ReceiveCount = int64(receiveCount)
		action, event, backoff := handleMessage(event, args...)
		var err error
		switch action {
		case models.PipelineForward:
			if len(event.Stages) < 2 {
				err = s.deleteMessage(ctx, message)
				break
			}
			event.Stages = event.Stages[1:]
			var forwardPayload []byte
			forwardPayload, err = json.Marshal(event)
			if err != nil {
				err = s.deadletterAndDelete(ctx, message, payload)
			} else {
				err = s.publishThenDelete(ctx, message, s.options.RouterQueue, forwardPayload)
			}
		case models.PipelineError:
			deadletterPayload, marshalErr := json.Marshal(event)
			if marshalErr != nil {
				deadletterPayload = payload
			}
			err = s.deadletterAndDelete(ctx, message, deadletterPayload)
		case models.PipelineRetry:
			err = s.retryOrDeadletter(ctx, message, payload, receiveCount, backoff)
		default:
			err = s.deleteMessage(ctx, message)
		}
		if err == nil {
			handlePrometheus(models.PipelineMetrics{ProcessingTime: time.Since(started).Seconds()})
		}
		return err
	})
}

func (s *SQS) RouteMessages(_ models.MessageHandler, handlePrometheus models.PrometheusHandler, _ ...any) error {
	return s.consume(func(ctx context.Context, message types.Message) error {
		started := time.Now()
		payload := []byte(aws.ToString(message.Body))
		var event models.PipelineEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return s.deadletterAndDelete(ctx, message, payload)
		}
		var err error
		if len(event.Stages) == 0 {
			err = s.deleteMessage(ctx, message)
		} else {
			err = s.publishThenDelete(ctx, message, "kcloud-"+event.Stages[0]+"-queue", payload)
		}
		if err == nil {
			handlePrometheus(models.PipelineMetrics{ProcessingTime: time.Since(started).Seconds()})
		}
		return err
	})
}

func (s *SQS) ReadRawMessages(handleMessage RawMessageHandler, handlePrometheus models.PrometheusHandler, args ...any) error {
	return s.consume(func(ctx context.Context, message types.Message) error {
		started := time.Now()
		payload := []byte(aws.ToString(message.Body))
		action, output, backoff := handleMessage(payload, args...)
		var err error
		switch action {
		case models.PipelineForward:
			if s.options.RouterQueue == "" {
				err = s.deadletterAndDelete(ctx, message, payload)
				break
			}
			if output == nil {
				output = payload
			}
			err = s.publishThenDelete(ctx, message, s.options.RouterQueue, output)
		case models.PipelineError:
			err = s.deadletterAndDelete(ctx, message, payload)
		case models.PipelineRetry:
			err = s.retryOrDeadletter(ctx, message, payload, sqsReceiveCount(message), backoff)
		default:
			err = s.deleteMessage(ctx, message)
		}
		if err == nil {
			handlePrometheus(models.PipelineMetrics{ProcessingTime: time.Since(started).Seconds()})
		}
		return err
	})
}

func (s *SQS) consume(process func(context.Context, types.Message) error) error {
	for {
		if err := s.ensureConnected(); err != nil {
			return err
		}
		client, consumerQueueURL := s.clientAndConsumerURL()
		result, err := client.ReceiveMessage(s.ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(consumerQueueURL),
			AttributeNames:      []types.QueueAttributeName{sqsApproximateReceiveCount},
			MaxNumberOfMessages: s.options.maxNumberOfMessages(),
			VisibilityTimeout:   s.options.visibilityTimeout(),
			WaitTimeSeconds:     s.options.waitTimeSeconds(),
		})
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			if reconnectErr := s.Reconnect(); reconnectErr != nil {
				return err
			}
			continue
		}
		for _, message := range result.Messages {
			if err := process(s.ctx, message); err != nil {
				return err
			}
		}
	}
}

func sqsReceiveCount(message types.Message) int {
	count, err := strconv.Atoi(message.Attributes[string(sqsApproximateReceiveCount)])
	if err != nil || count < 1 {
		return 1
	}
	return count
}

func (s *SQS) retryOrDeadletter(ctx context.Context, message types.Message, payload []byte, receiveCount, backoff int) error {
	// ApproximateReceiveCount includes the initial delivery. MaxRetries counts
	// only retries, matching the retry contract used by the other providers.
	if receiveCount > s.options.maxRetries() {
		return s.deadletterAndDelete(ctx, message, payload)
	}
	if backoff <= 0 {
		backoff = 5
	}
	if backoff > 43200 {
		backoff = 43200
	}
	client, consumerQueueURL := s.clientAndConsumerURL()
	_, err := client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(consumerQueueURL),
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: int32(backoff),
	})
	return err
}

func (s *SQS) publishThenDelete(ctx context.Context, message types.Message, queueName string, payload []byte) error {
	if queueName == "" {
		return s.deadletterAndDelete(ctx, message, payload)
	}
	if err := s.Publish(queueName, payload); err != nil {
		return err
	}
	return s.deleteMessage(ctx, message)
}

func (s *SQS) deadletterAndDelete(ctx context.Context, message types.Message, payload []byte) error {
	if err := s.AddToDeadletter(payload); err != nil {
		return err
	}
	return s.deleteMessage(ctx, message)
}

func (s *SQS) deleteMessage(ctx context.Context, message types.Message) error {
	if message.ReceiptHandle == nil || *message.ReceiptHandle == "" {
		return fmt.Errorf("SQS message has no receipt handle")
	}
	client, consumerQueueURL := s.clientAndConsumerURL()
	_, err := client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(consumerQueueURL),
		ReceiptHandle: message.ReceiptHandle,
	})
	return err
}

func (s *SQS) AddToDeadletter(payload []byte) error {
	return s.Publish(s.options.DeadletterQueue, payload)
}

func (s *SQS) SetDisasterRecoveryHandler(handler DisasterRecoveryHandler) {
	s.disasterRecoveryHandler = handler
}

func (s *SQS) DisasterRecovery(payload []byte) error {
	if s.disasterRecoveryHandler != nil {
		return s.disasterRecoveryHandler(payload)
	}
	return nil
}

func (s *SQS) LoadMessages(filename string) error {
	file, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	var events []models.PipelineEvent
	if err := json.Unmarshal(file, &events); err != nil {
		return err
	}
	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if err := s.Publish(s.options.ConsumerQueue, payload); err != nil {
			return err
		}
	}
	return nil
}
