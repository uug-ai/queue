package queue

import (
	"fmt"

	"github.com/go-playground/validator/v10"
)

const (
	defaultSQSWaitTimeSeconds     int32 = 20
	defaultSQSVisibilityTimeout   int32 = 30
	defaultSQSMaxNumberOfMessages int32 = 10
)

// SQSOptions holds the configuration for Amazon SQS.
type SQSOptions struct {
	ConsumerQueue       string `validate:"required"`
	RouterQueue         string
	DeadletterQueue     string `validate:"required"`
	Region              string `validate:"required"`
	Endpoint            string
	AccessKeyID         string
	SecretAccessKey     string
	SessionToken        string
	WaitTimeSeconds     int32 `validate:"omitempty,gte=0,lte=20"`
	VisibilityTimeout   int32 `validate:"omitempty,gte=0,lte=43200"`
	MaxNumberOfMessages int32 `validate:"omitempty,gte=1,lte=10"`
	MaxRetries          int
	MessageGroupID      string
}

func (s *SQSOptions) Validate() error {
	validate := validator.New()
	if err := validate.Struct(s); err != nil {
		return err
	}
	if (s.AccessKeyID == "") != (s.SecretAccessKey == "") {
		return fmt.Errorf("SQS access key ID and secret access key must be configured together")
	}
	if s.MaxRetries < 0 {
		return fmt.Errorf("SQS max retries cannot be negative")
	}
	return nil
}

func (s *SQSOptions) waitTimeSeconds() int32 {
	if s.WaitTimeSeconds == 0 {
		return defaultSQSWaitTimeSeconds
	}
	return s.WaitTimeSeconds
}

func (s *SQSOptions) visibilityTimeout() int32 {
	if s.VisibilityTimeout == 0 {
		return defaultSQSVisibilityTimeout
	}
	return s.VisibilityTimeout
}

func (s *SQSOptions) maxNumberOfMessages() int32 {
	if s.MaxNumberOfMessages == 0 {
		return defaultSQSMaxNumberOfMessages
	}
	return s.MaxNumberOfMessages
}

func (s *SQSOptions) maxRetries() int {
	if s.MaxRetries == 0 {
		return defaultMaxRetries
	}
	return s.MaxRetries
}

type SQSOptionsBuilder struct {
	options *SQSOptions
}

func NewSQSOptions() *SQSOptionsBuilder {
	return &SQSOptionsBuilder{options: &SQSOptions{}}
}

func (b *SQSOptionsBuilder) SetConsumerQueue(name string) *SQSOptionsBuilder {
	b.options.ConsumerQueue = name
	return b
}

func (b *SQSOptionsBuilder) SetWorkflowsQueue(name string) *SQSOptionsBuilder {
	return b.SetConsumerQueue(name)
}

func (b *SQSOptionsBuilder) SetWorkflowsStageQueue(name string) *SQSOptionsBuilder {
	return b.SetConsumerQueue(name)
}

func (b *SQSOptionsBuilder) SetRouterQueue(name string) *SQSOptionsBuilder {
	b.options.RouterQueue = name
	return b
}

func (b *SQSOptionsBuilder) SetDeadletterQueue(name string) *SQSOptionsBuilder {
	b.options.DeadletterQueue = name
	return b
}

func (b *SQSOptionsBuilder) SetRegion(region string) *SQSOptionsBuilder {
	b.options.Region = region
	return b
}

func (b *SQSOptionsBuilder) SetEndpoint(endpoint string) *SQSOptionsBuilder {
	b.options.Endpoint = endpoint
	return b
}

func (b *SQSOptionsBuilder) SetCredentials(accessKeyID, secretAccessKey string) *SQSOptionsBuilder {
	b.options.AccessKeyID = accessKeyID
	b.options.SecretAccessKey = secretAccessKey
	return b
}

func (b *SQSOptionsBuilder) SetSessionToken(token string) *SQSOptionsBuilder {
	b.options.SessionToken = token
	return b
}

func (b *SQSOptionsBuilder) SetWaitTimeSeconds(seconds int32) *SQSOptionsBuilder {
	b.options.WaitTimeSeconds = seconds
	return b
}

func (b *SQSOptionsBuilder) SetVisibilityTimeout(seconds int32) *SQSOptionsBuilder {
	b.options.VisibilityTimeout = seconds
	return b
}

func (b *SQSOptionsBuilder) SetMaxNumberOfMessages(count int32) *SQSOptionsBuilder {
	b.options.MaxNumberOfMessages = count
	return b
}

func (b *SQSOptionsBuilder) SetMaxRetries(maxRetries int) *SQSOptionsBuilder {
	b.options.MaxRetries = maxRetries
	return b
}

func (b *SQSOptionsBuilder) SetMessageGroupID(groupID string) *SQSOptionsBuilder {
	b.options.MessageGroupID = groupID
	return b
}

func (b *SQSOptionsBuilder) Build() *SQSOptions {
	return b.options
}
