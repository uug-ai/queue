package queue

import "github.com/go-playground/validator/v10"

// KafkaOptions holds the configuration for Kafka.
type KafkaOptions struct {
	ConsumerTopic   string `validate:"required"`
	RouterTopic     string
	DeadletterTopic string `validate:"required"`
	Broker          string `validate:"required"`
	GroupID         string `validate:"required"`
	Username        string
	Password        string
	Mechanism       string
	Security        string
	SessionTimeout  int
	AutoOffsetReset string
	MaxRetries      int
}

// Validate validates the KafkaOptions configuration.
func (k *KafkaOptions) Validate() error {
	validate := validator.New()
	return validate.Struct(k)
}

// KafkaOptionsBuilder provides a fluent interface for building Kafka options.
type KafkaOptionsBuilder struct {
	options *KafkaOptions
}

// NewKafkaOptions creates a new Kafka options builder.
func NewKafkaOptions() *KafkaOptionsBuilder {
	return &KafkaOptionsBuilder{options: &KafkaOptions{}}
}

func (b *KafkaOptionsBuilder) SetConsumerTopic(topic string) *KafkaOptionsBuilder {
	b.options.ConsumerTopic = topic
	return b
}

func (b *KafkaOptionsBuilder) SetConsumerQueue(topic string) *KafkaOptionsBuilder {
	return b.SetConsumerTopic(topic)
}

func (b *KafkaOptionsBuilder) SetWorkflowsQueue(topic string) *KafkaOptionsBuilder {
	return b.SetConsumerTopic(topic)
}

func (b *KafkaOptionsBuilder) SetWorkflowsStageQueue(topic string) *KafkaOptionsBuilder {
	return b.SetConsumerTopic(topic)
}

func (b *KafkaOptionsBuilder) SetRouterTopic(topic string) *KafkaOptionsBuilder {
	b.options.RouterTopic = topic
	return b
}

func (b *KafkaOptionsBuilder) SetRouterQueue(topic string) *KafkaOptionsBuilder {
	return b.SetRouterTopic(topic)
}

func (b *KafkaOptionsBuilder) SetDeadletterTopic(topic string) *KafkaOptionsBuilder {
	b.options.DeadletterTopic = topic
	return b
}

func (b *KafkaOptionsBuilder) SetDeadletterQueue(topic string) *KafkaOptionsBuilder {
	return b.SetDeadletterTopic(topic)
}

func (b *KafkaOptionsBuilder) SetBroker(broker string) *KafkaOptionsBuilder {
	b.options.Broker = broker
	return b
}

func (b *KafkaOptionsBuilder) SetGroupID(groupID string) *KafkaOptionsBuilder {
	b.options.GroupID = groupID
	return b
}

func (b *KafkaOptionsBuilder) SetUsername(username string) *KafkaOptionsBuilder {
	b.options.Username = username
	return b
}

func (b *KafkaOptionsBuilder) SetPassword(password string) *KafkaOptionsBuilder {
	b.options.Password = password
	return b
}

func (b *KafkaOptionsBuilder) SetMechanism(mechanism string) *KafkaOptionsBuilder {
	b.options.Mechanism = mechanism
	return b
}

func (b *KafkaOptionsBuilder) SetSecurity(security string) *KafkaOptionsBuilder {
	b.options.Security = security
	return b
}

func (b *KafkaOptionsBuilder) SetSessionTimeout(timeoutMilliseconds int) *KafkaOptionsBuilder {
	b.options.SessionTimeout = timeoutMilliseconds
	return b
}

func (b *KafkaOptionsBuilder) SetAutoOffsetReset(reset string) *KafkaOptionsBuilder {
	b.options.AutoOffsetReset = reset
	return b
}

func (b *KafkaOptionsBuilder) SetMaxRetries(maxRetries int) *KafkaOptionsBuilder {
	b.options.MaxRetries = maxRetries
	return b
}

func (b *KafkaOptionsBuilder) Build() *KafkaOptions {
	return b.options
}
