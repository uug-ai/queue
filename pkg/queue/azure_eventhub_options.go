package queue

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/go-playground/validator/v10"
)

const azureEventHubKafkaPort = "9093"

// AzureEventHubOptions configures Azure Event Hubs through its Kafka endpoint.
type AzureEventHubOptions struct {
	Namespace          string
	ConnectionString   string `validate:"required"`
	ConsumerEventHub   string `validate:"required"`
	RouterEventHub     string
	DeadletterEventHub string `validate:"required"`
	ConsumerGroup      string `validate:"required"`
	SessionTimeout     int
	AutoOffsetReset    string
	MaxRetries         int
}

func (a *AzureEventHubOptions) Validate() error {
	validate := validator.New()
	if err := validate.Struct(a); err != nil {
		return err
	}
	if _, err := eventHubHostFromConnectionString(a.ConnectionString); err != nil {
		return err
	}
	_, err := a.kafkaBroker()
	return err
}

func (a *AzureEventHubOptions) kafkaOptions() (*KafkaOptions, error) {
	broker, err := a.kafkaBroker()
	if err != nil {
		return nil, err
	}
	return NewKafkaOptions().
		SetConsumerTopic(a.ConsumerEventHub).
		SetRouterTopic(a.RouterEventHub).
		SetDeadletterTopic(a.DeadletterEventHub).
		SetBroker(broker).
		SetGroupID(a.ConsumerGroup).
		SetUsername("$ConnectionString").
		SetPassword(a.ConnectionString).
		SetMechanism("PLAIN").
		SetSecurity("SASL_SSL").
		SetSessionTimeout(a.SessionTimeout).
		SetAutoOffsetReset(a.AutoOffsetReset).
		SetMaxRetries(a.MaxRetries).
		Build(), nil
}

func (a *AzureEventHubOptions) kafkaBroker() (string, error) {
	host := strings.TrimSpace(a.Namespace)
	if host == "" {
		var err error
		host, err = eventHubHostFromConnectionString(a.ConnectionString)
		if err != nil {
			return "", err
		}
	} else {
		host = strings.TrimPrefix(host, "sb://")
		host = strings.TrimSuffix(host, "/")
		if !strings.Contains(host, ".") {
			host += ".servicebus.windows.net"
		}
	}
	return host + ":" + azureEventHubKafkaPort, nil
}

func eventHubHostFromConnectionString(connectionString string) (string, error) {
	for _, field := range strings.Split(connectionString, ";") {
		key, value, found := strings.Cut(field, "=")
		if !found || !strings.EqualFold(strings.TrimSpace(key), "Endpoint") {
			continue
		}
		endpoint, err := url.Parse(strings.TrimSpace(value))
		if err != nil || endpoint.Hostname() == "" {
			return "", fmt.Errorf("invalid Azure Event Hubs Endpoint in connection string")
		}
		return endpoint.Hostname(), nil
	}
	return "", fmt.Errorf("Azure Event Hubs connection string must contain an Endpoint")
}

type AzureEventHubOptionsBuilder struct {
	options *AzureEventHubOptions
}

func NewAzureEventHubOptions() *AzureEventHubOptionsBuilder {
	return &AzureEventHubOptionsBuilder{options: &AzureEventHubOptions{}}
}

func (b *AzureEventHubOptionsBuilder) SetNamespace(namespace string) *AzureEventHubOptionsBuilder {
	b.options.Namespace = namespace
	return b
}

func (b *AzureEventHubOptionsBuilder) SetConnectionString(connectionString string) *AzureEventHubOptionsBuilder {
	b.options.ConnectionString = connectionString
	return b
}

func (b *AzureEventHubOptionsBuilder) SetConsumerEventHub(name string) *AzureEventHubOptionsBuilder {
	b.options.ConsumerEventHub = name
	return b
}

func (b *AzureEventHubOptionsBuilder) SetConsumerQueue(name string) *AzureEventHubOptionsBuilder {
	return b.SetConsumerEventHub(name)
}

func (b *AzureEventHubOptionsBuilder) SetWorkflowsQueue(name string) *AzureEventHubOptionsBuilder {
	return b.SetConsumerEventHub(name)
}

func (b *AzureEventHubOptionsBuilder) SetWorkflowsStageQueue(name string) *AzureEventHubOptionsBuilder {
	return b.SetConsumerEventHub(name)
}

func (b *AzureEventHubOptionsBuilder) SetRouterEventHub(name string) *AzureEventHubOptionsBuilder {
	b.options.RouterEventHub = name
	return b
}

func (b *AzureEventHubOptionsBuilder) SetRouterQueue(name string) *AzureEventHubOptionsBuilder {
	return b.SetRouterEventHub(name)
}

func (b *AzureEventHubOptionsBuilder) SetDeadletterEventHub(name string) *AzureEventHubOptionsBuilder {
	b.options.DeadletterEventHub = name
	return b
}

func (b *AzureEventHubOptionsBuilder) SetDeadletterQueue(name string) *AzureEventHubOptionsBuilder {
	return b.SetDeadletterEventHub(name)
}

func (b *AzureEventHubOptionsBuilder) SetConsumerGroup(group string) *AzureEventHubOptionsBuilder {
	b.options.ConsumerGroup = group
	return b
}

func (b *AzureEventHubOptionsBuilder) SetGroupID(group string) *AzureEventHubOptionsBuilder {
	return b.SetConsumerGroup(group)
}

func (b *AzureEventHubOptionsBuilder) SetSessionTimeout(timeoutMilliseconds int) *AzureEventHubOptionsBuilder {
	b.options.SessionTimeout = timeoutMilliseconds
	return b
}

func (b *AzureEventHubOptionsBuilder) SetAutoOffsetReset(reset string) *AzureEventHubOptionsBuilder {
	b.options.AutoOffsetReset = reset
	return b
}

func (b *AzureEventHubOptionsBuilder) SetMaxRetries(maxRetries int) *AzureEventHubOptionsBuilder {
	b.options.MaxRetries = maxRetries
	return b
}

func (b *AzureEventHubOptionsBuilder) Build() *AzureEventHubOptions {
	return b.options
}
