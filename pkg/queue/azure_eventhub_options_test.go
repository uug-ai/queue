package queue

import "testing"

const testEventHubConnectionString = "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=queue;SharedAccessKey=secret"

func TestAzureEventHubOptionsBuildKafkaConfiguration(t *testing.T) {
	options := NewAzureEventHubOptions().
		SetConnectionString(testEventHubConnectionString).
		SetConsumerEventHub("events").
		SetRouterEventHub("router").
		SetDeadletterEventHub("deadletter").
		SetConsumerGroup("pipeline").
		SetSessionTimeout(12000).
		SetAutoOffsetReset("latest").
		SetMaxRetries(4).
		Build()

	if err := options.Validate(); err != nil {
		t.Fatalf("expected options to validate: %v", err)
	}
	kafkaOptions, err := options.kafkaOptions()
	if err != nil {
		t.Fatalf("kafkaOptions: %v", err)
	}
	if kafkaOptions.Broker != "example.servicebus.windows.net:9093" {
		t.Fatalf("broker = %q", kafkaOptions.Broker)
	}
	if kafkaOptions.Username != "$ConnectionString" || kafkaOptions.Password != testEventHubConnectionString {
		t.Fatalf("unexpected Event Hubs Kafka credentials")
	}
	if kafkaOptions.Mechanism != "PLAIN" || kafkaOptions.Security != "SASL_SSL" {
		t.Fatalf("unexpected Event Hubs Kafka security: %+v", kafkaOptions)
	}
	if kafkaOptions.ConsumerTopic != "events" || kafkaOptions.RouterTopic != "router" || kafkaOptions.DeadletterTopic != "deadletter" {
		t.Fatalf("unexpected Event Hub names: %+v", kafkaOptions)
	}
	if kafkaOptions.GroupID != "pipeline" || kafkaOptions.SessionTimeout != 12000 || kafkaOptions.AutoOffsetReset != "latest" || kafkaOptions.MaxRetries != 4 {
		t.Fatalf("unexpected consumer options: %+v", kafkaOptions)
	}
}

func TestAzureEventHubOptionsNamespace(t *testing.T) {
	options := NewAzureEventHubOptions().
		SetNamespace("example").
		SetConnectionString(testEventHubConnectionString).
		SetConsumerEventHub("events").
		SetDeadletterEventHub("deadletter").
		SetGroupID("pipeline").
		Build()

	broker, err := options.kafkaBroker()
	if err != nil {
		t.Fatalf("kafkaBroker: %v", err)
	}
	if broker != "example.servicebus.windows.net:9093" {
		t.Fatalf("broker = %q", broker)
	}
	if options.ConsumerGroup != "pipeline" {
		t.Fatalf("consumer group = %q", options.ConsumerGroup)
	}
}

func TestAzureEventHubOptionsValidation(t *testing.T) {
	tests := []struct {
		name    string
		options *AzureEventHubOptions
	}{
		{
			name: "missing endpoint and namespace",
			options: NewAzureEventHubOptions().
				SetConnectionString("SharedAccessKeyName=queue;SharedAccessKey=secret").
				SetConsumerEventHub("events").
				SetDeadletterEventHub("deadletter").
				SetConsumerGroup("pipeline").
				Build(),
		},
		{
			name: "missing consumer Event Hub",
			options: NewAzureEventHubOptions().
				SetConnectionString(testEventHubConnectionString).
				SetDeadletterEventHub("deadletter").
				SetConsumerGroup("pipeline").
				Build(),
		},
		{
			name: "missing deadletter Event Hub",
			options: NewAzureEventHubOptions().
				SetConnectionString(testEventHubConnectionString).
				SetConsumerEventHub("events").
				SetConsumerGroup("pipeline").
				Build(),
		},
		{
			name: "missing consumer group",
			options: NewAzureEventHubOptions().
				SetConnectionString(testEventHubConnectionString).
				SetConsumerEventHub("events").
				SetDeadletterEventHub("deadletter").
				Build(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.options.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
