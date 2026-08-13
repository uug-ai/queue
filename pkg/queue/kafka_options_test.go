package queue

import "testing"

func TestKafkaOptionsBuilder(t *testing.T) {
	opts := NewKafkaOptions().
		SetConsumerQueue("events").
		SetRouterTopic("router").
		SetDeadletterQueue("deadletter").
		SetBroker("kafka:9092").
		SetGroupID("pipeline").
		SetUsername("user").
		SetPassword("pass").
		SetMechanism("PLAIN").
		SetSecurity("SASL_PLAINTEXT").
		SetSessionTimeout(12000).
		SetAutoOffsetReset("latest").
		SetMaxRetries(4).
		Build()

	if opts.ConsumerTopic != "events" || opts.RouterTopic != "router" || opts.DeadletterTopic != "deadletter" {
		t.Fatalf("unexpected Kafka topics: %+v", opts)
	}
	if opts.Broker != "kafka:9092" || opts.GroupID != "pipeline" {
		t.Fatalf("unexpected Kafka connection options: %+v", opts)
	}
	if opts.Username != "user" || opts.Password != "pass" || opts.Mechanism != "PLAIN" || opts.Security != "SASL_PLAINTEXT" {
		t.Fatalf("unexpected Kafka security options: %+v", opts)
	}
	if opts.SessionTimeout != 12000 || opts.AutoOffsetReset != "latest" || opts.MaxRetries != 4 {
		t.Fatalf("unexpected Kafka consumer options: %+v", opts)
	}
	if err := opts.Validate(); err != nil {
		t.Fatalf("expected options to validate: %v", err)
	}
}

func TestKafkaOptionsValidation(t *testing.T) {
	valid := NewKafkaOptions().
		SetConsumerTopic("events").
		SetDeadletterTopic("deadletter").
		SetBroker("kafka:9092").
		SetGroupID("pipeline").
		Build()
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected minimal options to validate: %v", err)
	}

	tests := []struct {
		name string
		opts *KafkaOptions
	}{
		{"consumer topic", NewKafkaOptions().SetDeadletterTopic("deadletter").SetBroker("kafka:9092").SetGroupID("pipeline").Build()},
		{"deadletter topic", NewKafkaOptions().SetConsumerTopic("events").SetBroker("kafka:9092").SetGroupID("pipeline").Build()},
		{"broker", NewKafkaOptions().SetConsumerTopic("events").SetDeadletterTopic("deadletter").SetGroupID("pipeline").Build()},
		{"group id", NewKafkaOptions().SetConsumerTopic("events").SetDeadletterTopic("deadletter").SetBroker("kafka:9092").Build()},
	}
	for _, test := range tests {
		t.Run("missing "+test.name, func(t *testing.T) {
			if err := test.opts.Validate(); err == nil {
				t.Fatalf("expected validation error for missing %s", test.name)
			}
		})
	}
}

func TestKafkaWorkflowsQueueAliases(t *testing.T) {
	if topic := NewKafkaOptions().SetWorkflowsQueue("workflows").Build().ConsumerTopic; topic != "workflows" {
		t.Fatalf("SetWorkflowsQueue topic = %q, want workflows", topic)
	}
	if topic := NewKafkaOptions().SetWorkflowsStageQueue("stage").Build().ConsumerTopic; topic != "stage" {
		t.Fatalf("SetWorkflowsStageQueue topic = %q, want stage", topic)
	}
}
