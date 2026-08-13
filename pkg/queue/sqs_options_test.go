package queue

import "testing"

func TestSQSOptionsBuilder(t *testing.T) {
	options := NewSQSOptions().
		SetConsumerQueue("events").
		SetRouterQueue("router").
		SetDeadletterQueue("deadletter").
		SetRegion("eu-west-1").
		SetEndpoint("http://localhost:4566").
		SetCredentials("access", "secret").
		SetSessionToken("token").
		SetWaitTimeSeconds(12).
		SetVisibilityTimeout(45).
		SetMaxNumberOfMessages(5).
		SetMaxRetries(4).
		SetMessageGroupID("pipeline").
		Build()

	if err := options.Validate(); err != nil {
		t.Fatalf("expected options to validate: %v", err)
	}
	if options.ConsumerQueue != "events" || options.RouterQueue != "router" || options.DeadletterQueue != "deadletter" {
		t.Fatalf("unexpected queue names: %+v", options)
	}
	if options.Region != "eu-west-1" || options.Endpoint != "http://localhost:4566" {
		t.Fatalf("unexpected connection options: %+v", options)
	}
	if options.AccessKeyID != "access" || options.SecretAccessKey != "secret" || options.SessionToken != "token" {
		t.Fatalf("unexpected credentials")
	}
	if options.waitTimeSeconds() != 12 || options.visibilityTimeout() != 45 || options.maxNumberOfMessages() != 5 || options.maxRetries() != 4 {
		t.Fatalf("unexpected receive options: %+v", options)
	}
	if options.MessageGroupID != "pipeline" {
		t.Fatalf("message group ID = %q", options.MessageGroupID)
	}
}

func TestSQSOptionsDefaults(t *testing.T) {
	options := &SQSOptions{}
	if options.waitTimeSeconds() != 20 || options.visibilityTimeout() != 30 || options.maxNumberOfMessages() != 10 || options.maxRetries() != defaultMaxRetries {
		t.Fatalf("unexpected defaults: %+v", options)
	}
}

func TestSQSOptionsValidation(t *testing.T) {
	valid := NewSQSOptions().
		SetConsumerQueue("events").
		SetDeadletterQueue("deadletter").
		SetRegion("eu-west-1").
		Build()
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected default credential chain options to validate: %v", err)
	}

	tests := []struct {
		name    string
		options *SQSOptions
	}{
		{"missing consumer queue", NewSQSOptions().SetDeadletterQueue("deadletter").SetRegion("eu-west-1").Build()},
		{"missing deadletter queue", NewSQSOptions().SetConsumerQueue("events").SetRegion("eu-west-1").Build()},
		{"missing region", NewSQSOptions().SetConsumerQueue("events").SetDeadletterQueue("deadletter").Build()},
		{"partial credentials", NewSQSOptions().SetConsumerQueue("events").SetDeadletterQueue("deadletter").SetRegion("eu-west-1").SetCredentials("access", "").Build()},
		{"wait time too large", NewSQSOptions().SetConsumerQueue("events").SetDeadletterQueue("deadletter").SetRegion("eu-west-1").SetWaitTimeSeconds(21).Build()},
		{"batch too large", NewSQSOptions().SetConsumerQueue("events").SetDeadletterQueue("deadletter").SetRegion("eu-west-1").SetMaxNumberOfMessages(11).Build()},
		{"negative retries", NewSQSOptions().SetConsumerQueue("events").SetDeadletterQueue("deadletter").SetRegion("eu-west-1").SetMaxRetries(-1).Build()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.options.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestSQSWorkflowAliases(t *testing.T) {
	if queueName := NewSQSOptions().SetWorkflowsQueue("workflows").Build().ConsumerQueue; queueName != "workflows" {
		t.Fatalf("SetWorkflowsQueue = %q", queueName)
	}
	if queueName := NewSQSOptions().SetWorkflowsStageQueue("stage").Build().ConsumerQueue; queueName != "stage" {
		t.Fatalf("SetWorkflowsStageQueue = %q", queueName)
	}
}
