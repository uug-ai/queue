package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	queue "github.com/uug-ai/queue/pkg/queue"
)

func TestParseReplayDefaultsToDryRun(t *testing.T) {
	config, err := parseDLQFlags("replay", []string{
		"--provider", "rabbitmq",
		"--dead-letter", "deadletter",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseDLQFlags: %v", err)
	}
	if config.execute {
		t.Fatal("replay must default to dry-run")
	}
}

func TestDLQHelpDoesNotExposeEnvironmentSecrets(t *testing.T) {
	secrets := []string{"rabbit-value-92", "kafka-value-47", "azure-value-31", "token-value-68"}
	t.Setenv("RABBITMQ_PASSWORD", secrets[0])
	t.Setenv("KAFKA_PASSWORD", secrets[1])
	t.Setenv("AZURE_EVENTHUB_CONNECTION_STRING", secrets[2])
	t.Setenv("SQS_SESSION_TOKEN", secrets[3])

	var output bytes.Buffer
	_, err := parseDLQFlags("inspect", []string{"--help"}, &output)
	if err == nil {
		t.Fatal("expected help sentinel")
	}
	for _, secret := range secrets {
		if strings.Contains(output.String(), secret) {
			t.Fatalf("help output exposed secret %q", secret)
		}
	}
}

func TestParseDLQFlagsLoadsSecretsFromEnvironment(t *testing.T) {
	t.Setenv("RABBITMQ_PASSWORD", "rabbit-secret")
	t.Setenv("KAFKA_PASSWORD", "kafka-secret")
	t.Setenv("AZURE_EVENTHUB_CONNECTION_STRING", "azure-secret")
	t.Setenv("SQS_SESSION_TOKEN", "sqs-secret")

	config, err := parseDLQFlags("inspect", nil, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseDLQFlags: %v", err)
	}
	if config.rabbitPassword != "rabbit-secret" ||
		config.kafkaPassword != "kafka-secret" ||
		config.azureConnectionString != "azure-secret" ||
		config.sqsSessionToken != "sqs-secret" {
		t.Fatalf("environment secrets were not loaded")
	}
}

func TestPrintInspectionGroupsSources(t *testing.T) {
	var output bytes.Buffer
	printInspection(&output, queue.DeadLetterInspectResult{
		Scanned: 3,
		Matched: 3,
		Legacy:  1,
		Groups: map[string]queue.DeadLetterGroup{
			"monitor": {
				Source: "monitor",
				Count:  2,
				Oldest: time.Date(2026, 9, 15, 1, 0, 0, 0, time.UTC),
				Newest: time.Date(2026, 9, 16, 1, 0, 0, 0, time.UTC),
			},
			queue.UnknownSourceQueue: {
				Source: queue.UnknownSourceQueue,
				Count:  1,
			},
		},
	})
	text := output.String()
	for _, expected := range []string{"SOURCE QUEUE", "monitor", "unknown", "TOTAL", "Legacy/unknown: 1"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("output %q does not contain %q", text, expected)
		}
	}
}

func TestSeedDeadLettersDryRunGroupsSources(t *testing.T) {
	result, err := seedDeadLetters(context.Background(), nil, dlqCommandConfig{
		deadLetterQueue: "deadletter",
		sources:         "monitor, analysis,monitor",
		count:           5,
		reason:          string(queue.DeadLetterReasonHandlerError),
	})
	if err != nil {
		t.Fatalf("seedDeadLetters: %v", err)
	}
	if result.Planned != 5 || result.Published != 0 || result.BySource["monitor"] != 3 || result.BySource["analysis"] != 2 {
		t.Fatalf("seed result = %+v", result)
	}
}

func TestBuildQueueDisablesKafkaAutoTopicCreationForDLQAdmin(t *testing.T) {
	options := newKafkaDLQOptions(dlqCommandConfig{
		provider:        "kafka",
		deadLetterQueue: "deadletter",
		kafkaBroker:     "kafka:9092",
		kafkaGroupID:    "dlq-admin",
	})
	if !options.DisableAutoTopicCreation {
		t.Fatal("DLQ administrative Kafka clients must disable automatic topic creation")
	}
}

func TestNewDLQClientRequiresExistingRabbitQueues(t *testing.T) {
	client, err := newDLQClient(dlqCommandConfig{
		provider:        "rabbitmq",
		deadLetterQueue: "deadletter",
		rabbitHost:      "rabbitmq:5672",
		rabbitUsername:  "guest",
		rabbitPassword:  "guest",
	})
	if err != nil {
		t.Fatalf("newDLQClient: %v", err)
	}
	options, ok := client.Options.(*queue.RabbitOptions)
	if !ok {
		t.Fatalf("options type = %T, want *queue.RabbitOptions", client.Options)
	}
	if !options.RequireExistingQueues {
		t.Fatal("DLQ administrative RabbitMQ clients must not declare missing queues")
	}
}
