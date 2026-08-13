//go:build cgo

package queue

import "testing"

func newTestAzureEventHubOptions() *AzureEventHubOptions {
	return NewAzureEventHubOptions().
		SetConnectionString(testEventHubConnectionString).
		SetConsumerEventHub("events").
		SetRouterEventHub("router").
		SetDeadletterEventHub("deadletter").
		SetConsumerGroup("pipeline").
		Build()
}

func TestNewAzureEventHub(t *testing.T) {
	client, err := NewAzureEventHub(newTestAzureEventHubOptions())
	if err != nil {
		t.Fatalf("NewAzureEventHub: %v", err)
	}
	if client.Kafka == nil {
		t.Fatal("expected embedded Kafka client")
	}
	if client.Kafka.options.Broker != "example.servicebus.windows.net:9093" {
		t.Fatalf("broker = %q", client.Kafka.options.Broker)
	}
}

func TestNewSelectsAzureEventHubClient(t *testing.T) {
	client, err := New(newTestAzureEventHubOptions())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := client.Client.(*AzureEventHub); !ok {
		t.Fatalf("expected *AzureEventHub, got %T", client.Client)
	}
}
