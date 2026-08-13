//go:build !cgo

package queue

import (
	"strings"
	"testing"
)

func TestNewAzureEventHubRequiresCGO(t *testing.T) {
	options := NewAzureEventHubOptions().
		SetConnectionString(testEventHubConnectionString).
		SetConsumerEventHub("events").
		SetDeadletterEventHub("deadletter").
		SetConsumerGroup("pipeline").
		Build()

	_, err := NewAzureEventHub(options)
	if err == nil || !strings.Contains(err.Error(), "CGO_ENABLED=1") {
		t.Fatalf("expected CGO availability error, got %v", err)
	}
}
