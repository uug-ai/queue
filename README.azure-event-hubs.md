# Azure Event Hubs Provider

[Back to Queue overview](README.md)

The Azure Event Hubs provider uses the service's Kafka-compatible endpoint. It
converts Azure namespace and connection-string settings into the package's Kafka
client, preserving synchronous delivery confirmation, manual offset commits,
routing, bounded retries, dead-lettering, and raw-message handling.

Each Event Hub is addressed as a Kafka topic. No Azure SDK dependency is needed.

## Build Requirement

Event Hubs support requires `CGO_ENABLED=1` because it uses the Confluent Kafka
client and `librdkafka`. RabbitMQ-only applications can still compile with
`CGO_ENABLED=0`. Selecting Event Hubs in a non-CGO build returns the same clear
Kafka-unavailable error.

## Azure Prerequisites

Create the following resources before connecting:

- An Event Hubs namespace with Kafka endpoint support
- A consumer Event Hub
- A dead-letter Event Hub
- A router Event Hub when handlers can return `PipelineForward`
- A consumer group for the application
- A shared access policy authorized for every Event Hub the client uses

Use a namespace-level connection string when one client consumes from one Event
Hub and publishes to router or dead-letter Event Hubs. An entity-scoped policy
usually cannot access the other destinations.

The package does not create Event Hubs, consumer groups, access policies, or
namespace resources.

## Quick Start

```go
package main

import (
    "log"

    "github.com/uug-ai/queue/pkg/queue"
)

func main() {
    options := queue.NewAzureEventHubOptions().
        SetConnectionString(
            "Endpoint=sb://example.servicebus.windows.net/;" +
                "SharedAccessKeyName=queue;SharedAccessKey=secret",
        ).
        SetConsumerEventHub("kcloud-analysis-queue").
        SetRouterEventHub("kcloud-event-queue").
        SetDeadletterEventHub("kcloud-deadletter-queue").
        SetConsumerGroup("hub-pipeline-analysis").
        Build()

    client, err := queue.New(options)
    if err != nil {
        log.Fatal(err)
    }
    if err := client.Client.Connect(); err != nil {
        log.Fatal(err)
    }
    defer client.Client.Close()

    payload := []byte(`{"request":"persist"}`)
    if err := client.Client.Publish("kcloud-event-queue", payload); err != nil {
        log.Fatal(err)
    }
}
```

`NewAzureEventHub(options)` can be used directly when access to the embedded
Kafka client is required.

## Configuration

| Option | Required | Description |
| --- | --- | --- |
| `ConnectionString` | Yes | Event Hubs SAS connection string |
| `ConsumerEventHub` | Yes | Event Hub consumed by read and route loops |
| `DeadletterEventHub` | Yes | Event Hub for failed or exhausted messages |
| `ConsumerGroup` | Yes | Consumer group used as Kafka `group.id` |
| `Namespace` | No | Short namespace or fully qualified host override |
| `RouterEventHub` | No | Destination used by `PipelineForward` |
| `SessionTimeout` | No | Session timeout in ms; defaults to `10000` |
| `AutoOffsetReset` | No | Initial offset policy; defaults to `earliest` |
| `MaxRetries` | No | Retry limit before dead-lettering; defaults to `10` |

The namespace is normally derived from the `Endpoint` field in the connection
string. `SetNamespace` can override it with either `example` or
`example.servicebus.windows.net`.

Queue-compatible aliases are available:

- `SetConsumerQueue` sets `ConsumerEventHub`
- `SetRouterQueue` sets `RouterEventHub`
- `SetDeadletterQueue` sets `DeadletterEventHub`
- `SetGroupID` sets `ConsumerGroup`
- `SetWorkflowsQueue` and `SetWorkflowsStageQueue` set `ConsumerEventHub`

### Environment Example

```text
AZURE_EVENTHUB_CONNECTION_STRING=Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=queue;SharedAccessKey=secret
AZURE_EVENTHUB_CONSUMER=kcloud-analysis-queue
AZURE_EVENTHUB_ROUTER=kcloud-event-queue
AZURE_EVENTHUB_DEADLETTER=kcloud-deadletter-queue
AZURE_EVENTHUB_CONSUMER_GROUP=hub-pipeline-analysis
```

The package does not read environment variables itself. Applications map their
configuration source into the options builder. Treat the connection string as a
secret and do not log the resulting options.

## Kafka Endpoint Mapping

The provider configures the underlying Kafka client with Azure's required
settings:

| Kafka setting | Value |
| --- | --- |
| `bootstrap.servers` | `<namespace-host>:9093` |
| `security.protocol` | `SASL_SSL` |
| `sasl.mechanisms` | `PLAIN` |
| `sasl.username` | `$ConnectionString` |
| `sasl.password` | Full Event Hubs connection string |
| `group.id` | Configured `ConsumerGroup` |

Automatic offset commit and storage remain disabled, as described in the
[Kafka guide](README.kafka.md).

## Delivery and Retry Semantics

Publishing, consumption, offset commits, retries, and dead-letter handling are
provided by the Kafka implementation:

- `Publish` waits for the Event Hubs delivery report.
- Consumed offsets are committed after the handler action completes.
- `PipelineForward` publishes to `RouterEventHub`.
- `PipelineRetry` republishes to `ConsumerEventHub` with `x-retry-count`.
- Exhausted and invalid messages are published to `DeadletterEventHub`.

Event Hubs has no automatic dead-letter behavior for this integration. The
configured dead-letter Event Hub is an application-level destination and must
be monitored and retained appropriately.

Processing is at least once. Handlers and downstream writes must tolerate
duplicates caused by a failure after publishing but before committing an offset.

## Workflows

Workflow services can use the intent-specific aliases:

```go
engineOptions := queue.NewAzureEventHubOptions().
    SetWorkflowsQueue("hub-workflows-queue")

stageOptions := queue.NewAzureEventHubOptions().
    SetWorkflowsStageQueue("hub-workflows-loitering")
```

A stage worker publishes completed runs back to the workflow engine Event Hub.

## Testing

Run Event Hubs broker-free tests:

```bash
go test ./pkg/queue -run 'TestAzureEventHub|TestNewAzureEventHub' -v
```

The repository does not provision an Event Hubs namespace for integration
tests. Connectivity, authorization, Event Hub names, consumer groups, quotas,
and retention must also be verified in the target Azure environment.
