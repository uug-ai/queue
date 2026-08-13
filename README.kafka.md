# Kafka Provider

[Back to Queue overview](README.md)

The Kafka provider uses
[`github.com/confluentinc/confluent-kafka-go/v2`](https://github.com/confluentinc/confluent-kafka-go)
and implements synchronous delivery confirmation, manual offset commits,
consumer-group subscriptions, reconnects, bounded retries, and dead-letter
topics.

## Build Requirement

Kafka support requires `CGO_ENABLED=1` because the Confluent client wraps
`librdkafka`. The package includes a `!cgo` stub, so RabbitMQ-only applications
can still compile with `CGO_ENABLED=0`. Selecting Kafka in a non-CGO build returns
an explanatory error.

## Quick Start

```go
package main

import (
    "log"

    "github.com/uug-ai/queue/pkg/queue"
)

func main() {
    options := queue.NewKafkaOptions().
        SetConsumerTopic("kcloud-analysis-queue").
        SetRouterTopic("kcloud-event-queue").
        SetDeadletterTopic("kcloud-deadletter-queue").
        SetBroker("kafka.example.com:9094").
        SetGroupID("hub-pipeline-analysis").
        SetUsername("username").
        SetPassword("password").
        SetMechanism("PLAIN").
        SetSecurity("SASL_PLAINTEXT").
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

`NewKafka(options)` can be used directly when access to Kafka-specific methods
or clients is required.

## Configuration

| Option | Required | Description |
| --- | --- | --- |
| `ConsumerTopic` | Yes | Topic consumed by read and route loops |
| `DeadletterTopic` | Yes | Destination for failed or exhausted messages |
| `Broker` | Yes | Comma-separated Kafka bootstrap servers |
| `GroupID` | Yes | Consumer group ID |
| `RouterTopic` | No | Destination used by `PipelineForward` |
| `Username` | No | SASL username |
| `Password` | No | SASL password |
| `Mechanism` | No | SASL mechanism, such as `PLAIN` or `SCRAM-SHA-512` |
| `Security` | No | Kafka security protocol |
| `SessionTimeout` | No | Session timeout in ms; defaults to `10000` |
| `AutoOffsetReset` | No | Initial offset policy; defaults to `earliest` |
| `MaxRetries` | No | Retry limit before dead-lettering; defaults to `10` |

Topic-oriented setters have queue aliases where useful:

- `SetConsumerTopic` and `SetConsumerQueue`
- `SetRouterTopic` and `SetRouterQueue`
- `SetDeadletterTopic` and `SetDeadletterQueue`
- `SetWorkflowsQueue` and `SetWorkflowsStageQueue`

### Environment Example

```text
KAFKA_BROKER=kafka.example.com:9094
KAFKA_GROUP_ID=hub-pipeline-analysis
KAFKA_USERNAME=username
KAFKA_PASSWORD=password
KAFKA_MECHANISM=PLAIN
KAFKA_SECURITY=SASL_PLAINTEXT
KAFKA_CONSUMER_TOPIC=kcloud-analysis-queue
KAFKA_ROUTER_TOPIC=kcloud-event-queue
KAFKA_DEADLETTER_TOPIC=kcloud-deadletter-queue
```

The package does not read environment variables itself. Applications map their
configuration source into the options builder.

## Topics

The client does not create topics. The consumer, router, and dead-letter topics
must exist or broker-side automatic topic creation must be enabled. Explicitly
provisioning topics is recommended so partition counts, replication, retention,
and access controls are intentional.

## Authentication and Encryption

Unauthenticated local clusters can omit username, password, mechanism, and
security settings. For SASL, configure all settings required by the cluster:

```go
options := queue.NewKafkaOptions().
    SetConsumerTopic("events").
    SetDeadletterTopic("events-deadletter").
    SetBroker("kafka-1.example.com:9094,kafka-2.example.com:9094").
    SetGroupID("events-worker").
    SetUsername("username").
    SetPassword("password").
    SetMechanism("SCRAM-SHA-512").
    SetSecurity("SASL_SSL").
    Build()
```

TLS certificate handling follows the Confluent client and system trust store.

## Publishing Semantics

`Publish` submits a message and waits up to five seconds for its delivery report.
Broker delivery errors are returned to the caller. On failure, the client
reconnects once and retries the publish before invoking the optional disaster
recovery handler.

`PublishWithDelay` schedules a publish in a goroutine after the requested number
of seconds. Its interface does not return asynchronous publish errors, so use
`SetDisasterRecoveryHandler` when delayed payloads must be preserved externally.

## Consumption and Offset Semantics

Automatic offset commits and automatic offset storage are disabled. The client
commits the consumed message only after its handler action and any required
publish complete.

- Invalid JSON is published to `DeadletterTopic` before commit.
- `PipelineForward` removes the completed stage and publishes to `RouterTopic`
  when another stage remains.
- `PipelineError` publishes the handler's event to `DeadletterTopic`.
- `PipelineRetry` republishes to `ConsumerTopic` after the requested backoff.
- `PipelineCancel` commits without forwarding.

This ordering provides at-least-once processing. A process failure after the
downstream publish but before the offset commit can produce duplicates, so
handlers and downstream writes should be idempotent.

## Retries and Dead-Lettering

Retries carry an `x-retry-count` Kafka header. The original offset is committed
only after the replacement message is delivered. Once `MaxRetries` is reached,
the payload is delivered to `DeadletterTopic` instead.

The retry backoff blocks the current consume loop to preserve publish-before-
commit ordering. Long backoffs should be chosen with the consumer group's poll
and processing expectations in mind.

## Routing and Workflows

`RouteMessages` reads the first pipeline stage and publishes to the legacy topic
name `kcloud-<stage>-queue`.

`SetWorkflowsQueue` and `SetWorkflowsStageQueue` both set `ConsumerTopic` while
making the caller's role explicit. Stage workers publish completed runs back to
the workflow engine topic.

## Raw Messages

The concrete `Kafka` client exposes `ReadRawMessages` for payloads that are not
`models.PipelineEvent`. A `RawMessageHandler` receives the message bytes and
returns a pipeline action, optional forwarding payload, and retry backoff.

## Disaster Recovery

Register a fallback for payloads that cannot be published:

```go
kafkaClient.SetDisasterRecoveryHandler(func(payload []byte) error {
    return persistForReplay(payload)
})
```

Without a handler, disaster recovery is a no-op and the caller still receives
the publish error.

## Testing

Run Kafka-focused broker-free tests:

```bash
go test ./pkg/queue -run 'TestKafka|TestNewSelectsKafka' -v
```

Verify the non-CGO fallback independently:

```bash
CGO_ENABLED=0 go test ./...
```

The repository does not currently start a Kafka broker for integration tests.
Connectivity, authentication, topic ACLs, and broker policy should also be
verified in the target environment.
