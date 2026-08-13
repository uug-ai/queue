# RabbitMQ Provider

[Back to Queue overview](README.md)

The RabbitMQ provider uses
[`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go). It
creates separate producer and consumer channels, declares quorum queues, applies
consumer prefetch, reconnects closed connections, and reports unroutable
publishes.

## Quick Start

```go
package main

import (
    "log"

    "github.com/uug-ai/queue/pkg/queue"
)

func main() {
    options := queue.NewRabbitOptions().
        SetConsumerQueue("kcloud-analysis-queue").
        SetRouterQueue("kcloud-event-queue").
        SetDeadletterQueue("kcloud-deadletter-queue").
        SetHost("rabbitmq.example.com:5672").
        SetUsername("username").
        SetPassword("password").
        SetPrefetchCount(10).
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

`NewRabbitMQ(options)` can be used directly when access to RabbitMQ-specific
methods or channels is required.

## Configuration

| Option | Required | Description |
| --- | --- | --- |
| `ConsumerQueue` | Yes | Queue consumed by `ReadMessages` and `RouteMessages` |
| `DeadletterQueue` | Yes | Destination for malformed or failed messages |
| `Host` | Yes | Host and port, with an optional AMQP scheme |
| `Username` | Yes | RabbitMQ username |
| `Password` | Yes | RabbitMQ password |
| `RouterQueue` | No | Destination used by `PipelineForward` |
| `AnalysisQueue` | No | Optional analysis destination |
| `PrefetchCount` | No | Maximum unacknowledged deliveries; defaults to `5` |
| `MaxRetries` | No | Retry limit before dead-lettering; defaults to `10` |
| `Exchange` | No | Compatibility field; the default exchange is used |
| `Uri` | No | Compatibility field; connection uses host and credentials |
| `TLS` | No | Enables an `amqps://` connection |
| `TLSInsecureSkipVerify` | No | Skips certificate verification |
| `TLSCACertFile` | No | PEM file containing a custom CA certificate |

The builder provides matching setters, including `SetConsumerQueue`,
`SetDeadletterQueue`, `SetRouterQueue`, `SetHost`, `SetUsername`, `SetPassword`,
`SetPrefetchCount`, `SetMaxRetries`, and the TLS setters.

### Environment Example

```text
RABBITMQ_HOST=rabbitmq.example.com:5672
RABBITMQ_USERNAME=username
RABBITMQ_PASSWORD=password
RABBITMQ_CONSUMER_QUEUE=kcloud-analysis-queue
RABBITMQ_ROUTER_QUEUE=kcloud-event-queue
RABBITMQ_DEADLETTER_QUEUE=kcloud-deadletter-queue
```

The package does not read environment variables itself. Applications map their
configuration source into the options builder.

## Queue Declaration

`Connect` declares the consumer and dead-letter destinations as durable quorum
queues. The router, analysis, and arbitrary publish destinations must already
exist or be declared by their owning consumers.

Publishing uses the default exchange with the destination queue as its routing
key. Messages are marked mandatory. If no queue is bound to the routing key,
RabbitMQ returns the message instead of silently dropping it.

Use `SetReturnHandler` on the concrete `RabbitMQ` client to report returned
messages to metrics or structured logging. Without a handler, returns are logged
through the standard logger.

```go
rabbit, err := queue.NewRabbitMQ(options)
if err != nil {
    log.Fatal(err)
}
rabbit.SetReturnHandler(func(returned amqp.Return) {
    log.Printf("unroutable queue=%s reason=%s", returned.RoutingKey, returned.ReplyText)
})
```

## TLS

Prefixing `Host` with `amqps://` automatically enables TLS. TLS can also be
enabled explicitly:

```go
options := queue.NewRabbitOptions().
    SetConsumerQueue("events").
    SetDeadletterQueue("events-deadletter").
    SetHost("rabbitmq.example.com:5671").
    SetUsername("username").
    SetPassword("password").
    SetTLS(true).
    SetTLSCACertFile("/etc/ssl/rabbitmq/ca.pem").
    Build()
```

Use `SetTLSInsecureSkipVerify(true)` only in controlled development environments.

## Consumption Semantics

RabbitMQ deliveries use manual acknowledgements. `ReadMessages` decodes each
payload as `models.PipelineEvent`, invokes the handler, performs the selected
pipeline action, and acknowledges the original delivery.

- Invalid JSON is sent to the dead-letter queue.
- `PipelineForward` removes the completed stage and publishes to `RouterQueue`
  when another stage remains.
- `PipelineError` publishes the handler's event to `DeadletterQueue`.
- `PipelineRetry` republishes to `ConsumerQueue` after a five-second backoff.
- `PipelineCancel` only acknowledges the delivery.

RabbitMQ retries carry an `x-retry-count` message header. After `MaxRetries`, the
payload is parked on `DeadletterQueue` rather than requeued indefinitely.

Processing is at least once. A connection failure after side effects but before
acknowledgement can cause redelivery.

## Raw Messages

The concrete `RabbitMQ` client also supports queues whose payload is not a
`PipelineEvent`:

- `ReadRawMessages` runs a long-lived raw consumer.
- `ReadOneRaw` reads at most one message for verification and drain tools.

`RawMessageHandler` receives the original bytes and returns a pipeline action,
an optional forwarding payload, and retry backoff seconds.

## Workflow Queue Aliases

`SetWorkflowsQueue` and `SetWorkflowsStageQueue` both set `ConsumerQueue` while
making the caller's role explicit:

```go
engineOptions := queue.NewRabbitOptions().
    SetWorkflowsQueue("hub-workflows-queue")

stageOptions := queue.NewRabbitOptions().
    SetWorkflowsStageQueue("hub-workflows-loitering")
```

A stage worker publishes its completed run back to the engine queue with
`Publish("hub-workflows-queue", payload)`.

## Reconnect and Failure Recovery

`Publish` and the consume loops reconnect when the AMQP connection or channel is
closed. A failed publish is passed to the optional `DisasterRecoveryHandler`:

```go
rabbit.SetDisasterRecoveryHandler(func(payload []byte) error {
    return persistForReplay(payload)
})
```

Without a handler, disaster recovery is a no-op and the caller still receives
the publish error.

## Testing

Run RabbitMQ-focused unit tests:

```bash
go test ./pkg/queue -run 'TestRabbit|TestRetry' -v
```

The integration test runs only when RabbitMQ connection environment variables
are provided; otherwise it is skipped.
