# Queue

[![Go Version](https://img.shields.io/badge/Go-1.25-blue.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![GoDoc](https://godoc.org/github.com/uug-ai/queue?status.svg)](https://godoc.org/github.com/uug-ai/queue)
[![Go Report Card](https://goreportcard.com/badge/github.com/uug-ai/queue)](https://goreportcard.com/report/github.com/uug-ai/queue)
[![codecov](https://codecov.io/gh/uug-ai/queue/graph/badge.svg?token=0mzdj1QR37)](https://codecov.io/gh/uug-ai/queue)
[![Release](https://img.shields.io/github/release/uug-ai/queue.svg)](https://github.com/uug-ai/queue/releases/latest)

Queue is a Go library that gives Hub pipeline and workflow services one API for
publishing, consuming, routing, retrying, and dead-lettering messages across
different message brokers.

## Supported Brokers

| Broker | Implementation | Guide |
| --- | --- | --- |
| RabbitMQ | `amqp091-go` | [RabbitMQ guide](README.rabbitmq.md) |
| Kafka | Confluent Kafka Go client | [Kafka guide](README.kafka.md) |
| Azure Event Hubs | Kafka-compatible endpoint | [Event Hubs guide](README.azure-event-hubs.md) |
| Amazon SQS | AWS SDK for Go v2 | [SQS guide](README.sqs.md) |

The broker guides contain provider-specific setup, configuration, security,
delivery semantics, examples, and troubleshooting notes.

## Installation

```bash
go get github.com/uug-ai/queue
```

## Core Abstractions

### Options

Each broker exposes a typed options builder that implements `QueueOptions`.
Options are validated when the client is created.

### Client

`New(options)` selects the broker from the concrete options type and returns a
`Queue` containing the validated options and a `QueueInterface` client.

```go
func connect(options queue.QueueOptions) (*queue.Queue, error) {
    client, err := queue.New(options)
    if err != nil {
        return nil, err
    }
    if err := client.Client.Connect(); err != nil {
        return nil, err
    }
    return client, nil
}
```

Call `Close` when the client is no longer needed.

### Common Operations

`QueueInterface` provides the shared broker lifecycle and messaging operations:

- `Connect`, `Reconnect`, and `Close`
- `Publish` and `PublishWithDelay`
- `ReadMessages` and `RouteMessages`
- `AddToDeadletter` and `DisasterRecovery`
- `SetDisasterRecoveryHandler`
- `LoadMessages`

Dead-letter inspection and replay are deliberately exposed through the separate
`DeadLetterAdmin` interface. Runtime consumers therefore do not need
administrative methods, while operational tooling can use one contract across
all supported brokers.

RabbitMQ services that need to deliberately park raw deliveries for operational
testing can enable confirmed delivery and call `ReadMessagesToDeadletter`. The
method preserves the original body and acknowledges it only after the broker
confirms the dead-letter envelope.

### Pipeline Actions

Message handlers return a `models.PipelineAction`. The queue client maps that
action to broker operations:

| Action | Result |
| --- | --- |
| `PipelineForward` | Advance and publish the event to the configured router |
| `PipelineCancel` | Complete the message without forwarding |
| `PipelineRetry` | Republish with a bounded retry count |
| `PipelineError` | Publish to the configured dead-letter destination |

All providers implement at-least-once processing. Handlers and downstream
writes should therefore be idempotent.

## Dead-Letter Envelopes

`AddToDeadletter` stores a versioned `uug.ai/dead-letter/v1` envelope containing:

- The exact original payload, encoded safely even when it is not valid JSON
- The source queue or topic
- The dead-letter destination
- The failure reason, attempt count, and UTC timestamp
- Optional service and provider-neutral attributes

For compatibility with existing services, calling `Publish` with the configured
dead-letter destination is also enveloped automatically. Existing callers do not
need to migrate in lockstep to start recording their source queue. Runtime
payloads are always wrapped, even if their JSON resembles an envelope, so payload
data cannot choose its own replay destination. Administrative tools that need to
set source metadata use `DeadLetterAdmin.PublishDeadLetter`.

Existing raw dead-letter messages remain readable. They are reported with the
source `unknown` and require an explicit replay destination because the library
cannot safely infer where they came from.

## Inspecting and Replaying Dead-Letter Messages

The operational commands are provided by the separate
[`uug-ai/cli`](https://github.com/uug-ai/cli) project. The underlying
`DeadLetterAdmin` implementations always publish first and settle the source
message only after broker acknowledgement:

- RabbitMQ uses a dedicated confirm-mode channel, then acknowledges the delivery.
- SQS waits for `SendMessage`, then deletes the message using its receipt handle.
- Kafka and Azure Event Hubs wait for delivery, then commit the source offset.

Inspection remains non-destructive but not side-effect-free: RabbitMQ
deliveries are durably republished before the originals are acknowledged, and
SQS visibility is reset after scanning. These operations may affect ordering.
Kafka/Event Hubs inspection does not commit offsets.

## Choosing a Broker

Use the broker-specific builder and follow its guide:

- [Configure RabbitMQ](README.rabbitmq.md)
- [Configure Kafka](README.kafka.md)
- [Configure Azure Event Hubs](README.azure-event-hubs.md)
- [Configure Amazon SQS](README.sqs.md)

Application code can depend on `QueueInterface` after construction, keeping
most processing logic independent of the selected broker.

## Testing

Run the complete test suite:

```bash
go test ./...
```

Verify the non-CGO build used by RabbitMQ-only applications:

```bash
CGO_ENABLED=0 go test ./...
```

## Contributing

New providers should implement `QueueInterface`, expose a typed options builder,
validate required configuration, and include broker-free unit tests for publish,
consume, retry, and dead-letter behavior.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).

## Support

- [GitHub Issues](https://github.com/uug-ai/queue/issues)
- [GitHub Discussions](https://github.com/uug-ai/queue/discussions)
