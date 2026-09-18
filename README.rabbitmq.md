# RabbitMQ Provider

[Back to Queue overview](README.md)

The RabbitMQ provider uses
[`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go). It
creates separate producer and consumer channels, declares quorum queues, applies
consumer prefetch, and reconnects closed connections. Services can explicitly
opt into persistent publisher confirms and delayed source acknowledgements when
their delivery guarantees justify the additional broker round trip.

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
| `ConfirmedDelivery` | No | Provisions the confirmed producer used by explicit confirmed methods; defaults to `false` |
| `DeadLetterObserver` | No | Optional `func(DeadLetterPublishEvent)` observing final DLQ publish outcomes |
| `Logger` | No | Optional `*logrus.Logger` warning before a new DLQ publication is attempted |
| `Exchange` | No | Compatibility field; the default exchange is used |
| `Uri` | No | Compatibility field; connection uses host and credentials |
| `TLS` | No | Enables an `amqps://` connection |
| `TLSInsecureSkipVerify` | No | Skips certificate verification |
| `TLSCACertFile` | No | PEM file containing a custom CA certificate |

The builder provides matching setters, including `SetConsumerQueue`,
`SetDeadletterQueue`, `SetRouterQueue`, `SetHost`, `SetUsername`, `SetPassword`,
`SetPrefetchCount`, `SetMaxRetries`, `SetConfirmedDelivery`, and the TLS setters.

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
key. `Publish` retains the legacy asynchronous behavior. Mandatory returns are
reported through `ReturnHandler`, but they can arrive after `Publish` returns.

For stronger delivery guarantees, enable and call the explicit confirmed API:

```go
options := queue.NewRabbitOptions().
    SetConsumerQueue("kcloud-export-queue").
    SetDeadletterQueue("dead-letter-queue").
    SetHost("rabbitmq.example.com:5672").
    SetUsername("username").
    SetPassword("password").
    SetConfirmedDelivery(true).
    Build()

rabbit, err := queue.NewRabbitMQ(options)
if err != nil {
    log.Fatal(err)
}
if err := rabbit.Connect(); err != nil {
    log.Fatal(err)
}
defer rabbit.Close()

if err := rabbit.PublishConfirmed("target-queue", payload); err != nil {
    log.Fatal(err)
}
```

`PublishConfirmed` marks messages persistent and waits up to five seconds for
RabbitMQ to confirm acceptance. It returns an error for a negative
acknowledgement, confirmation timeout, closed channel, or mandatory return.
Confirmed publishes on one client are serialized so returns correlate with
their confirmation, and reconnect cannot replace that producer mid-publish.
This synchronous broker round trip can reduce throughput, so enable it per
service rather than as a blanket dependency upgrade.

Use `SetReturnHandler` on the concrete `RabbitMQ` client to report returned
messages to metrics or structured logging. The callback runs asynchronously.
`PublishConfirmed` also returns the unroutable error synchronously. Without a
handler, returns are logged through the standard logger.

```go
rabbit, err := queue.NewRabbitMQ(options)
if err != nil {
    log.Fatal(err)
}
rabbit.SetReturnHandler(func(returned amqp.Return) {
    log.Printf("unroutable queue=%s reason=%s", returned.RoutingKey, returned.ReplyText)
})
```

### Dead-letter request logging

Inject your application's logger to log the reason immediately before a logical
new dead-letter publication is attempted:

```go
logger := logrus.New()
logger.SetFormatter(&logrus.JSONFormatter{})

options := queue.NewRabbitOptions().
    SetConsumerQueue("kcloud-sequence-queue").
    SetDeadletterQueue("dead-letter-queue").
    SetHost("rabbitmq.example.com:5672").
    SetUsername("username").
    SetPassword("password").
    SetLogger(logger).
    Build()
```

Continue using the normal `ReadMessages` or `ReadMessagesConfirmed` handlers.
The single WARN uses the shared `models/pkg/api` envelope:

- `applicationStatusCode`: `pipeline_warning`
- `entityStatusCode`: `dead_letter_requested`
- `metadata.data`: `sourceQueue`, `deadLetterQueue`, `reason`, `attempts`
- `metadata.traceId` and `metadata.mediaFileName`: copied only from an already
  decoded pipeline event, when available. The media identifier prefers
  `Payload.Key`, falling back to `FileName`.

The existing `DeadLetterReason` catalog supplies the terminal reason:

| Reason | Meaning |
| --- | --- |
| `unspecified` | Direct/manual dead-letter request without a more specific reason |
| `malformed` | Invalid message JSON or an event that cannot be encoded |
| `handler_error` | The handler returned `PipelineError` |
| `retry_exhausted` | The retry count reached `MaxRetries` |
| `publish_failed` | Publishing to the next stage failed |

`attempts` is the retry count for exhaustion and otherwise the value known by the
publication path (normally zero). Forced transfers may supply their own reason.
Correlate the terminal request with the consumer's preceding safe error logs for
the underlying application failure.

This logs a **request, not success or durable delivery**. A publish failure still
has a request log; reconnect attempts do not emit duplicate request logs.
Redelivery can create another logical request. Payloads, whole events, signed
URLs, credentials, and raw exception text are not logged.

Logging does not parse payloads or envelopes. Raw/direct/forced transfers have no
correlation fields unless the caller already supplied an event through the
internal pipeline path. Successful forwards, cancellations, and retries below
the limit do not log a DLQ request. Inspection, scanning, replay, discard, and
restoration do not count as new requests.

`Logger` is nil by default, with no log construction when disabled. Configure it
before starting the client. No handler API, payload, headers, settlement behavior,
or background worker changes are needed. `DeadLetterObserver` below remains a
separate, optional mechanism; request logging does not enable it.

### Dead-letter publish outcomes

Set `RabbitOptions.DeadLetterObserver` or use `SetDeadLetterObserver` before
creating the client:

```go
options := queue.NewRabbitOptions().
    SetDeadLetterObserver(func(event queue.DeadLetterPublishEvent) {
        // Send these fields to your application's structured logger or metrics.
        // Do not treat Err == nil alone as proof of durable delivery.
    })
```

The callback runs once per logical publish to the configured DLQ, **after** its
final result (including reconnect), not when a handler returns `PipelineError`.
It covers malformed messages, handler errors, failed forwarding, exhausted
retries, forced `ReadMessagesToDeadletter` transfers, `AddToDeadletter`, direct
`Publish`/`PublishConfirmed`, delayed DLQ publishes, and `PublishDeadLetter`.
Inspection, scanning, replay, discard, and restoration of retained messages do
not emit new-entry events. Validation failures before publishing do not emit.

`Source`, `Reason`, and `Attempts` come from the envelope. `Destination` is the
actual configured DLQ, not legacy envelope destination metadata that may identify
a replay queue. `TraceID` and `MediaFileName` are extracted only from the inner
JSON's `traceId` and `payload.key` (falling back to top-level `fileName`).
Malformed inner bodies still produce outcomes, with empty correlation fields.
No payload or arbitrary attributes are exposed, and the observer adds no logger
or tracing dependency.

`Confirmed` is true only when a broker-confirmed publish succeeds (`Err == nil`).
An unconfirmed publish can return nil before a mandatory return arrives; its
event remains `Confirmed: false`. Failed publishes retain their error even if
subsequent disaster recovery succeeds. Observation precedes source settlement,
so success does not imply the source acknowledgement succeeded. Redelivery can
produce another logical publish and event.

Callbacks run synchronously outside publishing locks, including on the delayed
publisher goroutine. They may run concurrently: handlers must be concurrency-safe,
return promptly, and not panic. A slow callback delays settlement; avoid recursive
DLQ publishing from the callback. Do not mutate options after starting the client.
When unset, no envelope or correlation decoding is performed.

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

RabbitMQ deliveries use manual acknowledgements. `ReadMessages` retains the
legacy behavior. `ReadMessagesConfirmed` uses persistent confirmed publishes
and acknowledges a source delivery only after its transfer is durable. It
requires `SetConfirmedDelivery(true)`.

- Invalid JSON is sent to the dead-letter queue.
- `PipelineForward` removes the completed stage and publishes to `RouterQueue`
  when another stage remains.
- `PipelineError` publishes the handler's event to `DeadletterQueue`.
- `PipelineRetry` waits for the configured backoff and synchronously republishes
    to `ConsumerQueue`. The original delivery is acknowledged only after that
    publish succeeds; a publish failure returns from the consumer with the
    original delivery unacknowledged so reconnect/redelivery can recover it.
- `PipelineCancel` only acknowledges the delivery.

Under `ReadMessagesConfirmed`, any failed forward, retry, or dead-letter
transfer is offered to the configured `DisasterRecoveryHandler`. The original
delivery is acknowledged only after the destination publish or disaster
recovery succeeds. If both fail (or no recovery handler is configured), the
library negatively acknowledges with requeue, closes the stale connection, and
returns the error so a fresh consumer can redeliver it.

RabbitMQ retries carry an `x-retry-count` message header. After `MaxRetries`, the
payload is parked on `DeadletterQueue` rather than requeued indefinitely.

Processing is at least once. A connection failure after side effects but before
acknowledgement can cause redelivery.

## Raw Messages

The concrete `RabbitMQ` client also supports queues whose payload is not a
`PipelineEvent`:

- `ReadRawMessages` runs a long-lived raw consumer.
- `ReadOneRaw` reads at most one message for verification and drain tools.
- `ReadRawMessagesConfirmed` and `ReadOneRawConfirmed` provide the corresponding
    confirmed settlement behavior when explicitly enabled.

`RouteMessagesConfirmed` is the confirmed counterpart to `RouteMessages`.

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

The integration suite runs only when `RABBITMQ_HOST`, `RABBITMQ_USERNAME`, and
`RABBITMQ_PASSWORD` are provided; otherwise it is skipped. In addition to basic
connectivity, it verifies that confirmed publishing rejects an unroutable queue
and that an unacknowledged persistent message is redelivered after a forced AMQP
connection interruption and reconnect.
