# Amazon SQS Provider

[Back to Queue overview](README.md)

The Amazon SQS provider uses the AWS SDK for Go v2. It supports the default AWS
credential chain, explicit credentials, long polling, visibility-based retries,
standard and FIFO queues, application dead-letter queues, LocalStack endpoints,
workflow aliases, and raw-message consumers.

## AWS Prerequisites

Create the following queues before connecting:

- A consumer queue
- A dead-letter queue
- A router queue when handlers can return `PipelineForward`

The package resolves queue names with `GetQueueUrl`; it does not create queues,
redrive policies, IAM policies, encryption keys, or CloudWatch alarms.

Grant the application these actions for every queue it uses:

- `sqs:GetQueueUrl`
- `sqs:ReceiveMessage` on the consumer queue
- `sqs:DeleteMessage` on the consumer queue
- `sqs:ChangeMessageVisibility` on the consumer queue
- `sqs:SendMessage` on router, consumer, and dead-letter queues

Configure an AWS redrive policy as an infrastructure safety net for process
crashes and permission failures. The package also performs explicit
application-level dead-letter publishing for malformed, failed, and exhausted
messages.

## Quick Start

```go
package main

import (
    "log"

    "github.com/uug-ai/queue/pkg/queue"
)

func main() {
    options := queue.NewSQSOptions().
        SetConsumerQueue("kcloud-analysis-queue").
        SetRouterQueue("kcloud-event-queue").
        SetDeadletterQueue("kcloud-deadletter-queue").
        SetRegion("eu-west-1").
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

`NewSQS(options)` can be used directly when access to SQS-specific methods or
the AWS SDK client is required.

## Configuration

| Option | Required | Description |
| --- | --- | --- |
| `ConsumerQueue` | Yes | Queue consumed by read and route loops |
| `DeadletterQueue` | Yes | Queue for failed or exhausted messages |
| `Region` | Yes | AWS region containing the queues |
| `RouterQueue` | No | Destination used by `PipelineForward` |
| `Endpoint` | No | SQS endpoint override, commonly LocalStack |
| `AccessKeyID` | No | Explicit access key; defaults to AWS credential chain |
| `SecretAccessKey` | No | Explicit secret key; required with access key |
| `SessionToken` | No | Session token for temporary explicit credentials |
| `WaitTimeSeconds` | No | Long-poll duration; defaults to `20`, maximum `20` |
| `VisibilityTimeout` | No | Initial visibility timeout; defaults to `30` seconds |
| `MaxNumberOfMessages` | No | Messages per receive; defaults to `10`, maximum `10` |
| `MaxRetries` | No | Retry count before dead-lettering; defaults to `10` |
| `MessageGroupID` | No | FIFO message group; defaults to `queue` |

Queue names can also be full SQS queue URLs. This is useful for cross-account
queues when URL discovery is handled outside the package.

## Credentials

When explicit credentials are omitted, AWS SDK v2 uses its default credential
chain, including environment variables, shared AWS configuration, web identity,
ECS task roles, and EC2 instance roles.

Explicit credentials are available for controlled environments:

```go
options := queue.NewSQSOptions().
    SetConsumerQueue("events").
    SetDeadletterQueue("events-deadletter").
    SetRegion("eu-west-1").
    SetCredentials("access-key", "secret-key").
    SetSessionToken("optional-session-token").
    Build()
```

Prefer workload identity or IAM roles in production. Do not log SQS options when
they contain explicit credentials.

## Environment Example

```text
AWS_REGION=eu-west-1
AWS_SQS_CONSUMER_QUEUE=kcloud-analysis-queue
AWS_SQS_ROUTER_QUEUE=kcloud-event-queue
AWS_SQS_DEADLETTER_QUEUE=kcloud-deadletter-queue
```

The package does not read environment variables itself. Applications map their
configuration source into the options builder. Standard AWS credential
environment variables are read by the SDK's default credential chain.

## LocalStack

Set an endpoint override and test credentials:

```go
options := queue.NewSQSOptions().
    SetConsumerQueue("events").
    SetDeadletterQueue("events-deadletter").
    SetRegion("us-east-1").
    SetEndpoint("http://localhost:4566").
    SetCredentials("test", "test").
    Build()
```

The queues must exist in LocalStack before `Connect` is called.

## Consumption Semantics

`ReadMessages` long-polls `ConsumerQueue` and requests
`ApproximateReceiveCount`. The original message is deleted only after the
handler action and any required downstream publish succeed.

- Invalid JSON is published to `DeadletterQueue`, then deleted.
- `PipelineForward` removes the completed stage, publishes to `RouterQueue`,
  then deletes the original message.
- `PipelineError` publishes the handler's event to `DeadletterQueue`, then
  deletes the original message.
- `PipelineCancel` deletes the original message.
- `PipelineRetry` changes the original message's visibility timeout without
  deleting or republishing it.

This provides at-least-once processing. Handlers and downstream writes must be
idempotent.

## Retries and Dead-Lettering

SQS supplies `ApproximateReceiveCount`, which includes the initial delivery.
`MaxRetries` counts only retry deliveries, matching the other providers. After
the configured retries are exhausted, the payload is sent to
`DeadletterQueue` and the original message is deleted.

The handler's backoff value becomes the message's new visibility timeout. A
non-positive value defaults to five seconds, and values above SQS's 12-hour
limit are capped at 43,200 seconds.

Set the queue's initial `VisibilityTimeout` longer than normal handler
processing. Otherwise SQS can redeliver a message while the first handler is
still working.

## Delayed Publishing

`PublishWithDelay` uses SQS's native per-message delay rather than sleeping in
the process. SQS accepts delays from 0 through 900 seconds.

Per-message delays are not supported for FIFO queues. For a FIFO queue,
`PublishWithDelay` invokes the configured disaster recovery handler when a
positive delay is requested.

## FIFO Queues

Queue names ending in `.fifo` are treated as FIFO queues. The provider sets:

- `MessageGroupId` from `MessageGroupID`, defaulting to `queue`
- A unique `MessageDeduplicationId` for every publish

All queues in one pipeline route should use a compatible standard/FIFO design.

## Routing, Raw Messages, and Workflows

`RouteMessages` publishes to the legacy queue name
`kcloud-<first-stage>-queue` before deleting the original message.

The concrete SQS client exposes `ReadRawMessages` for payloads that are not
`models.PipelineEvent`.

Workflow services can use the intent-specific aliases:

```go
engineOptions := queue.NewSQSOptions().
    SetWorkflowsQueue("hub-workflows-queue")

stageOptions := queue.NewSQSOptions().
    SetWorkflowsStageQueue("hub-workflows-loitering")
```

A stage worker publishes completed runs back to the workflow engine queue.

## Disaster Recovery

Register a fallback for payloads that cannot be published:

```go
sqsClient.SetDisasterRecoveryHandler(func(payload []byte) error {
    return persistForReplay(payload)
})
```

Without a handler, disaster recovery is a no-op and synchronous publish errors
are still returned to the caller.

## Testing

Run SQS broker-free tests:

```bash
go test ./pkg/queue -run 'TestSQS|TestNewSelectsSQS' -v
```

The repository does not provision AWS queues for integration tests. IAM,
encryption, queue policies, redrive behavior, quotas, and live connectivity
must also be verified in the target environment.
