# Queue

[![Go Version](https://img.shields.io/badge/Go-1.24-blue.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![GoDoc](https://godoc.org/github.com/uug-ai/queue?status.svg)](https://godoc.org/github.com/uug-ai/queue)
[![Go Report Card](https://goreportcard.com/badge/github.com/uug-ai/queue)](https://goreportcard.com/report/github.com/uug-ai/queue)
[![codecov](https://codecov.io/gh/uug-ai/queue/graph/badge.svg?token=0mzdj1QR37)](https://codecov.io/gh/uug-ai/queue)
[![Release](https://img.shields.io/github/release/uug-ai/queue.svg)](https://github.com/uug-ai/queue/releases/latest)

Universal message queue client for Go with RabbitMQ support and built-in validation using the functional options builder pattern.

A Go library for connecting to and managing message queues with a unified interface using the functional options builder pattern. Currently supports RabbitMQ with built-in producer/consumer channels for high-performance messaging applications.

## Features

• **RabbitMQ Support**: Full RabbitMQ integration with dedicated producer and consumer channels
• **Options Builder Pattern**: Clean, fluent interface for configuration
• **Built-in Validation**: Compile-time type safety with validator support
• **Producer/Consumer Separation**: Dedicated channels for publishing and consuming messages
• **Prefetch Configuration**: Configurable prefetch count for QoS management
• **Exchange Support**: Flexible exchange configuration for advanced routing
• **Connection Management**: Automatic connection string building with protocol detection
• **Production Ready**: Optimized for high-performance messaging applications

## Installation

```bash
go get github.com/uug-ai/queue
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    "time"
    "github.com/uug-ai/queue/pkg/queue"
)

func main() {
    // Build RabbitMQ options
    opts := queue.NewRabbitOptions().
        SetQueueName("my-queue").
        SetHost("localhost:5672").
        SetUsername("guest").
        SetPassword("guest").
        SetExchange("my-exchange").
        SetPrefetchCount(10).
        Build()

    // Create queue client with options
    mq, err := queue.NewRabbitMQ(opts)
    if err != nil {
        log.Fatal(err)
    }

    // Connect to RabbitMQ
    if err := mq.Connect(); err != nil {
        log.Fatal(err)
    }

    log.Println("Successfully connected to RabbitMQ!")
}
```

## Core Concepts

### Options Builder Pattern

All components use the options builder pattern (similar to MongoDB's official driver). This provides:

• **Clean Syntax**: Build options separately, then pass to constructor
• **Readability**: Self-documenting method chains
• **Separation of Concerns**: Options building is separate from client creation
• **Validation**: Built-in validation when creating the client
• **Type Safety**: Compile-time type checking
• **Flexibility**: Configure only what you need

### Creating a Queue Client

Each queue connection follows this pattern:

1. Build Options using `queue.NewRabbitOptions()` with method chaining
2. Call `.Build()` to get the options object
3. Create Client by passing options to `queue.NewRabbitMQ(opts)`
4. Call `.Connect()` to establish the connection
5. Use the client for messaging operations

## Usage Examples

### RabbitMQ Connection

The RabbitMQ integration demonstrates the options builder pattern:

```go
package main

import (
    "log"
    "github.com/uug-ai/queue/pkg/queue"
)

func main() {
    // Build RabbitMQ options
    opts := queue.NewRabbitOptions().
        SetQueueName("tasks").
        SetHost("rabbitmq.example.com:5672").
        SetUsername("admin").
        SetPassword("secret").
        SetExchange("task-exchange").
        SetPrefetchCount(20).
        Build()

    // Create queue client with options
    mq, err := queue.NewRabbitMQ(opts)
    if err != nil {
        log.Fatal(err)
    }

    // Connect to RabbitMQ
    err = mq.Connect()
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Connected to RabbitMQ successfully!")
}
```

**Available Methods:**

• `.SetQueueName(name string)` - Queue name for message routing
• `.SetHost(host string)` - RabbitMQ host address and port
• `.SetUsername(username string)` - Authentication username
• `.SetPassword(password string)` - Authentication password
• `.SetExchange(exchange string)` - Exchange name for message publishing
• `.SetPrefetchCount(count int)` - Maximum unacknowledged messages per consumer
• `.SetUri(uri string)` - Alternative connection URI format
• `.Build()` - Returns the RabbitOptions object

## Project Structure

```
.
├── pkg/
│   └── queue/                 # Core queue implementation
│       ├── queue.go          # Queue interface definition
│       ├── rabbitmq.go       # RabbitMQ client implementation
│       ├── rabbitmq_test.go  # RabbitMQ tests
│       ├── mock.go           # Mock implementations for testing
│       └── mock_test.go      # Mock tests
├── main.go
├── go.mod
├── go.sum
├── Dockerfile
└── README.md
```

## Configuration

### Using the Options Builder Pattern (Recommended)

```go
opts := queue.NewRabbitOptions().
    SetQueueName("my-queue").
    SetHost("localhost:5672").
    SetUsername("guest").
    SetPassword("guest").
    SetExchange("my-exchange").
    SetPrefetchCount(10).
    Build()

mq, err := queue.NewRabbitMQ(opts)
```

### Environment Variables

You can load configuration from environment variables:

```go
import "os"

opts := queue.NewRabbitOptions().
    SetQueueName(os.Getenv("QUEUE_NAME")).
    SetHost(os.Getenv("RABBITMQ_HOST")).
    SetUsername(os.Getenv("RABBITMQ_USER")).
    SetPassword(os.Getenv("RABBITMQ_PASSWORD")).
    SetExchange(os.Getenv("RABBITMQ_EXCHANGE")).
    SetPrefetchCount(10).
    Build()

mq, err := queue.NewRabbitMQ(opts)
```

**Example `.env` file:**

```
QUEUE_NAME=my-queue
RABBITMQ_HOST=localhost:5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_EXCHANGE=my-exchange
```

## Validation

RabbitMQ options use [go-playground/validator](https://github.com/go-playground/validator) for configuration validation. All required fields must be provided:

• `QueueName` - Queue name (required)
• `Host` - RabbitMQ host address (required)
• `Username` - Authentication username (required)
• `Password` - Authentication password (required)

Validation is automatically performed when calling `queue.NewRabbitMQ(opts)`, ensuring invalid configurations are caught before the connection is established.

## Error Handling

The options builder pattern provides clear error handling:

```go
// Build options (no validation here)
opts := queue.NewRabbitOptions().
    SetQueueName("my-queue").
    SetHost("localhost:5672").
    // Missing required fields...
    Build()

// Validation happens when creating the client
mq, err := queue.NewRabbitMQ(opts)
if err != nil {
    // Validation error caught at client creation time
    log.Printf("Configuration error: %v", err)
    return
}

// If we get here, the configuration is valid
err = mq.Connect()
if err != nil {
    // Connection error during runtime
    log.Printf("Connection error: %v", err)
    return
}
```

## Testing

Run the test suite:

```bash
go test ./...
```

Run tests with coverage:

```bash
go test -cover ./...
```

Run tests for specific components:

```bash
# Queue tests
go test ./pkg/queue -v

# RabbitMQ tests
go test ./pkg/queue -run TestRabbitMQ -v

# Mock tests
go test ./pkg/queue -run TestMock -v
```

## Contributing

Contributions are welcome! When adding new features or queue implementations, please follow the options builder pattern demonstrated in this repository.

### Development Guidelines

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/amazing-feature`)
3. Follow the options builder pattern
4. Add comprehensive tests for your changes
5. Ensure all tests pass: `go test ./...`
6. Commit your changes following [Conventional Commits](https://www.conventionalcommits.org/)
7. Push to your branch (`git push origin feat/amazing-feature`)
8. Open a Pull Request

### Commit Message Format

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `types`

Scopes:

• `rabbitmq` - RabbitMQ implementation
• `queue` - Core queue functionality
• `options` - Options builder
• `docs` - Documentation updates
• `tests` - Test updates

Examples:

```
feat(rabbitmq): add connection pooling support
fix(queue): correct prefetch count validation
docs(readme): update RabbitMQ configuration examples
refactor(options): simplify builder interface
test(rabbitmq): add connection retry tests
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Dependencies

This project uses the following key libraries:

• [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) - Official RabbitMQ Go client
• [go-playground/validator](https://github.com/go-playground/validator) - Struct validation
• [uug-ai/models](https://github.com/uug-ai/models) - Shared model types

See [go.mod](go.mod) for the complete list of dependencies.

## Support

• **Issues**: [GitHub Issues](https://github.com/uug-ai/queue/issues)
• **Discussions**: [GitHub Discussions](https://github.com/uug-ai/queue/discussions)
• **Documentation**: See inline code comments and examples above
