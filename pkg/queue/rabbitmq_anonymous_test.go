package queue

import (
	"strings"
	"testing"
)

func anonymousTestOptions() *RabbitOptionsBuilder {
	return NewRabbitOptions().
		SetHost("broker.example.com:5672").
		SetConsumerQueue("target").
		SetDeadletterQueue("target.dead-letter")
}

func TestRabbitAnonymousOptionsValidation(t *testing.T) {
	tests := []struct {
		name    string
		options *RabbitOptions
		valid   bool
	}{
		{"anonymous without credentials", anonymousTestOptions().SetAnonymous(true).Build(), true},
		{"password login with credentials", anonymousTestOptions().SetUsername("user").SetPassword("secret").Build(), true},
		{"password login without username", anonymousTestOptions().SetPassword("secret").Build(), false},
		{"password login without password", anonymousTestOptions().SetUsername("user").Build(), false},
		{"anonymous with username", anonymousTestOptions().SetAnonymous(true).SetUsername("user").Build(), false},
		{"anonymous with password", anonymousTestOptions().SetAnonymous(true).SetPassword("secret").Build(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.options.Validate()
			if tt.valid && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if !tt.valid && err == nil {
				t.Fatal("Validate() = nil, want an error")
			}
		})
	}
}

func TestRabbitAnonymousConnection(t *testing.T) {
	rabbit, err := NewRabbitMQ(anonymousTestOptions().SetAnonymous(true).Build())
	if err != nil {
		t.Fatalf("NewRabbitMQ() = %v", err)
	}
	if want := "amqp://broker.example.com:5672/"; rabbit.connectionString != want {
		t.Fatalf("connection string = %q, want %q", rabbit.connectionString, want)
	}
	mechanisms := rabbit.saslMechanisms()
	if len(mechanisms) != 1 || mechanisms[0].Mechanism() != "ANONYMOUS" || mechanisms[0].Response() != "" {
		t.Fatalf("SASL mechanisms = %#v, want only ANONYMOUS with an empty response", mechanisms)
	}
}

func TestRabbitPasswordConnectionKeepsPlain(t *testing.T) {
	rabbit, err := NewRabbitMQ(anonymousTestOptions().SetUsername("user").SetPassword("secret").Build())
	if err != nil {
		t.Fatalf("NewRabbitMQ() = %v", err)
	}
	if !strings.HasPrefix(rabbit.connectionString, "amqp://user:secret@") {
		t.Fatalf("connection string = %q, want the credentials in it", rabbit.connectionString)
	}
	if mechanisms := rabbit.saslMechanisms(); mechanisms != nil {
		t.Fatalf("SASL mechanisms = %#v, want nil so amqp091 uses PLAIN from the URL", mechanisms)
	}
}
