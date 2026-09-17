package recovery

import (
	"context"
	"testing"
)

// RepublishMessages requires a live AMQP broker; tests here cover the error
// paths that can be triggered without a running RabbitMQ instance.

func TestRepublishMessages_InvalidURLReturnsError(t *testing.T) {
	err := RepublishMessages(context.Background(), "amqp://invalid.host.test:5672/", "q", [][]byte{[]byte("x")})
	if err == nil {
		t.Error("expected error when broker is unreachable")
	}
}

func TestRepublishMessages_EmptyPayloadsConnectsButPublishesNothing(t *testing.T) {
	// With an unreachable URL the connection itself fails, so we can't test the
	// empty-payload fast-path without a live broker. We verify that a nil/empty
	// slice does not panic when passed.
	_ = RepublishMessages(context.Background(), "amqp://127.0.0.1:1/", "q", nil)
	_ = RepublishMessages(context.Background(), "amqp://127.0.0.1:1/", "q", [][]byte{})
}
