// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RepublishMessages requires a live AMQP broker; tests here cover the error
// paths that can be triggered without a running RabbitMQ instance.

func TestRepublishMessages_InvalidURLReturnsError(t *testing.T) {
	err := RepublishMessages(context.Background(), "amqp://invalid.host.test:5672/", "q", []amqp.Publishing{{Body: []byte("x")}})
	assert.Error(t, err, "expected error when broker is unreachable")
}

func TestRepublishMessages_EmptyPayloadsConnectsButPublishesNothing(t *testing.T) {
	// With an unreachable URL the connection itself fails, so we can't test the
	// empty-payload fast-path without a live broker. We verify that a nil/empty
	// slice does not panic when passed.
	_ = RepublishMessages(context.Background(), "amqp://127.0.0.1:1/", "q", nil)
	_ = RepublishMessages(context.Background(), "amqp://127.0.0.1:1/", "q", []amqp.Publishing{})
}

func TestWaitForPublishConfirmation_Acknowledged(t *testing.T) {
	confirms := make(chan amqp.Confirmation, 1)
	confirms <- amqp.Confirmation{Ack: true}

	err := waitForPublishConfirmation(context.Background(), confirms, 0)

	require.NoError(t, err)
}

func TestWaitForPublishConfirmation_NegativelyAcknowledged(t *testing.T) {
	confirms := make(chan amqp.Confirmation, 1)
	confirms <- amqp.Confirmation{Ack: false}

	err := waitForPublishConfirmation(context.Background(), confirms, 2)

	require.ErrorContains(t, err, "negatively acknowledged message 2")
}

func TestWaitForPublishConfirmation_ClosedChannel(t *testing.T) {
	confirms := make(chan amqp.Confirmation)
	close(confirms)

	err := waitForPublishConfirmation(context.Background(), confirms, 0)

	require.ErrorContains(t, err, "confirmation channel closed")
}

func TestWaitForPublishConfirmation_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := waitForPublishConfirmation(ctx, make(chan amqp.Confirmation), 1)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "message 1")
}
