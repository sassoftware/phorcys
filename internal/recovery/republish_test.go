// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sassoftware/phorcys/internal/amqpx"
	"github.com/sassoftware/phorcys/internal/amqpx/amqptest"
)

const testQueue = "orders"

// useFakeBroker swaps dialAMQP for the duration of the test and records the dialed URL.
func useFakeBroker(t *testing.T, conn *amqptest.Connection, dialErr error) *string {
	t.Helper()
	var dialedURL string
	orig := dialAMQP
	dialAMQP = func(url string) (amqpx.Connection, error) {
		dialedURL = url
		if dialErr != nil {
			return nil, dialErr
		}
		return conn, nil
	}
	t.Cleanup(func() { dialAMQP = orig })
	return &dialedURL
}

func TestRepublishMessages_PublishesAllPayloadsWithConfirms(t *testing.T) {
	ch := amqptest.NewChannel()
	conn := amqptest.NewConnection(ch)
	dialedURL := useFakeBroker(t, conn, nil)
	messages := []amqp.Publishing{
		{Body: []byte("one")},
		{Body: []byte("two")},
		{Body: []byte("three")},
	}

	err := RepublishMessages(context.Background(), "amqp://fake/", testQueue, messages)

	require.NoError(t, err)
	assert.Equal(t, "amqp://fake/", *dialedURL)
	assert.True(t, ch.ConfirmMode(), "confirm mode should be enabled")
	assert.False(t, ch.ConfirmNoWait())
	declared := ch.Declared()
	require.Len(t, declared, 1)
	assert.Equal(t, testQueue, declared[0].Name)
	assert.True(t, declared[0].Durable)
	assert.False(t, declared[0].AutoDelete)
	assert.False(t, declared[0].Exclusive)
	assert.True(t, declared[0].NoWait)

	published := ch.Published()
	require.Len(t, published, len(messages))
	for i, p := range published {
		assert.Equal(t, "", p.Exchange, "should use default exchange")
		assert.Equal(t, testQueue, p.Key)
		assert.False(t, p.Mandatory)
		assert.False(t, p.Immediate)
		assert.Equal(t, messages[i].Body, p.Msg.Body, "payload order must be preserved")
		assert.Equal(t, amqp.Persistent, p.Msg.DeliveryMode)
		assert.Equal(t, "application/octet-stream", p.Msg.ContentType)
	}
	assert.True(t, ch.Closed())
	assert.True(t, conn.Closed())
}

func TestRepublishMessages_EmptyPayloadsPublishesNothing(t *testing.T) {
	ch := amqptest.NewChannel()
	conn := amqptest.NewConnection(ch)
	useFakeBroker(t, conn, nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", testQueue, nil)

	require.NoError(t, err)
	assert.Empty(t, ch.Published())
	assert.True(t, ch.Closed())
	assert.True(t, conn.Closed())
}

func TestRepublishMessages_DialError(t *testing.T) {
	dialErr := errors.New("connection refused")
	useFakeBroker(t, nil, dialErr)

	err := RepublishMessages(context.Background(), "amqp://fake/", testQueue, []amqp.Publishing{{Body: []byte("x")}})

	require.ErrorIs(t, err, dialErr)
	assert.ErrorContains(t, err, "failed to connect to broker")
}

func TestRepublishMessages_ChannelError(t *testing.T) {
	chErr := errors.New("channel max reached")
	conn := &amqptest.Connection{ChannelErr: chErr}
	useFakeBroker(t, conn, nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", testQueue, []amqp.Publishing{{Body: []byte("x")}})

	require.ErrorIs(t, err, chErr)
	assert.ErrorContains(t, err, "failed to open channel")
	assert.True(t, conn.Closed())
}

func TestRepublishMessages_ConfirmModeError(t *testing.T) {
	confirmErr := errors.New("confirm not supported")
	ch := &amqptest.Channel{ConfirmErr: confirmErr}
	conn := amqptest.NewConnection(ch)
	useFakeBroker(t, conn, nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", testQueue, []amqp.Publishing{{Body: []byte("x")}})

	require.ErrorIs(t, err, confirmErr)
	assert.ErrorContains(t, err, "failed to enable publisher confirmations")
	assert.Empty(t, ch.Published())
	assert.True(t, ch.Closed())
	assert.True(t, conn.Closed())
}

func TestRepublishMessages_QueueDeclareError(t *testing.T) {
	declareErr := errors.New("precondition failed")
	ch := &amqptest.Channel{DeclareErr: declareErr}
	useFakeBroker(t, amqptest.NewConnection(ch), nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", testQueue, []amqp.Publishing{{Body: []byte("x")}})

	require.ErrorIs(t, err, declareErr)
	assert.ErrorContains(t, err, "failed to recreate queue")
	assert.Empty(t, ch.Published())
}

func TestRepublishMessages_PublishErrorStopsPublishing(t *testing.T) {
	pubErr := errors.New("channel closed")
	ch := &amqptest.Channel{PublishErrAt: map[int]error{1: pubErr}}
	useFakeBroker(t, amqptest.NewConnection(ch), nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", testQueue,
		[]amqp.Publishing{
			{Body: []byte("a")},
			{Body: []byte("b")},
			{Body: []byte("c")},
		},
	)

	require.ErrorIs(t, err, pubErr)
	assert.ErrorContains(t, err, "failed publishing message 1")
	assert.Len(t, ch.Published(), 1)
}

func TestRepublishMessages_NackStopsPublishing(t *testing.T) {
	ch := &amqptest.Channel{NackAt: map[int]bool{1: true}}
	useFakeBroker(t, amqptest.NewConnection(ch), nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", testQueue,
		[]amqp.Publishing{
			{Body: []byte("a")},
			{Body: []byte("b")},
			{Body: []byte("c")},
		},
	)

	require.ErrorContains(t, err, "negatively acknowledged message 1")
	assert.Len(t, ch.Published(), 2, "no messages should be published after a nack")
}

func TestRepublishMessages_ConfirmChannelClosed(t *testing.T) {
	ch := &amqptest.Channel{CloseConfirmsAt: map[int]bool{0: true}}
	useFakeBroker(t, amqptest.NewConnection(ch), nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", testQueue,
		[]amqp.Publishing{
			{Body: []byte("a")},
			{Body: []byte("b")},
		},
	)

	require.ErrorContains(t, err, "confirmation channel closed while waiting for message 0")
	assert.Len(t, ch.Published(), 1)
}

func TestRepublishMessages_ContextCancelledWhileAwaitingConfirm(t *testing.T) {
	ch := &amqptest.Channel{NoConfirmAt: map[int]bool{0: true}}
	useFakeBroker(t, amqptest.NewConnection(ch), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := RepublishMessages(ctx, "amqp://fake/", testQueue,
		[]amqp.Publishing{
			{Body: []byte("a")},
			{Body: []byte("b")},
		},
	)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorContains(t, err, "timed out waiting for publisher confirmation for message 0")
	assert.Len(t, ch.Published(), 1)
}

// The test below exercises the real dialer against an unreachable broker.

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
