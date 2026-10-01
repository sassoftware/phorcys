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
)

type declaredQueue struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
}

type sentMessage struct {
	exchange  string
	key       string
	mandatory bool
	immediate bool
	msg       amqp.Publishing
}

// fakeChannel simulates a broker channel in confirm mode. By default, every
// publish is acked; behavior can be altered per message index.
type fakeChannel struct {
	confirmErr     error
	declareErr     error
	publishErrAt   map[int]error
	nackAt         map[int]bool
	noConfirmAt    map[int]bool
	closeConfirmAt map[int]bool

	confirmCalled bool
	confirmNoWait bool
	declared      []declaredQueue
	published     []sentMessage
	closed        bool
	confirms      chan amqp.Confirmation
}

func (f *fakeChannel) Confirm(noWait bool) error {
	f.confirmCalled = true
	f.confirmNoWait = noWait
	return f.confirmErr
}

func (f *fakeChannel) NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	f.confirms = c
	return c
}

func (f *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, _ amqp.Table) (amqp.Queue, error) {
	f.declared = append(f.declared, declaredQueue{name, durable, autoDelete, exclusive, noWait})
	return amqp.Queue{Name: name}, f.declareErr
}

func (f *fakeChannel) PublishWithContext(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	i := len(f.published)
	if err := f.publishErrAt[i]; err != nil {
		return err
	}
	f.published = append(f.published, sentMessage{exchange, key, mandatory, immediate, msg})
	switch {
	case f.closeConfirmAt[i]:
		close(f.confirms)
	case f.noConfirmAt[i]:
	default:
		f.confirms <- amqp.Confirmation{DeliveryTag: uint64(i + 1), Ack: !f.nackAt[i]}
	}
	return nil
}

func (f *fakeChannel) Close() error {
	f.closed = true
	return nil
}

type fakeConnection struct {
	ch         *fakeChannel
	channelErr error
	closed     bool
}

func (f *fakeConnection) Channel() (amqpChannel, error) {
	if f.channelErr != nil {
		return nil, f.channelErr
	}
	return f.ch, nil
}

func (f *fakeConnection) Close() error {
	f.closed = true
	return nil
}

// useFakeBroker swaps dialAMQP for the duration of the test and records the dialed URL.
func useFakeBroker(t *testing.T, conn *fakeConnection, dialErr error) *string {
	t.Helper()
	var dialedURL string
	orig := dialAMQP
	dialAMQP = func(url string) (amqpConnection, error) {
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
	ch := &fakeChannel{}
	conn := &fakeConnection{ch: ch}
	dialedURL := useFakeBroker(t, conn, nil)
	payloads := [][]byte{[]byte("one"), []byte("two"), []byte("three")}

	err := RepublishMessages(context.Background(), "amqp://fake/", "orders", payloads)

	require.NoError(t, err)
	assert.Equal(t, "amqp://fake/", *dialedURL)
	assert.True(t, ch.confirmCalled, "confirm mode should be enabled")
	assert.False(t, ch.confirmNoWait)
	require.Len(t, ch.declared, 1)
	assert.Equal(t, declaredQueue{name: "orders", durable: true, noWait: true}, ch.declared[0])

	require.Len(t, ch.published, len(payloads))
	for i, p := range ch.published {
		assert.Equal(t, "", p.exchange, "should use default exchange")
		assert.Equal(t, "orders", p.key)
		assert.False(t, p.mandatory)
		assert.False(t, p.immediate)
		assert.Equal(t, payloads[i], p.msg.Body, "payload order must be preserved")
		assert.Equal(t, amqp.Persistent, p.msg.DeliveryMode)
		assert.Equal(t, "application/octet-stream", p.msg.ContentType)
	}
	assert.True(t, ch.closed)
	assert.True(t, conn.closed)
}

func TestRepublishMessages_EmptyPayloadsPublishesNothing(t *testing.T) {
	ch := &fakeChannel{}
	conn := &fakeConnection{ch: ch}
	useFakeBroker(t, conn, nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", "orders", nil)

	require.NoError(t, err)
	assert.Empty(t, ch.published)
	assert.True(t, ch.closed)
	assert.True(t, conn.closed)
}

func TestRepublishMessages_DialError(t *testing.T) {
	dialErr := errors.New("connection refused")
	useFakeBroker(t, nil, dialErr)

	err := RepublishMessages(context.Background(), "amqp://fake/", "orders", [][]byte{[]byte("x")})

	require.ErrorIs(t, err, dialErr)
	assert.ErrorContains(t, err, "failed to connect to broker")
}

func TestRepublishMessages_ChannelError(t *testing.T) {
	chErr := errors.New("channel max reached")
	conn := &fakeConnection{channelErr: chErr}
	useFakeBroker(t, conn, nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", "orders", [][]byte{[]byte("x")})

	require.ErrorIs(t, err, chErr)
	assert.ErrorContains(t, err, "failed to open channel")
	assert.True(t, conn.closed)
}

func TestRepublishMessages_ConfirmModeError(t *testing.T) {
	confirmErr := errors.New("confirm not supported")
	ch := &fakeChannel{confirmErr: confirmErr}
	conn := &fakeConnection{ch: ch}
	useFakeBroker(t, conn, nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", "orders", [][]byte{[]byte("x")})

	require.ErrorIs(t, err, confirmErr)
	assert.ErrorContains(t, err, "failed to enable publisher confirmations")
	assert.Empty(t, ch.published)
	assert.True(t, ch.closed)
	assert.True(t, conn.closed)
}

func TestRepublishMessages_QueueDeclareError(t *testing.T) {
	declareErr := errors.New("precondition failed")
	ch := &fakeChannel{declareErr: declareErr}
	useFakeBroker(t, &fakeConnection{ch: ch}, nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", "orders", [][]byte{[]byte("x")})

	require.ErrorIs(t, err, declareErr)
	assert.ErrorContains(t, err, "failed to recreate queue")
	assert.Empty(t, ch.published)
}

func TestRepublishMessages_PublishErrorStopsPublishing(t *testing.T) {
	pubErr := errors.New("channel closed")
	ch := &fakeChannel{publishErrAt: map[int]error{1: pubErr}}
	useFakeBroker(t, &fakeConnection{ch: ch}, nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", "orders",
		[][]byte{[]byte("a"), []byte("b"), []byte("c")})

	require.ErrorIs(t, err, pubErr)
	assert.ErrorContains(t, err, "failed publishing message 1")
	assert.Len(t, ch.published, 1)
}

func TestRepublishMessages_NackStopsPublishing(t *testing.T) {
	ch := &fakeChannel{nackAt: map[int]bool{1: true}}
	useFakeBroker(t, &fakeConnection{ch: ch}, nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", "orders",
		[][]byte{[]byte("a"), []byte("b"), []byte("c")})

	require.ErrorContains(t, err, "negatively acknowledged message 1")
	assert.Len(t, ch.published, 2, "no messages should be published after a nack")
}

func TestRepublishMessages_ConfirmChannelClosed(t *testing.T) {
	ch := &fakeChannel{closeConfirmAt: map[int]bool{0: true}}
	useFakeBroker(t, &fakeConnection{ch: ch}, nil)

	err := RepublishMessages(context.Background(), "amqp://fake/", "orders",
		[][]byte{[]byte("a"), []byte("b")})

	require.ErrorContains(t, err, "confirmation channel closed while waiting for message 0")
	assert.Len(t, ch.published, 1)
}

func TestRepublishMessages_ContextCancelledWhileAwaitingConfirm(t *testing.T) {
	ch := &fakeChannel{noConfirmAt: map[int]bool{0: true}}
	useFakeBroker(t, &fakeConnection{ch: ch}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := RepublishMessages(ctx, "amqp://fake/", "orders", [][]byte{[]byte("a"), []byte("b")})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorContains(t, err, "timed out waiting for publisher confirmation for message 0")
	assert.Len(t, ch.published, 1)
}

// The tests below exercise the real dialer against unreachable brokers.

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
