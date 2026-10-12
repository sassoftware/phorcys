// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sassoftware/phorcys/internal/amqpx/amqptest"
)

func testAnnouncement() RecoveryAnnouncement {
	return RecoveryAnnouncement{
		Hostname:  "host1",
		VHost:     "/",
		Queue:     "orders",
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Term:      3,
		Index:     17,
	}
}

func TestPublishRecoveryAnnouncement_PublishesJSONToFanoutExchange(t *testing.T) {
	ch := amqptest.NewChannel()
	conn := amqptest.NewConnection(ch)
	dialedURL := useFakeBroker(t, conn, nil)
	ann := testAnnouncement()

	err := PublishRecoveryAnnouncement(context.Background(), "amqp://fake/", ann)

	require.NoError(t, err)
	assert.Equal(t, "amqp://fake/", *dialedURL)

	exchanges := ch.DeclaredExchanges()
	require.Len(t, exchanges, 1)
	assert.Equal(t, ConsensusExchange, exchanges[0].Name)
	assert.Equal(t, "fanout", exchanges[0].Kind)
	assert.True(t, exchanges[0].Durable)
	assert.False(t, exchanges[0].AutoDelete)
	assert.False(t, exchanges[0].Internal)
	assert.False(t, exchanges[0].NoWait)

	pubs := ch.Published()
	require.Len(t, pubs, 1)
	assert.Equal(t, ConsensusExchange, pubs[0].Exchange)
	assert.Empty(t, pubs[0].Key)
	assert.False(t, pubs[0].Mandatory)
	assert.False(t, pubs[0].Immediate)
	assert.Equal(t, "application/json", pubs[0].Msg.ContentType)
	assert.True(t, ann.Timestamp.Equal(pubs[0].Msg.Timestamp))

	var got RecoveryAnnouncement
	require.NoError(t, json.Unmarshal(pubs[0].Msg.Body, &got))
	assert.True(t, ann.Timestamp.Equal(got.Timestamp))
	got.Timestamp = ann.Timestamp
	assert.Equal(t, ann, got)

	assert.True(t, ch.Closed(), "channel should be closed")
	assert.True(t, conn.Closed(), "connection should be closed")
}

func TestPublishRecoveryAnnouncement_DialError(t *testing.T) {
	dialErr := errors.New("dial boom")
	useFakeBroker(t, nil, dialErr)

	err := PublishRecoveryAnnouncement(context.Background(), "amqp://fake/", testAnnouncement())

	require.ErrorIs(t, err, dialErr)
	assert.Contains(t, err.Error(), "failed to connect to broker")
}

func TestPublishRecoveryAnnouncement_ChannelError(t *testing.T) {
	chErr := errors.New("channel boom")
	conn := &amqptest.Connection{ChannelErr: chErr}
	useFakeBroker(t, conn, nil)

	err := PublishRecoveryAnnouncement(context.Background(), "amqp://fake/", testAnnouncement())

	require.ErrorIs(t, err, chErr)
	assert.Contains(t, err.Error(), "failed to open channel")
	assert.True(t, conn.Closed(), "connection should be closed")
}

func TestPublishRecoveryAnnouncement_ExchangeDeclareError(t *testing.T) {
	declErr := errors.New("declare boom")
	ch := &amqptest.Channel{ExchangeDeclareErr: declErr}
	useFakeBroker(t, amqptest.NewConnection(ch), nil)

	err := PublishRecoveryAnnouncement(context.Background(), "amqp://fake/", testAnnouncement())

	require.ErrorIs(t, err, declErr)
	assert.Contains(t, err.Error(), "failed to declare consensus exchange")
	assert.Empty(t, ch.Published())
	assert.True(t, ch.Closed())
}

func TestPublishRecoveryAnnouncement_MarshalError(t *testing.T) {
	ch := amqptest.NewChannel()
	useFakeBroker(t, amqptest.NewConnection(ch), nil)
	ann := testAnnouncement()
	ann.Timestamp = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) // year outside RFC 3339 range

	err := PublishRecoveryAnnouncement(context.Background(), "amqp://fake/", ann)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to marshal announcement")
	assert.Empty(t, ch.Published())
}

func TestPublishRecoveryAnnouncement_PublishError(t *testing.T) {
	pubErr := errors.New("publish boom")
	ch := &amqptest.Channel{PublishErrAt: map[int]error{0: pubErr}}
	useFakeBroker(t, amqptest.NewConnection(ch), nil)

	err := PublishRecoveryAnnouncement(context.Background(), "amqp://fake/", testAnnouncement())

	require.ErrorIs(t, err, pubErr)
	assert.Contains(t, err.Error(), "failed to publish announcement")
}

func TestPublishRecoveryAnnouncement_UnreachableBrokerReturnsError(t *testing.T) {
	ann := RecoveryAnnouncement{
		Hostname:  "host1",
		VHost:     "/",
		Queue:     "orders",
		Timestamp: time.Now(),
		Term:      1,
		Index:     2,
	}

	err := PublishRecoveryAnnouncement(context.Background(), unreachableAMQPURL, ann)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to broker")
}

func TestRecoveryAnnouncement_JSONFieldNames(t *testing.T) {
	ann := RecoveryAnnouncement{
		Hostname:  "host1",
		VHost:     "/",
		Queue:     "orders",
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Term:      7,
		Index:     42,
	}

	body, err := json.Marshal(ann)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "host1", decoded["hostname"])
	assert.Equal(t, "/", decoded["vhost"])
	assert.Equal(t, "orders", decoded["queue"])
	assert.Equal(t, float64(7), decoded["term"])
	assert.Equal(t, float64(42), decoded["index"])
	assert.Equal(t, "2026-01-02T03:04:05Z", decoded["timestamp"])
}

func TestRecoveryAnnouncement_JSONRoundTrip(t *testing.T) {
	want := RecoveryAnnouncement{
		Hostname:  "rabbit-node-2",
		VHost:     "production",
		Queue:     "payments",
		Timestamp: time.Date(2026, 6, 15, 12, 30, 0, 0, time.UTC),
		Term:      3,
		Index:     99,
	}

	body, err := json.Marshal(want)
	require.NoError(t, err)

	var got RecoveryAnnouncement
	require.NoError(t, json.Unmarshal(body, &got))
	assert.True(t, want.Timestamp.Equal(got.Timestamp))
	got.Timestamp = want.Timestamp // compare the rest with a plain Equal
	assert.Equal(t, want, got)
}

func TestConsensusExchange_Name(t *testing.T) {
	assert.Equal(t, "phorcys.consensus", ConsensusExchange)
}
