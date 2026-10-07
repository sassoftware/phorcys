// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
