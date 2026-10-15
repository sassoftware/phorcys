// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package broker_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/testutil"
)

// Fixture values for the fake RabbitMQ management API responses. These are
// declared here rather than reused from the in-package broker tests because
// this file lives in the external broker_test package.
const (
	testNode          = "rabbit@n1"
	testTypeQuorum    = "quorum"
	testStatusRunning = "running"
	testStatusDown    = "down"
)

// ---------------------------------------------------------------------------
// EvaluateQueueHealth
// ---------------------------------------------------------------------------

func TestEvaluateQueueHealth_Green(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: testNode, Running: true}}
	queue := &broker.RabbitQueue{
		Name: "healthy.q", VHost: "/", Type: testTypeQuorum,
		Status: testStatusRunning, Leader: testNode, Node: testNode,
	}
	server := testutil.TestHealthServer(t, nodes, queue, 0, nil)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "healthy.q")
	require.NoError(t, err, "unexpected error: %v", err)
	if health != broker.HealthGreen {
		assert.Equal(t, broker.HealthGreen, health, "health = %v, want %v", health, broker.HealthGreen)
	}
}

func TestEvaluateQueueHealth_Transient_MemAlarm(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: testNode, Running: true, MemAlarm: true}}
	server := testutil.TestHealthServer(t, nodes, nil, 0, nil)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "any.q")
	require.NoError(t, err, "unexpected error: %v", err)
	if health != broker.HealthTransient {
		assert.Equal(t, broker.HealthTransient, health, "health = %v, want %v", health, broker.HealthTransient)
	}
}

func TestEvaluateQueueHealth_Transient_DiskAlarm(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: testNode, Running: true, DiskFreeAlarm: true}}
	server := testutil.TestHealthServer(t, nodes, nil, 0, nil)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "any.q")
	require.NoError(t, err, "unexpected error: %v", err)
	if health != broker.HealthTransient {
		assert.Equal(t, broker.HealthTransient, health, "health = %v, want %v", health, broker.HealthTransient)
	}
}

func TestEvaluateQueueHealth_Unrecoverable_Queue500(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: testNode, Running: true}}
	server := testutil.TestHealthServer(t, nodes, nil, http.StatusInternalServerError, nil)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "crashed.q")
	require.NoError(t, err, "unexpected error: %v", err)
	if health != broker.HealthUnrecoverable {
		assert.Equal(t, broker.HealthUnrecoverable, health, "health = %v, want %v", health, broker.HealthUnrecoverable)
	}
}

func TestEvaluateQueueHealth_Transient_HostNodeDown(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: testNode, Running: false}}
	queue := &broker.RabbitQueue{
		Name: "q1", VHost: "/", Type: testTypeQuorum,
		Status: testStatusRunning, Node: testNode,
	}
	server := testutil.TestHealthServer(t, nodes, queue, 0, nil)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "q1")
	require.NoError(t, err, "unexpected error: %v", err)
	if health != broker.HealthTransient {
		assert.Equal(t, broker.HealthTransient, health, "health = %v, want %v", health, broker.HealthTransient)
	}
}

func TestEvaluateQueueHealth_Unrecoverable_NoLeaderHealthyNeighbors(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: testNode, Running: true}}
	queue := &broker.RabbitQueue{
		Name: "dead.q", VHost: "/", Type: testTypeQuorum,
		Status: testStatusDown, Leader: "", Node: testNode,
	}
	allQueues := []broker.RabbitQueue{
		*queue,
		{Name: "alive.q", VHost: "/", Type: testTypeQuorum, Status: testStatusRunning, Node: testNode},
	}
	server := testutil.TestHealthServer(t, nodes, queue, 0, allQueues)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "dead.q")
	require.NoError(t, err, "unexpected error: %v", err)
	if health != broker.HealthUnrecoverable {
		assert.Equal(t, broker.HealthUnrecoverable, health, "health = %v, want %v", health, broker.HealthUnrecoverable)
	}
}

func TestEvaluateQueueHealth_Transient_AllNeighborsDown(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: testNode, Running: true}}
	queue := &broker.RabbitQueue{
		Name: "q1", VHost: "/", Type: testTypeQuorum,
		Status: testStatusDown, Leader: "", Node: testNode,
	}
	allQueues := []broker.RabbitQueue{
		*queue,
		{Name: "q2", VHost: "/", Type: testTypeQuorum, Status: testStatusDown, Node: testNode},
	}
	server := testutil.TestHealthServer(t, nodes, queue, 0, allQueues)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "q1")
	require.NoError(t, err, "unexpected error: %v", err)
	if health != broker.HealthTransient {
		assert.Equal(t, broker.HealthTransient, health, "health = %v, want %v", health, broker.HealthTransient)
	}
}

func TestEvaluateQueueHealth_Unrecoverable_LeaderNone(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: testNode, Running: true}}
	queue := &broker.RabbitQueue{
		Name: "q1", VHost: "/", Type: testTypeQuorum,
		Status: testStatusDown, Leader: "none", Node: testNode,
	}
	// no neighbors → neighborsTotal=0, so goes straight to Unrecoverable
	allQueues := []broker.RabbitQueue{*queue}
	server := testutil.TestHealthServer(t, nodes, queue, 0, allQueues)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "q1")
	require.NoError(t, err, "unexpected error: %v", err)
	if health != broker.HealthUnrecoverable {
		assert.Equal(t, broker.HealthUnrecoverable, health, "health = %v, want %v", health, broker.HealthUnrecoverable)
	}
}

func TestEvaluateQueueHealth_FetchNodesError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("boom"))
		require.NoError(t, err)
	}))
	defer server.Close()

	// /api/nodes returns 500 → fetchNodes returns error → EvaluateQueueHealth returns error
	_, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "q")
	require.Error(t, err, "expected error when fetchNodes fails")
	if !strings.Contains(err.Error(), "failed cluster node check") {
		assert.Contains(t, err.Error(), "failed cluster node check", "unexpected error message: %v", err)
	}
}

func newDiagnosticsManager(server *httptest.Server) *broker.DiagnosticsManager {
	return &broker.DiagnosticsManager{
		APIURL:   server.URL,
		Username: "test",
		Password: "test",
		Client:   server.Client(),
	}
}
