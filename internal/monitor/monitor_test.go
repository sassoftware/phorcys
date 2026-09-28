// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/runtime"
	"github.com/sassoftware/phorcys/internal/testutil"
)

// ---------------------------------------------------------------------------
// processLogLine
// ---------------------------------------------------------------------------

func TestProcessLogLine_NoRelevantKeywords_IgnoresLine(t *testing.T) {
	lm := &LogMonitorWorker{
		quorumList: []TrackedQueue{{VHost: "/", Name: "important.queue"}},
		jobChannel: make(chan QueueJob, 10),
	}

	lm.processLogLine("INFO: application started successfully on port 5672")

	// No goroutine should have been spawned, jobChannel stays empty.
	time.Sleep(20 * time.Millisecond)
	select {
	case job := <-lm.jobChannel:
		assert.Failf(t, "did not expect a job for a line with no quorum keywords", "got %+v", job)
	default:
	}
}

func TestProcessLogLine_KeywordButNoMatchingQueueName(t *testing.T) {
	lm := &LogMonitorWorker{
		quorumList: []TrackedQueue{{VHost: "/", Name: "unrelated.queue"}},
		jobChannel: make(chan QueueJob, 10),
	}

	// Has "raft" keyword but the line does not mention the tracked queue.
	lm.processLogLine("ERROR: raft leader election timeout on node rabbit@n1")

	time.Sleep(20 * time.Millisecond)
	select {
	case job := <-lm.jobChannel:
		assert.Failf(t, "did not expect a job", "got %+v", job)
	default:
	}
}

func TestProcessLogLine_MatchEnqueuesPendingCheck(t *testing.T) {
	lm := &LogMonitorWorker{
		quorumList: []TrackedQueue{{VHost: "/", Name: "events.queue"}},
		jobChannel: make(chan QueueJob, 10),
	}

	// Line contains "quorum" and the tracked queue name.
	lm.processLogLine("ERROR: ra quorum leader crash detected for queue events.queue")

	// coalesceLogEvent runs in a goroutine and calls LoadOrStore before sleeping.
	// Give it a brief moment to store the key.
	time.Sleep(30 * time.Millisecond)

	key := "/" + "/" + "events.queue"
	if _, ok := lm.pendingChecks.Load(key); !ok {
		assert.Failf(t, "expected key in pendingChecks after log match", "%q", key)
	}
}

func TestProcessLogLine_StopsAtFirstMatch(t *testing.T) {
	// Two queues both mentioned in the same log line; only the first should fire.
	lm := &LogMonitorWorker{
		quorumList: []TrackedQueue{
			{VHost: "/", Name: "alpha.queue"},
			{VHost: "/", Name: "beta.queue"},
		},
		jobChannel: make(chan QueueJob, 10),
	}

	lm.processLogLine("ERROR: quorum failure on alpha.queue and beta.queue")

	time.Sleep(30 * time.Millisecond)

	alphaKey := "/" + "/" + "alpha.queue"
	betaKey := "/" + "/" + "beta.queue"

	_, alphaStored := lm.pendingChecks.Load(alphaKey)
	_, betaStored := lm.pendingChecks.Load(betaKey)

	assert.True(t, alphaStored || betaStored, "expected at least one queue to be stored in pendingChecks")
	assert.False(t, alphaStored && betaStored, "only the first matching queue should be stored (break after first match)")
}

// ---------------------------------------------------------------------------
// coalesceLogEvent
// ---------------------------------------------------------------------------

func TestCoalesceLogEvent_Deduplication(t *testing.T) {
	lm := &LogMonitorWorker{
		jobChannel: make(chan QueueJob, 10),
	}

	key := "myvhost/my.queue"

	// Pre-populate the key so the second call returns early immediately.
	lm.pendingChecks.Store(key, true)

	// This call should detect the loaded key and return immediately (no sleep, no send).
	done := make(chan struct{})
	go func() {
		lm.coalesceLogEvent("myvhost", "my.queue")
		close(done)
	}()

	select {
	case <-done:
		// Good: returned quickly.
	case <-time.After(500 * time.Millisecond):
		assert.Fail(t, "coalesceLogEvent with duplicate key should return immediately, not sleep 4s")
	}

	select {
	case job := <-lm.jobChannel:
		assert.Failf(t, "no job expected for duplicate key", "got %+v", job)
	default:
	}
}

// ---------------------------------------------------------------------------
// syncInventory
// ---------------------------------------------------------------------------

func TestSyncInventory_OnlyQuorumQueuesCached(t *testing.T) {
	queues := []broker.RabbitQueue{
		{Name: "q1", VHost: "/", Type: "quorum"},
		{Name: "q2", VHost: "/", Type: "classic"},
		{Name: "q3", VHost: "vh2", Type: "quorum"},
		{Name: "q4", VHost: "/", Type: "stream"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode(queues)
		require.NoError(t, err)
	}))
	defer server.Close()

	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	lm := &LogMonitorWorker{diagnostics: dm, jobChannel: make(chan QueueJob, 10)}

	err := lm.syncInventory(context.Background())
	require.NoError(t, err)

	lm.registryLock.RLock()
	list := lm.quorumList
	lm.registryLock.RUnlock()

	require.Equal(t, len(list), 2)
	for _, q := range list {
		assert.False(t, q.Name == "q2" || q.Name == "q4", "non-quorum queue must not be in quorumList", "%q", q.Name)
	}
}

func TestSyncInventory_EmptyListOnAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	lm := &LogMonitorWorker{diagnostics: dm, jobChannel: make(chan QueueJob, 10)}

	err := lm.syncInventory(context.Background())
	require.Error(t, err, "expected error when management API returns non-200")
}

func TestSyncInventory_ReplacesExistingList(t *testing.T) {
	// First sync
	queues1 := []broker.RabbitQueue{{Name: "old.q", VHost: "/", Type: "quorum"}}
	// Second sync returns a different set
	queues2 := []broker.RabbitQueue{{Name: "new.q", VHost: "/", Type: "quorum"}}

	callN := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callN++
		if callN == 1 {
			err := json.NewEncoder(w).Encode(queues1)
			require.NoError(t, err)
		} else {
			err := json.NewEncoder(w).Encode(queues2)
			require.NoError(t, err)
		}
	}))
	defer server.Close()

	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	lm := &LogMonitorWorker{diagnostics: dm, jobChannel: make(chan QueueJob, 10)}

	err := lm.syncInventory(context.Background())
	require.NoError(t, err)
	err = lm.syncInventory(context.Background())
	require.NoError(t, err)

	lm.registryLock.RLock()
	list := lm.quorumList
	lm.registryLock.RUnlock()

	if len(list) != 1 || list[0].Name != "new.q" {
		assert.Failf(t, "expected list to contain only 'new.q'", "got %+v", list)
	}
}

// ---------------------------------------------------------------------------
// healthCheckWorker — dispatch path (Transient/Green do not trigger pipeline)
// ---------------------------------------------------------------------------

func TestHealthCheckWorker_GreenReleasesLatch(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: "rabbit@n1", Running: true}}
	queue := &broker.RabbitQueue{
		Name: "ok.q", VHost: "/", Type: "quorum",
		Status: "running", Leader: "rabbit@n1", Node: "rabbit@n1",
	}
	server := testutil.TestHealthServer(t, nodes, queue, 0, nil)
	defer server.Close()

	dm := newTestManager(server)
	lm := &LogMonitorWorker{
		diagnostics:   dm,
		cfg:           runtime.Config{AMQPURL: "amqp://invalid/"},
		jobChannel:    make(chan QueueJob, 1),
		pendingChecks: sync.Map{},
	}

	key := "//" + "ok.q"
	lm.pendingChecks.Store(key, true)
	lm.jobChannel <- QueueJob{VHost: "/", Name: "ok.q"}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go lm.healthCheckWorker(ctx, 0)

	// Wait until the latch is released.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if _, exists := lm.pendingChecks.Load(key); !exists {
			return // success: latch was cleared
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Fail(t, "pendingChecks key was not deleted after a Green health result")
}

func newTestManager(server *httptest.Server) *broker.DiagnosticsManager {
	return &broker.DiagnosticsManager{
		APIURL:   server.URL,
		Username: "test",
		Password: "test",
		Client:   server.Client(),
	}
}
