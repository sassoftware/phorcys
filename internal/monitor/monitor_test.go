// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sassoftware/phorcys/internal/amqpx/amqptest"
	"github.com/sassoftware/phorcys/internal/common"
	"github.com/sassoftware/phorcys/internal/jobqueue"
	"github.com/sassoftware/phorcys/internal/recovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/runtime"
	"github.com/sassoftware/phorcys/internal/testutil"
)

const (
	testNode     = "rabbit@n1"
	quorumType   = "quorum"
	classicType  = "classic"
	streamType   = "stream"
	testHostname = "host-a"
)

// ---------------------------------------------------------------------------
// processLogLine
// ---------------------------------------------------------------------------

func TestProcessLogLine_NoRelevantKeywords_IgnoresLine(t *testing.T) {
	lm := &LogMonitorWorker{
		quorumList: []common.QueueDef{{VHost: "/", Name: "important.queue"}},
		jobQueue:   jobqueue.New(),
	}

	lm.processLogLine("INFO: application started successfully on port 5672")

	// No goroutine should have been spawned, jobChannel stays empty.
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 0, lm.jobQueue.Len(), "did not expect a job")
}

func TestProcessLogLine_KeywordButNoMatchingQueueName(t *testing.T) {
	lm := &LogMonitorWorker{
		quorumList: []common.QueueDef{{VHost: "/", Name: "unrelated.queue"}},
		jobQueue:   jobqueue.New(),
	}

	// Has "raft" keyword but the line does not mention the tracked queue.
	lm.processLogLine("ERROR: raft leader election timeout on node rabbit@n1")

	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 0, lm.jobQueue.Len(), "did not expect a job")
}

func TestProcessLogLine_MatchEnqueuesPendingCheck(t *testing.T) {
	lm := &LogMonitorWorker{
		quorumList: []common.QueueDef{{VHost: "/", Name: "events.queue"}},
		jobQueue:   jobqueue.New(),
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
		quorumList: []common.QueueDef{
			{VHost: "/", Name: "alpha.queue"},
			{VHost: "/", Name: "beta.queue"},
		},
		jobQueue: jobqueue.New(),
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
		jobQueue: jobqueue.New(),
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
		assert.Fail(t, "coalesceLogEvent with duplicate key should return immediately, not sleep 500ms")
	}
	assert.Equal(t, 0, lm.jobQueue.Len(), "no job should have been queued for duplicate key")
}

// ---------------------------------------------------------------------------
// syncInventory
// ---------------------------------------------------------------------------

func TestSyncInventory_OnlyQuorumQueuesCached(t *testing.T) {
	queues := []broker.RabbitQueue{
		{Name: "q1", VHost: "/", Type: quorumType},
		{Name: "q2", VHost: "/", Type: classicType},
		{Name: "q3", VHost: "vh2", Type: quorumType},
		{Name: "q4", VHost: "/", Type: streamType},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode(queues)
		require.NoError(t, err)
	}))
	defer server.Close()

	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	lm := &LogMonitorWorker{diagnostics: dm, jobQueue: jobqueue.New()}

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
	lm := &LogMonitorWorker{diagnostics: dm, jobQueue: jobqueue.New()}

	err := lm.syncInventory(context.Background())
	require.Error(t, err, "expected error when management API returns non-200")
}

func TestSyncInventory_ReplacesExistingList(t *testing.T) {
	// First sync
	queues1 := []broker.RabbitQueue{{Name: "old.q", VHost: "/", Type: quorumType}}
	// Second sync returns a different set
	queues2 := []broker.RabbitQueue{{Name: "new.q", VHost: "/", Type: quorumType}}

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
	lm := &LogMonitorWorker{diagnostics: dm, jobQueue: jobqueue.New()}

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
	nodes := []broker.RabbitNode{{Name: testNode, Running: true}}
	queue := &broker.RabbitQueue{
		Name: "ok.q", VHost: "/", Type: quorumType,
		Status: "running", Leader: testNode, Node: testNode,
	}
	server := testutil.TestHealthServer(t, nodes, queue, 0, nil)
	defer server.Close()

	dm := newTestManager(server)
	lm := &LogMonitorWorker{
		diagnostics:   dm,
		cfg:           runtime.Config{AMQPURL: "amqp://invalid/"},
		jobQueue:      jobqueue.New(),
		pendingChecks: sync.Map{},
	}

	key := "//" + "ok.q"
	lm.pendingChecks.Store(key, true)
	lm.jobQueue.Add("/", "ok.q")

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

func waitForLatchRelease(t *testing.T, lm *LogMonitorWorker, key string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, exists := lm.pendingChecks.Load(key)
		return !exists
	}, 2*time.Second, 10*time.Millisecond, "pendingChecks key %q was not deleted", key)
}

func runSingleJob(t *testing.T, lm *LogMonitorWorker, vhost, name string) {
	t.Helper()
	key := vhost + "/" + name
	lm.pendingChecks.Store(key, true)
	lm.jobQueue.Add(vhost, name)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		lm.healthCheckWorker(ctx, 0)
		close(done)
	}()
	waitForLatchRelease(t, lm, key)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		assert.Fail(t, "healthCheckWorker did not exit after context cancellation")
	}
}

func quorumListServer(t *testing.T, queues []broker.RabbitQueue) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(queues))
	}))
}

// ---------------------------------------------------------------------------
// NewLogMonitorWorker / UseCoordinator
// ---------------------------------------------------------------------------

func TestNewLogMonitorWorker_SetsDefaults(t *testing.T) {
	conn := amqptest.NewConnection(amqptest.NewChannel())
	dm := &broker.DiagnosticsManager{}
	cfg := runtime.Config{Hostname: testHostname}
	jq := jobqueue.New()

	lm := NewLogMonitorWorker(conn, dm, cfg, jq, 3)

	assert.Same(t, conn, lm.amqpConn)
	assert.Same(t, dm, lm.diagnostics)
	assert.Equal(t, cfg, lm.cfg)
	assert.Same(t, jq, lm.jobQueue)
	assert.Equal(t, 3, lm.workerCount)
	assert.Equal(t, defaultInventoryInterval, lm.inventoryInterval)
	assert.Equal(t, defaultCoalesceDelay, lm.coalesceDelay)
	assert.NotNil(t, lm.runRecovery)
}

func TestUseCoordinator_RoutesRecoveryThroughCoordinator(t *testing.T) {
	cfg := runtime.Config{Hostname: testHostname, QuorumBasePath: t.TempDir(), BackupBaseDir: t.TempDir()}
	lm := NewLogMonitorWorker(nil, nil, cfg, jobqueue.New(), 1)
	coord := recovery.NewCoordinator(amqptest.NewConnection(amqptest.NewChannel()), nil, cfg)

	lm.UseCoordinator(coord)
	require.NotNil(t, lm.runRecovery)

	// The queue directory does not exist, so the coordinator's prepare phase fails.
	err := lm.runRecovery(context.Background(), runtime.Config{}, nil, "/", "missing.q")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "locate phase")
}

// ---------------------------------------------------------------------------
// syncInventory
// ---------------------------------------------------------------------------

func TestSyncInventory_NilDiagnosticsReturnsError(t *testing.T) {
	lm := &LogMonitorWorker{jobQueue: jobqueue.New()}
	err := lm.syncInventory(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DiagnosticsManager not initialized")
}

func TestProcessLogLine_SyncFailureStillMatchesCachedList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	lm := &LogMonitorWorker{
		diagnostics: newTestManager(server),
		quorumList:  []common.QueueDef{{VHost: "/", Name: "cached.q"}},
		jobQueue:    jobqueue.New(),
	}

	lm.processLogLine("ERROR: quorum failure on cached.q")

	require.Eventually(t, func() bool {
		_, ok := lm.pendingChecks.Load("//cached.q")
		return ok
	}, time.Second, 10*time.Millisecond)
}

// ---------------------------------------------------------------------------
// coalesceLogEvent
// ---------------------------------------------------------------------------

func TestCoalesceLogEvent_AddsJobAfterDelay(t *testing.T) {
	lm := &LogMonitorWorker{jobQueue: jobqueue.New(), coalesceDelay: 10 * time.Millisecond}

	lm.coalesceLogEvent("/", "burst.q")

	assert.Equal(t, 1, lm.jobQueue.Len())
	_, ok := lm.pendingChecks.Load("//burst.q")
	assert.True(t, ok, "latch should remain held until the worker processes the job")
}

// ---------------------------------------------------------------------------
// consumeSystemLogs
// ---------------------------------------------------------------------------

func TestConsumeSystemLogs_ChannelError(t *testing.T) {
	conn := amqptest.NewConnection(amqptest.NewChannel())
	conn.ChannelErr = errors.New("no channel")
	lm := &LogMonitorWorker{amqpConn: conn}

	err := lm.consumeSystemLogs(context.Background())
	require.EqualError(t, err, "no channel")
}

func TestConsumeSystemLogs_DeclareError(t *testing.T) {
	ch := amqptest.NewChannel()
	ch.DeclareErr = errors.New("declare failed")
	lm := &LogMonitorWorker{amqpConn: amqptest.NewConnection(ch)}

	err := lm.consumeSystemLogs(context.Background())
	require.EqualError(t, err, "declare failed")
	assert.True(t, ch.Closed(), "channel should be closed on return")
}

func TestConsumeSystemLogs_BindError(t *testing.T) {
	ch := amqptest.NewChannel()
	ch.BindErr = errors.New("bind failed")
	lm := &LogMonitorWorker{amqpConn: amqptest.NewConnection(ch)}

	err := lm.consumeSystemLogs(context.Background())
	require.EqualError(t, err, "bind failed")
	assert.Len(t, ch.Bindings(), 1, "should stop after the first failed bind")
	assert.True(t, ch.Closed())
}

func TestConsumeSystemLogs_ConsumeError(t *testing.T) {
	ch := amqptest.NewChannel()
	ch.ConsumeErr = errors.New("consume failed")
	lm := &LogMonitorWorker{amqpConn: amqptest.NewConnection(ch)}

	err := lm.consumeSystemLogs(context.Background())
	require.EqualError(t, err, "consume failed")
	assert.True(t, ch.Closed())
}

func TestConsumeSystemLogs_DeclaresBindsAndConsumes(t *testing.T) {
	ch := amqptest.NewChannel()
	ch.Deliveries = make(chan amqp.Delivery, 1)
	close(ch.Deliveries)
	lm := &LogMonitorWorker{
		amqpConn: amqptest.NewConnection(ch),
		cfg:      runtime.Config{Hostname: "node-1"},
	}

	err := lm.consumeSystemLogs(context.Background())
	require.NoError(t, err, "closed delivery channel should end consumption cleanly")

	queueName := logScannerQueuePrefix + "node-1"
	declared := ch.Declared()
	require.Len(t, declared, 1)
	assert.Equal(t, queueName, declared[0].Name)
	assert.True(t, declared[0].Durable)
	assert.False(t, declared[0].AutoDelete)
	assert.False(t, declared[0].Exclusive)
	assert.Equal(t, amqp.Table{"x-max-length": int32(2000)}, declared[0].Args)

	bindings := ch.Bindings()
	require.Len(t, bindings, 2)
	assert.Equal(t, "warning", bindings[0].Key)
	assert.Equal(t, "error", bindings[1].Key)
	for _, b := range bindings {
		assert.Equal(t, queueName, b.Queue)
		assert.Equal(t, logExchange, b.Exchange)
	}

	consumers := ch.Consumers()
	require.Len(t, consumers, 1)
	assert.Equal(t, queueName, consumers[0].Queue)
	assert.True(t, consumers[0].AutoAck)
	assert.True(t, consumers[0].Exclusive)
	assert.True(t, ch.Closed())
}

func TestConsumeSystemLogs_ProcessesMessagesUntilCancelled(t *testing.T) {
	server := quorumListServer(t, []broker.RabbitQueue{{Name: "logs.q", VHost: "/", Type: quorumType}})
	defer server.Close()

	ch := amqptest.NewChannel()
	lm := &LogMonitorWorker{
		amqpConn:      amqptest.NewConnection(ch),
		diagnostics:   newTestManager(server),
		jobQueue:      jobqueue.New(),
		coalesceDelay: time.Hour,
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- lm.consumeSystemLogs(ctx) }()

	ch.Deliveries <- amqp.Delivery{Body: []byte("ERROR: ra quorum crash for logs.q")}

	require.Eventually(t, func() bool {
		_, ok := lm.pendingChecks.Load("//logs.q")
		return ok
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		assert.Fail(t, "consumeSystemLogs did not return after cancellation")
	}
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

func TestStart_InventoryErrorAborts(t *testing.T) {
	ch := amqptest.NewChannel()
	lm := &LogMonitorWorker{amqpConn: amqptest.NewConnection(ch), jobQueue: jobqueue.New()}

	err := lm.Start(context.Background())
	require.Error(t, err)
	assert.Empty(t, ch.Declared(), "should not consume logs when inventory sync fails")
}

func TestStart_RefreshesInventoryAndStopsOnCancel(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		// Fail one periodic refresh to exercise the error-logging path.
		if n == 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode([]broker.RabbitQueue{{Name: "q", VHost: "/", Type: quorumType}}))
	}))
	defer server.Close()

	ch := amqptest.NewChannel()
	lm := &LogMonitorWorker{
		amqpConn:          amqptest.NewConnection(ch),
		diagnostics:       newTestManager(server),
		jobQueue:          jobqueue.New(),
		workerCount:       2,
		inventoryInterval: 10 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- lm.Start(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 3
	}, 2*time.Second, 10*time.Millisecond, "inventory should be refreshed periodically")

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		assert.Fail(t, "Start did not return after cancellation")
	}
	assert.Len(t, ch.Consumers(), 1)
}

func TestStart_NonPositiveIntervalUsesDefault(t *testing.T) {
	server := quorumListServer(t, nil)
	defer server.Close()

	ch := amqptest.NewChannel()
	ch.Deliveries = make(chan amqp.Delivery)
	close(ch.Deliveries)
	lm := &LogMonitorWorker{
		amqpConn:    amqptest.NewConnection(ch),
		diagnostics: newTestManager(server),
		jobQueue:    jobqueue.New(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, lm.Start(ctx))
}

// ---------------------------------------------------------------------------
// healthCheckWorker — remaining branches
// ---------------------------------------------------------------------------

func TestHealthCheckWorker_EvaluationErrorReleasesLatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	called := false
	lm := &LogMonitorWorker{
		diagnostics: newTestManager(server),
		jobQueue:    jobqueue.New(),
		runRecovery: func(context.Context, runtime.Config, *broker.DiagnosticsManager, string, string) error {
			called = true
			return nil
		},
	}

	runSingleJob(t, lm, "/", "err.q")
	assert.False(t, called, "recovery must not run when health evaluation fails")
}

func TestHealthCheckWorker_TransientDoesNotRecover(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: testNode, Running: true, MemAlarm: true}}
	server := testutil.TestHealthServer(t, nodes, nil, 0, nil)
	defer server.Close()

	called := false
	lm := &LogMonitorWorker{
		diagnostics: newTestManager(server),
		jobQueue:    jobqueue.New(),
		runRecovery: func(context.Context, runtime.Config, *broker.DiagnosticsManager, string, string) error {
			called = true
			return nil
		},
	}

	runSingleJob(t, lm, "/", "transient.q")
	assert.False(t, called, "recovery must not run for transient health")
}

func unrecoverableServer(t *testing.T) *httptest.Server {
	t.Helper()
	nodes := []broker.RabbitNode{{Name: testNode, Running: true}}
	return testutil.TestHealthServer(t, nodes, nil, http.StatusInternalServerError, nil)
}

func TestHealthCheckWorker_UnrecoverableRunsRecovery(t *testing.T) {
	server := unrecoverableServer(t)
	defer server.Close()

	cfg := runtime.Config{Hostname: testHostname}
	dm := newTestManager(server)
	type call struct {
		cfg         runtime.Config
		dm          *broker.DiagnosticsManager
		vhost, name string
	}
	calls := make(chan call, 1)
	lm := &LogMonitorWorker{
		diagnostics: dm,
		cfg:         cfg,
		jobQueue:    jobqueue.New(),
		runRecovery: func(_ context.Context, c runtime.Config, d *broker.DiagnosticsManager, vhost, name string) error {
			calls <- call{c, d, vhost, name}
			return nil
		},
	}

	runSingleJob(t, lm, "vh", "broken.q")

	select {
	case got := <-calls:
		assert.Equal(t, cfg, got.cfg)
		assert.Same(t, dm, got.dm)
		assert.Equal(t, "vh", got.vhost)
		assert.Equal(t, "broken.q", got.name)
	default:
		assert.Fail(t, "expected recovery to be invoked")
	}
}

func TestHealthCheckWorker_RecoveryErrorReleasesLatch(t *testing.T) {
	server := unrecoverableServer(t)
	defer server.Close()

	lm := &LogMonitorWorker{
		diagnostics: newTestManager(server),
		jobQueue:    jobqueue.New(),
		runRecovery: func(context.Context, runtime.Config, *broker.DiagnosticsManager, string, string) error {
			return errors.New("recovery failed")
		},
	}

	runSingleJob(t, lm, "/", "fail.q")
}

func TestHealthCheckWorker_RecoveryPanicIsRecovered(t *testing.T) {
	server := unrecoverableServer(t)
	defer server.Close()

	lm := &LogMonitorWorker{
		diagnostics: newTestManager(server),
		jobQueue:    jobqueue.New(),
		runRecovery: func(context.Context, runtime.Config, *broker.DiagnosticsManager, string, string) error {
			panic("boom")
		},
	}

	// Two jobs: the worker must survive the first panic to process the second.
	lm.pendingChecks.Store("//second.q", true)
	lm.jobQueue.Add("/", "second.q")
	runSingleJob(t, lm, "/", "panic.q")
	waitForLatchRelease(t, lm, "//second.q")
}

func TestHealthCheckWorker_NilRecoveryFallsBackToDefaultPipeline(t *testing.T) {
	server := unrecoverableServer(t)
	defer server.Close()

	lm := &LogMonitorWorker{
		diagnostics: newTestManager(server),
		// An empty quorum dir makes recovery.Run fail fast at the locate phase.
		cfg:      runtime.Config{QuorumBasePath: t.TempDir(), BackupBaseDir: t.TempDir()},
		jobQueue: jobqueue.New(),
	}

	runSingleJob(t, lm, "/", "default.q")
}
