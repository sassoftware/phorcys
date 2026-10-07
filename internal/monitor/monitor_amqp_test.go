// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sassoftware/phorcys/internal/amqpx/amqptest"
	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/runtime"
	"github.com/sassoftware/phorcys/internal/testutil"
)

const (
	eventsQueue     = "events.queue"
	eventsKey       = "//" + eventsQueue
	matchingLogLine = "ERROR: ra quorum leader crash detected for queue " + eventsQueue
	waitTimeout     = 2 * time.Second
	pollInterval    = 5 * time.Millisecond
	testNode        = "rabbit@n1"
	quorumType      = "quorum"
)

var healthyNodes = []broker.RabbitNode{{Name: testNode, Running: true}}

// recordingRecovery returns a recoveryFunc that reports each invocation on the returned channel.
func recordingRecovery(err error) (recoveryFunc, <-chan QueueJob) {
	calls := make(chan QueueJob, 10)
	return func(_ context.Context, _ runtime.Config, _ *broker.DiagnosticsManager, vhost, name string) error {
		calls <- QueueJob{VHost: vhost, Name: name}
		return err
	}, calls
}

func receiveJob(t *testing.T, calls <-chan QueueJob) QueueJob {
	t.Helper()
	select {
	case job := <-calls:
		return job
	case <-time.After(waitTimeout):
		require.FailNow(t, "timed out waiting for recovery to be invoked")
		return QueueJob{}
	}
}

func assertNoJob(t *testing.T, calls <-chan QueueJob) {
	t.Helper()
	select {
	case job := <-calls:
		assert.Failf(t, "recovery must not be invoked", "got %+v", job)
	case <-time.After(50 * time.Millisecond):
	}
}

func assertLatchReleased(t *testing.T, lm *LogMonitorWorker, key string) {
	t.Helper()
	assert.Eventually(t, func() bool {
		_, exists := lm.pendingChecks.Load(key)
		return !exists
	}, waitTimeout, pollInterval, "pendingChecks key %q was not released", key)
}

// startResult runs fn in a goroutine and returns a channel yielding its error.
func startResult(fn func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	return done
}

func awaitResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(waitTimeout):
		require.FailNow(t, "timed out waiting for function to return")
		return nil
	}
}

func deliver(t *testing.T, ch *amqptest.Channel, body string) {
	t.Helper()
	select {
	case ch.Deliveries <- amqp.Delivery{Body: []byte(body)}:
	case <-time.After(waitTimeout):
		require.FailNow(t, "timed out delivering message; is the consumer running?")
	}
}

// ---------------------------------------------------------------------------
// NewLogMonitorWorker
// ---------------------------------------------------------------------------

func TestNewLogMonitorWorker_SetsDefaults(t *testing.T) {
	conn := amqptest.NewConnection(amqptest.NewChannel())
	dm := &broker.DiagnosticsManager{}
	cfg := runtime.Config{AMQPURL: "amqp://fake/"}

	lm := NewLogMonitorWorker(conn, dm, cfg, 3)

	assert.Same(t, conn, lm.amqpConn)
	assert.Same(t, dm, lm.diagnostics)
	assert.Equal(t, cfg, lm.cfg)
	assert.Equal(t, 3, lm.workerCount)
	assert.Equal(t, 1000, cap(lm.jobChannel))
	assert.Equal(t, defaultInventoryInterval, lm.inventoryInterval)
	assert.Equal(t, defaultCoalesceDelay, lm.coalesceDelay)
	assert.NotNil(t, lm.runRecovery)
}

// ---------------------------------------------------------------------------
// consumeSystemLogs
// ---------------------------------------------------------------------------

func TestConsumeSystemLogs_DeclaresBindsAndConsumes(t *testing.T) {
	ch := amqptest.NewChannel()
	lm := &LogMonitorWorker{amqpConn: amqptest.NewConnection(ch), jobChannel: make(chan QueueJob, 1)}

	done := startResult(func() error { return lm.consumeSystemLogs(context.Background()) })
	close(ch.Deliveries)
	require.NoError(t, awaitResult(t, done), "closed delivery channel should end consumption cleanly")

	declared := ch.Declared()
	require.Len(t, declared, 1)
	assert.Equal(t, amqptest.DeclaredQueue{
		Name:       logScannerQueueName,
		Durable:    false,
		AutoDelete: true,
		Exclusive:  true,
		NoWait:     false,
		Args:       amqp.Table{"x-max-length": int32(2000)},
	}, declared[0])

	bindings := ch.Bindings()
	require.Len(t, bindings, 2)
	for i, severity := range []string{"warning", "error"} {
		assert.Equal(t, logScannerQueueName, bindings[i].Queue)
		assert.Equal(t, severity, bindings[i].Key)
		assert.Equal(t, logExchange, bindings[i].Exchange)
	}

	consumers := ch.Consumers()
	require.Len(t, consumers, 1)
	assert.Equal(t, logScannerQueueName, consumers[0].Queue)
	assert.True(t, consumers[0].AutoAck)
	assert.True(t, consumers[0].Exclusive)

	assert.True(t, ch.Closed(), "channel should be closed on return")
}

func TestConsumeSystemLogs_ProcessesDeliveries(t *testing.T) {
	ch := amqptest.NewChannel()
	lm := &LogMonitorWorker{
		amqpConn:   amqptest.NewConnection(ch),
		quorumList: []TrackedQueue{{VHost: "/", Name: eventsQueue}},
		jobChannel: make(chan QueueJob, 1),
	}

	done := startResult(func() error { return lm.consumeSystemLogs(context.Background()) })
	deliver(t, ch, "INFO: unrelated line")
	deliver(t, ch, matchingLogLine)
	close(ch.Deliveries)
	require.NoError(t, awaitResult(t, done))

	select {
	case job := <-lm.jobChannel:
		assert.Equal(t, QueueJob{VHost: "/", Name: eventsQueue}, job)
	case <-time.After(waitTimeout):
		assert.Fail(t, "expected a job for the matching log line")
	}
}

func TestConsumeSystemLogs_ContextCancellation(t *testing.T) {
	ch := amqptest.NewChannel()
	lm := &LogMonitorWorker{amqpConn: amqptest.NewConnection(ch)}
	ctx, cancel := context.WithCancel(context.Background())

	done := startResult(func() error { return lm.consumeSystemLogs(ctx) })
	assert.Eventually(t, func() bool { return len(ch.Consumers()) == 1 }, waitTimeout, pollInterval)
	cancel()

	require.ErrorIs(t, awaitResult(t, done), context.Canceled)
	assert.True(t, ch.Closed())
}

func TestConsumeSystemLogs_SetupErrors(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name          string
		conn          *amqptest.Connection
		wantChClosed  bool
		wantBindCalls int
	}{
		{name: "channel", conn: &amqptest.Connection{ChannelErr: boom}},
		{name: "declare", conn: amqptest.NewConnection(&amqptest.Channel{DeclareErr: boom}), wantChClosed: true},
		{name: "bind", conn: amqptest.NewConnection(&amqptest.Channel{BindErr: boom}), wantChClosed: true, wantBindCalls: 1},
		{name: "consume", conn: amqptest.NewConnection(&amqptest.Channel{ConsumeErr: boom}), wantChClosed: true, wantBindCalls: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lm := &LogMonitorWorker{amqpConn: tt.conn}

			err := lm.consumeSystemLogs(context.Background())

			require.ErrorIs(t, err, boom)
			if tt.conn.Ch != nil {
				assert.Equal(t, tt.wantChClosed, tt.conn.Ch.Closed())
				assert.Len(t, tt.conn.Ch.Bindings(), tt.wantBindCalls)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// coalesceLogEvent
// ---------------------------------------------------------------------------

func TestCoalesceLogEvent_EnqueuesJobAfterDelay(t *testing.T) {
	lm := &LogMonitorWorker{jobChannel: make(chan QueueJob, 1), coalesceDelay: 20 * time.Millisecond}

	start := time.Now()
	lm.coalesceLogEvent("/", eventsQueue)

	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	_, latched := lm.pendingChecks.Load(eventsKey)
	assert.True(t, latched, "latch should stay set until a worker handles the job")
	select {
	case job := <-lm.jobChannel:
		assert.Equal(t, QueueJob{VHost: "/", Name: eventsQueue}, job)
	default:
		assert.Fail(t, "expected a job to be enqueued")
	}
}

// ---------------------------------------------------------------------------
// healthCheckWorker
// ---------------------------------------------------------------------------

// runWorker starts a healthCheckWorker with the job latched and enqueued,
// stopping it when the test ends.
func runWorker(t *testing.T, lm *LogMonitorWorker, jobs ...QueueJob) {
	t.Helper()
	for _, job := range jobs {
		lm.pendingChecks.Store(job.VHost+"/"+job.Name, true)
		lm.jobChannel <- job
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go lm.healthCheckWorker(ctx, 0)
}

func newWorkerFor(server *httptest.Server, rec recoveryFunc) *LogMonitorWorker {
	return &LogMonitorWorker{
		diagnostics: newTestManager(server),
		jobChannel:  make(chan QueueJob, 10),
		runRecovery: rec,
	}
}

func TestHealthCheckWorker_UnrecoverableRunsRecovery(t *testing.T) {
	server := testutil.TestHealthServer(t, healthyNodes, nil, http.StatusInternalServerError, nil)
	defer server.Close()
	rec, calls := recordingRecovery(nil)
	lm := newWorkerFor(server, rec)

	runWorker(t, lm, QueueJob{VHost: "/", Name: eventsQueue})

	assert.Equal(t, QueueJob{VHost: "/", Name: eventsQueue}, receiveJob(t, calls))
	assertLatchReleased(t, lm, eventsKey)
}

func TestHealthCheckWorker_RecoveryFailureReleasesLatch(t *testing.T) {
	server := testutil.TestHealthServer(t, healthyNodes, nil, http.StatusInternalServerError, nil)
	defer server.Close()
	rec, calls := recordingRecovery(errors.New("pipeline failed"))
	lm := newWorkerFor(server, rec)

	runWorker(t, lm, QueueJob{VHost: "/", Name: eventsQueue})

	receiveJob(t, calls)
	assertLatchReleased(t, lm, eventsKey)
}

func TestHealthCheckWorker_RecoveryPanicIsContained(t *testing.T) {
	server := testutil.TestHealthServer(t, healthyNodes, nil, http.StatusInternalServerError, nil)
	defer server.Close()
	var invocations atomic.Int32
	lm := newWorkerFor(server, func(context.Context, runtime.Config, *broker.DiagnosticsManager, string, string) error {
		if invocations.Add(1) == 1 {
			panic("corrupt segment")
		}
		return nil
	})

	runWorker(t, lm, QueueJob{VHost: "/", Name: "first.q"}, QueueJob{VHost: "/", Name: "second.q"})

	assert.Eventually(t, func() bool { return invocations.Load() == 2 }, waitTimeout, pollInterval,
		"worker should survive a recovery panic and process the next job")
	assertLatchReleased(t, lm, "//first.q")
	assertLatchReleased(t, lm, "//second.q")
}

func TestHealthCheckWorker_DefaultsToRecoveryRun(t *testing.T) {
	server := testutil.TestHealthServer(t, healthyNodes, nil, http.StatusInternalServerError, nil)
	defer server.Close()
	lm := newWorkerFor(server, nil)
	// An empty quorum directory makes recovery.Run fail fast in its locate phase.
	lm.cfg = runtime.Config{QuorumBasePath: t.TempDir()}

	runWorker(t, lm, QueueJob{VHost: "/", Name: eventsQueue})

	assertLatchReleased(t, lm, eventsKey)
}

func TestHealthCheckWorker_TransientSkipsRecovery(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: testNode, Running: true, MemAlarm: true}}
	server := testutil.TestHealthServer(t, nodes, nil, 0, nil)
	defer server.Close()
	rec, calls := recordingRecovery(nil)
	lm := newWorkerFor(server, rec)

	runWorker(t, lm, QueueJob{VHost: "/", Name: eventsQueue})

	assertLatchReleased(t, lm, eventsKey)
	assertNoJob(t, calls)
}

func TestHealthCheckWorker_EvaluationErrorReleasesLatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	rec, calls := recordingRecovery(nil)
	lm := newWorkerFor(server, rec)

	runWorker(t, lm, QueueJob{VHost: "/", Name: eventsQueue})

	assertLatchReleased(t, lm, eventsKey)
	assertNoJob(t, calls)
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

func TestStart_InventoryErrorAbortsBeforeConsuming(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	conn := amqptest.NewConnection(amqptest.NewChannel())
	lm := NewLogMonitorWorker(conn, newTestManager(server), runtime.Config{}, 1)

	err := lm.Start(context.Background())

	require.Error(t, err)
	assert.Empty(t, conn.Ch.Declared(), "no AMQP setup should happen when inventory sync fails")
}

func TestStart_LogLineTriggersRecoveryEndToEnd(t *testing.T) {
	allQueues := []broker.RabbitQueue{{Name: eventsQueue, VHost: "/", Type: quorumType}}
	server := testutil.TestHealthServer(t, healthyNodes, nil, http.StatusInternalServerError, allQueues)
	defer server.Close()
	ch := amqptest.NewChannel()
	rec, calls := recordingRecovery(nil)
	lm := NewLogMonitorWorker(amqptest.NewConnection(ch), newTestManager(server), runtime.Config{}, 2)
	lm.coalesceDelay = 0
	lm.runRecovery = rec
	ctx := t.Context()

	done := startResult(func() error { return lm.Start(ctx) })
	deliver(t, ch, matchingLogLine)

	assert.Equal(t, QueueJob{VHost: "/", Name: eventsQueue}, receiveJob(t, calls))
	close(ch.Deliveries)
	require.NoError(t, awaitResult(t, done))
}

func TestStart_PeriodicallyRefreshesInventory(t *testing.T) {
	var syncs atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := syncs.Add(1)
		if n == 2 {
			// A failed refresh is logged and does not stop the monitor.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		queues := []broker.RabbitQueue{{Name: fmt.Sprintf("q%d", n), VHost: "/", Type: quorumType}}
		assert.NoError(t, json.NewEncoder(w).Encode(queues))
	}))
	defer server.Close()
	ch := amqptest.NewChannel()
	lm := NewLogMonitorWorker(amqptest.NewConnection(ch), newTestManager(server), runtime.Config{}, 0)
	lm.inventoryInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())

	done := startResult(func() error { return lm.Start(ctx) })

	assert.Eventually(t, func() bool { return syncs.Load() >= 3 }, waitTimeout, pollInterval)
	assert.Eventually(t, func() bool {
		lm.registryLock.RLock()
		defer lm.registryLock.RUnlock()
		return len(lm.quorumList) == 1 && lm.quorumList[0].Name != "q1"
	}, waitTimeout, pollInterval, "inventory should be replaced by a later refresh")
	cancel()
	require.ErrorIs(t, awaitResult(t, done), context.Canceled)
}

func TestStart_NonPositiveIntervalUsesDefault(t *testing.T) {
	server := testutil.TestHealthServer(t, healthyNodes, nil, 0, nil)
	defer server.Close()
	ch := amqptest.NewChannel()
	lm := &LogMonitorWorker{
		amqpConn:    amqptest.NewConnection(ch),
		diagnostics: newTestManager(server),
		jobChannel:  make(chan QueueJob, 1),
	}
	ctx := t.Context()

	done := startResult(func() error { return lm.Start(ctx) })
	close(ch.Deliveries)

	require.NoError(t, awaitResult(t, done), "a zero interval must not panic time.NewTicker")
}
