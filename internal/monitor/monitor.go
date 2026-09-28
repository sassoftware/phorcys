// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package monitor

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/recovery"
	"github.com/sassoftware/phorcys/internal/runtime"
)

const logScannerQueueName = "phorcys.logs.health.scanner"

// QueueJob is a single health-check work item dispatched to the worker pool.
type QueueJob struct {
	VHost string
	Name  string
}

// TrackedQueue is a lightweight snapshot entry used for fast log-line text matching.
type TrackedQueue struct {
	VHost string
	Name  string
}

// LogMonitorWorker subscribes to the amq.rabbitmq.log exchange, detects quorum queue
// error signatures in log lines, and triggers the recovery pipeline when warranted.
type LogMonitorWorker struct {
	amqpConn      *amqp.Connection
	diagnostics   *broker.DiagnosticsManager
	cfg           runtime.Config
	quorumList    []TrackedQueue
	registryLock  sync.RWMutex
	pendingChecks sync.Map
	jobChannel    chan QueueJob
	workerCount   int
}

// NewLogMonitorWorker constructs a LogMonitorWorker wired to the provided connection and config.
func NewLogMonitorWorker(conn *amqp.Connection, dm *broker.DiagnosticsManager, cfg runtime.Config, concurrentWorkers int) *LogMonitorWorker {
	return &LogMonitorWorker{
		amqpConn:    conn,
		diagnostics: dm,
		cfg:         cfg,
		jobChannel:  make(chan QueueJob, 1000),
		workerCount: concurrentWorkers,
	}
}

// Start loads the initial queue inventory, launches background workers, and begins log consumption.
func (lm *LogMonitorWorker) Start(ctx context.Context) error {
	if err := lm.syncInventory(ctx); err != nil {
		return err
	}

	// Refresh the quorum queue inventory every 10 minutes so newly created queues are picked up.
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := lm.syncInventory(ctx); err != nil {
					log.Printf("ERROR: Failed syncing cluster queue names: %v", err)
				}
			}
		}
	}()

	for i := 0; i < lm.workerCount; i++ {
		go lm.healthCheckWorker(ctx, i)
	}

	return lm.consumeSystemLogs(ctx)
}

// syncInventory fetches all queues from the management API and caches quorum queue names.
func (lm *LogMonitorWorker) syncInventory(ctx context.Context) error {
	allQueues, err := lm.diagnostics.FetchAllQueues(ctx)
	if err != nil {
		return err
	}

	var updatedList []TrackedQueue
	for _, q := range allQueues {
		if q.Type == "quorum" {
			updatedList = append(updatedList, TrackedQueue{VHost: q.VHost, Name: q.Name})
		}
	}

	lm.registryLock.Lock()
	lm.quorumList = updatedList
	lm.registryLock.Unlock()

	log.Printf("Log System Sync: Loaded %d quorum queues into scanner memory.", len(updatedList))
	return nil
}

// consumeSystemLogs creates a transient queue bound to amq.rabbitmq.log and reads all messages.
func (lm *LogMonitorWorker) consumeSystemLogs(ctx context.Context) error {
	ch, err := lm.amqpConn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Transient, auto-expiring monitoring sink with a bounded length to cap memory usage.
	q, err := ch.QueueDeclare(
		logScannerQueueName,
		false, // non-durable
		true,  // auto-delete when last consumer detaches
		true,  // exclusive to this connection
		false,
		amqp.Table{"x-max-length": int32(2000)},
	)
	if err != nil {
		return err
	}

	for _, severity := range []string{"warning", "error"} {
		if err = ch.QueueBind(q.Name, severity, "amq.rabbitmq.log", false, nil); err != nil {
			return err
		}
	}

	msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}
			lm.processLogLine(string(msg.Body))
		}
	}
}

// processLogLine filters log output for Raft/quorum error signatures and enqueues matching queues.
func (lm *LogMonitorWorker) processLogLine(logLine string) {
	// Fast pre-filter: drop lines that have no quorum/Raft subsystem keywords.
	if !strings.Contains(logLine, "ra") &&
		!strings.Contains(logLine, "quorum") &&
		!strings.Contains(logLine, "raft") {
		return
	}

	lm.registryLock.RLock()
	defer lm.registryLock.RUnlock()

	for _, tq := range lm.quorumList {
		if strings.Contains(logLine, tq.Name) {
			go lm.coalesceLogEvent(tq.VHost, tq.Name)
			break
		}
	}
}

// coalesceLogEvent debounces rapid log bursts for the same queue into a single health check.
func (lm *LogMonitorWorker) coalesceLogEvent(vhost, name string) {
	key := vhost + "/" + name
	if _, loaded := lm.pendingChecks.LoadOrStore(key, true); loaded {
		return // A check is already queued or in flight.
	}
	// Brief pause to let the broker finish printing consecutive stack traces before we query it.
	time.Sleep(4 * time.Second)
	lm.jobChannel <- QueueJob{VHost: vhost, Name: name}
}

// healthCheckWorker consumes jobs from jobChannel, evaluates queue health, and triggers recovery.
func (lm *LogMonitorWorker) healthCheckWorker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-lm.jobChannel:
			key := job.VHost + "/" + job.Name
			log.Printf("[Worker #%d] Evaluating health for: %s", id, key)

			health, err := lm.diagnostics.EvaluateQueueHealth(ctx, job.VHost, job.Name)
			if err != nil {
				log.Printf("[Worker #%d] Health evaluation failed for %s: %v", id, key, err)
				lm.pendingChecks.Delete(key)
				continue
			}

			switch health {
			case broker.HealthUnrecoverable:
				log.Printf("[Worker #%d] CRITICAL: %s is unrecoverable — initiating recovery pipeline", id, key)
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("[Worker #%d] Recovery pipeline PANIC for %s: %v", id, key, r)
						}
					}()
					if err := recovery.Run(ctx, lm.cfg, lm.diagnostics, job.VHost, job.Name); err != nil {
						log.Printf("[Worker #%d] Recovery pipeline FAILED for %s: %v", id, key, err)
					} else {
						log.Printf("[Worker #%d] Recovery pipeline completed for %s", id, key)
					}
				}()
			case broker.HealthTransient:
				log.Printf("[Worker #%d] Queue %s is transiently down — monitoring, no action taken", id, key)
			case broker.HealthGreen:
				log.Printf("[Worker #%d] Queue %s is healthy (%v) — no action required", id, key, health)
			default:
				log.Printf("[Worker #%d] Queue %s has unknown health status (%v) — no action taken", id, key, health)
			}

			lm.pendingChecks.Delete(key)
		}
	}
}
