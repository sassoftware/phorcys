package monitor

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sassoftware/argus/internal/actions"
	"github.com/sassoftware/argus/internal/manager"
	"github.com/sassoftware/argus/internal/runtime"
)

const logScannerQueueName = "argus.logs.health.scanner"

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
	diagnostics   *manager.DiagnosticsManager
	cfg           runtime.Config
	quorumList    []TrackedQueue
	registryLock  sync.RWMutex
	pendingChecks sync.Map
	jobChannel    chan QueueJob
	workerCount   int
}

// NewLogMonitorWorker constructs a LogMonitorWorker wired to the provided connection and config.
func NewLogMonitorWorker(conn *amqp.Connection, dm *manager.DiagnosticsManager, cfg runtime.Config, concurrentWorkers int) *LogMonitorWorker {
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
			case manager.HealthUnrecoverable:
				log.Printf("[Worker #%d] CRITICAL: %s is unrecoverable — initiating recovery pipeline", id, key)
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("[Worker #%d] Recovery pipeline PANIC for %s: %v", id, key, r)
						}
					}()
					if err := runRecoveryPipeline(ctx, lm.cfg, lm.diagnostics, job.VHost, job.Name); err != nil {
						log.Printf("[Worker #%d] Recovery pipeline FAILED for %s: %v", id, key, err)
					} else {
						log.Printf("[Worker #%d] Recovery pipeline completed for %s", id, key)
					}
				}()
			case manager.HealthTransient:
				log.Printf("[Worker #%d] Queue %s is transiently down — monitoring, no action taken", id, key)
			default:
				log.Printf("[Worker #%d] Queue %s is healthy (%v) — no action required", id, key, health)
			}

			lm.pendingChecks.Delete(key)
		}
	}
}

// runRecoveryPipeline executes the full automated recovery for a confirmed unrecoverable queue:
//  1. Locate the quorum queue's data directory on disk
//  2. Back up segment files to a timestamped archive folder
//  3. Back up WAL files that contain records for this queue (only if any exist)
//  4. Delete the corrupted queue from the broker
//  5. Carve recoverable payloads from backed-up segment files
//  6. Carve recoverable payloads from backed-up WAL files (queue-UID filtered)
//  7. Republish all payloads — segments first, then WAL — to preserve ordering
func runRecoveryPipeline(ctx context.Context, cfg runtime.Config, dm *manager.DiagnosticsManager, vhost, queueName string) error {
	log.Printf("[Recovery] Starting pipeline for queue %s/%s", vhost, queueName)

	// Phase 1: Locate the queue's Raft data directory.
	// The directory basename IS the Ra UID used to identify this queue's records in the WAL.
	queueDir, err := actions.FindQueueDirectory(cfg.QuorumBasePath, vhost, queueName)
	if err != nil {
		return fmt.Errorf("locate phase: %w", err)
	}
	queueUID := filepath.Base(queueDir)
	log.Printf("[Recovery] Located quorum data at: %s (UID: %s)", queueDir, queueUID)

	// Phase 2: Backup segment files before any destructive operations.
	backupDir, err := actions.BackupQueueData(queueDir, cfg.BackupBaseDir, vhost, queueName)
	if err != nil {
		return fmt.Errorf("backup phase: %w", err)
	}
	log.Printf("[Recovery] Segment data backed up to: %s", backupDir)

	// Phase 3: Backup WAL files — but only those that actually contain records for
	// this queue's UID. The WAL is shared across all quorum queues on the node.
	walDir := cfg.QuorumBasePath
	walBackupDir := filepath.Join(backupDir, "wal")
	walsBacked, err := actions.BackupWALFiles(walDir, walBackupDir, queueUID)
	if err != nil {
		log.Printf("[Recovery] WARNING: WAL backup failed (non-fatal): %v", err)
	} else if walsBacked > 0 {
		log.Printf("[Recovery] Backed up %d WAL file(s) containing records for UID %s", walsBacked, queueUID)
	} else {
		log.Printf("[Recovery] No WAL records found for UID %s — skipping WAL backup", queueUID)
	}

	// Phase 4: Delete the corrupted queue from the broker.
	if err := dm.DeleteOrForceEvict(ctx, vhost, queueName); err != nil {
		return fmt.Errorf("delete phase: %w", err)
	}
	log.Printf("[Recovery] Queue %s deleted from broker", queueName)

	// Phase 5: Carve messages from backed-up segment files.
	// CarveMessagesFromDir already skips non-segment/wal extensions; WAL files
	// in the main backup dir have been separated into the wal/ subdirectory so
	// this call only processes .segment files here.
	segPayloads, err := actions.CarveMessagesFromDir(backupDir)
	if err != nil {
		return fmt.Errorf("segment carve phase: %w", err)
	}
	log.Printf("[Recovery] Extracted %d payload(s) from segment backup", len(segPayloads))

	// Phase 6: Carve messages from backed-up WAL files, filtered to this queue's UID.
	walPayloads, err := actions.CarveWALMessages(walBackupDir, queueUID)
	if err != nil {
		log.Printf("[Recovery] WARNING: WAL carve failed (non-fatal): %v", err)
	}
	log.Printf("[Recovery] Extracted %d payload(s) from WAL backup", len(walPayloads))

	// Segments are written before WAL entries are flushed, so publish segments first.
	payloads := append(segPayloads, walPayloads...)
	if len(payloads) == 0 {
		log.Printf("[Recovery] No messages found in backup for %s — pipeline complete.", queueName)
		return nil
	}

	// Phase 7: Republish to default exchange; routing key = queue name.
	if err := actions.RepublishMessages(ctx, cfg.AMQPURL, queueName, payloads); err != nil {
		return fmt.Errorf("republish phase: %w", err)
	}
	log.Printf("[Recovery] Successfully republished %d message(s) to queue %s (%d from segments, %d from WAL)",
		len(payloads), queueName, len(segPayloads), len(walPayloads))

	return nil
}
