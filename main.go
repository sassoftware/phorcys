package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Config holds all runtime configuration for Argus.
type Config struct {
	// AMQP broker connection URL
	AMQPURL string
	// RabbitMQ Management API base URL (e.g., "http://localhost:15672")
	ManagementURL string
	// Management API credentials
	ManagementUser     string
	ManagementPassword string
	// Base filesystem path for quorum queue Raft storage
	QuorumBasePath string
	// Directory where queue data will be backed up before deletion
	BackupBaseDir string
	// Number of concurrent health-check workers
	WorkerCount int
}

func main() {
	cfg := Config{
		AMQPURL:            getEnv("ARGUS_AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		ManagementURL:      getEnv("ARGUS_MGMT_URL", "http://localhost:15672"),
		ManagementUser:     getEnv("ARGUS_MGMT_USER", "guest"),
		ManagementPassword: getEnv("ARGUS_MGMT_PASS", "guest"),
		BackupBaseDir:      getEnv("ARGUS_BACKUP_DIR", "/var/lib/rabbitmq/argus-backups"),
		WorkerCount:        3,
	}

	// If the quorum path is not explicitly set, derive it from the node name reported
	// by the management API so the default works for any node name (e.g. rabbit@rabbitmq).
	if path := os.Getenv("ARGUS_QUORUM_PATH"); path != "" {
		cfg.QuorumBasePath = path
	} else {
		dm0 := NewDiagnosticsManager(cfg.ManagementURL, cfg.ManagementUser, cfg.ManagementPassword)
		cfg.QuorumBasePath = dm0.resolveQuorumBasePath(context.Background())
	}

	conn, err := amqp.Dial(cfg.AMQPURL)
	if err != nil {
		log.Fatalf("FATAL: Cannot connect to broker: %v", err)
	}
	defer conn.Close()

	dm := NewDiagnosticsManager(cfg.ManagementURL, cfg.ManagementUser, cfg.ManagementPassword)
	monitor := NewLogMonitorWorker(conn, dm, cfg, cfg.WorkerCount)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Println("Argus started — monitoring RabbitMQ log exchange for errors...")
	if err := monitor.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("FATAL: Monitor exited unexpectedly: %v", err)
	}
	log.Println("Argus shutdown complete.")
}

// runRecoveryPipeline executes the full automated recovery for a confirmed unrecoverable queue:
//  1. Locate the quorum queue's data directory on disk
//  2. Back up segment files to a timestamped archive folder
//  3. Back up WAL files that contain records for this queue (only if any exist)
//  4. Delete the corrupted queue from the broker
//  5. Carve recoverable payloads from backed-up segment files
//  6. Carve recoverable payloads from backed-up WAL files (queue-UID filtered)
//  7. Republish all payloads — segments first, then WAL — to preserve ordering
func runRecoveryPipeline(ctx context.Context, cfg Config, dm *DiagnosticsManager, vhost, queueName string) error {
	log.Printf("[Recovery] Starting pipeline for queue %s/%s", vhost, queueName)

	// Phase 1: Locate the queue's Raft data directory.
	// The directory basename IS the Ra UID used to identify this queue's records in the WAL.
	queueDir, err := FindQueueDirectory(cfg.QuorumBasePath, vhost, queueName)
	if err != nil {
		return fmt.Errorf("locate phase: %w", err)
	}
	queueUID := filepath.Base(queueDir)
	log.Printf("[Recovery] Located quorum data at: %s (UID: %s)", queueDir, queueUID)

	// Phase 2: Backup segment files before any destructive operations.
	backupDir, err := backupQueueData(queueDir, cfg.BackupBaseDir, vhost, queueName)
	if err != nil {
		return fmt.Errorf("backup phase: %w", err)
	}
	log.Printf("[Recovery] Segment data backed up to: %s", backupDir)

	// Phase 3: Backup WAL files — but only those that actually contain records for
	// this queue's UID. The WAL is shared across all quorum queues on the node.
	walDir := cfg.QuorumBasePath
	walBackupDir := filepath.Join(backupDir, "wal")
	walsBacked, err := backupWALFiles(walDir, walBackupDir, queueUID)
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
	segPayloads, err := CarveMessagesFromDir(backupDir)
	if err != nil {
		return fmt.Errorf("segment carve phase: %w", err)
	}
	log.Printf("[Recovery] Extracted %d payload(s) from segment backup", len(segPayloads))

	// Phase 6: Carve messages from backed-up WAL files, filtered to this queue's UID.
	walPayloads, err := CarveWALMessages(walBackupDir, queueUID)
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
	if err := RepublishMessages(ctx, cfg.AMQPURL, queueName, payloads); err != nil {
		return fmt.Errorf("republish phase: %w", err)
	}
	log.Printf("[Recovery] Successfully republished %d message(s) to queue %s (%d from segments, %d from WAL)",
		len(payloads), queueName, len(segPayloads), len(walPayloads))

	return nil
}

// backupWALFiles scans walDir for *.wal files that contain at least one record for queueUID
// and copies matching files into destDir. It returns the number of files copied.
// destDir is created only when at least one matching WAL is found.
func backupWALFiles(walDir, destDir, queueUID string) (int, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // WAL directory absent — nothing to back up
		}
		return 0, fmt.Errorf("reading WAL directory %s: %w", walDir, err)
	}

	copied := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".wal" {
			continue
		}
		walPath := filepath.Join(walDir, entry.Name())

		// Peek: check whether this WAL has any records for the target queue.
		recs, err := ParseWALRecords(walPath, queueUID)
		if err != nil {
			log.Printf("[Backup] WARNING: Could not parse %s: %v", entry.Name(), err)
			continue
		}
		if len(recs) == 0 {
			continue // no records for this queue in this WAL file — skip
		}

		// Lazy-create the destination directory on first match.
		if copied == 0 {
			if err := os.MkdirAll(destDir, 0750); err != nil {
				return copied, fmt.Errorf("creating WAL backup dir: %w", err)
			}
		}

		dst := filepath.Join(destDir, entry.Name())
		if err := copyFile(walPath, dst); err != nil {
			log.Printf("[Backup] WARNING: Failed to copy WAL %s: %v", entry.Name(), err)
			continue
		}
		copied++
	}
	return copied, nil
}

// backupQueueData copies all files from srcDir to a timestamped subdirectory under backupBaseDir.
func backupQueueData(srcDir, backupBaseDir, vhost, queueName string) (string, error) {
	timestamp := time.Now().UTC().Format("20060102T150405Z")

	// Sanitize vhost for filesystem use ("/" → "default")
	safeVHost := filepath.Base(vhost)
	if safeVHost == "/" || safeVHost == "." {
		safeVHost = "default"
	}

	destDir := filepath.Join(backupBaseDir, safeVHost, queueName, timestamp)
	if err := os.MkdirAll(destDir, 0750); err != nil {
		return "", fmt.Errorf("failed to create backup directory: %w", err)
	}

	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return "", fmt.Errorf("failed to read source directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := copyFile(filepath.Join(srcDir, entry.Name()), filepath.Join(destDir, entry.Name())); err != nil {
			log.Printf("[Backup] WARNING: Failed to copy %s: %v", entry.Name(), err)
		}
	}

	return destDir, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
