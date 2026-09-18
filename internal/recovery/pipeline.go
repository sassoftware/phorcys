// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	"github.com/sassoftware/argus/internal/broker"
	"github.com/sassoftware/argus/internal/runtime"
)

// Run executes the full automated recovery for a confirmed unrecoverable queue:
//  1. Locate the quorum queue's data directory on disk
//  2. Back up segment files to a timestamped archive folder
//  3. Back up WAL files that contain records for this queue (only if any exist)
//  4. Delete the corrupted queue from the broker
//  5. Carve recoverable payloads from backed-up segment files
//  6. Carve recoverable payloads from backed-up WAL files (queue-UID filtered)
//  7. Republish all payloads — segments first, then WAL — to preserve ordering
func Run(ctx context.Context, cfg runtime.Config, dm *broker.DiagnosticsManager, vhost, queueName string) error {
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
	backupDir, err := BackupQueueData(queueDir, cfg.BackupBaseDir, vhost, queueName)
	if err != nil {
		return fmt.Errorf("backup phase: %w", err)
	}
	log.Printf("[Recovery] Segment data backed up to: %s", backupDir)

	// Phase 3: Backup WAL files — but only those that actually contain records for
	// this queue's UID. The WAL is shared across all quorum queues on the node.
	walDir := cfg.QuorumBasePath
	walBackupDir := filepath.Join(backupDir, "wal")
	walsBacked, err := BackupWALFiles(walDir, walBackupDir, queueUID)
	switch {
	case err != nil:
		log.Printf("[Recovery] WARNING: WAL backup failed (non-fatal): %v", err)
	case walsBacked > 0:
		log.Printf("[Recovery] Backed up %d WAL file(s) containing records for UID %s", walsBacked, queueUID)
	default:
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
	segCount := len(segPayloads)
	segPayloads = append(segPayloads, walPayloads...)
	if len(segPayloads) == 0 {
		log.Printf("[Recovery] No messages found in backup for %s — pipeline complete.", queueName)
		return nil
	}

	// Phase 7: Republish to default exchange; routing key = queue name.
	if err := RepublishMessages(ctx, cfg.AMQPURL, queueName, segPayloads); err != nil {
		return fmt.Errorf("republish phase: %w", err)
	}
	log.Printf("[Recovery] Successfully republished %d message(s) to queue %s (%d from segments, %d from WAL)",
		len(segPayloads), queueName, segCount, len(walPayloads))

	return nil
}
