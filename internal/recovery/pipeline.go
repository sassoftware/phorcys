// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/runtime"
)

// PreparedRecovery is the output of Prepare: everything Execute needs to
// finish recovering a queue, plus the RaftPosition a consensus process
// compares across a queue's member nodes to pick which one calls Execute.
type PreparedRecovery struct {
	Queue         string
	VHost         string
	QueueUID      string
	BackupDir     string
	WALBackupDir  string
	Position      RaftPosition
	PositionFound bool
}

// Prepare runs the read-only, non-destructive half of recovery for a
// confirmed unrecoverable queue (steps 1-4):
//  1. Locate the quorum queue's data directory on disk
//  2. Back up segment files to a timestamped archive folder
//  3. Back up WAL files that contain records for this queue (only if any exist)
//  4. Determine the latest Raft term/index found in the backup
//
// It performs no destructive broker calls, so every node hosting a replica of
// the queue can call Prepare independently and in parallel; it does publish a
// RecoveryAnnouncement to ConsensusExchange so the other nodes learn this
// node's RaftPosition, which a consensus process compares — via
// RaftPosition.After — to pick which node calls Execute.
func Prepare(ctx context.Context, cfg runtime.Config, vhost, queueName string) (*PreparedRecovery, error) {
	log.Printf("[Recovery] Preparing pipeline for queue %s/%s", vhost, queueName)

	// Phase 1: Locate the queue's Raft data directory.
	// The directory basename IS the Ra UID used to identify this queue's records in the WAL.
	queueDir, err := FindQueueDirectory(cfg.QuorumBasePath, vhost, queueName)
	if err != nil {
		return nil, fmt.Errorf("locate phase: %w", err)
	}
	queueUID := filepath.Base(queueDir)
	log.Printf("[Recovery] Located quorum data at: %s (UID: %s)", queueDir, queueUID)

	// Phase 2: Backup segment files before any destructive operations.
	backupDir, err := BackupQueueData(queueDir, cfg.BackupBaseDir, vhost, queueName)
	if err != nil {
		return nil, fmt.Errorf("backup phase: %w", err)
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

	// Phase 4: Determine the latest Raft position in this backup so the caller
	// can compare it against other cluster nodes to decide who holds the most
	// recent data and should therefore run Execute.
	pos, found, err := LatestQueuePosition(backupDir, walBackupDir, queueUID)
	switch {
	case err != nil:
		log.Printf("[Recovery] WARNING: failed to determine latest Raft position (non-fatal): %v", err)
	case found:
		log.Printf("[Recovery] Latest backed-up Raft position for %s: term=%d idx=%d", queueName, pos.Term, pos.Idx)
	default:
		log.Printf("[Recovery] No Raft position found in backup for %s", queueName)
	}

	// Announce this node's position so other nodes hosting the queue can run
	// their own consensus comparison; a hostname lookup/publish failure must
	// not block the local backup that already succeeded.
	hostname, hErr := os.Hostname()
	if hErr != nil {
		log.Printf("[Recovery] WARNING: failed to determine hostname (non-fatal): %v", hErr)
		hostname = "unknown"
	}
	ann := RecoveryAnnouncement{
		Hostname:  hostname,
		VHost:     vhost,
		Queue:     queueName,
		Timestamp: time.Now().UTC(),
		Term:      pos.Term,
		Index:     pos.Idx,
	}
	if err := PublishRecoveryAnnouncement(ctx, cfg.AMQPURL, ann); err != nil {
		log.Printf("[Recovery] WARNING: failed to publish recovery announcement (non-fatal): %v", err)
	}

	return &PreparedRecovery{
		Queue:         queueName,
		VHost:         vhost,
		QueueUID:      queueUID,
		BackupDir:     backupDir,
		WALBackupDir:  walBackupDir,
		Position:      pos,
		PositionFound: found,
	}, nil
}

// Execute runs the destructive half of recovery (steps 5-8) against the
// backup a prior call to Prepare produced:
//  5. Delete the corrupted queue from the broker
//  6. Carve recoverable payloads from backed-up segment files
//  7. Carve recoverable payloads from backed-up WAL files (queue-UID filtered)
//  8. Republish all payloads — segments first, then WAL — to preserve ordering
//
// Callers must only invoke Execute on the single node a consensus process has
// determined should own recovery for this queue; running it on more than one
// node would delete the queue out from under the others and double-publish
// messages.
func Execute(ctx context.Context, cfg runtime.Config, dm *broker.DiagnosticsManager, prep *PreparedRecovery) error {
	// Phase 5: Delete the corrupted queue from the broker.
	if err := dm.DeleteOrForceEvict(ctx, prep.VHost, prep.Queue); err != nil {
		return fmt.Errorf("delete phase: %w", err)
	}
	log.Printf("[Recovery] Queue %s deleted from broker", prep.Queue)

	// Phase 6: Carve messages from backed-up segment files.
	// CarveMessagesFromDir already skips non-segment/wal extensions; WAL files
	// in the main backup dir have been separated into the wal/ subdirectory so
	// this call only processes .segment files here.
	segMessages, err := CarveMessagesFromDir(prep.BackupDir)
	if err != nil {
		return fmt.Errorf("segment carve phase: %w", err)
	}
	log.Printf("[Recovery] Extracted %d message(s) from segment backup", len(segMessages))

	// Phase 7: Carve messages from backed-up WAL files, filtered to this queue's UID.
	walMessages, err := CarveWALMessages(prep.WALBackupDir, prep.QueueUID)
	if err != nil {
		log.Printf("[Recovery] WARNING: WAL carve failed (non-fatal): %v", err)
	}
	log.Printf("[Recovery] Extracted %d message(s) from WAL backup", len(walMessages))

	// Segments are written before WAL entries are flushed, so publish segments first.
	segCount := len(segMessages)
	segMessages = append(segMessages, walMessages...)
	if len(segMessages) == 0 {
		log.Printf("[Recovery] No messages found in backup for %s — pipeline complete.", prep.Queue)
		return nil
	}

	// Phase 8: Republish to default exchange; routing key = queue name.
	if err := RepublishMessages(ctx, cfg.AMQPURL, prep.Queue, segMessages); err != nil {
		return fmt.Errorf("republish phase: %w", err)
	}
	log.Printf("[Recovery] Successfully republished %d message(s) to queue %s (%d from segments, %d from WAL)",
		len(segMessages), prep.Queue, segCount, len(walMessages))

	return nil
}

// Run performs the full single-node recovery pipeline (Prepare then
// Execute). It is a convenience for callers that don't need multi-node
// consensus — on a cluster where every node runs Phorcys, use Prepare and
// Execute directly so only the node holding the most recent data executes.
func Run(ctx context.Context, cfg runtime.Config, dm *broker.DiagnosticsManager, vhost, queueName string) error {
	log.Printf("[Recovery] Starting pipeline for queue %s/%s", vhost, queueName)

	prep, err := Prepare(ctx, cfg, vhost, queueName)
	if err != nil {
		return err
	}
	return Execute(ctx, cfg, dm, prep)
}
