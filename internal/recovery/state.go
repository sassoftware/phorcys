// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sassoftware/phorcys/internal/common"
	"github.com/sassoftware/phorcys/internal/util"
)

const (
	// recoveryStateDirName is the directory under the backup base dir that
	// holds one subdirectory per vhost, each holding one per queue.
	recoveryStateDirName = ".active_recovery"
	// recoveryArchiveDirName holds state files of finished recoveries, laid
	// out like the active directory.
	recoveryArchiveDirName = ".archive_recovery"
	archiveTimeFormat      = "20060102T150405.000000Z"
	recoveryStateFile      = "state.json"

	recoveryStateVersion = 1

	// StagePrepared means the winner has been chosen but nothing destructive
	// has been attempted: the original queue data is intact.
	StagePrepared = "PREPARED"
	// StageExecuting means phase 7 has been entered or passed, so the queue
	// may be gone and the backup is the only copy of its messages.
	StageExecuting = "EXECUTING"

	// FirstExecutePhase and LastExecutePhase bound RecoveryState.NextPhase.
	FirstExecutePhase = 6
	LastExecutePhase  = 11

	stateDirPerm  = 0o750
	stateFilePerm = 0o640
)

// ErrInvalidState means a state file exists but cannot be trusted: it is
// unparseable, of an unknown version, or inconsistent with the queue it is
// stored for. The file is left in place, so the queue stays blocked, until an
// operator fixes or archives it.
var ErrInvalidState = errors.New("invalid recovery state")

// RecoveryState is the durable record that this node is the executor for a
// queue's recovery and how far Execute has progressed. See state_tracking.md.
type RecoveryState struct {
	Version   int    `json:"version"`
	VHost     string `json:"vhost"`
	QueueName string `json:"queue_name"`
	QueueUID  string `json:"queue_uid"`

	Stage string `json:"stage"`
	// NextPhase is the Execute phase (6-11) to run next.
	NextPhase int `json:"next_phase"`

	BackupDir     string           `json:"backup_dir"`
	WALBackupDir  string           `json:"wal_backup_dir"`
	Position      RaftPosition     `json:"position"`
	PositionFound bool             `json:"position_found"`
	Definition    *QueueDefinition `json:"definition,omitempty"`

	// PublishedCount is the number of carved messages already confirmed
	// republished in phase 10.
	PublishedCount int `json:"published_count"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Save durably writes the state under baseDir. It sets Version and UpdatedAt,
// and CreatedAt if unset. The write is atomic and returns an error if the
// write can not be completed.
func (rs *RecoveryState) Save(baseDir string) error {
	rs.Version = recoveryStateVersion
	now := time.Now().UTC()
	if rs.CreatedAt.IsZero() {
		rs.CreatedAt = now
	}
	rs.UpdatedAt = now

	if err := rs.validate(); err != nil {
		return err
	}
	path, err := recoveryStatePath(baseDir, rs.VHost, rs.QueueName)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(rs, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding recovery state: %w", err)
	}
	if err := mkdirAllSynced(filepath.Dir(path)); err != nil {
		return fmt.Errorf("creating recovery state directory: %w", err)
	}
	if err := util.WriteFileAtomic(path, data, stateFilePerm); err != nil {
		return fmt.Errorf("writing recovery state: %w", err)
	}
	return nil
}

// LoadRecoveryState reads the state for a queue. It returns nil when no
// state file exists, and an error wrapping ErrInvalidState when one exists but
// is invalid.
func LoadRecoveryState(baseDir, vhost, queueName string) (*RecoveryState, error) {
	path, err := recoveryStatePath(baseDir, vhost, queueName)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading recovery state: %w", err)
	}

	var rs RecoveryState
	if err := json.Unmarshal(data, &rs); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidState, err)
	}
	if err := rs.validate(); err != nil {
		return nil, err
	}
	if rs.VHost != vhost || rs.QueueName != queueName {
		return nil, fmt.Errorf("%w: state is for %s, expected %s",
			ErrInvalidState, queueKey(rs.VHost, rs.QueueName), queueKey(vhost, queueName))
	}
	return &rs, nil
}

// HasRecoveryState reports whether a state file exists for the queue, valid or
// not. Any error other than "does not exist" counts as present, so callers err
// on the side of not starting a competing recovery.
func HasRecoveryState(baseDir, vhost, queueName string) bool {
	path, err := recoveryStatePath(baseDir, vhost, queueName)
	if err != nil {
		return false
	}
	_, err = os.Stat(path)
	return !errors.Is(err, fs.ErrNotExist)
}

// ArchiveRecoveryState durably moves a queue's state file out of the active
// state directory into the archive directory, as
// <archive>/<vhost>/<queue>/state-<timestamp>.json, so the queue no longer
// counts as a pending recovery but its final state is kept. Each archive gets
// a unique name, so a queue recovered more than once keeps every record. It is
// a no-op if no state file exists.
func ArchiveRecoveryState(baseDir, vhost, queueName string) error {
	path, err := recoveryStatePath(baseDir, vhost, queueName)
	if err != nil {
		return err
	}
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	archiveDir := filepath.Join(baseDir, recoveryArchiveDirName, encodePathSegment(vhost), encodePathSegment(queueName))
	if err := mkdirAllSynced(archiveDir); err != nil {
		return fmt.Errorf("creating recovery archive directory: %w", err)
	}
	archived := uniqueArchivePath(archiveDir, time.Now().UTC())
	if err := os.Rename(path, archived); err != nil {
		return fmt.Errorf("archiving recovery state: %w", err)
	}

	queueDir := filepath.Dir(path)
	vhostDir := filepath.Dir(queueDir)
	if err := util.SyncDir(archiveDir); err != nil {
		return fmt.Errorf("syncing recovery archive directory: %w", err)
	}
	if err := util.SyncDir(queueDir); err != nil {
		return fmt.Errorf("syncing recovery state directory: %w", err)
	}
	// Tidy the active directories once empty; these fail harmlessly if not.
	_ = os.Remove(path + ".tmp")
	_ = os.Remove(queueDir)
	_ = os.Remove(vhostDir)
	return nil
}

// ListRecoveryStates returns every queue under baseDir that has a state file.
// A missing state directory yields an empty list.
func ListRecoveryStates(baseDir string) ([]common.QueueDef, error) {
	root := filepath.Join(baseDir, recoveryStateDirName)
	vhostEntries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading recovery state directory: %w", err)
	}

	var refs []common.QueueDef
	for _, ve := range vhostEntries {
		vhost, err := url.PathUnescape(ve.Name())
		if !ve.IsDir() || err != nil {
			continue
		}
		queueEntries, err := os.ReadDir(filepath.Join(root, ve.Name()))
		if err != nil {
			return nil, fmt.Errorf("reading recovery state directory: %w", err)
		}
		for _, qe := range queueEntries {
			queueName, err := url.PathUnescape(qe.Name())
			if !qe.IsDir() || err != nil {
				continue
			}
			file := filepath.Join(root, ve.Name(), qe.Name(), recoveryStateFile)
			if _, err := os.Stat(file); err != nil {
				continue
			}
			refs = append(refs, common.QueueDef{VHost: vhost, Name: queueName})
		}
	}
	return refs, nil
}

// uniqueArchivePath returns a state-<timestamp>.json path in dir that does not
// exist yet, so an archive is never overwritten.
func uniqueArchivePath(dir string, now time.Time) string {
	base := "state-" + now.Format(archiveTimeFormat)
	path := filepath.Join(dir, base+".json")
	for i := 1; ; i++ {
		if _, err := os.Lstat(path); errors.Is(err, fs.ErrNotExist) {
			return path
		}
		path = filepath.Join(dir, fmt.Sprintf("%s-%d.json", base, i))
	}
}

func (rs *RecoveryState) validate() error {
	switch {
	case rs.Version != recoveryStateVersion:
		return fmt.Errorf("%w: unsupported version %d", ErrInvalidState, rs.Version)
	case rs.VHost == "" || rs.QueueName == "":
		return fmt.Errorf("%w: missing vhost or queue name", ErrInvalidState)
	case rs.Stage != StagePrepared && rs.Stage != StageExecuting:
		return fmt.Errorf("%w: unknown stage %q", ErrInvalidState, rs.Stage)
	case rs.NextPhase < FirstExecutePhase || rs.NextPhase > LastExecutePhase:
		return fmt.Errorf("%w: next phase %d outside %d-%d", ErrInvalidState, rs.NextPhase, FirstExecutePhase, LastExecutePhase)
	case rs.BackupDir == "":
		return fmt.Errorf("%w: missing backup directory", ErrInvalidState)
	case rs.PublishedCount < 0:
		return fmt.Errorf("%w: negative published count", ErrInvalidState)
	}
	return nil
}

// recoveryStatePath returns the state file path for a queue. vhost and queue
// are percent-encoded (including "."), so names containing "/" or "." cannot
// escape or collide in the directory tree.
func recoveryStatePath(baseDir, vhost, queueName string) (string, error) {
	if vhost == "" || queueName == "" {
		return "", errors.New("recovery state requires a vhost and queue name")
	}
	return filepath.Join(baseDir, recoveryStateDirName, encodePathSegment(vhost), encodePathSegment(queueName), recoveryStateFile), nil
}

func encodePathSegment(s string) string {
	return strings.ReplaceAll(url.PathEscape(s), ".", "%2E")
}

// mkdirAllSynced creates dir and any missing parents, fsyncing each parent so
// the new directory entries survive a crash.
func mkdirAllSynced(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		return nil
	}
	parent := filepath.Dir(dir)
	if parent != dir {
		if err := mkdirAllSynced(parent); err != nil {
			return err
		}
	}
	if err := os.Mkdir(dir, stateDirPerm); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	return util.SyncDir(parent)
}
