// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"

	"github.com/DeedleFake/etf"
)

// resourcePattern matches the Erlang resource tuple in a ra 'config' file:
//
//	{resource,<<"VHOST">>,queue,<<"QUEUENAME">>}
var resourcePattern = regexp.MustCompile(`\{resource,<<"([^"]*)">>,[^,]*,<<"([^"]*)">>}`)

// uidPattern matches the Ra server UID entry in a ra 'config' file:
//
//	uid => <<"UID">>
var uidPattern = regexp.MustCompile(`\buid\s*=>\s*<<"([^"]*)">>`)

// QueueLocation describes a quorum queue found on disk.
type QueueLocation struct {
	VHost string
	Name  string
	Path  string
}

// FindQueueDirectory returns the absolute disk path for the given vhost/queue pair.
func FindQueueDirectory(quorumBasePath, targetVHost, targetQueue string) (string, error) {
	locations, err := ScanAllQuorumDirectories(quorumBasePath)
	if err != nil {
		return "", err
	}
	for _, loc := range locations {
		if loc.VHost == targetVHost && loc.Name == targetQueue {
			return loc.Path, nil
		}
	}
	return "", fmt.Errorf("directory not found for vhost: %s, queue: %s", targetVHost, targetQueue)
}

// ScanAllQuorumDirectories reads every subfolder in quorumBasePath and extracts queue identity
// from the plain-text Erlang 'config' file written by the ra library inside each directory.
func ScanAllQuorumDirectories(quorumBasePath string) ([]QueueLocation, error) {
	entries, err := os.ReadDir(quorumBasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read quorum base path: %w", err)
	}

	var locations []QueueLocation

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		configPath := filepath.Join(quorumBasePath, entry.Name(), "config")
		configBytes, err := os.ReadFile(configPath)
		if err != nil {
			// Skip directories that are initializing or lack a config file.
			continue
		}

		if vhost, name, found := extractQueueMetadata(configBytes); found {
			locations = append(locations, QueueLocation{
				VHost: vhost,
				Name:  name,
				Path:  filepath.Join(quorumBasePath, entry.Name()),
			})
		}
	}

	return locations, nil
}

// backupLocation is a queue backup directory discovered under the backup base dir.
type backupLocation struct {
	QueueLocation
	UID string
}

// scanBackupDirectories walks backupBaseDir recursively and returns every directory
// containing a ra 'config' file with a queue resource tuple. This matches the
// <vhost>/<queue>/<timestamp> layout written by BackupQueueData without depending
// on its exact depth. Results are in lexical path order, so timestamped backups of
// the same queue come oldest first.
func scanBackupDirectories(backupBaseDir string) ([]backupLocation, error) {
	if _, err := os.Stat(backupBaseDir); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// No backup has ever been written (e.g. first run) — nothing to recover.
			return nil, nil
		}
		return nil, err
	}

	var locations []backupLocation
	err := filepath.WalkDir(backupBaseDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if path == backupBaseDir {
				return err
			}
			log.Printf("[Recovery] WARNING: skipping unreadable backup path %s: %v", path, err)
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		configBytes, err := os.ReadFile(filepath.Join(path, "config"))
		if err != nil {
			return nil
		}
		vhost, name, found := extractQueueMetadata(configBytes)
		if !found {
			return nil
		}
		uid, ok := extractQueueUID(configBytes)
		if !ok {
			// Ra always writes uid to config, but fall back to the directory name
			// (the Ra UID for directories copied verbatim from the quorum data dir).
			uid = filepath.Base(path)
		}
		locations = append(locations, backupLocation{
			VHost: vhost,
			Name:  name,
			Path:  path,
			UID:   uid,
		})
		// A queue backup's subdirectories (e.g. wal/) are part of that backup, not separate backups.
		return fs.SkipDir
	})
	if err != nil {
		return nil, err
	}
	return locations, nil
}

// ParseRecoveriesFromBackupFiles discovers every queue backup written by Prepare under
// backupBaseDir and returns a PreparedRecovery for each, including its latest Raft position.
func ParseRecoveriesFromBackupFiles(backupBaseDir string) ([]*PreparedRecovery, error) {
	backupLocations, err := scanBackupDirectories(backupBaseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to scan backup base directory: %w", err)
	}

	var recoveries []*PreparedRecovery
	for _, loc := range backupLocations {
		queueName := loc.Name
		walBackupDir := filepath.Join(loc.Path, "wal")
		queueUID := loc.UID
		pos, found, err := LatestQueuePosition(loc.Path, walBackupDir, queueUID)
		switch {
		case err != nil:
			log.Printf("[Recovery] WARNING: failed to determine latest Raft position (non-fatal): %v", err)
		case found:
			log.Printf("[Recovery] Latest backed-up Raft position for %s: term=%d idx=%d", queueName, pos.Term, pos.Idx)
		default:
			log.Printf("[Recovery] No Raft position found in backup for %s", queueName)
		}
		recoveries = append(recoveries, &PreparedRecovery{
			Queue:         queueName,
			VHost:         loc.VHost,
			QueueUID:      queueUID,
			BackupDir:     loc.Path,
			WALBackupDir:  walBackupDir,
			Position:      pos,
			PositionFound: found,
		})
	}
	return recoveries, nil
}

// extractQueueMetadata parses the ra 'config' text file and returns the vhost and queue name
// by matching the {resource,<<"vhost">>,queue,<<"name">>} pattern.
func extractQueueMetadata(config []byte) (string, string, bool) {
	m := resourcePattern.FindSubmatch(config)
	if m == nil {
		return "", "", false
	}
	return string(m[1]), string(m[2]), true
}

// extractQueueUID returns the Ra server UID from the 'uid => <<"...">>' entry of a ra 'config' file.
func extractQueueUID(config []byte) (string, bool) {
	m := uidPattern.FindSubmatch(config)
	if m == nil {
		return "", false
	}
	return string(m[1]), true
}

// convertToString normalises the three ways RabbitMQ can encode a string in ETF.
// Retained for use by unit tests.
func convertToString(val any) (string, bool) {
	switch v := val.(type) {
	case []byte:
		return string(v), true
	case string:
		return v, true
	case etf.Atom:
		return string(v), true
	default:
		return "", false
	}
}
