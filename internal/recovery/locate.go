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
	return scanQueueDirectories(quorumBasePath, false)
}

// ScanBackupDirectories walks backupBaseDir recursively and returns every directory
// containing a ra 'config' file with a queue resource tuple.
func ScanBackupDirectories(backupBaseDir string) ([]QueueLocation, error) {
	return scanQueueDirectories(backupBaseDir, true)
}

// scanQueueDirectories walks baseDir and returns every directory containing a ra 'config'
// file with a queue resource tuple. A matched directory is not descended into, since its
// subdirectories (e.g. wal/) belong to that queue. Results are in lexical path order.
func scanQueueDirectories(baseDir string, missingOk bool) ([]QueueLocation, error) {
	if _, err := os.Stat(baseDir); err != nil {
		if missingOk && errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var locations []QueueLocation
	err := filepath.WalkDir(baseDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if path == baseDir {
				return fmt.Errorf("failed to read directory: %w", err)
			}
			log.Printf("[Recovery] WARNING: skipping unreadable path under directory %s: %v", path, err)
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
		locations = append(locations, QueueLocation{
			VHost: vhost,
			Name:  name,
			Path:  path,
		})
		return fs.SkipDir
	})
	if err != nil {
		return nil, err
	}
	return locations, nil
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
