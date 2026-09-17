package recovery

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// BackupWALFiles scans walDir for *.wal files that contain at least one record for queueUID
// and copies matching files into destDir. It returns the number of files copied.
// destDir is created only when at least one matching WAL is found.
func BackupWALFiles(walDir, destDir, queueUID string) (int, error) {
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

// BackupQueueData copies all files from srcDir to a timestamped subdirectory under backupBaseDir.
func BackupQueueData(srcDir, backupBaseDir, vhost, queueName string) (string, error) {
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
