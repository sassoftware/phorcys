// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// BackupQueueData
// ---------------------------------------------------------------------------

func TestBackupQueueData_CopiesSegmentAndWalFiles(t *testing.T) {
	src := t.TempDir()
	base := t.TempDir()

	err := os.WriteFile(filepath.Join(src, "00000001.segment"), []byte("seg"), 0600)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(src, "00000001.wal"), []byte("wal"), 0600)
	require.NoError(t, err)
	// Subdirectories must be skipped.
	err = os.MkdirAll(filepath.Join(src, "inner"), 0755)
	require.NoError(t, err)

	destDir, err := BackupQueueData(src, base, "/", "events.queue")
	require.NoError(t, err, "BackupQueueData: %v", err)

	for _, name := range []string{"00000001.segment", "00000001.wal"} {
		_, err := os.Stat(filepath.Join(destDir, name))
		assert.NoErrorf(t, err, "expected file %q in backup dir", name)
	}
	_, err = os.Stat(filepath.Join(destDir, "inner"))
	assert.True(t, os.IsNotExist(err), "subdirectory must not be copied")
}

func TestBackupQueueData_SanitizesRootVhost(t *testing.T) {
	src := t.TempDir()
	base := t.TempDir()

	err := os.WriteFile(filepath.Join(src, "data.segment"), []byte("x"), 0600)
	require.NoError(t, err)

	destDir, err := BackupQueueData(src, base, "/", "my.queue")
	require.NoError(t, err, "BackupQueueData: %v", err)

	// destDir == base/default/my.queue/<timestamp>
	rel, err := filepath.Rel(base, destDir)
	require.NoError(t, err, "filepath.Rel: %v", err)
	parts := strings.Split(rel, string(filepath.Separator))
	require.GreaterOrEqual(t, len(parts), 3, "expected at least 3 path segments, got %q", rel)
	assert.Equal(t, "default", parts[0])
	assert.Equal(t, "my.queue", parts[1])
}

func TestBackupQueueData_UsesNamedVhost(t *testing.T) {
	src := t.TempDir()
	base := t.TempDir()

	err := os.WriteFile(filepath.Join(src, "data.segment"), []byte("x"), 0600)
	require.NoError(t, err)

	destDir, err := BackupQueueData(src, base, "production", "order.queue")
	require.NoError(t, err, "BackupQueueData: %v", err)

	rel, _ := filepath.Rel(base, destDir)
	parts := strings.Split(rel, string(filepath.Separator))
	assert.Equal(t, "production", parts[0])
}

func TestBackupQueueData_TimestampDirFormat(t *testing.T) {
	src := t.TempDir()
	base := t.TempDir()

	err := os.WriteFile(filepath.Join(src, "data.segment"), []byte("x"), 0600)
	require.NoError(t, err)

	before := time.Now().UTC()
	destDir, err := BackupQueueData(src, base, "vh", "q")
	after := time.Now().UTC()

	require.NoError(t, err, "BackupQueueData: %v", err)

	tsStr := filepath.Base(destDir)
	ts, err := time.Parse("20060102T150405Z", tsStr)
	require.NoError(t, err, "timestamp dir %q is not in expected format: %v", tsStr, err)
	assert.True(t, !ts.Before(before.Truncate(time.Second)) && !ts.After(after.Add(time.Second)), "timestamp %v is outside [%v, %v]", ts, before, after)
}

func TestBackupQueueData_InvalidSrcReturnsError(t *testing.T) {
	_, err := BackupQueueData("/no/such/directory", t.TempDir(), "/", "q")
	assert.Error(t, err, "expected error for non-existent source dir")
}

// ---------------------------------------------------------------------------
// backupWALFiles
// ---------------------------------------------------------------------------

func TestBackupWALFiles_AbsentWALDir_ReturnsZeroNoError(t *testing.T) {
	n, err := BackupWALFiles("/no/such/wal/dir", t.TempDir(), "any-uid")
	require.NoError(t, err, "expected no error for absent WAL dir, got: %v", err)
	assert.Zero(t, n)
}

func TestBackupWALFiles_SkipsNonWALFiles(t *testing.T) {
	walDir := t.TempDir()
	// Write a non-WAL file alongside a valid fixture WAL.
	err := os.WriteFile(filepath.Join(walDir, "somefile.txt"), []byte("data"), 0600)
	require.NoError(t, err)

	dest := t.TempDir()
	n, err := BackupWALFiles(walDir, dest, fixtureUID2)
	require.NoError(t, err, "backupWALFiles: %v", err)
	// .txt file must be ignored; no WAL found → 0 copies.
	assert.Zero(t, n)
}

func TestBackupWALFiles_CopiesWALWithMatchingUID(t *testing.T) {
	// Use the fixture WAL which contains records for fixtureUID2.
	walDir := t.TempDir()
	require.NoError(t, copyFile("testdata/0000000000000002.wal", filepath.Join(walDir, "0000000000000002.wal")), "setup")

	dest := t.TempDir()
	n, err := BackupWALFiles(walDir, dest, fixtureUID2)
	require.NoError(t, err, "backupWALFiles: %v", err)
	assert.Equal(t, 1, n)
	_, err = os.Stat(filepath.Join(dest, "0000000000000002.wal"))
	assert.NoError(t, err, "backed-up WAL file not found")
}

func TestBackupWALFiles_SkipsWALWithNoMatchingUID(t *testing.T) {
	walDir := t.TempDir()
	require.NoError(t, copyFile("testdata/0000000000000002.wal", filepath.Join(walDir, "0000000000000002.wal")), "setup")

	dest := t.TempDir()
	n, err := BackupWALFiles(walDir, dest, "nonexistent-uid-xyz")
	require.NoError(t, err, "backupWALFiles: %v", err)
	assert.Zero(t, n)
	// destDir must not have been created (lazy creation).
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		// dest was pre-created by t.TempDir() — check it has no WAL inside.
		entries, _ := os.ReadDir(dest)
		assert.Empty(t, entries, "expected empty dest dir when no WAL matched")
	}
}

func TestBackupWALFiles_DestDirCreatedLazily(t *testing.T) {
	walDir := t.TempDir()
	// No WAL files → dest should not be created.
	dest := filepath.Join(t.TempDir(), "wal-backup")

	n, err := BackupWALFiles(walDir, dest, "any-uid")
	require.NoError(t, err, "backupWALFiles: %v", err)
	assert.Zero(t, n)
	_, err = os.Stat(dest)
	assert.True(t, os.IsNotExist(err), "dest dir must not be created when no WAL files are copied")
}
