// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/DeedleFake/etf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeConfigFile writes a minimal ra-style 'config' text file for the given vhost and queue name.
func writeConfigFile(t *testing.T, base, dirName, vhost, queueName string) {
	t.Helper()
	dir := filepath.Join(base, dirName)
	err := os.MkdirAll(dir, 0755)
	require.NoError(t, err)
	content := fmt.Sprintf(
		`#{id => {'something',rabbit@node}, machine => {module,rabbit_fifo, #{queue_resource => {resource,<<"%s">>,queue,<<"%s">>}}}}`,
		vhost, queueName,
	)
	err = os.WriteFile(filepath.Join(dir, "config"), []byte(content), 0600)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// convertToString
// ---------------------------------------------------------------------------

func TestConvertToString_ByteSlice(t *testing.T) {
	s, ok := convertToString([]byte("hello"))
	assert.True(t, ok && s == "hello", "got (%q, %v), want (\"hello\", true)", s, ok)
}

func TestConvertToString_String(t *testing.T) {
	s, ok := convertToString("world")
	assert.True(t, ok && s == "world", "got (%q, %v), want (\"world\", true)", s, ok)
}

func TestConvertToString_Atom(t *testing.T) {
	s, ok := convertToString(etf.Atom("my_atom"))
	assert.True(t, ok && s == "my_atom", "got (%q, %v), want (\"my_atom\", true)", s, ok)
}

func TestConvertToString_UnsupportedTypeReturnsFalse(t *testing.T) {
	for _, v := range []any{42, 3.14, nil, true, struct{}{}} {
		_, ok := convertToString(v)
		assert.Falsef(t, ok, "convertToString(%T) should return false", v)
	}
}

// ---------------------------------------------------------------------------
// extractQueueMetadata
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// extractQueueMetadata
// ---------------------------------------------------------------------------

func TestExtractQueueMetadata_ValidConfig(t *testing.T) {
	config := []byte(`#{queue_resource => {resource,<<"myvhost">>,queue,<<"myqueue">>}}`)
	vhost, name, found := extractQueueMetadata(config)
	require.True(t, found, "expected found=true")
	assert.Equal(t, "myvhost", vhost)
	assert.Equal(t, "myqueue", name)
}

func TestExtractQueueMetadata_DefaultVhost(t *testing.T) {
	config := []byte(`#{queue_resource => {resource,<<"/">>,queue,<<"test-target-quorum-queue">>}}`)
	vhost, name, found := extractQueueMetadata(config)
	require.True(t, found, "expected found=true")
	assert.True(t, vhost == "/" && name == "test-target-quorum-queue", "got vhost=%q name=%q", vhost, name)
}

func TestExtractQueueMetadata_NoMatch(t *testing.T) {
	_, _, found := extractQueueMetadata([]byte(`#{id => something_else}`))
	assert.False(t, found, "expected found=false for config with no resource tuple")
}

func TestExtractQueueMetadata_EmptyBytes(t *testing.T) {
	_, _, found := extractQueueMetadata([]byte{})
	assert.False(t, found, "expected found=false for empty input")
}

// ---------------------------------------------------------------------------
// ScanAllQuorumDirectories
// ---------------------------------------------------------------------------

func TestScanAllQuorumDirectories_MultipleQueues(t *testing.T) {
	base := t.TempDir()

	writeConfigFile(t, base, "hash1", "/", "queue.a")
	writeConfigFile(t, base, "hash2", "/", "queue.b")
	writeConfigFile(t, base, "hash3", "vh2", "queue.c")

	// A directory with no config file should be silently skipped.
	err := os.MkdirAll(filepath.Join(base, "nometa"), 0755)
	require.NoError(t, err)
	// A regular file (not a directory) should also be skipped.
	err = os.WriteFile(filepath.Join(base, "notadir"), []byte("x"), 0600)
	require.NoError(t, err)

	locs, err := ScanAllQuorumDirectories(base)
	require.NoError(t, err, "unexpected error: %v", err)
	require.Len(t, locs, 3, "expected 3 locations, got %d: %+v", len(locs), locs)
}

func TestScanAllQuorumDirectories_EmptyBaseDir(t *testing.T) {
	locs, err := ScanAllQuorumDirectories(t.TempDir())
	require.NoError(t, err, "unexpected error: %v", err)
	assert.Empty(t, locs)
}

func TestScanAllQuorumDirectories_InvalidBaseDirReturnsError(t *testing.T) {
	_, err := ScanAllQuorumDirectories("/no/such/directory")
	assert.Error(t, err, "expected error for non-existent base path")
}

func TestScanAllQuorumDirectories_BadConfigSkipped(t *testing.T) {
	base := t.TempDir()

	// Valid queue
	writeConfigFile(t, base, "good", "/", "valid.q")
	// Directory with a config file that has no resource tuple
	badDir := filepath.Join(base, "bad")
	err := os.MkdirAll(badDir, 0755)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(badDir, "config"), []byte("#{id => something_else}"), 0600)
	require.NoError(t, err)

	locs, err := ScanAllQuorumDirectories(base)
	require.NoError(t, err, "unexpected error: %v", err)
	assert.Len(t, locs, 1)
}

func TestScanAllQuorumDirectories_PathIsCorrect(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "abc123", "prod", testQueue)

	locs, _ := ScanAllQuorumDirectories(base)
	require.Len(t, locs, 1, "expected 1 location")
	expected := filepath.Join(base, "abc123")
	assert.Equal(t, expected, locs[0].Path)
}

// ---------------------------------------------------------------------------
// FindQueueDirectory
// ---------------------------------------------------------------------------

func TestFindQueueDirectory_Found(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "somehash", "testvhost", "target.queue")

	path, err := FindQueueDirectory(base, "testvhost", "target.queue")
	require.NoError(t, err, "unexpected error: %v", err)
	expected := filepath.Join(base, "somehash")
	assert.Equal(t, expected, path)
}

func TestFindQueueDirectory_NotFound(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "somehash", "testvhost", "other.queue")

	_, err := FindQueueDirectory(base, "testvhost", "missing.queue")
	assert.Error(t, err, "expected error for missing queue")
}

func TestFindQueueDirectory_WrongVhost(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "h1", "vhostA", "q1")

	_, err := FindQueueDirectory(base, "vhostB", "q1")
	assert.Error(t, err, "expected error for wrong vhost")
}

func TestFindQueueDirectory_InvalidBasePath(t *testing.T) {
	_, err := FindQueueDirectory("/no/such/path", "/", "q")
	assert.Error(t, err, "expected error for invalid base path")
}

// ---------------------------------------------------------------------------
// ParseRecoveriesFromBackupFiles
// ---------------------------------------------------------------------------

func recoveriesByQueue(recs []*PreparedRecovery) map[string]*PreparedRecovery {
	m := make(map[string]*PreparedRecovery, len(recs))
	for _, r := range recs {
		m[r.Queue] = r
	}
	return m
}

func TestParseRecoveriesFromBackupFiles_MissingBaseDirReturnsEmpty(t *testing.T) {
	recs, err := ParseRecoveriesFromBackupFiles(filepath.Join(t.TempDir(), "does-not-exist"))
	require.NoError(t, err)
	assert.Empty(t, recs)
}

func TestParseRecoveriesFromBackupFiles_BaseIsFileReturnsError(t *testing.T) {
	file := filepath.Join(t.TempDir(), "notadir")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0600))

	recs, err := ParseRecoveriesFromBackupFiles(filepath.Join(file, "sub"))
	assert.Error(t, err)
	assert.Nil(t, recs)
}

func TestParseRecoveriesFromBackupFiles_EmptyBaseDir(t *testing.T) {
	recs, err := ParseRecoveriesFromBackupFiles(t.TempDir())
	require.NoError(t, err)
	assert.Empty(t, recs)
}

func TestParseRecoveriesFromBackupFiles_PopulatesFields(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, testUIDA, "prod", testQueue)

	recs, err := ParseRecoveriesFromBackupFiles(base)
	require.NoError(t, err)
	require.Len(t, recs, 1)

	queueDir := filepath.Join(base, testUIDA)
	assert.Equal(t, &PreparedRecovery{
		Queue:         testQueue,
		VHost:         "prod",
		QueueUID:      testUIDA,
		BackupDir:     queueDir,
		WALBackupDir:  filepath.Join(queueDir, "wal"),
		Position:      RaftPosition{},
		PositionFound: false,
	}, recs[0])
}

func TestParseRecoveriesFromBackupFiles_SkipsInvalidEntries(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "good", "/", "valid.q")
	require.NoError(t, os.MkdirAll(filepath.Join(base, "noconfig"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(base, "badconfig"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(base, "badconfig", "config"), []byte("#{id => x}"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(base, "notadir"), []byte("x"), 0600))

	recs, err := ParseRecoveriesFromBackupFiles(base)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, "valid.q", recs[0].Queue)
}

func TestParseRecoveriesFromBackupFiles_PositionFromSegments(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, testUIDA, "/", "q.a")
	writeSegmentBackup(t, filepath.Join(base, testUIDA), "0000000000000001.segment", []segmentEntry{
		{idx: 7, term: 1, data: []byte("a")},
		{idx: 8, term: 2, data: []byte("b")},
	})

	recs, err := ParseRecoveriesFromBackupFiles(base)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.True(t, recs[0].PositionFound)
	assert.Equal(t, RaftPosition{Idx: 8, Term: 2}, recs[0].Position)
}

func TestParseRecoveriesFromBackupFiles_PositionFromWALFilteredByDirUID(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, testUIDA, "/", "q.a")
	walDir := filepath.Join(base, testUIDA, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0755))
	writeWALBackup(t, walDir, "0000000000000001.wal", []walEntry{
		{uid: testUIDA, idx: 4, term: 3, data: []byte("a")},
		{uid: testUIDB, idx: 99, term: 9, data: []byte("b")}, // different queue, must be ignored
	})

	recs, err := ParseRecoveriesFromBackupFiles(base)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.True(t, recs[0].PositionFound)
	assert.Equal(t, RaftPosition{Idx: 4, Term: 3}, recs[0].Position)
}

func TestParseRecoveriesFromBackupFiles_CombinesSegmentAndWAL(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, testUIDA, "/", "q.a")
	queueDir := filepath.Join(base, testUIDA)
	writeSegmentBackup(t, queueDir, "0000000000000001.segment", []segmentEntry{
		{idx: 1000, term: 1, data: []byte("seg")},
	})
	walDir := filepath.Join(queueDir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0755))
	writeWALBackup(t, walDir, "0000000000000001.wal", []walEntry{
		{uid: testUIDA, idx: 2, term: 3, data: []byte("wal")},
	})

	recs, err := ParseRecoveriesFromBackupFiles(base)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.True(t, recs[0].PositionFound)
	assert.Equal(t, RaftPosition{Idx: 2, Term: 3}, recs[0].Position)
}

func TestParseRecoveriesFromBackupFiles_PositionsIsolatedPerQueue(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, testUIDA, "/", "q.a")
	writeConfigFile(t, base, testUIDB, "vh2", "q.b")
	writeSegmentBackup(t, filepath.Join(base, testUIDA), "0000000000000001.segment", []segmentEntry{
		{idx: 10, term: 1, data: []byte("a")},
	})
	writeSegmentBackup(t, filepath.Join(base, testUIDB), "0000000000000001.segment", []segmentEntry{
		{idx: 20, term: 5, data: []byte("b")},
	})
	// A segment at the base root belongs to no queue and must not leak into any result.
	writeSegmentBackup(t, base, "0000000000000009.segment", []segmentEntry{
		{idx: 999, term: 99, data: []byte("stray")},
	})

	recs, err := ParseRecoveriesFromBackupFiles(base)
	require.NoError(t, err)
	require.Len(t, recs, 2)

	byQueue := recoveriesByQueue(recs)
	require.Contains(t, byQueue, "q.a")
	require.Contains(t, byQueue, "q.b")
	assert.Equal(t, RaftPosition{Idx: 10, Term: 1}, byQueue["q.a"].Position)
	assert.Equal(t, "/", byQueue["q.a"].VHost)
	assert.Equal(t, testUIDA, byQueue["q.a"].QueueUID)
	assert.Equal(t, RaftPosition{Idx: 20, Term: 5}, byQueue["q.b"].Position)
	assert.Equal(t, "vh2", byQueue["q.b"].VHost)
	assert.Equal(t, testUIDB, byQueue["q.b"].QueueUID)
}

func TestParseRecoveriesFromBackupFiles_CorruptedFilesNonFatal(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, testUIDA, "/", "q.a")
	queueDir := filepath.Join(base, testUIDA)
	require.NoError(t, os.WriteFile(filepath.Join(queueDir, "0000000000000001.segment"), []byte("garbage"), 0600))
	walDir := filepath.Join(queueDir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(walDir, "0000000000000001.wal"), []byte("garbage"), 0600))

	recs, err := ParseRecoveriesFromBackupFiles(base)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.False(t, recs[0].PositionFound)
	assert.Equal(t, RaftPosition{}, recs[0].Position)
}

// writeBackupConfig writes a ra-style 'config' file (including uid) into dir, creating it.
func writeBackupConfig(t *testing.T, dir, uid, vhost, queueName string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0755))
	content := fmt.Sprintf(
		"#{id => {'%s',rabbit@node},\n  uid => <<\"%s\">>,\n  machine => {module,rabbit_fifo, #{queue_resource => {resource,<<\"%s\">>,queue,<<\"%s\">>}}}}.",
		uid, uid, vhost, queueName,
	)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config"), []byte(content), 0600))
}

func TestExtractQueueUID_Found(t *testing.T) {
	uid, ok := extractQueueUID([]byte(`#{friendly_name => "x", uid => <<"2F_TES4NVJD9D5IKL4">>, log_init_args => #{uid => <<"2F_TES4NVJD9D5IKL4">>}}`))
	require.True(t, ok)
	assert.Equal(t, "2F_TES4NVJD9D5IKL4", uid)
}

func TestExtractQueueUID_NotFound(t *testing.T) {
	_, ok := extractQueueUID([]byte(`#{id => x, fluid => <<"nope">>}`))
	assert.False(t, ok)
}

func TestParseRecoveriesFromBackupFiles_NestedLayoutUsesConfigUID(t *testing.T) {
	base := t.TempDir()
	backupDir := filepath.Join(base, "default", testQueue, "20261103T150405Z")
	writeBackupConfig(t, backupDir, testUIDA, "/", testQueue)
	writeSegmentBackup(t, backupDir, "0000000000000001.segment", []segmentEntry{
		{idx: 3, term: 1, data: []byte("seg")},
	})
	walDir := filepath.Join(backupDir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0755))
	writeWALBackup(t, walDir, "0000000000000001.wal", []walEntry{
		{uid: testUIDA, idx: 9, term: 2, data: []byte("a")},
		{uid: testUIDB, idx: 99, term: 9, data: []byte("b")},
	})

	recs, err := ParseRecoveriesFromBackupFiles(base)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, &PreparedRecovery{
		Queue:         testQueue,
		VHost:         "/",
		QueueUID:      testUIDA,
		BackupDir:     backupDir,
		WALBackupDir:  walDir,
		Position:      RaftPosition{Idx: 9, Term: 2},
		PositionFound: true,
	}, recs[0])
}

func TestParseRecoveriesFromBackupFiles_WALSubdirNotTreatedAsBackup(t *testing.T) {
	base := t.TempDir()
	backupDir := filepath.Join(base, "default", "q", "20261103T150405Z")
	writeBackupConfig(t, backupDir, testUIDA, "/", "q")
	// Even a stray config inside wal/ must not produce a second recovery.
	writeBackupConfig(t, filepath.Join(backupDir, "wal"), testUIDB, "/", "other")

	recs, err := ParseRecoveriesFromBackupFiles(base)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, "q", recs[0].Queue)
}

func TestParseRecoveriesFromBackupFiles_MultipleBackupsOrderedOldestFirst(t *testing.T) {
	base := t.TempDir()
	newer := filepath.Join(base, "vh", "q", "20261103T160000Z")
	older := filepath.Join(base, "vh", "q", "20261103T150000Z")
	other := filepath.Join(base, "default", "p", "20261103T170000Z")
	writeBackupConfig(t, newer, testUIDA, "vh", "q")
	writeBackupConfig(t, older, testUIDA, "vh", "q")
	writeBackupConfig(t, other, testUIDB, "/", "p")

	recs, err := ParseRecoveriesFromBackupFiles(base)
	require.NoError(t, err)
	require.Len(t, recs, 3)
	assert.Equal(t, other, recs[0].BackupDir)
	assert.Equal(t, older, recs[1].BackupDir)
	assert.Equal(t, newer, recs[2].BackupDir)
}

func TestParseRecoveriesFromBackupFiles_RoundTripWithBackupQueueData(t *testing.T) {
	// Simulate a real quorum queue directory whose basename is the Ra UID.
	quorumDir := filepath.Join(t.TempDir(), testUIDA)
	writeBackupConfig(t, quorumDir, testUIDA, "/", testQueue)
	writeSegmentBackup(t, quorumDir, "0000000000000001.segment", []segmentEntry{
		{idx: 42, term: 4, data: []byte("m")},
	})

	base := t.TempDir()
	backupDir, err := BackupQueueData(quorumDir, base, "/", testQueue)
	require.NoError(t, err)

	recs, err := ParseRecoveriesFromBackupFiles(base)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, backupDir, recs[0].BackupDir)
	assert.Equal(t, testUIDA, recs[0].QueueUID, "UID must come from config, not the timestamp dir name")
	assert.Equal(t, "/", recs[0].VHost)
	assert.Equal(t, testQueue, recs[0].Queue)
	assert.True(t, recs[0].PositionFound)
	assert.Equal(t, RaftPosition{Idx: 42, Term: 4}, recs[0].Position)
}
