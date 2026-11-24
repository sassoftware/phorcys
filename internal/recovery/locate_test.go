// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"fmt"
	"io/fs"
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
	assert.NotNil(t, locs[expected], "expected path %q to be present in results", expected)
	assert.Equal(t, "prod", locs[expected].VHost)
	assert.Equal(t, testQueue, locs[expected].Name)
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
// scanQueueDirectories
// ---------------------------------------------------------------------------

func writeRawConfig(t *testing.T, dir string, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config"), []byte(content), 0600))
}

func TestScanQueueDirectories_EmptyBaseDir(t *testing.T) {
	locs, err := scanQueueDirectories(t.TempDir(), false)
	require.NoError(t, err)
	assert.Empty(t, locs)
}

func TestScanQueueDirectories_PopulatesAllFields(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "uid1", "prod", "orders.q")

	locs, err := scanQueueDirectories(base, false)
	require.NoError(t, err)
	require.Len(t, locs, 1)
	expected := filepath.Join(base, "uid1")
	assert.NotNil(t, locs[expected], "expected path %q to be present in results", expected)
	assert.Equal(t, "prod", locs[expected].VHost)
	assert.Equal(t, "orders.q", locs[expected].Name)
}

func TestScanQueueDirectories_FindsNestedQueues(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "top", "/", "top.q")
	writeConfigFile(t, base, filepath.Join("vh", "q", "20261103T150000Z"), "vh", "deep.q")

	locs, err := scanQueueDirectories(base, false)
	require.NoError(t, err)
	require.Len(t, locs, 2)
	assert.NotNil(t, locs[filepath.Join(base, "top")], "expected path %q to be present in results", filepath.Join(base, "top"))
	assert.NotNil(t, locs[filepath.Join(base, "vh", "q", "20261103T150000Z")], "expected path %q to be present in results", filepath.Join(base, "vh", "q", "20261103T150000Z"))
	assert.Equal(t, "deep.q", locs[filepath.Join(base, "vh", "q", "20261103T150000Z")].Name)
}

func TestScanQueueDirectories_DoesNotDescendIntoMatchedDir(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "q1", "/", "outer.q")
	// A config under a matched queue directory (e.g. wal/) belongs to that queue.
	writeConfigFile(t, base, filepath.Join("q1", "wal"), "/", "inner.q")

	locs, err := scanQueueDirectories(base, false)
	require.NoError(t, err)
	require.Len(t, locs, 1)
	assert.Equal(t, "outer.q", locs[filepath.Join(base, "q1")].Name)
}

func TestScanQueueDirectories_DescendsBelowUnmatchedConfig(t *testing.T) {
	base := t.TempDir()
	writeRawConfig(t, filepath.Join(base, "bad"), "#{id => something_else}")
	writeConfigFile(t, base, filepath.Join("bad", "child"), "/", "child.q")

	locs, err := scanQueueDirectories(base, false)
	require.NoError(t, err)
	require.Len(t, locs, 1)
	assert.Equal(t, "child.q", locs[filepath.Join(base, "bad", "child")].Name)
}

func TestScanQueueDirectories_SkipsInvalidEntries(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "good", "/", "valid.q")
	require.NoError(t, os.MkdirAll(filepath.Join(base, "noconfig"), 0755))
	writeRawConfig(t, filepath.Join(base, "badconfig"), "#{id => x}")
	writeRawConfig(t, filepath.Join(base, "emptyconfig"), "")
	require.NoError(t, os.WriteFile(filepath.Join(base, "notadir"), []byte("x"), 0600))
	// A directory named 'config' is not a config file.
	require.NoError(t, os.MkdirAll(filepath.Join(base, "dirconfig", "config"), 0755))

	locs, err := scanQueueDirectories(base, false)
	require.NoError(t, err)
	require.Len(t, locs, 1)
	assert.Equal(t, "valid.q", locs[filepath.Join(base, "good")].Name)
}

func TestScanQueueDirectories_ResultsInLexicalPathOrder(t *testing.T) {
	base := t.TempDir()
	for _, dir := range []string{"c", "a", "b"} {
		writeConfigFile(t, base, dir, "/", "q."+dir)
	}

	locs, err := scanQueueDirectories(base, false)
	require.NoError(t, err)
	require.Len(t, locs, 3)
	for _, want := range []string{"a", "b", "c"} {
		assert.NotNil(t, locs[filepath.Join(base, want)], "expected path %q to be present in results", filepath.Join(base, want))
	}
}

func TestScanQueueDirectories_DuplicateQueueInMultipleDirs(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "20261103T150000Z", "vh", "q")
	writeConfigFile(t, base, "20261103T160000Z", "vh", "q")

	locs, err := scanQueueDirectories(base, false)
	require.NoError(t, err)
	require.Len(t, locs, 2)
	assert.NotNil(t, locs[filepath.Join(base, "20261103T150000Z")], "expected path %q to be present in results", filepath.Join(base, "20261103T150000Z"))
	assert.NotNil(t, locs[filepath.Join(base, "20261103T160000Z")], "expected path %q to be present in results", filepath.Join(base, "20261103T160000Z"))
}

func TestScanQueueDirectories_ConfigInBaseDirMatchesBase(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, ".", "/", "root.q")
	writeConfigFile(t, base, "child", "/", "child.q")

	locs, err := scanQueueDirectories(base, false)
	require.NoError(t, err)
	require.Len(t, locs, 1)
	assert.Equal(t, "/", locs[filepath.Join(base, ".")].VHost)
	assert.Equal(t, "root.q", locs[filepath.Join(base, ".")].Name)
}

func TestScanQueueDirectories_UnreadableSubdirSkipped(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission checks are bypassed when running as root")
	}
	base := t.TempDir()
	writeConfigFile(t, base, "good", "/", "valid.q")
	locked := filepath.Join(base, "locked")
	writeConfigFile(t, locked, "inner", "/", "hidden.q")
	require.NoError(t, os.Chmod(locked, 0000))
	t.Cleanup(func() { _ = os.Chmod(locked, 0755) })

	locs, err := scanQueueDirectories(base, false)
	require.NoError(t, err)
	require.Len(t, locs, 1)
	assert.Equal(t, "valid.q", locs[filepath.Join(base, "good")].Name)
}

func TestScanQueueDirectories_BaseIsFileReturnsEmpty(t *testing.T) {
	file := filepath.Join(t.TempDir(), "notadir")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0600))

	locs, err := scanQueueDirectories(file, false)
	require.NoError(t, err)
	assert.Empty(t, locs)
}

func TestScanQueueDirectories_PathUnderFileReturnsError(t *testing.T) {
	file := filepath.Join(t.TempDir(), "notadir")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0600))

	// ENOTDIR is not ErrNotExist, so this fails even when missingOk is set.
	locs, err := scanQueueDirectories(filepath.Join(file, "sub"), true)
	assert.Error(t, err)
	assert.Nil(t, locs)
}

// ---------------------------------------------------------------------------
// Missing base directory handling (differs between scanners)
// ---------------------------------------------------------------------------

func TestScanAllQuorumDirectories_MissingBaseDirReturnsError(t *testing.T) {
	locs, err := ScanAllQuorumDirectories(filepath.Join(t.TempDir(), "does-not-exist"))
	require.Error(t, err)
	assert.ErrorIs(t, err, fs.ErrNotExist)
	assert.Nil(t, locs)
}

func TestScanBackupDirectories_MissingBaseDirReturnsEmpty(t *testing.T) {
	locs, err := ScanBackupDirectories(filepath.Join(t.TempDir(), "does-not-exist"))
	require.NoError(t, err)
	assert.Nil(t, locs)
}

func TestScanBackupDirectories_BackupQueueDataLayout(t *testing.T) {
	base := t.TempDir()
	older := filepath.Join("vh", "q", "20261103T150000Z")
	newer := filepath.Join("vh", "q", "20261103T160000Z")
	other := filepath.Join("default", "p", "20261103T170000Z")
	writeConfigFile(t, base, newer, "vh", "q")
	writeConfigFile(t, base, older, "vh", "q")
	writeConfigFile(t, base, other, "/", "p")
	// Each backup carries a wal/ subdirectory that must not be reported separately.
	require.NoError(t, os.MkdirAll(filepath.Join(base, older, "wal"), 0755))

	locs, err := ScanBackupDirectories(base)
	require.NoError(t, err)
	require.Len(t, locs, 3)
	assert.NotNil(t, locs[filepath.Join(base, other)], "expected path %q to be present in results", filepath.Join(base, other))
	assert.NotNil(t, locs[filepath.Join(base, older)], "expected path %q to be present in results", filepath.Join(base, older))
	assert.NotNil(t, locs[filepath.Join(base, newer)], "expected path %q to be present in results", filepath.Join(base, newer))
}
