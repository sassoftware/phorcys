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
	writeConfigFile(t, base, "abc123", "prod", "orders")

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
