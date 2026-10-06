// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// RaftPosition.After
// ---------------------------------------------------------------------------

func TestRaftPosition_After_HigherTermWins(t *testing.T) {
	newer := RaftPosition{Idx: 1, Term: 5}
	older := RaftPosition{Idx: 100, Term: 4}
	assert.True(t, newer.After(older))
	assert.False(t, older.After(newer))
}

func TestRaftPosition_After_TermTieBreaksOnIdx(t *testing.T) {
	newer := RaftPosition{Idx: 10, Term: 3}
	older := RaftPosition{Idx: 9, Term: 3}
	assert.True(t, newer.After(older))
	assert.False(t, older.After(newer))
}

func TestRaftPosition_After_Equal(t *testing.T) {
	a := RaftPosition{Idx: 5, Term: 2}
	assert.False(t, a.After(a))
}

// ---------------------------------------------------------------------------
// LatestQueuePosition
// ---------------------------------------------------------------------------

func writeSegmentBackup(t *testing.T, dir, name string, entries []segmentEntry) {
	t.Helper()
	data := buildSegmentV2(uint16(len(entries)+1), entries) //nolint:gosec
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0600))
}

func writeWALBackup(t *testing.T, dir, name string, entries []walEntry) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), buildWAL(entries, "", false), 0600))
}

func TestLatestQueuePosition_SegmentOnly(t *testing.T) {
	segDir := t.TempDir()
	writeSegmentBackup(t, segDir, "0000000000000001.segment", []segmentEntry{
		{idx: 10, term: 1, data: []byte("a")},
		{idx: 11, term: 2, data: []byte("b")},
	})

	pos, found, err := LatestQueuePosition(segDir, "", "any-uid")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, RaftPosition{Idx: 11, Term: 2}, pos)
}

func TestLatestQueuePosition_WALOnly_FiltersByUID(t *testing.T) {
	walDir := t.TempDir()
	writeWALBackup(t, walDir, "0000000000000001.wal", []walEntry{
		{uid: "queue-A", idx: 5, term: 1, data: []byte("a1")},
		{uid: "queue-B", idx: 99, term: 9, data: []byte("b1")}, // higher, but different queue
	})

	pos, found, err := LatestQueuePosition("", walDir, "queue-A")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, RaftPosition{Idx: 5, Term: 1}, pos)
}

func TestLatestQueuePosition_CombinesSegmentAndWAL_HigherTermWins(t *testing.T) {
	segDir := t.TempDir()
	walDir := t.TempDir()
	writeSegmentBackup(t, segDir, "0000000000000001.segment", []segmentEntry{
		{idx: 1000, term: 1, data: []byte("seg")},
	})
	writeWALBackup(t, walDir, "0000000000000001.wal", []walEntry{
		{uid: "queue-A", idx: 2, term: 3, data: []byte("wal")}, // lower idx but newer term
	})

	pos, found, err := LatestQueuePosition(segDir, walDir, "queue-A")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, RaftPosition{Idx: 2, Term: 3}, pos)
}

func TestLatestQueuePosition_CombinesSegmentAndWAL_SegmentWinsOnTermTie(t *testing.T) {
	segDir := t.TempDir()
	walDir := t.TempDir()
	writeSegmentBackup(t, segDir, "0000000000000001.segment", []segmentEntry{
		{idx: 50, term: 2, data: []byte("seg")},
	})
	writeWALBackup(t, walDir, "0000000000000001.wal", []walEntry{
		{uid: "queue-A", idx: 10, term: 2, data: []byte("wal")},
	})

	pos, found, err := LatestQueuePosition(segDir, walDir, "queue-A")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, RaftPosition{Idx: 50, Term: 2}, pos)
}

func TestLatestQueuePosition_NoDirs_NotFound(t *testing.T) {
	pos, found, err := LatestQueuePosition("", "", "any-uid")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, RaftPosition{}, pos)
}

func TestLatestQueuePosition_AbsentDirs_NotFoundNoError(t *testing.T) {
	pos, found, err := LatestQueuePosition("/no/such/segment/dir", "/no/such/wal/dir", "any-uid")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, RaftPosition{}, pos)
}

func TestLatestQueuePosition_SkipsNonMatchingFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not relevant"), 0600))

	pos, found, err := LatestQueuePosition(dir, dir, "any-uid")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, RaftPosition{}, pos)
}

func TestLatestQueuePosition_CorruptedSegmentFileSkippedNotFatal(t *testing.T) {
	segDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(segDir, "0000000000000001.segment"), []byte{0xe0, 0xfa, 0x32, 0xb1}, 0600))
	writeSegmentBackup(t, segDir, "0000000000000002.segment", []segmentEntry{
		{idx: 7, term: 1, data: []byte("good")},
	})

	pos, found, err := LatestQueuePosition(segDir, "", "any-uid")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, RaftPosition{Idx: 7, Term: 1}, pos)
}

func TestLatestQueuePosition_CorruptedWALFileSkippedNotFatal(t *testing.T) {
	walDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(walDir, "0000000000000001.wal"), []byte{0xe0, 0xfa, 0x32, 0xb1}, 0600))
	writeWALBackup(t, walDir, "0000000000000002.wal", []walEntry{
		{uid: "queue-A", idx: 3, term: 1, data: []byte("good")},
	})

	pos, found, err := LatestQueuePosition("", walDir, "queue-A")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, RaftPosition{Idx: 3, Term: 1}, pos)
}

func TestLatestQueuePosition_AllSegmentFilesCorrupted_NotFoundNoError(t *testing.T) {
	segDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(segDir, "0000000000000001.segment"), []byte{0xe0, 0xfa, 0x32, 0xb1}, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, "0000000000000002.segment"), []byte("garbage"), 0600))

	// All files present are unreadable: still reported as "not found" rather
	// than a hard error (so the pipeline doesn't abort), but a louder
	// aggregate WARNING is logged distinguishing this from an empty backup.
	pos, found, err := LatestQueuePosition(segDir, "", "any-uid")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, RaftPosition{}, pos)
}

func TestLatestQueuePosition_AllWALFilesCorrupted_NotFoundNoError(t *testing.T) {
	walDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(walDir, "0000000000000001.wal"), []byte{0xe0, 0xfa, 0x32, 0xb1}, 0600))

	pos, found, err := LatestQueuePosition("", walDir, "queue-A")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, RaftPosition{}, pos)
}
