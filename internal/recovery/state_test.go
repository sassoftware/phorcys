// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sassoftware/phorcys/internal/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestState(vhost, queue string) *RecoveryState {
	return &RecoveryState{
		VHost:         vhost,
		QueueName:     queue,
		QueueUID:      "raft-123abc",
		Stage:         StagePrepared,
		NextPhase:     FirstExecutePhase,
		BackupDir:     "/backups/default/" + queue + "/20261119T100000Z",
		WALBackupDir:  "/backups/default/" + queue + "/20261119T100000Z/wal",
		Position:      RaftPosition{Term: 42, Idx: 1000},
		PositionFound: true,
		Definition: &QueueDefinition{
			Durable:   true,
			Arguments: map[string]any{"x-queue-type": "quorum"},
		},
	}
}

func statePathFor(t *testing.T, base, queue string) string {
	t.Helper()
	p, err := recoveryStatePath(base, "/", queue)
	require.NoError(t, err)
	return p
}

// ---------------------------------------------------------------------------
// Save / Load
// ---------------------------------------------------------------------------

func TestRecoveryState_SaveLoadRoundTrip(t *testing.T) {
	base := t.TempDir()
	rs := newTestState("/", "my.queue")
	rs.Stage = StageExecuting
	rs.NextPhase = 10
	rs.PublishedCount = 500

	require.NoError(t, rs.Save(base))

	got, err := LoadRecoveryState(base, "/", "my.queue")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, rs.QueueUID, got.QueueUID)
	assert.Equal(t, StageExecuting, got.Stage)
	assert.Equal(t, 10, got.NextPhase)
	assert.Equal(t, 500, got.PublishedCount)
	assert.Equal(t, RaftPosition{Term: 42, Idx: 1000}, got.Position)
	assert.True(t, got.PositionFound)
	assert.Equal(t, rs.BackupDir, got.BackupDir)
	assert.Equal(t, rs.WALBackupDir, got.WALBackupDir)
	require.NotNil(t, got.Definition)
	assert.True(t, got.Definition.Durable)
	assert.Equal(t, "quorum", got.Definition.Arguments["x-queue-type"])
	assert.Equal(t, recoveryStateVersion, got.Version)
}

func TestRecoveryState_SaveWritesDocumentedJSONFields(t *testing.T) {
	base := t.TempDir()
	require.NoError(t, newTestState("/", "q1").Save(base))

	data, err := os.ReadFile(statePathFor(t, base, "q1"))
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))

	assert.Equal(t, map[string]any{"term": float64(42), "index": float64(1000)}, raw["position"])
	for _, key := range []string{"version", "vhost", "queue_name", "queue_uid", "stage", "next_phase",
		"backup_dir", "wal_backup_dir", "position_found", "published_count", "created_at", "updated_at"} {
		assert.Contains(t, raw, key)
	}
}

func TestRecoveryState_SaveKeepsCreatedAtAndAdvancesUpdatedAt(t *testing.T) {
	base := t.TempDir()
	rs := newTestState("/", "q")
	require.NoError(t, rs.Save(base))
	created, updated := rs.CreatedAt, rs.UpdatedAt
	require.False(t, created.IsZero())

	rs.NextPhase = 8
	require.NoError(t, rs.Save(base))

	assert.Equal(t, created, rs.CreatedAt)
	assert.False(t, rs.UpdatedAt.Before(updated))
	got, err := LoadRecoveryState(base, "/", "q")
	require.NoError(t, err)
	assert.Equal(t, 8, got.NextPhase)
}

func TestRecoveryState_SaveLeavesNoTempFile(t *testing.T) {
	base := t.TempDir()
	require.NoError(t, newTestState("/", "q").Save(base))

	assert.NoFileExists(t, statePathFor(t, base, "q")+".tmp")
}

func TestRecoveryState_SaveRejectsInvalidState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RecoveryState)
	}{
		{"missing vhost", func(rs *RecoveryState) { rs.VHost = "" }},
		{"missing queue", func(rs *RecoveryState) { rs.QueueName = "" }},
		{"unknown stage", func(rs *RecoveryState) { rs.Stage = "DONE" }},
		{"phase below range", func(rs *RecoveryState) { rs.NextPhase = FirstExecutePhase - 1 }},
		{"phase above range", func(rs *RecoveryState) { rs.NextPhase = LastExecutePhase + 1 }},
		{"missing backup dir", func(rs *RecoveryState) { rs.BackupDir = "" }},
		{"negative published count", func(rs *RecoveryState) { rs.PublishedCount = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			rs := newTestState("/", "q")
			tt.mutate(rs)

			assert.Error(t, rs.Save(base))
			assert.NoDirExists(t, filepath.Join(base, recoveryStateDirName))
		})
	}
}

func TestLoadRecoveryState_MissingReturnsNil(t *testing.T) {
	got, err := LoadRecoveryState(t.TempDir(), "/", "q")

	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestLoadRecoveryState_InvalidFilesWrapErrInvalidState(t *testing.T) {
	mustJSON := func(rs *RecoveryState) string {
		data, err := json.Marshal(rs)
		require.NoError(t, err)
		return string(data)
	}
	otherQueue := newTestState("/", "other")
	otherQueue.Version = recoveryStateVersion
	futureVersion := newTestState("/", "q")
	futureVersion.Version = recoveryStateVersion + 1
	badPhase := newTestState("/", "q")
	badPhase.Version = recoveryStateVersion
	badPhase.NextPhase = 99

	tests := []struct {
		name    string
		content string
	}{
		{"truncated json", `{"version": 1, "vhost": "/"`},
		{"empty file", ``},
		{"not json", `garbage`},
		{"unsupported version", mustJSON(futureVersion)},
		{"phase out of range", mustJSON(badPhase)},
		{"identity mismatch", mustJSON(otherQueue)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			path := statePathFor(t, base, "q")
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))

			got, err := LoadRecoveryState(base, "/", "q")

			assert.Nil(t, got)
			assert.ErrorIs(t, err, ErrInvalidState)
		})
	}
}

func TestLoadRecoveryState_IgnoresStaleTempFile(t *testing.T) {
	base := t.TempDir()
	path := statePathFor(t, base, "q")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
	require.NoError(t, os.WriteFile(path+".tmp", []byte(`{"partial`), 0o600))

	got, err := LoadRecoveryState(base, "/", "q")

	require.NoError(t, err)
	assert.Nil(t, got)
}

// ---------------------------------------------------------------------------
// Path encoding
// ---------------------------------------------------------------------------

func TestRecoveryStatePath_EncodesUnsafeNames(t *testing.T) {
	base := t.TempDir()
	stateRoot := filepath.Join(base, recoveryStateDirName)

	for _, name := range []string{"/", "a/b", "..", ".", "100%", "dotted.queue.name"} {
		t.Run(name, func(t *testing.T) {
			path, err := recoveryStatePath(base, name, name)
			require.NoError(t, err)

			rel, err := filepath.Rel(stateRoot, path)
			require.NoError(t, err)
			// vhost dir, queue dir, state file: nothing escaped into extra or parent segments.
			parts := strings.Split(rel, string(filepath.Separator))
			require.Len(t, parts, 3)
			assert.Equal(t, recoveryStateFile, parts[2])
			for _, p := range parts[:2] {
				assert.NotContains(t, p, ".")
				assert.NotEmpty(t, p)
			}
		})
	}
}

func TestRecoveryStatePath_RejectsEmptyNames(t *testing.T) {
	_, err := recoveryStatePath(t.TempDir(), "", "q")
	assert.Error(t, err)
	_, err = recoveryStatePath(t.TempDir(), "/", "")
	assert.Error(t, err)
}

func TestRecoveryState_DistinctNamesDoNotCollide(t *testing.T) {
	base := t.TempDir()
	slash := newTestState("/", "q")
	slash.NextPhase = 7
	named := newTestState("default", "q")
	named.NextPhase = 9
	nested := newTestState("/", "a/b")
	nested.NextPhase = 11
	require.NoError(t, slash.Save(base))
	require.NoError(t, named.Save(base))
	require.NoError(t, nested.Save(base))

	for _, c := range []struct {
		vhost, queue string
		phase        int
	}{{"/", "q", 7}, {"default", "q", 9}, {"/", "a/b", 11}} {
		got, err := LoadRecoveryState(base, c.vhost, c.queue)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, c.phase, got.NextPhase, "%s %s", c.vhost, c.queue)
	}
}

// ---------------------------------------------------------------------------
// Has / Archive
// ---------------------------------------------------------------------------

func TestHasRecoveryState(t *testing.T) {
	base := t.TempDir()
	assert.False(t, HasRecoveryState(base, "/", "q"))

	require.NoError(t, newTestState("/", "q").Save(base))
	assert.True(t, HasRecoveryState(base, "/", "q"))
	assert.False(t, HasRecoveryState(base, "/", "other"))
}

func TestHasRecoveryState_CountsUnreadableFileAsPresent(t *testing.T) {
	base := t.TempDir()
	path := statePathFor(t, base, "q")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
	require.NoError(t, os.WriteFile(path, []byte("garbage"), 0o600))

	assert.True(t, HasRecoveryState(base, "/", "q"))
}

func archivedFiles(t *testing.T, base, queue string) []string {
	t.Helper()
	dir := filepath.Join(base, recoveryArchiveDirName, encodePathSegment("/"), encodePathSegment(queue))
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	var files []string
	for _, e := range entries {
		files = append(files, filepath.Join(dir, e.Name()))
	}
	return files
}

func TestArchiveRecoveryState_MovesStateToArchive(t *testing.T) {
	base := t.TempDir()
	rs := newTestState("/", "q")
	rs.Stage = StageExecuting
	rs.NextPhase = LastExecutePhase
	require.NoError(t, rs.Save(base))
	path := statePathFor(t, base, "q")
	original, err := os.ReadFile(path)
	require.NoError(t, err)

	require.NoError(t, ArchiveRecoveryState(base, "/", "q"))

	assert.NoFileExists(t, path)
	assert.NoDirExists(t, filepath.Dir(path))
	assert.False(t, HasRecoveryState(base, "/", "q"))
	files := archivedFiles(t, base, "q")
	require.Len(t, files, 1)
	assert.Regexp(t, `state-\d{8}T\d{6}\.\d{6}Z\.json$`, files[0])
	archived, err := os.ReadFile(files[0])
	require.NoError(t, err)
	assert.Equal(t, string(original), string(archived))
}

func TestArchiveRecoveryState_NoStateIsNoop(t *testing.T) {
	base := t.TempDir()

	require.NoError(t, ArchiveRecoveryState(base, "/", "never-existed"))

	assert.NoDirExists(t, filepath.Join(base, recoveryArchiveDirName))
}

func TestArchiveRecoveryState_RepeatedRecoveriesKeepEveryArchive(t *testing.T) {
	base := t.TempDir()
	for phase := FirstExecutePhase; phase < FirstExecutePhase+3; phase++ {
		rs := newTestState("/", "q")
		rs.NextPhase = phase
		require.NoError(t, rs.Save(base))
		require.NoError(t, ArchiveRecoveryState(base, "/", "q"))
	}

	assert.Len(t, archivedFiles(t, base, "q"), 3)
}

func TestUniqueArchivePath_DoesNotOverwriteExisting(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	first := uniqueArchivePath(dir, now)
	require.NoError(t, os.WriteFile(first, []byte("a"), 0o600))

	second := uniqueArchivePath(dir, now)

	assert.NotEqual(t, first, second)
	assert.NoFileExists(t, second)
}

func TestArchiveRecoveryState_KeepsOtherQueuesInSameVHost(t *testing.T) {
	base := t.TempDir()
	require.NoError(t, newTestState("/", "a").Save(base))
	require.NoError(t, newTestState("/", "b").Save(base))

	require.NoError(t, ArchiveRecoveryState(base, "/", "a"))

	assert.False(t, HasRecoveryState(base, "/", "a"))
	assert.True(t, HasRecoveryState(base, "/", "b"))
	assert.Len(t, archivedFiles(t, base, "a"), 1)
	assert.Empty(t, archivedFiles(t, base, "b"))
}

func TestArchiveRecoveryState_ArchivedStateIsNotListedAsPending(t *testing.T) {
	base := t.TempDir()
	require.NoError(t, newTestState("/", "q").Save(base))
	require.NoError(t, ArchiveRecoveryState(base, "/", "q"))

	refs, err := ListRecoveryStates(base)

	require.NoError(t, err)
	assert.Empty(t, refs)
}

func TestArchiveRecoveryState_ArchivesUnreadableStateFile(t *testing.T) {
	base := t.TempDir()
	path := statePathFor(t, base, "q")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
	require.NoError(t, os.WriteFile(path, []byte("garbage"), 0o600))

	require.NoError(t, ArchiveRecoveryState(base, "/", "q"))

	assert.False(t, HasRecoveryState(base, "/", "q"))
	files := archivedFiles(t, base, "q")
	require.Len(t, files, 1)
	content, err := os.ReadFile(files[0])
	require.NoError(t, err)
	assert.Equal(t, "garbage", string(content))
}

// ---------------------------------------------------------------------------
// ListRecoveryStates
// ---------------------------------------------------------------------------

func TestListRecoveryStates_ReturnsDecodedQueues(t *testing.T) {
	base := t.TempDir()
	require.NoError(t, newTestState("/", "a").Save(base))
	require.NoError(t, newTestState("/", "a/b.c").Save(base))
	require.NoError(t, newTestState("other", "z").Save(base))

	refs, err := ListRecoveryStates(base)

	require.NoError(t, err)
	assert.ElementsMatch(t, []common.QueueDef{
		{VHost: "/", Name: "a"},
		{VHost: "/", Name: "a/b.c"},
		{VHost: "other", Name: "z"},
	}, refs)
}

func TestListRecoveryStates_MissingDirectoryIsEmpty(t *testing.T) {
	refs, err := ListRecoveryStates(t.TempDir())

	require.NoError(t, err)
	assert.Empty(t, refs)
}

func TestListRecoveryStates_IncludesUnreadableStateFiles(t *testing.T) {
	base := t.TempDir()
	path := statePathFor(t, base, "q")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
	require.NoError(t, os.WriteFile(path, []byte("garbage"), 0o600))

	refs, err := ListRecoveryStates(base)

	require.NoError(t, err)
	assert.Equal(t, []common.QueueDef{{VHost: "/", Name: "q"}}, refs)
}
