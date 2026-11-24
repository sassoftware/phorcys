// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteFileAtomic_CreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	require.NoError(t, WriteFileAtomic(path, []byte("hello"), 0o640))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(got))
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o640), info.Mode().Perm())
}

func TestWriteFileAtomic_ReplacesExistingAndLeavesNoTempFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	require.NoError(t, os.WriteFile(path, []byte("old contents that are longer"), 0o600))

	require.NoError(t, WriteFileAtomic(path, []byte("new"), 0o640))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "new", string(got))
	assert.NoFileExists(t, path+".tmp")
}

func TestWriteFileAtomic_OverwritesStaleTempFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	require.NoError(t, os.WriteFile(path+".tmp", []byte("partial garbage from a crashed write"), 0o600))

	require.NoError(t, WriteFileAtomic(path, []byte("ok"), 0o640))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "ok", string(got))
	assert.NoFileExists(t, path+".tmp")
}

func TestWriteFileAtomic_MissingDirectoryFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "state.json")

	assert.Error(t, WriteFileAtomic(path, []byte("x"), 0o640))
}

func TestWriteFileAtomic_FailedRenameKeepsOriginalAndRemovesTemp(t *testing.T) {
	// Renaming a file over a non-empty directory fails, which exercises the
	// error path after the temp file has been fully written.
	path := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.MkdirAll(filepath.Join(path, "child"), 0o750))

	require.Error(t, WriteFileAtomic(path, []byte("x"), 0o640))

	assert.DirExists(t, path)
	assert.NoFileExists(t, path+".tmp")
}

func TestSyncDir(t *testing.T) {
	assert.NoError(t, SyncDir(t.TempDir()))
	assert.Error(t, SyncDir(filepath.Join(t.TempDir(), "missing")))
}
