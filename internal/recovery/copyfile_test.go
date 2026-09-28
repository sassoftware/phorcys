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
// copyFile
// ---------------------------------------------------------------------------

func TestCopyFile_CopiesContent(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.dat")
	dst := filepath.Join(dir, "dst.dat")

	content := []byte("hello phorcys 1234")
	require.NoError(t, os.WriteFile(src, content, 0600), "write src")

	require.NoError(t, copyFile(src, dst), "copyFile")

	got, err := os.ReadFile(dst)
	require.NoError(t, err, "read dst: %v", err)
	assert.Equal(t, string(content), string(got))
}

func TestCopyFile_MissingSourceReturnsError(t *testing.T) {
	dir := t.TempDir()
	err := copyFile(filepath.Join(dir, "no-such.dat"), filepath.Join(dir, "dst.dat"))
	assert.Error(t, err, "expected error for missing source file")
}

func TestCopyFile_OverwritesExistingDestination(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.dat")
	dst := filepath.Join(dir, "dst.dat")

	err := os.WriteFile(src, []byte("new content"), 0600)
	require.NoError(t, err)
	err = os.WriteFile(dst, []byte("old content that should be gone"), 0600)
	require.NoError(t, err)

	require.NoError(t, copyFile(src, dst), "copyFile")
	got, _ := os.ReadFile(dst)
	assert.Equal(t, "new content", string(got))
}
