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

const (
	testSrcFile    = "src.dat"
	testDstFile    = "dst.dat"
	testSrcContent = "0123456789"
	testDstContent = "01234567"
)

// ---------------------------------------------------------------------------
// copyFile
// ---------------------------------------------------------------------------

func TestCopyFile_CopiesContent(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, testSrcFile)
	dst := filepath.Join(dir, testDstFile)

	require.NoError(t, os.WriteFile(src, []byte(testSrcContent), 0600), "write src")

	require.NoError(t, CopyFile(src, dst), "copyFile")

	got, err := os.ReadFile(dst)
	require.NoError(t, err, "read dst: %v", err)
	assert.Equal(t, testSrcContent, string(got))
}

func TestCopyFile_MissingSourceReturnsError(t *testing.T) {
	dir := t.TempDir()
	err := CopyFile(filepath.Join(dir, "no-such.dat"), filepath.Join(dir, "dst.dat"))
	assert.Error(t, err, "expected error for missing source file")
}

func TestCopyFile_OverwritesExistingDestination(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, testSrcFile)
	dst := filepath.Join(dir, testDstFile)

	err := os.WriteFile(src, []byte(testSrcContent), 0600)
	require.NoError(t, err)
	err = os.WriteFile(dst, []byte(testDstContent), 0600)
	require.NoError(t, err)

	require.NoError(t, CopyFile(src, dst), "copyFile")
	got, _ := os.ReadFile(dst)
	assert.Equal(t, testSrcContent, string(got))
}

func stubAvailableSpace(t *testing.T, avail uint64) {
	t.Helper()
	orig := availableSpace
	availableSpace = func(string) (uint64, error) { return avail, nil }
	t.Cleanup(func() { availableSpace = orig })
}

func TestCopyFile_InsufficientSpaceReturnsError(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, testSrcFile)
	dst := filepath.Join(dir, testDstFile)
	require.NoError(t, os.WriteFile(src, []byte(testSrcContent), 0600))
	stubAvailableSpace(t, 5)

	err := CopyFile(src, dst)
	require.Error(t, err)
	_, statErr := os.Stat(dst)
	assert.True(t, os.IsNotExist(statErr), "dst must not be created when space is insufficient")
}

func TestCopyFile_CreditsSpaceFromExistingDestination(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, testSrcFile)
	dst := filepath.Join(dir, testDstFile)
	require.NoError(t, os.WriteFile(src, []byte(testSrcContent), 0600))
	require.NoError(t, os.WriteFile(dst, []byte(testDstContent), 0600))
	stubAvailableSpace(t, 2)

	err := CopyFile(src, dst)
	require.NoError(t, err)
	got, _ := os.ReadFile(dst)
	assert.Equal(t, testSrcContent, string(got))
}

func TestCopyFile_RealSpaceCheckPasses(t *testing.T) {
	avail, err := diskAvailable(t.TempDir())
	require.NoError(t, err)
	assert.Positive(t, avail)
}
