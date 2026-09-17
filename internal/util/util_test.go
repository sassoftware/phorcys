package util

import (
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// GetEnv
// ---------------------------------------------------------------------------

func TestGetEnv_ReturnsFallbackWhenUnset(t *testing.T) {
	got := GetEnv("__ARGUS_NONEXISTENT_VAR_XYZ__", "fallback")
	if got != "fallback" {
		t.Errorf("got %q, want %q", got, "fallback")
	}
}

func TestGetEnv_ReturnsEnvValueWhenSet(t *testing.T) {
	t.Setenv("__ARGUS_TEST_VAR__", "custom_value")
	got := GetEnv("__ARGUS_TEST_VAR__", "fallback")
	if got != "custom_value" {
		t.Errorf("got %q, want %q", got, "custom_value")
	}
}

func TestGetEnv_EmptyEnvValueUsesFallback(t *testing.T) {
	// When the env var is set to empty string, the fallback should be used.
	t.Setenv("__ARGUS_EMPTY_VAR__", "")
	got := GetEnv("__ARGUS_EMPTY_VAR__", "fallback")
	if got != "fallback" {
		t.Errorf("expected fallback for empty env var, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// CopyFile
// ---------------------------------------------------------------------------

func TestCopyFile_CopiesContent(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.dat")
	dst := filepath.Join(dir, "dst.dat")

	content := []byte("hello argus 1234")
	if err := os.WriteFile(src, content, 0644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	if err := CopyFile(src, dst); err != nil {
		t.Fatalf("CopyFile: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("dst content = %q, want %q", got, content)
	}
}

func TestCopyFile_MissingSourceReturnsError(t *testing.T) {
	dir := t.TempDir()
	err := CopyFile(filepath.Join(dir, "no-such.dat"), filepath.Join(dir, "dst.dat"))
	if err == nil {
		t.Error("expected error for missing source file")
	}
}

func TestCopyFile_OverwritesExistingDestination(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.dat")
	dst := filepath.Join(dir, "dst.dat")

	os.WriteFile(src, []byte("new content"), 0644)
	os.WriteFile(dst, []byte("old content that should be gone"), 0644)

	if err := CopyFile(src, dst); err != nil {
		t.Fatalf("CopyFile: %v", err)
	}
	got, _ := os.ReadFile(dst)
	if string(got) != "new content" {
		t.Errorf("dst = %q, expected overwrite with %q", got, "new content")
	}
}
