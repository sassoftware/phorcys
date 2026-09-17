package recovery

import (
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// copyFile
// ---------------------------------------------------------------------------

func TestCopyFile_CopiesContent(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.dat")
	dst := filepath.Join(dir, "dst.dat")

	content := []byte("hello argus 1234")
	if err := os.WriteFile(src, content, 0644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
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
	err := copyFile(filepath.Join(dir, "no-such.dat"), filepath.Join(dir, "dst.dat"))
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

	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	got, _ := os.ReadFile(dst)
	if string(got) != "new content" {
		t.Errorf("dst = %q, expected overwrite with %q", got, "new content")
	}
}
