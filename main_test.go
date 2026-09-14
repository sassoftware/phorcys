package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// getEnv
// ---------------------------------------------------------------------------

func TestGetEnv_ReturnsFallbackWhenUnset(t *testing.T) {
	got := getEnv("__ARGUS_NONEXISTENT_VAR_XYZ__", "fallback")
	if got != "fallback" {
		t.Errorf("got %q, want %q", got, "fallback")
	}
}

func TestGetEnv_ReturnsEnvValueWhenSet(t *testing.T) {
	t.Setenv("__ARGUS_TEST_VAR__", "custom_value")
	got := getEnv("__ARGUS_TEST_VAR__", "fallback")
	if got != "custom_value" {
		t.Errorf("got %q, want %q", got, "custom_value")
	}
}

func TestGetEnv_EmptyEnvValueUsesFallback(t *testing.T) {
	// When the env var is set to empty string, the fallback should be used.
	t.Setenv("__ARGUS_EMPTY_VAR__", "")
	got := getEnv("__ARGUS_EMPTY_VAR__", "fallback")
	if got != "fallback" {
		t.Errorf("expected fallback for empty env var, got %q", got)
	}
}

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

// ---------------------------------------------------------------------------
// backupQueueData
// ---------------------------------------------------------------------------

func TestBackupQueueData_CopiesSegmentAndWalFiles(t *testing.T) {
	src := t.TempDir()
	base := t.TempDir()

	os.WriteFile(filepath.Join(src, "00000001.segment"), []byte("seg"), 0644)
	os.WriteFile(filepath.Join(src, "00000001.wal"), []byte("wal"), 0644)
	// Subdirectories must be skipped.
	os.MkdirAll(filepath.Join(src, "inner"), 0755)

	destDir, err := backupQueueData(src, base, "/", "events.queue")
	if err != nil {
		t.Fatalf("backupQueueData: %v", err)
	}

	for _, name := range []string{"00000001.segment", "00000001.wal"} {
		if _, err := os.Stat(filepath.Join(destDir, name)); err != nil {
			t.Errorf("expected file %q in backup dir: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(destDir, "inner")); !os.IsNotExist(err) {
		t.Error("subdirectory must not be copied")
	}
}

func TestBackupQueueData_SanitizesRootVhost(t *testing.T) {
	src := t.TempDir()
	base := t.TempDir()

	os.WriteFile(filepath.Join(src, "data.segment"), []byte("x"), 0644)

	destDir, err := backupQueueData(src, base, "/", "my.queue")
	if err != nil {
		t.Fatalf("backupQueueData: %v", err)
	}

	// destDir == base/default/my.queue/<timestamp>
	rel, err := filepath.Rel(base, destDir)
	if err != nil {
		t.Fatalf("filepath.Rel: %v", err)
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) < 3 {
		t.Fatalf("expected at least 3 path segments, got %q", rel)
	}
	if parts[0] != "default" {
		t.Errorf("vhost dir = %q, want %q", parts[0], "default")
	}
	if parts[1] != "my.queue" {
		t.Errorf("queue dir = %q, want %q", parts[1], "my.queue")
	}
}

func TestBackupQueueData_UsesNamedVhost(t *testing.T) {
	src := t.TempDir()
	base := t.TempDir()

	os.WriteFile(filepath.Join(src, "data.segment"), []byte("x"), 0644)

	destDir, err := backupQueueData(src, base, "production", "order.queue")
	if err != nil {
		t.Fatalf("backupQueueData: %v", err)
	}

	rel, _ := filepath.Rel(base, destDir)
	parts := strings.Split(rel, string(filepath.Separator))
	if parts[0] != "production" {
		t.Errorf("vhost dir = %q, want %q", parts[0], "production")
	}
}

func TestBackupQueueData_TimestampDirFormat(t *testing.T) {
	src := t.TempDir()
	base := t.TempDir()

	os.WriteFile(filepath.Join(src, "data.segment"), []byte("x"), 0644)

	before := time.Now().UTC()
	destDir, err := backupQueueData(src, base, "vh", "q")
	after := time.Now().UTC()

	if err != nil {
		t.Fatalf("backupQueueData: %v", err)
	}

	tsStr := filepath.Base(destDir)
	ts, err := time.Parse("20060102T150405Z", tsStr)
	if err != nil {
		t.Fatalf("timestamp dir %q is not in expected format: %v", tsStr, err)
	}
	if ts.Before(before.Truncate(time.Second)) || ts.After(after.Add(time.Second)) {
		t.Errorf("timestamp %v is outside [%v, %v]", ts, before, after)
	}
}

func TestBackupQueueData_InvalidSrcReturnsError(t *testing.T) {
	_, err := backupQueueData("/no/such/directory", t.TempDir(), "/", "q")
	if err == nil {
		t.Error("expected error for non-existent source dir")
	}
}

// ---------------------------------------------------------------------------
// backupWALFiles
// ---------------------------------------------------------------------------

func TestBackupWALFiles_AbsentWALDir_ReturnsZeroNoError(t *testing.T) {
	n, err := backupWALFiles("/no/such/wal/dir", t.TempDir(), "any-uid")
	if err != nil {
		t.Fatalf("expected no error for absent WAL dir, got: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 files copied, got %d", n)
	}
}

func TestBackupWALFiles_SkipsNonWALFiles(t *testing.T) {
	walDir := t.TempDir()
	// Write a non-WAL file alongside a valid fixture WAL.
	os.WriteFile(filepath.Join(walDir, "somefile.txt"), []byte("data"), 0644)

	dest := t.TempDir()
	n, err := backupWALFiles(walDir, dest, fixtureUID2)
	if err != nil {
		t.Fatalf("backupWALFiles: %v", err)
	}
	// .txt file must be ignored; no WAL found → 0 copies.
	if n != 0 {
		t.Errorf("expected 0 copies (no .wal files), got %d", n)
	}
}

func TestBackupWALFiles_CopiesWALWithMatchingUID(t *testing.T) {
	// Use the fixture WAL which contains records for fixtureUID2.
	walDir := t.TempDir()
	if err := copyFile("testdata/0000000000000002.wal", filepath.Join(walDir, "0000000000000002.wal")); err != nil {
		t.Fatalf("setup: %v", err)
	}

	dest := t.TempDir()
	n, err := backupWALFiles(walDir, dest, fixtureUID2)
	if err != nil {
		t.Fatalf("backupWALFiles: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 WAL file copied, got %d", n)
	}
	if _, err := os.Stat(filepath.Join(dest, "0000000000000002.wal")); err != nil {
		t.Errorf("backed-up WAL file not found: %v", err)
	}
}

func TestBackupWALFiles_SkipsWALWithNoMatchingUID(t *testing.T) {
	walDir := t.TempDir()
	if err := copyFile("testdata/0000000000000002.wal", filepath.Join(walDir, "0000000000000002.wal")); err != nil {
		t.Fatalf("setup: %v", err)
	}

	dest := t.TempDir()
	n, err := backupWALFiles(walDir, dest, "nonexistent-uid-xyz")
	if err != nil {
		t.Fatalf("backupWALFiles: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 files for unknown UID, got %d", n)
	}
	// destDir must not have been created (lazy creation).
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		// dest was pre-created by t.TempDir() — check it has no WAL inside.
		entries, _ := os.ReadDir(dest)
		if len(entries) != 0 {
			t.Error("expected empty dest dir when no WAL matched")
		}
	}
}

func TestBackupWALFiles_DestDirCreatedLazily(t *testing.T) {
	walDir := t.TempDir()
	// No WAL files → dest should not be created.
	dest := filepath.Join(t.TempDir(), "wal-backup")

	n, err := backupWALFiles(walDir, dest, "any-uid")
	if err != nil {
		t.Fatalf("backupWALFiles: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 copies, got %d", n)
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Error("dest dir must not be created when no WAL files are copied")
	}
}

// ---------------------------------------------------------------------------
// CarveWALMessages
// ---------------------------------------------------------------------------

func TestCarveWALMessages_AbsentDir_ReturnsNilNoError(t *testing.T) {
	payloads, err := CarveWALMessages("/no/such/wal/backup", "any-uid")
	if err != nil {
		t.Fatalf("expected no error for absent dir, got: %v", err)
	}
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads, got %d", len(payloads))
	}
}

func TestCarveWALMessages_EmptyDir_ReturnsEmpty(t *testing.T) {
	payloads, err := CarveWALMessages(t.TempDir(), "any-uid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads from empty dir, got %d", len(payloads))
	}
}

func TestCarveWALMessages_FixtureUID2_ExtractsPayloads(t *testing.T) {
	// Set up a WAL backup dir containing the fixture WAL.
	walBackupDir := t.TempDir()
	if err := copyFile("testdata/0000000000000002.wal", filepath.Join(walBackupDir, "0000000000000002.wal")); err != nil {
		t.Fatalf("setup: %v", err)
	}

	payloads, err := CarveWALMessages(walBackupDir, fixtureUID2)
	if err != nil {
		t.Fatalf("CarveWALMessages: %v", err)
	}
	if len(payloads) == 0 {
		t.Fatal("expected payloads for fixture UID2, got none")
	}
	t.Logf("CarveWALMessages extracted %d payloads for %q", len(payloads), fixtureUID2)
}

func TestCarveWALMessages_UnknownUID_ReturnsEmpty(t *testing.T) {
	walBackupDir := t.TempDir()
	if err := copyFile("testdata/0000000000000002.wal", filepath.Join(walBackupDir, "0000000000000002.wal")); err != nil {
		t.Fatalf("setup: %v", err)
	}

	payloads, err := CarveWALMessages(walBackupDir, "nonexistent-uid")
	if err != nil {
		t.Fatalf("CarveWALMessages: %v", err)
	}
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads for unknown UID, got %d", len(payloads))
	}
}

func TestCarveWALMessages_SkipsNonWALFiles(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not a wal"), 0644)

	payloads, err := CarveWALMessages(dir, fixtureUID2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads (non-WAL files ignored), got %d", len(payloads))
	}
}
