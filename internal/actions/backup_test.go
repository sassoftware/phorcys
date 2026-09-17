package actions

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sassoftware/argus/internal/util"
)

// ---------------------------------------------------------------------------
// BackupQueueData
// ---------------------------------------------------------------------------

func TestBackupQueueData_CopiesSegmentAndWalFiles(t *testing.T) {
	src := t.TempDir()
	base := t.TempDir()

	os.WriteFile(filepath.Join(src, "00000001.segment"), []byte("seg"), 0644)
	os.WriteFile(filepath.Join(src, "00000001.wal"), []byte("wal"), 0644)
	// Subdirectories must be skipped.
	os.MkdirAll(filepath.Join(src, "inner"), 0755)

	destDir, err := BackupQueueData(src, base, "/", "events.queue")
	if err != nil {
		t.Fatalf("BackupQueueData: %v", err)
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

	destDir, err := BackupQueueData(src, base, "/", "my.queue")
	if err != nil {
		t.Fatalf("BackupQueueData: %v", err)
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

	destDir, err := BackupQueueData(src, base, "production", "order.queue")
	if err != nil {
		t.Fatalf("BackupQueueData: %v", err)
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
	destDir, err := BackupQueueData(src, base, "vh", "q")
	after := time.Now().UTC()

	if err != nil {
		t.Fatalf("BackupQueueData: %v", err)
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
	_, err := BackupQueueData("/no/such/directory", t.TempDir(), "/", "q")
	if err == nil {
		t.Error("expected error for non-existent source dir")
	}
}

// ---------------------------------------------------------------------------
// backupWALFiles
// ---------------------------------------------------------------------------

func TestBackupWALFiles_AbsentWALDir_ReturnsZeroNoError(t *testing.T) {
	n, err := BackupWALFiles("/no/such/wal/dir", t.TempDir(), "any-uid")
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
	n, err := BackupWALFiles(walDir, dest, fixtureUID2)
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
	if err := util.CopyFile("testdata/0000000000000002.wal", filepath.Join(walDir, "0000000000000002.wal")); err != nil {
		t.Fatalf("setup: %v", err)
	}

	dest := t.TempDir()
	n, err := BackupWALFiles(walDir, dest, fixtureUID2)
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
	if err := util.CopyFile("testdata/0000000000000002.wal", filepath.Join(walDir, "0000000000000002.wal")); err != nil {
		t.Fatalf("setup: %v", err)
	}

	dest := t.TempDir()
	n, err := BackupWALFiles(walDir, dest, "nonexistent-uid-xyz")
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

	n, err := BackupWALFiles(walDir, dest, "any-uid")
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
