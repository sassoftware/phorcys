package actions

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/DeedleFake/etf"
)

// writeConfigFile writes a minimal ra-style 'config' text file for the given vhost and queue name.
func writeConfigFile(t *testing.T, base, dirName, vhost, queueName string) {
	t.Helper()
	dir := filepath.Join(base, dirName)
	os.MkdirAll(dir, 0755)
	content := fmt.Sprintf(
		`#{id => {'something',rabbit@node}, machine => {module,rabbit_fifo, #{queue_resource => {resource,<<"%s">>,queue,<<"%s">>}}}}`,
		vhost, queueName,
	)
	os.WriteFile(filepath.Join(dir, "config"), []byte(content), 0644)
}

// ---------------------------------------------------------------------------
// convertToString
// ---------------------------------------------------------------------------

func TestConvertToString_ByteSlice(t *testing.T) {
	s, ok := convertToString([]byte("hello"))
	if !ok || s != "hello" {
		t.Errorf("got (%q, %v), want (\"hello\", true)", s, ok)
	}
}

func TestConvertToString_String(t *testing.T) {
	s, ok := convertToString("world")
	if !ok || s != "world" {
		t.Errorf("got (%q, %v), want (\"world\", true)", s, ok)
	}
}

func TestConvertToString_Atom(t *testing.T) {
	s, ok := convertToString(etf.Atom("my_atom"))
	if !ok || s != "my_atom" {
		t.Errorf("got (%q, %v), want (\"my_atom\", true)", s, ok)
	}
}

func TestConvertToString_UnsupportedTypeReturnsFalse(t *testing.T) {
	for _, v := range []any{42, 3.14, nil, true, struct{}{}} {
		_, ok := convertToString(v)
		if ok {
			t.Errorf("convertToString(%T) should return false", v)
		}
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
	if !found {
		t.Fatal("expected found=true")
	}
	if vhost != "myvhost" {
		t.Errorf("vhost = %q, want %q", vhost, "myvhost")
	}
	if name != "myqueue" {
		t.Errorf("name = %q, want %q", name, "myqueue")
	}
}

func TestExtractQueueMetadata_DefaultVhost(t *testing.T) {
	config := []byte(`#{queue_resource => {resource,<<"/">>,queue,<<"test-target-quorum-queue">>}}`)
	vhost, name, found := extractQueueMetadata(config)
	if !found {
		t.Fatal("expected found=true")
	}
	if vhost != "/" || name != "test-target-quorum-queue" {
		t.Errorf("got vhost=%q name=%q", vhost, name)
	}
}

func TestExtractQueueMetadata_NoMatch(t *testing.T) {
	_, _, found := extractQueueMetadata([]byte(`#{id => something_else}`))
	if found {
		t.Error("expected found=false for config with no resource tuple")
	}
}

func TestExtractQueueMetadata_EmptyBytes(t *testing.T) {
	_, _, found := extractQueueMetadata([]byte{})
	if found {
		t.Error("expected found=false for empty input")
	}
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
	os.MkdirAll(filepath.Join(base, "nometa"), 0755)
	// A regular file (not a directory) should also be skipped.
	os.WriteFile(filepath.Join(base, "notadir"), []byte("x"), 0644)

	locs, err := ScanAllQuorumDirectories(base)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(locs) != 3 {
		t.Fatalf("expected 3 locations, got %d: %+v", len(locs), locs)
	}
}

func TestScanAllQuorumDirectories_EmptyBaseDir(t *testing.T) {
	locs, err := ScanAllQuorumDirectories(t.TempDir())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(locs) != 0 {
		t.Errorf("expected 0 locations for empty dir, got %d", len(locs))
	}
}

func TestScanAllQuorumDirectories_InvalidBaseDirReturnsError(t *testing.T) {
	_, err := ScanAllQuorumDirectories("/no/such/directory")
	if err == nil {
		t.Error("expected error for non-existent base path")
	}
}

func TestScanAllQuorumDirectories_BadConfigSkipped(t *testing.T) {
	base := t.TempDir()

	// Valid queue
	writeConfigFile(t, base, "good", "/", "valid.q")
	// Directory with a config file that has no resource tuple
	badDir := filepath.Join(base, "bad")
	os.MkdirAll(badDir, 0755)
	os.WriteFile(filepath.Join(badDir, "config"), []byte("#{id => something_else}"), 0644)

	locs, err := ScanAllQuorumDirectories(base)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(locs) != 1 {
		t.Errorf("expected 1 valid location, got %d", len(locs))
	}
}

func TestScanAllQuorumDirectories_PathIsCorrect(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "abc123", "prod", "orders")

	locs, _ := ScanAllQuorumDirectories(base)
	if len(locs) != 1 {
		t.Fatalf("expected 1 location")
	}
	expected := filepath.Join(base, "abc123")
	if locs[0].Path != expected {
		t.Errorf("Path = %q, want %q", locs[0].Path, expected)
	}
}

// ---------------------------------------------------------------------------
// FindQueueDirectory
// ---------------------------------------------------------------------------

func TestFindQueueDirectory_Found(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "somehash", "testvhost", "target.queue")

	path, err := FindQueueDirectory(base, "testvhost", "target.queue")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := filepath.Join(base, "somehash")
	if path != expected {
		t.Errorf("path = %q, want %q", path, expected)
	}
}

func TestFindQueueDirectory_NotFound(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "somehash", "testvhost", "other.queue")

	_, err := FindQueueDirectory(base, "testvhost", "missing.queue")
	if err == nil {
		t.Error("expected error for missing queue")
	}
}

func TestFindQueueDirectory_WrongVhost(t *testing.T) {
	base := t.TempDir()
	writeConfigFile(t, base, "h1", "vhostA", "q1")

	_, err := FindQueueDirectory(base, "vhostB", "q1")
	if err == nil {
		t.Error("expected error for wrong vhost")
	}
}

func TestFindQueueDirectory_InvalidBasePath(t *testing.T) {
	_, err := FindQueueDirectory("/no/such/path", "/", "q")
	if err == nil {
		t.Error("expected error for invalid base path")
	}
}
