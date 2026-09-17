package actions

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/DeedleFake/etf"
	"github.com/sassoftware/argus/internal/testware"
)

// ---------------------------------------------------------------------------
// extractRawBinaries
// ---------------------------------------------------------------------------

func TestExtractRawBinaries_FlatTuple(t *testing.T) {
	term := etf.Tuple{
		etf.Atom("header"),
		[]byte("payload one"),
		[]byte("payload two"),
	}
	bins := extractRawBinaries(term)
	if len(bins) != 2 {
		t.Fatalf("expected 2 binaries, got %d", len(bins))
	}
	if string(bins[0]) != "payload one" {
		t.Errorf("bins[0] = %q", bins[0])
	}
	if string(bins[1]) != "payload two" {
		t.Errorf("bins[1] = %q", bins[1])
	}
}

func TestExtractRawBinaries_NestedList(t *testing.T) {
	term := etf.Tuple{
		etf.Atom("outer"),
		etf.List{
			[]byte("nested"),
		},
	}
	bins := extractRawBinaries(term)
	if len(bins) != 1 || string(bins[0]) != "nested" {
		t.Errorf("got %d bins, expected 1 with value 'nested'", len(bins))
	}
}

func TestExtractRawBinaries_EmptyBinaryIgnored(t *testing.T) {
	term := etf.Tuple{[]byte{}, []byte("ok")}
	bins := extractRawBinaries(term)
	if len(bins) != 1 || string(bins[0]) != "ok" {
		t.Errorf("empty binary should be ignored; got %d bins", len(bins))
	}
}

func TestExtractRawBinaries_CopiesData(t *testing.T) {
	original := []byte("original content")
	term := etf.Tuple{original}
	bins := extractRawBinaries(term)
	if len(bins) != 1 {
		t.Fatalf("expected 1 binary")
	}
	// Mutate the result; original slice must be unchanged.
	bins[0][0] = 'X'
	if original[0] == 'X' {
		t.Error("extractRawBinaries should copy data, not return the original slice")
	}
}

func TestExtractRawBinaries_NonBinaryTermsIgnored(t *testing.T) {
	term := etf.Tuple{etf.Atom("just"), etf.Atom("atoms"), int64(42)}
	bins := extractRawBinaries(term)
	if len(bins) != 0 {
		t.Errorf("expected 0 binaries for non-binary terms, got %d", len(bins))
	}
}

// ---------------------------------------------------------------------------
// findBasicMessagePayloads
// ---------------------------------------------------------------------------

func TestFindBasicMessagePayloads_ValidMessage(t *testing.T) {
	msg := testware.MakeBasicMessageTerm([]byte("the body"))
	payloads := findBasicMessagePayloads(msg)
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
	if string(payloads[0]) != "the body" {
		t.Errorf("payload = %q, want %q", payloads[0], "the body")
	}
}

func TestFindBasicMessagePayloads_NestedInList(t *testing.T) {
	msg := testware.MakeBasicMessageTerm([]byte("nested body"))
	outer := etf.List{etf.Atom("irrelevant"), msg}
	payloads := findBasicMessagePayloads(outer)
	if len(payloads) == 0 {
		t.Fatal("expected at least 1 payload from nested list")
	}
}

func TestFindBasicMessagePayloads_NestedInTuple(t *testing.T) {
	msg := testware.MakeBasicMessageTerm([]byte("deep body"))
	outer := etf.Tuple{etf.Atom("raft_entry"), int64(1), msg}
	payloads := findBasicMessagePayloads(outer)
	if len(payloads) == 0 {
		t.Fatal("expected at least 1 payload from nested tuple")
	}
}

func TestFindBasicMessagePayloads_WrongAtomIgnored(t *testing.T) {
	term := etf.Tuple{
		etf.Atom("not_basic_message"),
		etf.List{etf.Atom("rk")},
		etf.Atom("exchange"),
		etf.Tuple{etf.Atom("content"), []byte("should not appear")},
	}
	payloads := findBasicMessagePayloads(term)
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads for wrong atom, got %d", len(payloads))
	}
}

func TestFindBasicMessagePayloads_TupleTooShort(t *testing.T) {
	// A tuple with atom basic_message but fewer than 4 elements.
	term := etf.Tuple{etf.Atom("basic_message"), etf.Atom("rk")}
	payloads := findBasicMessagePayloads(term)
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads for short tuple, got %d", len(payloads))
	}
}

func TestFindBasicMessagePayloads_NonTupleTermIgnored(t *testing.T) {
	payloads := findBasicMessagePayloads(etf.Atom("nothing"))
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads for bare atom, got %d", len(payloads))
	}
}

// ---------------------------------------------------------------------------
// CarveMessagesFromFile
// ---------------------------------------------------------------------------

// writeSegmentFile creates a temp file with the given extension containing the
// ETF-encoded message wrapped in some surrounding junk bytes.
func writeSegmentFile(t *testing.T, dir, ext string, msg any) string {
	t.Helper()
	encoded, err := testware.EncodeETFTerm(msg)
	if err != nil {
		t.Fatalf("encode ETF: %v", err)
	}
	f, err := os.CreateTemp(dir, "*"+ext)
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer f.Close()
	f.Write([]byte{0x00, 0x01, 0x02}) // junk prefix
	f.Write(encoded)
	f.Write([]byte{0xFF, 0xFE}) // junk suffix
	return f.Name()
}

func TestCarveMessagesFromFile_ExtractsPayload(t *testing.T) {
	dir := t.TempDir()
	path := writeSegmentFile(t, dir, ".segment", testware.MakeBasicMessageTerm([]byte("file payload")))

	payloads, err := CarveMessagesFromFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) == 0 {
		t.Fatal("expected at least 1 payload")
	}
	found := false
	for _, p := range payloads {
		if string(p) == "file payload" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'file payload' in results; got: %q", payloads)
	}
}

func TestCarveMessagesFromFile_MissingFileReturnsError(t *testing.T) {
	_, err := CarveMessagesFromFile("/no/such/file.segment")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestCarveMessagesFromFile_AllJunkReturnsEmpty(t *testing.T) {
	f, _ := os.CreateTemp(t.TempDir(), "*.segment")
	f.Write([]byte{0x01, 0x02, 0x03, 0x04, 0x05})
	f.Close()

	payloads, err := CarveMessagesFromFile(f.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads from junk data, got %d", len(payloads))
	}
}

func TestCarveMessagesFromFile_MultipleMessages(t *testing.T) {
	dir := t.TempDir()
	msg1, _ := testware.EncodeETFTerm(testware.MakeBasicMessageTerm([]byte("first-message")))
	msg2, _ := testware.EncodeETFTerm(testware.MakeBasicMessageTerm([]byte("second-message")))

	f, _ := os.CreateTemp(dir, "*.wal")
	// Write messages back-to-back; no junk bytes between them so the scanner
	// finds msg2's magic byte immediately after consuming msg1.
	f.Write(msg1)
	f.Write(msg2)
	f.Close()

	payloads, err := CarveMessagesFromFile(f.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var foundFirst, foundSecond bool
	for _, p := range payloads {
		switch string(p) {
		case "first-message":
			foundFirst = true
		case "second-message":
			foundSecond = true
		}
	}
	if !foundFirst || !foundSecond {
		t.Errorf("expected both 'first-message' and 'second-message' payloads; got %d payloads: %q",
			len(payloads), payloads)
	}
}

// ---------------------------------------------------------------------------
// CarveMessagesFromDir
// ---------------------------------------------------------------------------

func TestCarveMessagesFromDir_ProcessesSegmentAndWal(t *testing.T) {
	dir := t.TempDir()
	msg, _ := testware.EncodeETFTerm(testware.MakeBasicMessageTerm([]byte("dir msg")))

	os.WriteFile(filepath.Join(dir, "0000000000000001.segment"), msg, 0644)
	os.WriteFile(filepath.Join(dir, "00000001.wal"), msg, 0644)
	// Files with other extensions must be ignored.
	os.WriteFile(filepath.Join(dir, "skip.tmp"), msg, 0644)
	os.WriteFile(filepath.Join(dir, "skip.log"), msg, 0644)

	payloads, err := CarveMessagesFromDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Each of the 2 valid files contributes at least 1 payload.
	if len(payloads) < 2 {
		t.Errorf("expected >= 2 payloads, got %d", len(payloads))
	}
}

func TestCarveMessagesFromDir_EmptyDirReturnsEmpty(t *testing.T) {
	payloads, err := CarveMessagesFromDir(t.TempDir())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payloads) != 0 {
		t.Errorf("expected 0 payloads for empty dir, got %d", len(payloads))
	}
}

func TestCarveMessagesFromDir_InvalidDirReturnsError(t *testing.T) {
	_, err := CarveMessagesFromDir("/no/such/dir")
	if err == nil {
		t.Error("expected error for non-existent directory")
	}
}

func TestCarveMessagesFromDir_SkipsBadFilesGracefully(t *testing.T) {
	dir := t.TempDir()

	// A good segment file.
	good, _ := testware.EncodeETFTerm(testware.MakeBasicMessageTerm([]byte("good")))
	os.WriteFile(filepath.Join(dir, "good.segment"), good, 0644)
	// A corrupt segment file (no valid ETF).
	os.WriteFile(filepath.Join(dir, "bad.segment"), []byte{0x01, 0x02}, 0644)

	payloads, err := CarveMessagesFromDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// We should still get payloads from the good file even though bad.segment had no messages.
	found := false
	for _, p := range payloads {
		if string(p) == "good" {
			found = true
		}
	}
	if !found {
		t.Error("expected payload 'good' to be recovered despite corrupt sibling")
	}
}
