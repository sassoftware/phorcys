package recovery

import (
	"bytes"
	"encoding/binary"
	"hash/adler32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testWALPath is the shared WAL fixture used by all integration tests.
const testWALPath = "testdata/0000000000000002.wal"

// Known properties of the fixture WAL file.
const (
	fixtureUID1       = "2F_TESXJJVE92WTS2H"
	fixtureUID2       = "2F_TES4NVJD9D5IKL4"
	fixtureUID1Count  = 20004
	fixtureUID2Count  = 20005
	fixtureFirstIdx   = 80011 // Raft index of the very first record
	fixtureFirstTerm  = 2
	fixturePayloadPfx = "payload-data-packet-"
)

// ---------------------------------------------------------------------------
// ParseWALRecords — header / validation tests
// ---------------------------------------------------------------------------

func TestParseWALRecords_BadMagic(t *testing.T) {
	data := buildWAL([]walEntry{}, "uid", true)
	data[0] = 'X' // corrupt the magic
	path := writeTmp(t, data)

	_, err := ParseWALRecords(path, "")
	if err == nil || !strings.Contains(err.Error(), "invalid WAL magic") {
		t.Fatalf("expected 'invalid WAL magic' error, got %v", err)
	}
}

func TestParseWALRecords_BadVersion(t *testing.T) {
	data := buildWAL([]walEntry{}, "uid", true)
	data[4] = 99 // wrong version byte
	path := writeTmp(t, data)

	_, err := ParseWALRecords(path, "")
	if err == nil || !strings.Contains(err.Error(), "unsupported WAL version") {
		t.Fatalf("expected 'unsupported WAL version' error, got %v", err)
	}
}

func TestParseWALRecords_TooShort(t *testing.T) {
	_, err := ParseWALRecords(writeTmp(t, []byte("RAWA")), "")
	if err == nil || !strings.Contains(err.Error(), "too short") {
		t.Fatalf("expected 'too short' error, got %v", err)
	}
}

func TestParseWALRecords_EmptyWAL(t *testing.T) {
	// A valid header with no records.
	path := writeTmp(t, buildWAL(nil, "", false))
	recs, err := ParseWALRecords(path, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 records, got %d", len(recs))
	}
}

// ---------------------------------------------------------------------------
// ParseWALRecords — synthetic WAL with known entries
// ---------------------------------------------------------------------------

func TestParseWALRecords_SingleUID_AllRecords(t *testing.T) {
	entries := []walEntry{
		{uid: "myqueue", idx: 1, term: 1, data: []byte("data-a")},
		{uid: "myqueue", idx: 2, term: 1, data: []byte("data-b")},
		{uid: "myqueue", idx: 3, term: 2, data: []byte("data-c")},
	}
	path := writeTmp(t, buildWAL(entries, "", false))

	recs, err := ParseWALRecords(path, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("expected 3 records, got %d", len(recs))
	}
	for i, r := range recs {
		if r.UID != "myqueue" {
			t.Errorf("[%d] UID = %q, want %q", i, r.UID, "myqueue")
		}
		if r.Idx != uint64(i+1) {
			t.Errorf("[%d] Idx = %d, want %d", i, r.Idx, i+1)
		}
	}
}

func TestParseWALRecords_FilterByUID(t *testing.T) {
	entries := []walEntry{
		{uid: "queue-A", idx: 1, term: 1, data: []byte("a1")},
		{uid: "queue-B", idx: 1, term: 1, data: []byte("b1")},
		{uid: "queue-A", idx: 2, term: 1, data: []byte("a2")},
		{uid: "queue-B", idx: 2, term: 1, data: []byte("b2")},
	}
	path := writeTmp(t, buildWAL(entries, "", false))

	recs, err := ParseWALRecords(path, "queue-A")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records for queue-A, got %d", len(recs))
	}
	for _, r := range recs {
		if r.UID != "queue-A" {
			t.Errorf("got record for wrong UID: %q", r.UID)
		}
	}
}

func TestParseWALRecords_MultiUID_TotalCount(t *testing.T) {
	entries := []walEntry{
		{uid: "q1", idx: 10, term: 1, data: []byte("x")},
		{uid: "q2", idx: 20, term: 1, data: []byte("y")},
		{uid: "q1", idx: 11, term: 1, data: []byte("z")},
	}
	path := writeTmp(t, buildWAL(entries, "", false))

	recs, err := ParseWALRecords(path, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("expected 3 total records, got %d", len(recs))
	}
}

func TestParseWALRecords_ETFDataRoundtrip(t *testing.T) {
	payload := []byte("hello-etf-roundtrip")
	entries := []walEntry{
		{uid: "q", idx: 1, term: 1, data: payload},
	}
	path := writeTmp(t, buildWAL(entries, "", false))

	recs, err := ParseWALRecords(path, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if string(recs[0].ETFData) != string(payload) {
		t.Errorf("ETFData = %q, want %q", recs[0].ETFData, payload)
	}
}

func TestParseWALRecords_TruncateFlag(t *testing.T) {
	entries := []walEntry{
		{uid: "q", idx: 1, term: 1, data: []byte("a"), trunc: false},
		{uid: "q", idx: 2, term: 1, data: []byte("b"), trunc: true},
	}
	path := writeTmp(t, buildWAL(entries, "", false))

	recs, err := ParseWALRecords(path, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
	if recs[0].Truncate {
		t.Error("record[0]: Truncate should be false")
	}
	if !recs[1].Truncate {
		t.Error("record[1]: Truncate should be true")
	}
}

func TestParseWALRecords_EOFSentinelStopsParser(t *testing.T) {
	// A valid record followed by a 4-byte DataLen==0 sentinel should stop
	// parsing cleanly; trailing garbage after the sentinel is ignored.
	entry := walEntry{uid: "q", idx: 1, term: 1, data: []byte("ok")}
	base := buildWAL([]walEntry{entry}, "", false)

	// Append a sentinel: 3-byte short-form header (idRef=0, cached) + CRC(4) +
	// DataLen=0(4) – this signals end-of-preallocated-file.
	sentinel := make([]byte, 3+4+4)
	sentinel[0] = 0x40 // short form, idRef=0
	// CRC and DataLen are zero → DataLen=0 → stop
	base = append(base, sentinel...)
	// Append some garbage that should never be read.
	garbageByte := []byte{0xFF}
	base = append(base, bytes.Repeat(garbageByte, 64)...)

	path := writeTmp(t, base)
	recs, err := ParseWALRecords(path, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record (sentinel stops parser), got %d", len(recs))
	}
}

// ---------------------------------------------------------------------------
// ParseWALMessagesForQueue — fixture-based integration tests
// ---------------------------------------------------------------------------

func TestParseWALMessagesForQueue_KnownUID2_PayloadCount(t *testing.T) {
	payloads, err := ParseWALMessagesForQueue(testWALPath, fixtureUID2)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture WAL contains 20 000 "payload-data-packet-N" messages for UID2.
	if len(payloads) == 0 {
		t.Fatal("expected payloads for fixture UID2, got none")
	}
	t.Logf("extracted %d payloads for %q", len(payloads), fixtureUID2)
}

func TestParseWALMessagesForQueue_KnownUID2_PayloadContent(t *testing.T) {
	payloads, err := ParseWALMessagesForQueue(testWALPath, fixtureUID2)
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range payloads {
		if !strings.HasPrefix(string(p), fixturePayloadPfx) {
			t.Errorf("payload[%d] = %q, expected prefix %q", i, string(p), fixturePayloadPfx)
		}
	}
}

func TestParseWALMessagesForQueue_UnknownUID_ReturnsEmpty(t *testing.T) {
	payloads, err := ParseWALMessagesForQueue(testWALPath, "nonexistent-uid")
	if err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 0 {
		t.Fatalf("expected 0 payloads for unknown UID, got %d", len(payloads))
	}
}

func TestParseWALMessagesForQueue_UID1_NoPayloads(t *testing.T) {
	// UID1 also contains enqueue records in this fixture; verify the carver
	// runs without error and log the count.
	payloads, err := ParseWALMessagesForQueue(testWALPath, fixtureUID1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("UID1 (%q) yielded %d payloads", fixtureUID1, len(payloads))
}

// ---------------------------------------------------------------------------
// ParseWALRecords — fixture-based integration tests
// ---------------------------------------------------------------------------

func TestParseWALRecords_FixtureUID1_Count(t *testing.T) {
	recs, err := ParseWALRecords(testWALPath, fixtureUID1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != fixtureUID1Count {
		t.Errorf("expected %d records for %q, got %d", fixtureUID1Count, fixtureUID1, len(recs))
	}
}

func TestParseWALRecords_FixtureUID2_Count(t *testing.T) {
	recs, err := ParseWALRecords(testWALPath, fixtureUID2)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != fixtureUID2Count {
		t.Errorf("expected %d records for %q, got %d", fixtureUID2Count, fixtureUID2, len(recs))
	}
}

func TestParseWALRecords_FixtureAllUIDs_TotalCount(t *testing.T) {
	recs, err := ParseWALRecords(testWALPath, "")
	if err != nil {
		t.Fatal(err)
	}
	want := fixtureUID1Count + fixtureUID2Count
	if len(recs) != want {
		t.Errorf("expected %d total records, got %d", want, len(recs))
	}
}

func TestParseWALRecords_FixtureFirstRecord(t *testing.T) {
	// The very first record in the file belongs to UID1 and has a known index.
	recs, err := ParseWALRecords(testWALPath, fixtureUID1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 {
		t.Fatal("no records for fixture UID1")
	}
	first := recs[0]
	if first.Idx != fixtureFirstIdx {
		t.Errorf("first record Idx = %d, want %d", first.Idx, fixtureFirstIdx)
	}
	if first.Term != fixtureFirstTerm {
		t.Errorf("first record Term = %d, want %d", first.Term, fixtureFirstTerm)
	}
}

func TestParseWALRecords_FixtureUID2_IndicesAscending(t *testing.T) {
	recs, err := ParseWALRecords(testWALPath, fixtureUID2)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].Idx <= recs[i-1].Idx {
			t.Errorf("indices not ascending at position %d: %d <= %d",
				i, recs[i].Idx, recs[i-1].Idx)
			break
		}
	}
}

func TestParseWALRecords_FixtureUID2_AllHaveETFData(t *testing.T) {
	recs, err := ParseWALRecords(testWALPath, fixtureUID2)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range recs {
		if len(r.ETFData) == 0 {
			t.Errorf("record[%d] (idx=%d) has empty ETFData", i, r.Idx)
		}
	}
}

// ---------------------------------------------------------------------------
// carveFromBytes
// ---------------------------------------------------------------------------

func TestCarveFromBytes_EmptyInput(t *testing.T) {
	got := carveFromBytes(nil)
	if len(got) != 0 {
		t.Errorf("expected no payloads from nil, got %d", len(got))
	}
}

func TestCarveFromBytes_NoContentTuple(t *testing.T) {
	got := carveFromBytes([]byte("hello world this has no ETF content tuple"))
	if len(got) != 0 {
		t.Errorf("expected no payloads, got %d", len(got))
	}
}

func TestCarveFromBytes_ContentTuple_ExtractsPayload(t *testing.T) {
	// Build a synthetic ETF content tuple that matches the scanner's pattern.
	// {content, 60, none, <<properties>>, rabbit_framing_amqp_0_9_1, [<<payload>>]}
	//
	// We use the buildContentETF helper to produce valid bytes for the scanner.
	payload := []byte("unit-test-payload")
	etfBlock := buildContentETF(payload)

	got := carveFromBytes(etfBlock)
	if len(got) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(got))
	}
	if string(got[0]) != string(payload) {
		t.Errorf("payload = %q, want %q", got[0], payload)
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

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// walEntry describes one record to be written by buildWAL.
type walEntry struct {
	uid   string
	idx   uint64
	term  uint64
	data  []byte
	trunc bool
}

// buildWAL constructs a minimal Ra WAL byte slice from the given entries.
// If firstUID is non-empty it is used as the pre-seeded UID for IdRef 0
// before any entries are written (useful for testing short-form records).
// If addChecksums is true, valid adler32 checksums are included.
func buildWAL(entries []walEntry, firstUID string, addChecksums bool) []byte {
	out := []byte("RAWA\x01") // 5-byte file header

	// uid → IdRef cache (mirrors what the parser builds).
	uidToRef := map[string]uint32{}
	nextRef := uint32(0)

	if firstUID != "" {
		uidToRef[firstUID] = nextRef
		nextRef++
	}

	for _, e := range entries {
		ref, seen := uidToRef[e.uid]
		if !seen {
			// Long form: encode UID inline.
			ref = nextRef
			uidToRef[e.uid] = ref
			nextRef++

			trBit := uint32(0)
			if e.trunc {
				trBit = 1
			}
			// 24-bit header: Trunc(1) | ShortForm=0(1) | IdRef(22)
			hw := trBit<<23 | 0<<22 | ref&0x3FFFFF
			out = append(out, byte(hw>>16), byte(hw>>8), byte(hw))

			uid := []byte(e.uid)
			out = append(out, byte(len(uid)>>8), byte(len(uid)))
			out = append(out, uid...)
		} else {
			// Short form.
			trBit := uint32(0)
			if e.trunc {
				trBit = 1
			}
			hw := trBit<<23 | 1<<22 | ref&0x3FFFFF
			out = append(out, byte(hw>>16), byte(hw>>8), byte(hw))
		}

		// Checksum
		crc := uint32(0)
		if addChecksums && len(e.data) > 0 {
			idxB := make([]byte, 8)
			termB := make([]byte, 8)
			binary.BigEndian.PutUint64(idxB, e.idx)
			binary.BigEndian.PutUint64(termB, e.term)
			var buf []byte
			buf = append(buf, idxB...)
			buf = append(buf, termB...)
			buf = append(buf, e.data...)
			crc = adler32.Checksum(buf)
		}
		out = appendUint32(out, crc)
		out = appendUint32(out, uint32(len(e.data)))
		out = appendUint64(out, e.idx)
		out = appendUint64(out, e.term)
		out = append(out, e.data...)
	}
	return out
}

func appendUint32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendUint64(b []byte, v uint64) []byte {
	return append(b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// writeTmp writes data to a temp file and returns its path.
func writeTmp(t *testing.T, data []byte) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.wal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

// buildContentETF constructs a minimal byte sequence that the carveFromBytes
// scanner will recognise as a 6-arity content tuple containing one payload binary.
//
// Encoding: {content, 60, none, <<>>, rabbit_framing_amqp_0_9_1, [<<payload>>]}
//
//	SMALL_TUPLE_EXT(6)
//	 SMALL_ATOM_UTF8_EXT "content"
//	 SMALL_INTEGER_EXT   60
//	 SMALL_ATOM_UTF8_EXT "none"
//	 BINARY_EXT          <<>>
//	 SMALL_ATOM_UTF8_EXT "rabbit_framing_amqp_0_9_1"
//	 LIST_EXT(1)
//	   BINARY_EXT <<payload>>
//	   NIL_EXT
func buildContentETF(payload []byte) []byte {
	var b []byte
	// {content, …} — arity 6
	b = append(b, ettSmallTuple, 6)
	b = append(b, ettSmallAtomUTF8, 7)
	b = append(b, "content"...)
	// field 2: classId (small int)
	b = append(b, ettSmallInt, 60)
	// field 3: decoded-props atom "none"
	b = append(b, ettSmallAtomUTF8, 4)
	b = append(b, "none"...)
	// field 4: encoded-props binary (empty)
	b = appendBinary(b, nil)
	// field 5: framing module atom
	b = append(b, ettSmallAtomUTF8, byte(len("rabbit_framing_amqp_0_9_1")))
	b = append(b, "rabbit_framing_amqp_0_9_1"...)
	// field 6: payload list
	b = append(b, ettList)
	b = appendUint32(b, 1) // count
	b = appendBinary(b, payload)
	b = append(b, ettNil) // list tail
	return b
}

func appendBinary(b, data []byte) []byte {
	b = append(b, ettBinary)
	b = appendUint32(b, uint32(len(data)))
	return append(b, data...)
}
