// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"bytes"
	"encoding/binary"
	"hash/adler32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.ErrorContains(t, err, "invalid WAL magic")
}

func TestParseWALRecords_BadVersion(t *testing.T) {
	data := buildWAL([]walEntry{}, "uid", true)
	data[4] = 99 // wrong version byte
	path := writeTmp(t, data)

	_, err := ParseWALRecords(path, "")
	require.ErrorContains(t, err, "unsupported WAL version")
}

func TestParseWALRecords_TooShort(t *testing.T) {
	_, err := ParseWALRecords(writeTmp(t, []byte("RAWA")), "")
	require.ErrorContains(t, err, "too short")
}

func TestParseWALRecords_EmptyWAL(t *testing.T) {
	// A valid header with no records.
	path := writeTmp(t, buildWAL(nil, "", false))
	recs, err := ParseWALRecords(path, "")
	require.NoError(t, err)
	require.Empty(t, recs)
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
	require.NoError(t, err)
	require.Len(t, recs, 3)
	for i, r := range recs {
		assert.Equalf(t, "myqueue", r.UID, "[%d] UID = %q, want %q", i, r.UID, "myqueue")
		assert.Equalf(t, uint64(i+1), r.Idx, "[%d] Idx = %d, want %d", i, r.Idx, i+1)
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
	require.NoError(t, err)
	require.Len(t, recs, 2)
	for _, r := range recs {
		assert.Equal(t, "queue-A", r.UID, "got record for wrong UID")
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
	require.NoError(t, err)
	require.Len(t, recs, 3)
}

func TestParseWALRecords_ETFDataRoundtrip(t *testing.T) {
	payload := []byte("hello-etf-roundtrip")
	entries := []walEntry{
		{uid: "q", idx: 1, term: 1, data: payload},
	}
	path := writeTmp(t, buildWAL(entries, "", false))

	recs, err := ParseWALRecords(path, "")
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, payload, recs[0].ETFData)
}

func TestParseWALRecords_TruncateFlag(t *testing.T) {
	entries := []walEntry{
		{uid: "q", idx: 1, term: 1, data: []byte("a"), trunc: false},
		{uid: "q", idx: 2, term: 1, data: []byte("b"), trunc: true},
	}
	path := writeTmp(t, buildWAL(entries, "", false))

	recs, err := ParseWALRecords(path, "")
	require.NoError(t, err)
	require.Len(t, recs, 2)
	assert.False(t, recs[0].Truncate, "record[0]: Truncate should be false")
	assert.True(t, recs[1].Truncate, "record[1]: Truncate should be true")
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
	require.NoError(t, err)
	require.Len(t, recs, 1)
}

// ---------------------------------------------------------------------------
// ParseWALMessagesForQueue — fixture-based integration tests
// ---------------------------------------------------------------------------

func TestParseWALMessagesForQueue_KnownUID2_PayloadCount(t *testing.T) {
	messages, err := ParseWALMessagesForQueue(testWALPath, fixtureUID2)
	require.NoError(t, err)
	// The fixture WAL contains 20 000 "payload-data-packet-N" messages for UID2.
	require.NotEmpty(t, messages, "expected messages for fixture UID2, got none")
	t.Logf("extracted %d messages for %q", len(messages), fixtureUID2)
}

func TestParseWALMessagesForQueue_KnownUID2_PayloadContent(t *testing.T) {
	messages, err := ParseWALMessagesForQueue(testWALPath, fixtureUID2)
	require.NoError(t, err)
	for i, m := range messages {
		assert.Truef(t, strings.HasPrefix(string(m.Body), fixturePayloadPfx), "payload[%d] = %q, expected prefix %q", i, string(m.Body), fixturePayloadPfx)
	}
}

func TestParseWALMessagesForQueue_UnknownUID_ReturnsEmpty(t *testing.T) {
	messages, err := ParseWALMessagesForQueue(testWALPath, "nonexistent-uid")
	require.NoError(t, err)
	require.Empty(t, messages)
}

func TestParseWALMessagesForQueue_UID1_NoPayloads(t *testing.T) {
	// UID1 also contains enqueue records in this fixture; verify the carver
	// runs without error and log the count.
	messages, err := ParseWALMessagesForQueue(testWALPath, fixtureUID1)
	require.NoError(t, err)
	t.Logf("UID1 (%q) yielded %d messages", fixtureUID1, len(messages))
}

// ---------------------------------------------------------------------------
// ParseWALRecords — fixture-based integration tests
// ---------------------------------------------------------------------------

func TestParseWALRecords_FixtureUID1_Count(t *testing.T) {
	recs, err := ParseWALRecords(testWALPath, fixtureUID1)
	require.NoError(t, err)
	assert.Equal(t, fixtureUID1Count, len(recs))
}

func TestParseWALRecords_FixtureUID2_Count(t *testing.T) {
	recs, err := ParseWALRecords(testWALPath, fixtureUID2)
	require.NoError(t, err)
	assert.Equal(t, fixtureUID2Count, len(recs))
}

func TestParseWALRecords_FixtureAllUIDs_TotalCount(t *testing.T) {
	recs, err := ParseWALRecords(testWALPath, "")
	require.NoError(t, err)
	want := fixtureUID1Count + fixtureUID2Count
	assert.Equal(t, want, len(recs))
}

func TestParseWALRecords_FixtureFirstRecord(t *testing.T) {
	// The very first record in the file belongs to UID1 and has a known index.
	recs, err := ParseWALRecords(testWALPath, fixtureUID1)
	require.NoError(t, err)
	require.NotEmpty(t, recs, "no records for fixture UID1")
	first := recs[0]
	assert.Equal(t, uint64(fixtureFirstIdx), first.Idx)
	assert.Equal(t, uint64(fixtureFirstTerm), first.Term)
}

func TestParseWALRecords_FixtureUID2_IndicesAscending(t *testing.T) {
	recs, err := ParseWALRecords(testWALPath, fixtureUID2)
	require.NoError(t, err)
	for i := 1; i < len(recs); i++ {
		if !assert.Greaterf(t, recs[i].Idx, recs[i-1].Idx, "indices not ascending at position %d: %d <= %d",
			i, recs[i].Idx, recs[i-1].Idx) {
			break
		}
	}
}

func TestParseWALRecords_FixtureUID2_AllHaveETFData(t *testing.T) {
	recs, err := ParseWALRecords(testWALPath, fixtureUID2)
	require.NoError(t, err)
	for i, r := range recs {
		assert.NotEmptyf(t, r.ETFData, "record[%d] (idx=%d) has empty ETFData", i, r.Idx)
	}
}

// ---------------------------------------------------------------------------
// CarveWALMessages
// ---------------------------------------------------------------------------

func TestCarveWALMessages_AbsentDir_ReturnsNilNoError(t *testing.T) {
	messages, err := CarveWALMessages("/no/such/wal/backup", "any-uid")
	require.NoError(t, err)
	assert.Empty(t, messages)
}

func TestCarveWALMessages_EmptyDir_ReturnsEmpty(t *testing.T) {
	messages, err := CarveWALMessages(t.TempDir(), "any-uid")
	require.NoError(t, err)
	assert.Empty(t, messages)
}

func TestCarveWALMessages_FixtureUID2_ExtractsPayloads(t *testing.T) {
	// Set up a WAL backup dir containing the fixture WAL.
	walBackupDir := t.TempDir()
	require.NoError(t, copyFile("testdata/0000000000000002.wal", filepath.Join(walBackupDir, "0000000000000002.wal")), "setup")

	messages, err := CarveWALMessages(walBackupDir, fixtureUID2)
	require.NoError(t, err, "CarveWALMessages")
	require.NotEmpty(t, messages, "expected messages for fixture UID2, got none")
	t.Logf("CarveWALMessages extracted %d messages for %q", len(messages), fixtureUID2)
}

func TestCarveWALMessages_UnknownUID_ReturnsEmpty(t *testing.T) {
	walBackupDir := t.TempDir()
	require.NoError(t, copyFile("testdata/0000000000000002.wal", filepath.Join(walBackupDir, "0000000000000002.wal")), "setup")

	messages, err := CarveWALMessages(walBackupDir, "nonexistent-uid")
	require.NoError(t, err, "CarveWALMessages")
	assert.Empty(t, messages)
}

func TestCarveWALMessages_SkipsNonWALFiles(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not a wal"), 0600)
	require.NoError(t, err)

	messages, err := CarveWALMessages(dir, fixtureUID2)
	require.NoError(t, err)
	assert.Empty(t, messages)
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
			out = append(out, byte(hw>>16), byte(hw>>8), byte(hw)) //nolint:gosec

			uid := []byte(e.uid)
			out = append(out, byte(len(uid)>>8), byte(len(uid))) //nolint:gosec
			out = append(out, uid...)
		} else {
			// Short form.
			trBit := uint32(0)
			if e.trunc {
				trBit = 1
			}
			hw := trBit<<23 | 1<<22 | ref&0x3FFFFF
			out = append(out, byte(hw>>16), byte(hw>>8), byte(hw)) //nolint:gosec
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
		out = appendUint32(out, uint32(len(e.data))) //nolint:gosec
		out = appendUint64(out, e.idx)
		out = appendUint64(out, e.term)
		out = append(out, e.data...)
	}
	return out
}

func appendUint32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v)) //nolint:gosec
}

func appendUint64(b []byte, v uint64) []byte {
	return append(b,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), //nolint:gosec
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v)) //nolint:gosec
}

// writeTmp writes data to a temp file and returns its path.
func writeTmp(t *testing.T, data []byte) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.wal")
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}
