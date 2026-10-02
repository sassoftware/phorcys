// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// ParseSegmentRecords — header / validation tests
// ---------------------------------------------------------------------------

func TestParseSegmentRecords_BadMagic(t *testing.T) {
	data := buildSegmentV2(4, []segmentEntry{})
	data[0] = 'X' // corrupt the magic
	path := writeTmp(t, data)

	_, err := ParseSegmentRecords(path)
	require.ErrorContains(t, err, "invalid segment magic")
}

func TestParseSegmentRecords_UnsupportedVersion(t *testing.T) {
	data := buildSegmentV2(4, []segmentEntry{})
	binary.BigEndian.PutUint16(data[4:6], 99) // unsupported version
	path := writeTmp(t, data)

	_, err := ParseSegmentRecords(path)
	require.ErrorContains(t, err, "unsupported segment version")
}

func TestParseSegmentRecords_TooShort(t *testing.T) {
	_, err := ParseSegmentRecords(writeTmp(t, []byte("RASG")))
	require.ErrorContains(t, err, "too short")
}

func TestParseSegmentRecords_EmptySegment_NoRecords(t *testing.T) {
	data := buildSegmentV2(4, []segmentEntry{})
	path := writeTmp(t, data)

	recs, err := ParseSegmentRecords(path)
	require.NoError(t, err)
	assert.Empty(t, recs)
}

// ---------------------------------------------------------------------------
// ParseSegmentRecords — synthetic segment with known entries
// ---------------------------------------------------------------------------

func TestParseSegmentRecords_DecodesIdxTermAndData(t *testing.T) {
	entries := []segmentEntry{
		{idx: 80011, term: 2, data: []byte("payload-data-packet-1")},
		{idx: 80012, term: 2, data: []byte("payload-data-packet-2")},
		{idx: 80013, term: 3, data: []byte("payload-data-packet-3")},
	}
	data := buildSegmentV2(4, entries)
	path := writeTmp(t, data)

	recs, err := ParseSegmentRecords(path)
	require.NoError(t, err)
	require.Len(t, recs, len(entries))

	for i, want := range entries {
		got := recs[i]
		assert.Equalf(t, want.idx, got.Idx, "record[%d].Idx", i)
		assert.Equalf(t, want.term, got.Term, "record[%d].Term", i)
		assert.Equalf(t, want.data, got.ETFData, "record[%d].ETFData", i)
	}
}

func TestParseSegmentRecords_StopsAtAllZeroIndexRecord(t *testing.T) {
	entries := []segmentEntry{
		{idx: 1, term: 1, data: []byte("a")},
	}
	// maxCount larger than the number of written entries leaves trailing
	// all-zero index slots, which must stop the parser.
	data := buildSegmentV2(4, entries)
	path := writeTmp(t, data)

	recs, err := ParseSegmentRecords(path)
	require.NoError(t, err)
	require.Len(t, recs, 1)
}

func TestParseSegmentRecords_TruncatedIndexTableStopsParser(t *testing.T) {
	entries := []segmentEntry{
		{idx: 1, term: 1, data: []byte("a")},
		{idx: 2, term: 1, data: []byte("b")},
	}
	data := buildSegmentV2(4, entries)
	// Truncate mid-way through the index table — since data always lives
	// past the *full* index table, no record's data is reachable here.
	data = data[:segmentHeaderSize+segmentIndexRecordSizeV2+4]
	path := writeTmp(t, data)

	recs, err := ParseSegmentRecords(path)
	require.NoError(t, err)
	assert.Empty(t, recs)
}

func TestParseSegmentRecords_BadChecksumStillReturnsRecord(t *testing.T) {
	entries := []segmentEntry{
		{idx: 1, term: 1, data: []byte("a")},
	}
	data := buildSegmentV2(4, entries)
	// Corrupt the checksum field of the single index record.
	crcOffset := segmentHeaderSize + segmentIndexRecordSizeV2 - 4
	binary.BigEndian.PutUint32(data[crcOffset:], 0xDEADBEEF)
	path := writeTmp(t, data)

	recs, err := ParseSegmentRecords(path)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, entries[0].data, recs[0].ETFData)
}

func TestParseSegmentRecords_DataBeyondFileLengthStopsParser(t *testing.T) {
	entries := []segmentEntry{
		{idx: 1, term: 1, data: []byte("a")},
		{idx: 2, term: 1, data: []byte("b")},
	}
	data := buildSegmentV2(4, entries)
	// Truncate the file so the second record's data is missing.
	dataStart := segmentHeaderSize + 4*segmentIndexRecordSizeV2
	data = data[:dataStart+1]
	path := writeTmp(t, data)

	recs, err := ParseSegmentRecords(path)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, uint64(1), recs[0].Idx)
}

func TestParseSegmentRecords_V1Format(t *testing.T) {
	entries := []segmentEntry{
		{idx: 1, term: 1, data: []byte("v1-payload")},
	}
	data := buildSegmentV1(4, entries)
	path := writeTmp(t, data)

	recs, err := ParseSegmentRecords(path)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, uint64(1), recs[0].Idx)
	assert.Equal(t, uint64(1), recs[0].Term)
	assert.Equal(t, entries[0].data, recs[0].ETFData)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// segmentEntry describes one record to be written by buildSegmentV1/V2.
type segmentEntry struct {
	idx  uint64
	term uint64
	data []byte
}

// buildSegmentV2 constructs a minimal Ra segment (version 2) byte slice with
// the given maxCount index-table slots, populated by entries in order.
func buildSegmentV2(maxCount uint16, entries []segmentEntry) []byte {
	out := make([]byte, 0, segmentHeaderSize)
	out = append(out, segmentMagic...)
	out = appendUint16(out, 2)
	out = appendUint16(out, maxCount)

	indexSize := int(maxCount) * segmentIndexRecordSizeV2
	dataStart := segmentHeaderSize + indexSize
	index := make([]byte, indexSize)
	var dataBuf []byte

	for i, e := range entries {
		offset := uint64(dataStart + len(dataBuf))
		crc := crc32.ChecksumIEEE(e.data)
		rec := index[i*segmentIndexRecordSizeV2 : (i+1)*segmentIndexRecordSizeV2]
		binary.BigEndian.PutUint64(rec[0:8], e.idx)
		binary.BigEndian.PutUint64(rec[8:16], e.term)
		binary.BigEndian.PutUint64(rec[16:24], offset)
		binary.BigEndian.PutUint32(rec[24:28], uint32(len(e.data))) //nolint:gosec
		binary.BigEndian.PutUint32(rec[28:32], crc)
		dataBuf = append(dataBuf, e.data...)
	}

	out = append(out, index...)
	out = append(out, dataBuf...)
	return out
}

// buildSegmentV1 constructs a minimal Ra segment (version 1) byte slice.
func buildSegmentV1(maxCount uint16, entries []segmentEntry) []byte {
	out := make([]byte, 0, segmentHeaderSize)
	out = append(out, segmentMagic...)
	out = appendUint16(out, 1)
	out = appendUint16(out, maxCount)

	indexSize := int(maxCount) * segmentIndexRecordSizeV1
	dataStart := segmentHeaderSize + indexSize
	index := make([]byte, indexSize)
	var dataBuf []byte

	for i, e := range entries {
		offset := uint32(dataStart + len(dataBuf)) //nolint:gosec
		crc := crc32.ChecksumIEEE(e.data)
		rec := index[i*segmentIndexRecordSizeV1 : (i+1)*segmentIndexRecordSizeV1]
		binary.BigEndian.PutUint64(rec[0:8], e.idx)
		binary.BigEndian.PutUint64(rec[8:16], e.term)
		binary.BigEndian.PutUint32(rec[16:20], offset)
		binary.BigEndian.PutUint32(rec[20:24], uint32(len(e.data))) //nolint:gosec
		binary.BigEndian.PutUint32(rec[24:28], crc)
		dataBuf = append(dataBuf, e.data...)
	}

	out = append(out, index...)
	out = append(out, dataBuf...)
	return out
}

func appendUint16(b []byte, v uint16) []byte {
	return append(b, byte(v>>8), byte(v)) //nolint:gosec
}
