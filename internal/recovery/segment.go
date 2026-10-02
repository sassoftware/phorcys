// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
)

// segmentMagic is the 4-byte magic number that opens every Ra segment file.
const segmentMagic = "RASG"

// segmentHeaderSize is MAGIC(4) + Version(2) + MaxCount(2).
const segmentHeaderSize = 8

// Index record layouts, keyed by segment version. Ra currently writes
// version 2 but still supports reading version 1 segments written by
// older nodes.
const (
	segmentIndexRecordSizeV1 = 28 // Idx(8) + Term(8) + Offset(4) + Length(4) + Crc(4)
	segmentIndexRecordSizeV2 = 32 // Idx(8) + Term(8) + Offset(8) + Length(4) + Crc(4)
)

// SegmentRecord holds the decoded fields of a single Ra segment log entry.
type SegmentRecord struct {
	Idx     uint64 // Raft log index
	Term    uint64 // Raft term
	Offset  uint64 // absolute file offset of ETFData
	Length  uint32 // byte length of ETFData
	ETFData []byte // raw ETF-encoded Ra machine command
}

// ParseSegmentRecords reads a Ra segment file and returns all decoded log
// entries in file order.
//
// Ra segment format:
//
//	Header:
//	    Magic      "RASG"
//	    Version    uint16 (big-endian; 1 or 2)
//	    MaxCount   uint16 (big-endian; number of index-table slots)
//
//	Index table (MaxCount fixed-size records, immediately after the header):
//	    version 1: Idx uint64, Term uint64, DataOffset uint32, Length uint32, Crc uint32
//	    version 2: Idx uint64, Term uint64, DataOffset uint64, Length uint32, Crc uint32
//	    (all fields big-endian; an all-zero record marks the end of written entries)
//
//	Data region (starts right after the index table):
//	    each entry's raw ETF bytes live at DataOffset for Length bytes, and
//	    are checksummed with IEEE CRC-32 (Crc == 0 disables the check).
func ParseSegmentRecords(segmentPath string) ([]*SegmentRecord, error) {
	data, err := os.ReadFile(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("reading segment file: %w", err)
	}

	if len(data) < segmentHeaderSize {
		return nil, fmt.Errorf("segment file too short")
	}
	if string(data[:4]) != segmentMagic {
		return nil, fmt.Errorf("invalid segment magic: %q", data[:4])
	}
	version := binary.BigEndian.Uint16(data[4:6])
	maxCount := binary.BigEndian.Uint16(data[6:8])

	var recordSize int
	switch version {
	case 1:
		recordSize = segmentIndexRecordSizeV1
	case 2:
		recordSize = segmentIndexRecordSizeV2
	default:
		return nil, fmt.Errorf("unsupported segment version: %d", version)
	}

	var records []*SegmentRecord
	pos := segmentHeaderSize
	for i := 0; i < int(maxCount); i++ {
		if pos+recordSize > len(data) {
			break // index table truncated — rest of file hasn't been written
		}
		entry := data[pos : pos+recordSize]
		pos += recordSize

		if isZero(entry) {
			break // all-zero record marks the end of written entries
		}

		idx, term, dataOffset, length, checksum := decodeSegmentIndexRecord(version, entry)

		if dataOffset+uint64(length) > uint64(len(data)) {
			log.Printf("[Segment] WARNING: record at idx=%d claims data [%d:%d] beyond file length %d",
				idx, dataOffset, dataOffset+uint64(length), len(data))
			break
		}
		etfData := make([]byte, length)
		copy(etfData, data[dataOffset:dataOffset+uint64(length)])

		if checksum != 0 {
			if got := crc32.ChecksumIEEE(etfData); got != checksum {
				log.Printf("[Segment] WARNING: checksum mismatch at idx=%d (want %08x got %08x)",
					idx, checksum, got)
			}
		}

		records = append(records, &SegmentRecord{
			Idx:     idx,
			Term:    term,
			Offset:  dataOffset,
			Length:  length,
			ETFData: etfData,
		})
	}

	return records, nil
}

// decodeSegmentIndexRecord unpacks one fixed-size index-table entry.
func decodeSegmentIndexRecord(version uint16, entry []byte) (idx, term, dataOffset uint64, length, checksum uint32) {
	idx = binary.BigEndian.Uint64(entry[0:8])
	term = binary.BigEndian.Uint64(entry[8:16])
	if version == 1 {
		dataOffset = uint64(binary.BigEndian.Uint32(entry[16:20]))
		length = binary.BigEndian.Uint32(entry[20:24])
		checksum = binary.BigEndian.Uint32(entry[24:28])
		return idx, term, dataOffset, length, checksum
	}
	dataOffset = binary.BigEndian.Uint64(entry[16:24])
	length = binary.BigEndian.Uint32(entry[24:28])
	checksum = binary.BigEndian.Uint32(entry[28:32])
	return idx, term, dataOffset, length, checksum
}

// isZero reports whether every byte in b is 0.
func isZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
