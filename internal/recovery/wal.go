// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"encoding/binary"
	"fmt"
	"hash/adler32"
	"log"
	"os"
	"path/filepath"
)

// walMagic is the 4-byte magic number that opens every Ra WAL file.
const walMagic = "RAWA"

// walVersion is the only version this parser supports.
const walVersion byte = 1

// WALRecord holds the decoded fields of a single Ra WAL log entry.
type WALRecord struct {
	UID      string // queue / server UID this entry belongs to
	Idx      uint64 // Raft log index
	Term     uint64 // Raft term
	ETFData  []byte // raw ETF-encoded Ra machine command
	Truncate bool   // true when this entry is a truncation marker
}

// ParseWALMessagesForQueue reads a Ra WAL file and returns all AMQP message
// payloads belonging to records whose UID matches targetUID.
//
// Ra WAL format (version 1) after the 5-byte file header:
//
//	[3-byte header word]
//	    bit 23   – Truncate flag
//	    bit 22   – 0 = long form (first occurrence of UID in this file)
//	             – 1 = short form (UID already cached)
//	    bits 0–21 – IdRef (22-bit integer used as cache key)
//
//	Long form:
//	    IdDataLen  uint16  (big-endian)
//	    UId        [IdDataLen]byte
//	    Checksum   uint32  (adler32 of Idx‖Term‖ETFData; 0 = no checksum)
//	    DataLen    uint32
//	    Idx        uint64
//	    Term       uint64
//	    ETFData    [DataLen]byte
//
//	Short form:
//	    Checksum   uint32
//	    DataLen    uint32
//	    Idx        uint64
//	    Term       uint64
//	    ETFData    [DataLen]byte
//
//nolint:gocognit
func ParseWALMessagesForQueue(walPath, targetUID string) ([][]byte, error) {
	data, err := os.ReadFile(walPath)
	if err != nil {
		return nil, fmt.Errorf("reading WAL file: %w", err)
	}

	if len(data) < 5 {
		return nil, fmt.Errorf("WAL file too short")
	}
	if string(data[:4]) != walMagic {
		return nil, fmt.Errorf("invalid WAL magic: %q", data[:4])
	}
	if data[4] != walVersion {
		return nil, fmt.Errorf("unsupported WAL version: %d", data[4])
	}

	// uidCache maps the 22-bit IdRef to a UID string.
	uidCache := make(map[uint32]string)
	pos := 5 // skip file header

	var payloads [][]byte

	for pos < len(data) {
		// Need at least 3 bytes for the record header word.
		if pos+3 > len(data) {
			break
		}

		// Read the 24-bit header word big-endian.
		hw := uint32(data[pos])<<16 | uint32(data[pos+1])<<8 | uint32(data[pos+2])
		pos += 3

		trunc := (hw >> 23) & 1
		shortForm := (hw >> 22) & 1
		idRef := hw & 0x3FFFFF // lower 22 bits

		var uid string

		if shortForm == 0 {
			// Long form: UId follows in this record.
			if pos+2 > len(data) {
				break
			}
			idDataLen := int(binary.BigEndian.Uint16(data[pos:]))
			pos += 2
			if pos+idDataLen > len(data) {
				break
			}
			uid = string(data[pos : pos+idDataLen])
			pos += idDataLen
			// Cache the UID for future short-form references.
			uidCache[idRef] = uid
		} else {
			// Short form: look up UID in the cache.
			var ok bool
			uid, ok = uidCache[idRef]
			if !ok {
				// Unknown IdRef – WAL may be corrupt or partially written.
				log.Printf("[WAL] WARNING: unknown IdRef %d at offset %d, skipping", idRef, pos-3)
				break
			}
		}

		// Common fields: Checksum(4) + DataLen(4) + Idx(8) + Term(8) + Data(DataLen)
		if pos+20 > len(data) {
			break
		}
		checksum := binary.BigEndian.Uint32(data[pos:])
		pos += 4
		dataLen := int(binary.BigEndian.Uint32(data[pos:]))
		pos += 4

		// DataLen == 0 is the EOF sentinel used by pre-allocated WAL files.
		if dataLen == 0 {
			break
		}

		if pos+8 > len(data) {
			break
		}
		idx := binary.BigEndian.Uint64(data[pos:])
		pos += 8
		if pos+8 > len(data) {
			break
		}
		term := binary.BigEndian.Uint64(data[pos:])
		pos += 8

		if pos+dataLen > len(data) {
			log.Printf("[WAL] WARNING: record at idx=%d claims %d bytes but only %d remain",
				idx, dataLen, len(data)-pos)
			break
		}
		etfData := data[pos : pos+dataLen]
		pos += dataLen

		_ = trunc // used for semantic completeness; not needed for payload extraction

		// Optionally verify the adler32 checksum (0 means checksums disabled).
		if checksum != 0 {
			idxB := make([]byte, 8)
			termB := make([]byte, 8)
			binary.BigEndian.PutUint64(idxB, idx)
			binary.BigEndian.PutUint64(termB, term)
			var buf []byte
			buf = append(buf, idxB...)
			buf = append(buf, termB...)
			buf = append(buf, etfData...)
			if got := adler32.Checksum(buf); got != checksum {
				log.Printf("[WAL] WARNING: checksum mismatch at idx=%d uid=%q (want %08x got %08x)",
					idx, uid, checksum, got)
				// Continue recovery; a bad checksum on the last record is acceptable.
			}
		}

		if uid != targetUID {
			continue
		}

		// This record belongs to our target queue. Carve AMQP payloads from
		// the ETF-encoded Ra machine command.
		extracted := carvePayloadsFromETF(etfData)
		payloads = append(payloads, extracted...)
	}

	return payloads, nil
}

// ParseWALRecords returns all WAL records from the file, optionally filtered
// to a specific UID (pass "" for all UIDs).
//
//nolint:gocognit
func ParseWALRecords(walPath, filterUID string) ([]*WALRecord, error) {
	data, err := os.ReadFile(walPath)
	if err != nil {
		return nil, fmt.Errorf("reading WAL file: %w", err)
	}
	if len(data) < 5 {
		return nil, fmt.Errorf("WAL file too short")
	}
	if string(data[:4]) != walMagic {
		return nil, fmt.Errorf("invalid WAL magic: %q", data[:4])
	}
	if data[4] != walVersion {
		return nil, fmt.Errorf("unsupported WAL version: %d", data[4])
	}

	uidCache := make(map[uint32]string)
	pos := 5

	var records []*WALRecord

	for pos < len(data) {
		if pos+3 > len(data) {
			break
		}
		hw := uint32(data[pos])<<16 | uint32(data[pos+1])<<8 | uint32(data[pos+2])
		pos += 3

		trunc := (hw>>23)&1 == 1
		shortForm := (hw >> 22) & 1
		idRef := hw & 0x3FFFFF

		var uid string
		if shortForm == 0 {
			if pos+2 > len(data) {
				break
			}
			idDataLen := int(binary.BigEndian.Uint16(data[pos:]))
			pos += 2
			if pos+idDataLen > len(data) {
				break
			}
			uid = string(data[pos : pos+idDataLen])
			pos += idDataLen
			uidCache[idRef] = uid
		} else {
			var ok bool
			uid, ok = uidCache[idRef]
			if !ok {
				log.Printf("[WAL] WARNING: unknown IdRef %d at offset %d", idRef, pos-3)
				break
			}
		}

		if pos+20 > len(data) {
			break
		}
		_ = binary.BigEndian.Uint32(data[pos:]) // checksum
		pos += 4
		dataLen := int(binary.BigEndian.Uint32(data[pos:]))
		pos += 4

		// DataLen == 0 is the EOF sentinel used by pre-allocated WAL files.
		if dataLen == 0 {
			break
		}

		if pos+8 > len(data) {
			break
		}
		idx := binary.BigEndian.Uint64(data[pos:])
		pos += 8
		if pos+8 > len(data) {
			break
		}
		term := binary.BigEndian.Uint64(data[pos:])
		pos += 8

		if pos+dataLen > len(data) {
			break
		}
		etfData := make([]byte, dataLen)
		copy(etfData, data[pos:pos+dataLen])
		pos += dataLen

		if filterUID != "" && uid != filterUID {
			continue
		}

		records = append(records, &WALRecord{
			UID:      uid,
			Idx:      idx,
			Term:     term,
			ETFData:  etfData,
			Truncate: trunc,
		})
	}

	return records, nil
}

// CarveWALMessages scans all *.wal files in walDir and extracts AMQP message payloads
// whose WAL record UID matches queueUID. Only records that belong to the target queue
// are decoded; records from other queues sharing the same WAL are ignored.
//
// Call this AFTER CarveMessagesFromDir (which handles .segment files) so that
// WAL-only messages are appended in the correct temporal order.
func CarveWALMessages(walDir, queueUID string) ([][]byte, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // WAL backup dir absent — nothing to carve
		}
		return nil, fmt.Errorf("reading WAL backup dir: %w", err)
	}

	var payloads [][]byte
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != walFileSuffix {
			continue
		}
		p, err := ParseWALMessagesForQueue(filepath.Join(walDir, entry.Name()), queueUID)
		if err != nil {
			log.Printf("[WAL] WARNING: failed to carve %s: %v", entry.Name(), err)
			continue
		}
		payloads = append(payloads, p...)
	}
	return payloads, nil
}

// carvePayloadsFromETF wraps a raw ETF data block (without the 0x83 version
// byte) in a synthetic file buffer and reuses the shared content-tuple
// scanner (etfscan.go) to extract AMQP payload binaries.
func carvePayloadsFromETF(etfData []byte) (payloads [][]byte) {
	// The ETF data from the WAL does NOT include the leading 0x83 version byte;
	// scanContentTuples looks for the content-tuple byte pattern regardless,
	// so we can just pass the raw bytes directly.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WAL] WARNING: recovered from panic in carvePayloadsFromETF: %v", r)
			payloads = nil
		}
	}()
	return scanContentTuples(etfData)
}
