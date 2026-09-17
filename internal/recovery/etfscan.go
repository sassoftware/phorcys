package recovery

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// ETF tag constants used by the direct content-tuple scanner.
const (
	ettSmallTuple    byte = 104 // 0x68: SMALL_TUPLE_EXT — tuple arity fits in 1 byte
	ettLargeTuple    byte = 105 // 0x69: LARGE_TUPLE_EXT — tuple arity in 4 bytes (rare)
	ettAtom          byte = 100 // 0x64: ATOM_EXT        — 2-byte length + UTF-8 bytes
	ettAtomUTF8      byte = 118 // 0x76: ATOM_UTF8_EXT   — 2-byte length + UTF-8 bytes
	ettSmallAtom     byte = 115 // 0x73: SMALL_ATOM_EXT  — 1-byte length + bytes
	ettSmallAtomUTF8 byte = 119 // 0x77: SMALL_ATOM_UTF8_EXT — 1-byte length + UTF-8 bytes
	ettBinary        byte = 109 // 0x6D: BINARY_EXT      — 4-byte length + bytes
	ettNil           byte = 106 // 0x6A: NIL_EXT         — empty list
	ettList          byte = 108 // 0x6C: LIST_EXT        — 4-byte count + elements + tail
	ettSmallInt      byte = 97  // 0x61: SMALL_INTEGER_EXT — 1-byte unsigned value
	ettInt           byte = 98  // 0x62: INTEGER_EXT     — 4-byte signed value
	ettNone          byte = 0   // sentinel: no tag read yet
)

// contentTupleMarkers holds all possible byte prefixes for a 6-arity tuple
// whose first element is the atom "content".  We pre-compute them once.
var contentTupleMarkers [][]byte

func init() {
	const arity = 6
	atomStr := []byte("content")
	// SMALL_TUPLE_EXT + arity byte
	prefix := []byte{ettSmallTuple, arity}
	for _, encoding := range [][]byte{
		// SMALL_ATOM_UTF8_EXT  (most common in modern OTP)
		append([]byte{ettSmallAtomUTF8, byte(len(atomStr))}, atomStr...),
		// SMALL_ATOM_EXT
		append([]byte{ettSmallAtom, byte(len(atomStr))}, atomStr...),
		// ATOM_UTF8_EXT  (2-byte length)
		append([]byte{ettAtomUTF8, 0, byte(len(atomStr))}, atomStr...),
		// ATOM_EXT       (2-byte length)
		append([]byte{ettAtom, 0, byte(len(atomStr))}, atomStr...),
	} {
		contentTupleMarkers = append(contentTupleMarkers, append(prefix, encoding...))
	}
}

// scanContentTuples scans an in-memory buffer for {content,6,...,[<<payload>>]} tuples
// and returns all recoverable AMQP message payload binaries it finds.
//
// RabbitMQ 4.x wraps every message in Erlang map terms that the ETF library cannot decode.
// We bypass full-term decoding entirely: scan for the literal byte pattern of a 6-arity
// tuple whose first element is the atom "content", then manually parse the remaining
// 5 fields (classId, decoded-props, encoded-props, framing-module, payload-list).
//
// Shared by CarveMessagesFromFile (reads a .segment/.wal file from disk) and the Ra WAL
// record carver (operates on an in-memory ETF blob decoded from a WAL entry).
func scanContentTuples(data []byte) (payloads [][]byte) {
	idx := 0
	for idx < len(data) {
		nearest := -1
		for _, marker := range contentTupleMarkers {
			pos := bytes.Index(data[idx:], marker)
			if pos != -1 && (nearest == -1 || pos < nearest) {
				nearest = pos
			}
		}
		if nearest == -1 {
			break
		}
		abs := idx + nearest
		markerLen := matchedMarkerLen(data[abs:])
		if markerLen == 0 {
			idx = abs + 1
			continue
		}
		cur := abs + markerLen

		// Skip fields 2–5: classId, decoded-props, encoded-props-binary, framing-module.
		var skipErr error
		for field := 0; field < 4; field++ {
			cur, skipErr = skipETFValue(data, cur)
			if skipErr != nil {
				break
			}
		}
		if skipErr != nil || cur >= len(data) {
			idx = abs + 1
			continue
		}

		// Field 6: list containing the payload binary.
		extracted, next, extractErr := extractBinaryList(data, cur)
		if extractErr == nil {
			payloads = append(payloads, extracted...)
		}
		if next > abs {
			idx = next
		} else {
			idx = abs + 1
		}
	}
	return payloads
}

// matchedMarkerLen returns the length of the contentTupleMarker that starts at data[0:].
func matchedMarkerLen(data []byte) int {
	for _, marker := range contentTupleMarkers {
		if bytes.HasPrefix(data, marker) {
			return len(marker)
		}
	}
	return 0
}

// skipETFValue advances past one ETF value at data[pos] and returns the new position.
// Handles only the types that appear in content-tuple fields 2–5.
func skipETFValue(data []byte, pos int) (int, error) {
	if pos >= len(data) {
		return pos, fmt.Errorf("out of bounds at %d", pos)
	}
	tag := data[pos]
	pos++
	switch tag {
	case ettSmallInt:
		return pos + 1, checkBounds(data, pos, 1)
	case ettInt:
		return pos + 4, checkBounds(data, pos, 4)
	case ettSmallAtom, ettSmallAtomUTF8:
		if pos >= len(data) {
			return pos, fmt.Errorf("short atom")
		}
		return pos + 1 + int(data[pos]), checkBounds(data, pos+1, int(data[pos]))
	case ettAtom, ettAtomUTF8:
		if pos+2 > len(data) {
			return pos, fmt.Errorf("short atom")
		}
		n := int(binary.BigEndian.Uint16(data[pos:]))
		return pos + 2 + n, checkBounds(data, pos+2, n)
	case ettBinary:
		if pos+4 > len(data) {
			return pos, fmt.Errorf("short binary")
		}
		n := int(binary.BigEndian.Uint32(data[pos:]))
		return pos + 4 + n, checkBounds(data, pos+4, n)
	case ettNil:
		return pos, nil
	default:
		return pos, fmt.Errorf("unsupported tag %d at pos %d", tag, pos-1)
	}
}

// extractBinaryList reads a LIST_EXT (or NIL_EXT) at data[pos] and returns all BINARY_EXT elements.
func extractBinaryList(data []byte, pos int) ([][]byte, int, error) {
	if pos >= len(data) {
		return nil, pos, fmt.Errorf("out of bounds")
	}
	tag := data[pos]
	pos++
	if tag == ettNil {
		return nil, pos, nil
	}
	if tag != ettList {
		return nil, pos, fmt.Errorf("expected list, got tag %d", tag)
	}
	if pos+4 > len(data) {
		return nil, pos, fmt.Errorf("short list header")
	}
	count := int(binary.BigEndian.Uint32(data[pos:]))
	pos += 4

	var out [][]byte
	for i := 0; i < count; i++ {
		if pos >= len(data) {
			return out, pos, fmt.Errorf("truncated list")
		}
		elemTag := data[pos]
		pos++
		if elemTag != ettBinary {
			var err error
			// skip non-binary element; back up 1 so skipETFValue reads the tag
			pos, err = skipETFValue(data, pos-1)
			if err != nil {
				return out, pos, err
			}
			continue
		}
		if pos+4 > len(data) {
			return out, pos, fmt.Errorf("short binary header")
		}
		n := int(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if n <= 0 || pos+n > len(data) {
			continue
		}
		cp := make([]byte, n)
		copy(cp, data[pos:pos+n])
		out = append(out, cp)
		pos += n
	}
	if pos < len(data) && data[pos] == ettNil {
		pos++
	}
	return out, pos, nil
}

func checkBounds(data []byte, start, n int) error {
	if start+n > len(data) {
		return fmt.Errorf("out of bounds: need %d at %d in %d", n, start, len(data))
	}
	return nil
}
