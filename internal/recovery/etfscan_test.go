// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// checkBounds
// ---------------------------------------------------------------------------

func TestCheckBounds_WithinRange(t *testing.T) {
	data := make([]byte, 10)
	err := checkBounds(data, 4, 6)
	assert.NoError(t, err)
}

func TestCheckBounds_ExactlyAtEnd(t *testing.T) {
	data := make([]byte, 10)
	err := checkBounds(data, 0, 10)
	assert.NoError(t, err)
}

func TestCheckBounds_OutOfRange(t *testing.T) {
	data := make([]byte, 10)
	assert.Error(t, checkBounds(data, 5, 6), "expected error when start+n exceeds len(data)")
}

// ---------------------------------------------------------------------------
// matchedMarkerLen
// ---------------------------------------------------------------------------

func TestMatchedMarkerLen_MatchesKnownMarker(t *testing.T) {
	marker := contentTupleMarkers[0]
	data := append(append([]byte{}, marker...), 0xAA, 0xBB) // trailing bytes shouldn't matter
	got := matchedMarkerLen(data)
	assert.Equal(t, len(marker), got)
}

func TestMatchedMarkerLen_NoMatchReturnsZero(t *testing.T) {
	data := []byte("this is definitely not an ETF content tuple marker")
	assert.Equal(t, 0, matchedMarkerLen(data))
}

func TestMatchedMarkerLen_EmptyInputReturnsZero(t *testing.T) {
	assert.Equal(t, 0, matchedMarkerLen(nil))
}

// ---------------------------------------------------------------------------
// skipETFValue
// ---------------------------------------------------------------------------

func TestSkipETFValue_PosOutOfBounds(t *testing.T) {
	data := []byte{ettSmallInt, 1}
	_, err := skipETFValue(data, len(data))
	assert.Error(t, err, "expected error when pos is out of bounds")
}

func TestSkipETFValue_SmallInt(t *testing.T) {
	data := []byte{ettSmallInt, 42}
	next, err := skipETFValue(data, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, next)
}

func TestSkipETFValue_SmallInt_Truncated(t *testing.T) {
	data := []byte{ettSmallInt} // missing the value byte
	_, err := skipETFValue(data, 0)
	assert.Error(t, err, "expected error for truncated SMALL_INTEGER_EXT")
}

func TestSkipETFValue_Int(t *testing.T) {
	data := []byte{ettInt, 0, 0, 0, 5}
	next, err := skipETFValue(data, 0)
	require.NoError(t, err)
	assert.Equal(t, 5, next)
}

func TestSkipETFValue_Int_Truncated(t *testing.T) {
	data := []byte{ettInt, 0, 0} // needs 4 bytes, only 2 present
	_, err := skipETFValue(data, 0)
	assert.Error(t, err, "expected error for truncated INTEGER_EXT")
}

func TestSkipETFValue_SmallAtom(t *testing.T) {
	data := append([]byte{ettSmallAtom, 3}, "abc"...)
	next, err := skipETFValue(data, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), next)
}

func TestSkipETFValue_SmallAtomUTF8(t *testing.T) {
	data := append([]byte{ettSmallAtomUTF8, 4}, "none"...)
	next, err := skipETFValue(data, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), next)
}

func TestSkipETFValue_SmallAtom_MissingLengthByte(t *testing.T) {
	data := []byte{ettSmallAtom} // tag only, no length byte
	_, err := skipETFValue(data, 0)
	assert.Error(t, err, "expected error when length byte is missing")
}

func TestSkipETFValue_SmallAtom_TruncatedBody(t *testing.T) {
	data := []byte{ettSmallAtom, 5, 'a', 'b'} // claims 5 bytes, only 2 present
	_, err := skipETFValue(data, 0)
	assert.Error(t, err, "expected error for truncated atom body")
}

func TestSkipETFValue_Atom(t *testing.T) {
	data := append([]byte{ettAtom, 0, 3}, "abc"...)
	next, err := skipETFValue(data, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), next)
}

func TestSkipETFValue_AtomUTF8(t *testing.T) {
	data := append([]byte{ettAtomUTF8, 0, 4}, "none"...)
	next, err := skipETFValue(data, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), next)
}

func TestSkipETFValue_Atom_MissingLengthHeader(t *testing.T) {
	data := []byte{ettAtom, 0} // only 1 of the 2 length bytes present
	_, err := skipETFValue(data, 0)
	assert.Error(t, err, "expected error when 2-byte length header is truncated")
}

func TestSkipETFValue_Atom_TruncatedBody(t *testing.T) {
	data := []byte{ettAtom, 0, 5, 'a', 'b'} // claims 5 bytes, only 2 present
	_, err := skipETFValue(data, 0)
	assert.Error(t, err, "expected error for truncated atom body")
}

func TestSkipETFValue_Binary(t *testing.T) {
	data := append([]byte{ettBinary, 0, 0, 0, 2}, "xy"...)
	next, err := skipETFValue(data, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), next)
}

func TestSkipETFValue_Binary_MissingLengthHeader(t *testing.T) {
	data := []byte{ettBinary, 0, 0} // needs 4 length bytes, only 2 present
	_, err := skipETFValue(data, 0)
	assert.Error(t, err, "expected error when 4-byte length header is truncated")
}

func TestSkipETFValue_Binary_TruncatedBody(t *testing.T) {
	data := append([]byte{ettBinary, 0, 0, 0, 5}, "xy"...) // claims 5 bytes, only 2 present
	_, err := skipETFValue(data, 0)
	assert.Error(t, err, "expected error for truncated binary body")
}

func TestSkipETFValue_Nil(t *testing.T) {
	data := []byte{ettNil}
	next, err := skipETFValue(data, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, next)
}

func TestSkipETFValue_UnsupportedTag(t *testing.T) {
	data := []byte{0xFF}
	_, err := skipETFValue(data, 0)
	assert.Error(t, err, "expected error for unsupported ETF tag")
}

// ---------------------------------------------------------------------------
// extractBinaryList
// ---------------------------------------------------------------------------

func TestExtractBinaryList_PosOutOfBounds(t *testing.T) {
	data := []byte{ettList}
	_, _, err := extractBinaryList(data, len(data))
	assert.Error(t, err, "expected error when pos is out of bounds")
}

func TestExtractBinaryList_Nil(t *testing.T) {
	data := []byte{ettNil}
	out, next, err := extractBinaryList(data, 0)
	require.NoError(t, err)
	assert.Nil(t, out, "expected nil binaries for NIL_EXT")
	assert.Equal(t, 1, next)
}

func TestExtractBinaryList_WrongTagReturnsError(t *testing.T) {
	data := []byte{ettSmallInt, 5}
	_, _, err := extractBinaryList(data, 0)
	assert.Error(t, err, "expected error when tag is neither LIST_EXT nor NIL_EXT")
}

func TestExtractBinaryList_ShortListHeader(t *testing.T) {
	data := []byte{ettList, 0, 0} // needs 4-byte count, only 2 present
	_, _, err := extractBinaryList(data, 0)
	assert.Error(t, err, "expected error for truncated list count header")
}

func TestExtractBinaryList_SingleBinaryWithNilTail(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 1)
	data = appendBinary(data, []byte("payload"))
	data = append(data, ettNil)

	out, next, err := extractBinaryList(data, 0)
	require.NoError(t, err)
	assert.True(t, len(out) == 1 && string(out[0]) == "payload", "out = %v, want [\"payload\"]", out)
	assert.Equal(t, len(data), next, "list tail should be consumed")
}

func TestExtractBinaryList_MultipleBinaries(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 2)
	data = appendBinary(data, []byte("first"))
	data = appendBinary(data, []byte("second"))
	data = append(data, ettNil)

	out, _, err := extractBinaryList(data, 0)
	require.NoError(t, err)
	assert.True(t, len(out) == 2 && string(out[0]) == "first" && string(out[1]) == "second", "out = %v, want [\"first\" \"second\"]", out)
}

func TestExtractBinaryList_NonBinaryElementSkipped(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 2)
	data = append(data, ettSmallInt, 7) // non-binary element, should be skipped
	data = appendBinary(data, []byte("kept"))
	data = append(data, ettNil)

	out, _, err := extractBinaryList(data, 0)
	require.NoError(t, err)
	assert.True(t, len(out) == 1 && string(out[0]) == "kept", "out = %v, want [\"kept\"] (non-binary element skipped)", out)
}

func TestExtractBinaryList_ZeroLengthBinarySkippedWithoutError(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 1)
	data = appendBinary(data, nil) // zero-length binary
	data = append(data, ettNil)

	out, _, err := extractBinaryList(data, 0)
	require.NoError(t, err)
	assert.Empty(t, out, "expected zero-length binary to be skipped")
}

func TestExtractBinaryList_TruncatedListReturnsError(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 2) // claims 2 elements
	data = appendBinary(data, []byte("only-one"))
	// second element missing entirely

	_, _, err := extractBinaryList(data, 0)
	assert.Error(t, err, "expected error for truncated list body")
}

func TestExtractBinaryList_ShortBinaryHeaderInList(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 1)
	data = append(data, ettBinary, 0, 0) // truncated 4-byte length header

	_, _, err := extractBinaryList(data, 0)
	assert.Error(t, err, "expected error for truncated binary length header inside list")
}

// ---------------------------------------------------------------------------
// scanContentTuples
// ---------------------------------------------------------------------------

func TestScanContentTuples_EmptyInput(t *testing.T) {
	got := scanContentTuples(nil)
	assert.Empty(t, got)
}

func TestScanContentTuples_NoContentTuple(t *testing.T) {
	got := scanContentTuples([]byte("hello world this has no ETF content tuple"))
	assert.Empty(t, got)
}

func TestScanContentTuples_ContentTuple_ExtractsPayload(t *testing.T) {
	// Build a synthetic ETF content tuple that matches the scanner's pattern.
	// {content, 60, none, <<properties>>, rabbit_framing_amqp_0_9_1, [<<payload>>]}
	//
	// We use the buildContentETF helper to produce valid bytes for the scanner.
	payload := []byte("unit-test-payload")
	etfBlock := buildContentETF(payload)

	got := scanContentTuples(etfBlock)
	require.Len(t, got, 1)
	assert.Equal(t, payload, got[0])
}

func TestScanContentTuples_MultipleTuples_ExtractsAllInOrder(t *testing.T) {
	var data []byte
	data = append(data, buildContentETF([]byte("first"))...)
	data = append(data, buildContentETF([]byte("second"))...)
	data = append(data, buildContentETF([]byte("third"))...)

	got := scanContentTuples(data)
	require.Len(t, got, 3)
	want := []string{"first", "second", "third"}
	for i, w := range want {
		assert.Equalf(t, w, string(got[i]), "payload[%d] = %q, want %q", i, got[i], w)
	}
}

func TestScanContentTuples_LeadingGarbageIsSkipped(t *testing.T) {
	payload := buildContentETF([]byte("real-payload"))
	junk := []byte("random noise that precedes the real message-----")
	payload = append(junk, payload...)

	got := scanContentTuples(payload)
	require.Len(t, got, 1)
	assert.Equal(t, "real-payload", string(got[0]))
}

func TestScanContentTuples_TruncatedTupleAfterMarkerIsSkipped(t *testing.T) {
	// A marker with no valid fields behind it must not panic or wedge the
	// scanner; it should simply find nothing and move on.
	marker := contentTupleMarkers[0]
	got := scanContentTuples(marker) // marker only, no field data follows
	assert.Empty(t, got)
}

func TestScanContentTuples_RecoversAfterTruncatedTuple(t *testing.T) {
	// A truncated/incomplete tuple marker followed by a valid one should not
	// prevent the valid message from being carved.
	marker := contentTupleMarkers[0]
	data := append(append([]byte{}, marker...), buildContentETF([]byte("recovered"))...)

	got := scanContentTuples(data)
	found := false
	for _, p := range got {
		if string(p) == "recovered" {
			found = true
		}
	}
	assert.True(t, found, "expected to recover payload after truncated leading tuple, got %v", got)
}

// ---------------------------------------------------------------------------
// Test fixture builders
// ---------------------------------------------------------------------------

// buildContentETF constructs a minimal byte sequence that the scanContentTuples
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
	b = appendUint32(b, uint32(len(data))) //nolint:gosec
	return append(b, data...)
}
