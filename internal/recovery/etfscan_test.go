package recovery

import (
	"testing"
)

// ---------------------------------------------------------------------------
// checkBounds
// ---------------------------------------------------------------------------

func TestCheckBounds_WithinRange(t *testing.T) {
	data := make([]byte, 10)
	if err := checkBounds(data, 4, 6); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckBounds_ExactlyAtEnd(t *testing.T) {
	data := make([]byte, 10)
	if err := checkBounds(data, 0, 10); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckBounds_OutOfRange(t *testing.T) {
	data := make([]byte, 10)
	if err := checkBounds(data, 5, 6); err == nil {
		t.Error("expected error when start+n exceeds len(data)")
	}
}

// ---------------------------------------------------------------------------
// matchedMarkerLen
// ---------------------------------------------------------------------------

func TestMatchedMarkerLen_MatchesKnownMarker(t *testing.T) {
	marker := contentTupleMarkers[0]
	data := append(append([]byte{}, marker...), 0xAA, 0xBB) // trailing bytes shouldn't matter
	got := matchedMarkerLen(data)
	if got != len(marker) {
		t.Errorf("got %d, want %d", got, len(marker))
	}
}

func TestMatchedMarkerLen_NoMatchReturnsZero(t *testing.T) {
	data := []byte("this is definitely not an ETF content tuple marker")
	if got := matchedMarkerLen(data); got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

func TestMatchedMarkerLen_EmptyInputReturnsZero(t *testing.T) {
	if got := matchedMarkerLen(nil); got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// skipETFValue
// ---------------------------------------------------------------------------

func TestSkipETFValue_PosOutOfBounds(t *testing.T) {
	data := []byte{ettSmallInt, 1}
	_, err := skipETFValue(data, len(data))
	if err == nil {
		t.Error("expected error when pos is out of bounds")
	}
}

func TestSkipETFValue_SmallInt(t *testing.T) {
	data := []byte{ettSmallInt, 42}
	next, err := skipETFValue(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != 2 {
		t.Errorf("next = %d, want 2", next)
	}
}

func TestSkipETFValue_SmallInt_Truncated(t *testing.T) {
	data := []byte{ettSmallInt} // missing the value byte
	_, err := skipETFValue(data, 0)
	if err == nil {
		t.Error("expected error for truncated SMALL_INTEGER_EXT")
	}
}

func TestSkipETFValue_Int(t *testing.T) {
	data := []byte{ettInt, 0, 0, 0, 5}
	next, err := skipETFValue(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != 5 {
		t.Errorf("next = %d, want 5", next)
	}
}

func TestSkipETFValue_Int_Truncated(t *testing.T) {
	data := []byte{ettInt, 0, 0} // needs 4 bytes, only 2 present
	_, err := skipETFValue(data, 0)
	if err == nil {
		t.Error("expected error for truncated INTEGER_EXT")
	}
}

func TestSkipETFValue_SmallAtom(t *testing.T) {
	data := append([]byte{ettSmallAtom, 3}, "abc"...)
	next, err := skipETFValue(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != len(data) {
		t.Errorf("next = %d, want %d", next, len(data))
	}
}

func TestSkipETFValue_SmallAtomUTF8(t *testing.T) {
	data := append([]byte{ettSmallAtomUTF8, 4}, "none"...)
	next, err := skipETFValue(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != len(data) {
		t.Errorf("next = %d, want %d", next, len(data))
	}
}

func TestSkipETFValue_SmallAtom_MissingLengthByte(t *testing.T) {
	data := []byte{ettSmallAtom} // tag only, no length byte
	_, err := skipETFValue(data, 0)
	if err == nil {
		t.Error("expected error when length byte is missing")
	}
}

func TestSkipETFValue_SmallAtom_TruncatedBody(t *testing.T) {
	data := []byte{ettSmallAtom, 5, 'a', 'b'} // claims 5 bytes, only 2 present
	_, err := skipETFValue(data, 0)
	if err == nil {
		t.Error("expected error for truncated atom body")
	}
}

func TestSkipETFValue_Atom(t *testing.T) {
	data := append([]byte{ettAtom, 0, 3}, "abc"...)
	next, err := skipETFValue(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != len(data) {
		t.Errorf("next = %d, want %d", next, len(data))
	}
}

func TestSkipETFValue_AtomUTF8(t *testing.T) {
	data := append([]byte{ettAtomUTF8, 0, 4}, "none"...)
	next, err := skipETFValue(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != len(data) {
		t.Errorf("next = %d, want %d", next, len(data))
	}
}

func TestSkipETFValue_Atom_MissingLengthHeader(t *testing.T) {
	data := []byte{ettAtom, 0} // only 1 of the 2 length bytes present
	_, err := skipETFValue(data, 0)
	if err == nil {
		t.Error("expected error when 2-byte length header is truncated")
	}
}

func TestSkipETFValue_Atom_TruncatedBody(t *testing.T) {
	data := []byte{ettAtom, 0, 5, 'a', 'b'} // claims 5 bytes, only 2 present
	_, err := skipETFValue(data, 0)
	if err == nil {
		t.Error("expected error for truncated atom body")
	}
}

func TestSkipETFValue_Binary(t *testing.T) {
	data := append([]byte{ettBinary, 0, 0, 0, 2}, "xy"...)
	next, err := skipETFValue(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != len(data) {
		t.Errorf("next = %d, want %d", next, len(data))
	}
}

func TestSkipETFValue_Binary_MissingLengthHeader(t *testing.T) {
	data := []byte{ettBinary, 0, 0} // needs 4 length bytes, only 2 present
	_, err := skipETFValue(data, 0)
	if err == nil {
		t.Error("expected error when 4-byte length header is truncated")
	}
}

func TestSkipETFValue_Binary_TruncatedBody(t *testing.T) {
	data := append([]byte{ettBinary, 0, 0, 0, 5}, "xy"...) // claims 5 bytes, only 2 present
	_, err := skipETFValue(data, 0)
	if err == nil {
		t.Error("expected error for truncated binary body")
	}
}

func TestSkipETFValue_Nil(t *testing.T) {
	data := []byte{ettNil}
	next, err := skipETFValue(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != 1 {
		t.Errorf("next = %d, want 1", next)
	}
}

func TestSkipETFValue_UnsupportedTag(t *testing.T) {
	data := []byte{0xFF}
	_, err := skipETFValue(data, 0)
	if err == nil {
		t.Error("expected error for unsupported ETF tag")
	}
}

// ---------------------------------------------------------------------------
// extractBinaryList
// ---------------------------------------------------------------------------

func TestExtractBinaryList_PosOutOfBounds(t *testing.T) {
	data := []byte{ettList}
	_, _, err := extractBinaryList(data, len(data))
	if err == nil {
		t.Error("expected error when pos is out of bounds")
	}
}

func TestExtractBinaryList_Nil(t *testing.T) {
	data := []byte{ettNil}
	out, next, err := extractBinaryList(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Errorf("expected nil binaries for NIL_EXT, got %v", out)
	}
	if next != 1 {
		t.Errorf("next = %d, want 1", next)
	}
}

func TestExtractBinaryList_WrongTagReturnsError(t *testing.T) {
	data := []byte{ettSmallInt, 5}
	_, _, err := extractBinaryList(data, 0)
	if err == nil {
		t.Error("expected error when tag is neither LIST_EXT nor NIL_EXT")
	}
}

func TestExtractBinaryList_ShortListHeader(t *testing.T) {
	data := []byte{ettList, 0, 0} // needs 4-byte count, only 2 present
	_, _, err := extractBinaryList(data, 0)
	if err == nil {
		t.Error("expected error for truncated list count header")
	}
}

func TestExtractBinaryList_SingleBinaryWithNilTail(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 1)
	data = appendBinary(data, []byte("payload"))
	data = append(data, ettNil)

	out, next, err := extractBinaryList(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 || string(out[0]) != "payload" {
		t.Errorf("out = %v, want [\"payload\"]", out)
	}
	if next != len(data) {
		t.Errorf("next = %d, want %d (list tail consumed)", next, len(data))
	}
}

func TestExtractBinaryList_MultipleBinaries(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 2)
	data = appendBinary(data, []byte("first"))
	data = appendBinary(data, []byte("second"))
	data = append(data, ettNil)

	out, _, err := extractBinaryList(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 2 || string(out[0]) != "first" || string(out[1]) != "second" {
		t.Errorf("out = %v, want [\"first\" \"second\"]", out)
	}
}

func TestExtractBinaryList_NonBinaryElementSkipped(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 2)
	data = append(data, ettSmallInt, 7) // non-binary element, should be skipped
	data = appendBinary(data, []byte("kept"))
	data = append(data, ettNil)

	out, _, err := extractBinaryList(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 || string(out[0]) != "kept" {
		t.Errorf("out = %v, want [\"kept\"] (non-binary element skipped)", out)
	}
}

func TestExtractBinaryList_ZeroLengthBinarySkippedWithoutError(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 1)
	data = appendBinary(data, nil) // zero-length binary
	data = append(data, ettNil)

	out, _, err := extractBinaryList(data, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected zero-length binary to be skipped, got %v", out)
	}
}

func TestExtractBinaryList_TruncatedListReturnsError(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 2) // claims 2 elements
	data = appendBinary(data, []byte("only-one"))
	// second element missing entirely

	_, _, err := extractBinaryList(data, 0)
	if err == nil {
		t.Error("expected error for truncated list body")
	}
}

func TestExtractBinaryList_ShortBinaryHeaderInList(t *testing.T) {
	var data []byte
	data = append(data, ettList)
	data = appendUint32(data, 1)
	data = append(data, ettBinary, 0, 0) // truncated 4-byte length header

	_, _, err := extractBinaryList(data, 0)
	if err == nil {
		t.Error("expected error for truncated binary length header inside list")
	}
}

// ---------------------------------------------------------------------------
// scanContentTuples
// ---------------------------------------------------------------------------

func TestScanContentTuples_EmptyInput(t *testing.T) {
	got := scanContentTuples(nil)
	if len(got) != 0 {
		t.Errorf("expected no payloads from nil, got %d", len(got))
	}
}

func TestScanContentTuples_NoContentTuple(t *testing.T) {
	got := scanContentTuples([]byte("hello world this has no ETF content tuple"))
	if len(got) != 0 {
		t.Errorf("expected no payloads, got %d", len(got))
	}
}

func TestScanContentTuples_ContentTuple_ExtractsPayload(t *testing.T) {
	// Build a synthetic ETF content tuple that matches the scanner's pattern.
	// {content, 60, none, <<properties>>, rabbit_framing_amqp_0_9_1, [<<payload>>]}
	//
	// We use the buildContentETF helper to produce valid bytes for the scanner.
	payload := []byte("unit-test-payload")
	etfBlock := buildContentETF(payload)

	got := scanContentTuples(etfBlock)
	if len(got) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(got))
	}
	if string(got[0]) != string(payload) {
		t.Errorf("payload = %q, want %q", got[0], payload)
	}
}

func TestScanContentTuples_MultipleTuples_ExtractsAllInOrder(t *testing.T) {
	var data []byte
	data = append(data, buildContentETF([]byte("first"))...)
	data = append(data, buildContentETF([]byte("second"))...)
	data = append(data, buildContentETF([]byte("third"))...)

	got := scanContentTuples(data)
	if len(got) != 3 {
		t.Fatalf("expected 3 payloads, got %d", len(got))
	}
	want := []string{"first", "second", "third"}
	for i, w := range want {
		if string(got[i]) != w {
			t.Errorf("payload[%d] = %q, want %q", i, got[i], w)
		}
	}
}

func TestScanContentTuples_LeadingGarbageIsSkipped(t *testing.T) {
	junk := []byte("random noise that precedes the real message-----")
	data := append(junk, buildContentETF([]byte("real-payload"))...)

	got := scanContentTuples(data)
	if len(got) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(got))
	}
	if string(got[0]) != "real-payload" {
		t.Errorf("payload = %q, want %q", got[0], "real-payload")
	}
}

func TestScanContentTuples_TruncatedTupleAfterMarkerIsSkipped(t *testing.T) {
	// A marker with no valid fields behind it must not panic or wedge the
	// scanner; it should simply find nothing and move on.
	marker := contentTupleMarkers[0]
	got := scanContentTuples(marker) // marker only, no field data follows
	if len(got) != 0 {
		t.Errorf("expected no payloads from a truncated tuple, got %d", len(got))
	}
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
	if !found {
		t.Errorf("expected to recover payload after truncated leading tuple, got %v", got)
	}
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
	b = appendUint32(b, uint32(len(data)))
	return append(b, data...)
}
