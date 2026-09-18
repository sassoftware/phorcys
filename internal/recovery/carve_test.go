// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/DeedleFake/etf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.Len(t, bins, 2, "expected 2 binaries, got %d", len(bins))
	assert.Equal(t, "payload one", string(bins[0]))
	assert.Equal(t, "payload two", string(bins[1]))
}

func TestExtractRawBinaries_NestedList(t *testing.T) {
	term := etf.Tuple{
		etf.Atom("outer"),
		etf.List{
			[]byte("nested"),
		},
	}
	bins := extractRawBinaries(term)
	assert.True(t, len(bins) == 1 && string(bins[0]) == "nested", "got %d bins, expected 1 with value 'nested'", len(bins))
}

func TestExtractRawBinaries_EmptyBinaryIgnored(t *testing.T) {
	term := etf.Tuple{[]byte{}, []byte("ok")}
	bins := extractRawBinaries(term)
	assert.True(t, len(bins) == 1 && string(bins[0]) == "ok", "empty binary should be ignored; got %d bins", len(bins))
}

func TestExtractRawBinaries_CopiesData(t *testing.T) {
	original := []byte("original content")
	term := etf.Tuple{original}
	bins := extractRawBinaries(term)
	require.Len(t, bins, 1, "expected 1 binary")
	// Mutate the result; original slice must be unchanged.
	bins[0][0] = 'X'
	assert.NotEqual(t, byte('X'), original[0], "extractRawBinaries should copy data, not return the original slice")
}

func TestExtractRawBinaries_NonBinaryTermsIgnored(t *testing.T) {
	term := etf.Tuple{etf.Atom("just"), etf.Atom("atoms"), int64(42)}
	bins := extractRawBinaries(term)
	assert.Empty(t, bins)
}

// ---------------------------------------------------------------------------
// findBasicMessagePayloads
// ---------------------------------------------------------------------------

func TestFindBasicMessagePayloads_ValidMessage(t *testing.T) {
	msg := makeBasicMessageTerm([]byte("the body"))
	payloads := findBasicMessagePayloads(msg)
	require.Len(t, payloads, 1, "expected 1 payload, got %d", len(payloads))
	assert.Equal(t, "the body", string(payloads[0]))
}

func TestFindBasicMessagePayloads_NestedInList(t *testing.T) {
	msg := makeBasicMessageTerm([]byte("nested body"))
	outer := etf.List{etf.Atom("irrelevant"), msg}
	payloads := findBasicMessagePayloads(outer)
	require.NotEmpty(t, payloads, "expected at least 1 payload from nested list")
}

func TestFindBasicMessagePayloads_NestedInTuple(t *testing.T) {
	msg := makeBasicMessageTerm([]byte("deep body"))
	outer := etf.Tuple{etf.Atom("raft_entry"), int64(1), msg}
	payloads := findBasicMessagePayloads(outer)
	require.NotEmpty(t, payloads, "expected at least 1 payload from nested tuple")
}

func TestFindBasicMessagePayloads_WrongAtomIgnored(t *testing.T) {
	term := etf.Tuple{
		etf.Atom("not_basic_message"),
		etf.List{etf.Atom("rk")},
		etf.Atom("exchange"),
		etf.Tuple{etf.Atom("content"), []byte("should not appear")},
	}
	payloads := findBasicMessagePayloads(term)
	assert.Empty(t, payloads)
}

func TestFindBasicMessagePayloads_TupleTooShort(t *testing.T) {
	// A tuple with atom basic_message but fewer than 4 elements.
	term := etf.Tuple{etf.Atom("basic_message"), etf.Atom("rk")}
	payloads := findBasicMessagePayloads(term)
	assert.Empty(t, payloads)
}

func TestFindBasicMessagePayloads_NonTupleTermIgnored(t *testing.T) {
	payloads := findBasicMessagePayloads(etf.Atom("nothing"))
	assert.Empty(t, payloads)
}

// ---------------------------------------------------------------------------
// CarveMessagesFromFile
// ---------------------------------------------------------------------------

// writeSegmentFile creates a temp file with the given extension containing the
// ETF-encoded message wrapped in some surrounding junk bytes.
func writeSegmentFile(t *testing.T, dir, ext string, msg any) string {
	t.Helper()
	encoded, err := encodeETFTerm(msg)
	require.NoError(t, err)
	f, err := os.CreateTemp(dir, "*"+ext)
	require.NoError(t, err)
	defer func() {
		err = f.Close()
		require.NoError(t, err)
	}()
	_, err = f.Write([]byte{0x00, 0x01, 0x02}) // junk prefix
	require.NoError(t, err)
	_, err = f.Write(encoded)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xFF, 0xFE}) // junk suffix
	require.NoError(t, err)
	return f.Name()
}

func TestCarveMessagesFromFile_ExtractsPayload(t *testing.T) {
	dir := t.TempDir()
	path := writeSegmentFile(t, dir, segmentFileSuffix, makeBasicMessageTerm([]byte("file payload")))

	payloads, err := CarveMessagesFromFile(path)
	require.NoError(t, err)
	require.NotEmpty(t, payloads)
	found := false
	for _, p := range payloads {
		if string(p) == "file payload" {
			found = true
		}
	}
	assert.True(t, found)
}

func TestCarveMessagesFromFile_MissingFileReturnsError(t *testing.T) {
	_, err := CarveMessagesFromFile("/no/such/file.segment")
	assert.Error(t, err)
}

func TestCarveMessagesFromFile_AllJunkReturnsEmpty(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "*.segment")
	require.NoError(t, err)
	_, err = f.Write([]byte{0x01, 0x02, 0x03, 0x04, 0x05})
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	payloads, err := CarveMessagesFromFile(f.Name())
	require.NoError(t, err)
	assert.Empty(t, payloads)
}

func TestCarveMessagesFromFile_MultipleMessages(t *testing.T) {
	dir := t.TempDir()
	msg1, _ := encodeETFTerm(makeBasicMessageTerm([]byte("first-message")))
	msg2, _ := encodeETFTerm(makeBasicMessageTerm([]byte("second-message")))

	f, _ := os.CreateTemp(dir, "*.wal")
	// Write messages back-to-back; no junk bytes between them so the scanner
	// finds msg2's magic byte immediately after consuming msg1.
	_, err := f.Write(msg1)
	require.NoError(t, err)
	_, err = f.Write(msg2)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	payloads, err := CarveMessagesFromFile(f.Name())
	require.NoError(t, err)

	var foundFirst, foundSecond bool
	for _, p := range payloads {
		switch string(p) {
		case "first-message":
			foundFirst = true
		case "second-message":
			foundSecond = true
		}
	}
	assert.True(t, foundFirst && foundSecond, "expected both 'first-message' and 'second-message' payloads; got %d payloads: %q",
		len(payloads), payloads)
}

// ---------------------------------------------------------------------------
// CarveMessagesFromDir
// ---------------------------------------------------------------------------

func TestCarveMessagesFromDir_ProcessesSegmentAndWal(t *testing.T) {
	dir := t.TempDir()
	msg, _ := encodeETFTerm(makeBasicMessageTerm([]byte("dir msg")))

	err := os.WriteFile(filepath.Join(dir, "0000000000000001.segment"), msg, 0600)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(dir, "00000001.wal"), msg, 0600)
	require.NoError(t, err)
	// Files with other extensions must be ignored.
	err = os.WriteFile(filepath.Join(dir, "skip.tmp"), msg, 0600)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(dir, "skip.log"), msg, 0600)
	require.NoError(t, err)

	payloads, err := CarveMessagesFromDir(dir)
	require.NoError(t, err)
	// Each of the 2 valid files contributes at least 1 payload.
	assert.GreaterOrEqual(t, len(payloads), 2)
}

func TestCarveMessagesFromDir_EmptyDirReturnsEmpty(t *testing.T) {
	payloads, err := CarveMessagesFromDir(t.TempDir())
	require.NoError(t, err)
	assert.Empty(t, payloads)
}

func TestCarveMessagesFromDir_InvalidDirReturnsError(t *testing.T) {
	_, err := CarveMessagesFromDir("/no/such/dir")
	assert.Error(t, err)
}

func TestCarveMessagesFromDir_SkipsBadFilesGracefully(t *testing.T) {
	dir := t.TempDir()

	// A good segment file.
	good, _ := encodeETFTerm(makeBasicMessageTerm([]byte("good")))
	err := os.WriteFile(filepath.Join(dir, "good.segment"), good, 0600)
	require.NoError(t, err)
	// A corrupt segment file (no valid ETF).
	err = os.WriteFile(filepath.Join(dir, "bad.segment"), []byte{0x01, 0x02}, 0600)
	require.NoError(t, err)

	payloads, err := CarveMessagesFromDir(dir)
	require.NoError(t, err)
	// We should still get payloads from the good file even though bad.segment had no messages.
	found := false
	for _, p := range payloads {
		if string(p) == "good" {
			found = true
		}
	}
	assert.True(t, found)
}

// ---------------------------------------------------------------------------
// Test-only helpers for building/inspecting synthetic ETF terms.
//
// Production carving uses the direct binary scanner in etfscan.go; these two
// helpers walk decoded etf.Tuple/etf.List trees and only exist to make it easy
// to assert against terms built by makeBasicMessageTerm and friends.
// ---------------------------------------------------------------------------

// extractRawBinaries recursively collects all byte slices from a term tree.
func extractRawBinaries(term any) [][]byte {
	var binaries [][]byte
	switch t := term.(type) {
	case []byte:
		if len(t) > 0 {
			cp := make([]byte, len(t))
			copy(cp, t)
			binaries = append(binaries, cp)
		}
	case etf.Tuple:
		for _, el := range t {
			binaries = append(binaries, extractRawBinaries(el)...)
		}
	case etf.List:
		for _, el := range t {
			binaries = append(binaries, extractRawBinaries(el)...)
		}
	}
	return binaries
}

// findBasicMessagePayloads walks a decoded term tree looking for the
// {content, ClassId, ..., [<<payload>>]} tuple shape and returns its payloads.
func findBasicMessagePayloads(term any) [][]byte {
	var payloads [][]byte
	switch t := term.(type) {
	case etf.Tuple:
		if len(t) >= 6 {
			if atom, ok := t[0].(etf.Atom); ok && atom == "content" {
				if payloadList, ok := t[len(t)-1].(etf.List); ok {
					for _, item := range payloadList {
						if b, ok := item.([]byte); ok && len(b) > 0 {
							cp := make([]byte, len(b))
							copy(cp, b)
							payloads = append(payloads, cp)
						}
					}
					return payloads
				}
			}
		}
		for _, el := range t {
			payloads = append(payloads, findBasicMessagePayloads(el)...)
		}
	case etf.List:
		for _, el := range t {
			payloads = append(payloads, findBasicMessagePayloads(el)...)
		}
	}
	return payloads
}

// ---------------------------------------------------------------------------
// Test fixture builders for synthetic ETF terms.
// ---------------------------------------------------------------------------

// encodeETFTerm serialises an ETF value to a byte slice using the same encoding
// the RabbitMQ broker uses when writing quorum queue meta files and WAL segments.
func encodeETFTerm(term any) ([]byte, error) {
	var ctx etf.Context
	var buf bytes.Buffer
	if err := ctx.Encoder(&buf).Encode(term); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// makeBasicMessageTerm returns the ETF structure matching the RabbitMQ 4.x mc_amqpl format:
//
//	{'$usr', Meta, {e, Seq, {mc, mc_amqpl, {content, ClassId, none, Props, Module, [Body]}, Annots}}}
func makeBasicMessageTerm(body []byte) etf.Tuple {
	contentTuple := etf.Tuple{
		etf.Atom("content"),
		int64(60), // class ID for basic
		etf.Atom("none"),
		[]byte{}, // encoded properties binary
		etf.Atom("rabbit_framing_amqp_0_9_1"),
		etf.List{body}, // payload list
	}
	mcTuple := etf.Tuple{
		etf.Atom("mc"),
		etf.Atom("mc_amqpl"),
		contentTuple,
		etf.Atom("annotations"),
	}
	cmd := etf.Tuple{
		etf.Atom("e"),
		int64(1),
		mcTuple,
	}
	return etf.Tuple{
		etf.Atom("$usr"),
		etf.Atom("meta"),
		cmd,
	}
}
