// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"bytes"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeBasicProperties_NilOrShortReturnsZeroValue(t *testing.T) {
	p, err := decodeBasicProperties(nil)
	require.NoError(t, err)
	assert.Empty(t, p.ContentType)
	assert.Nil(t, p.Headers)

	p, err = decodeBasicProperties([]byte{0x00})
	require.NoError(t, err)
	assert.Nil(t, p.Headers)
}

func TestDecodeBasicProperties_ContentTypeHeadersAndDeliveryMode(t *testing.T) {
	data := buildEncodedProperties(t, "text/plain", "x-custom", "hello", 2)

	p, err := decodeBasicProperties(data)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", p.ContentType)
	require.NotNil(t, p.Headers)
	assert.Equal(t, "hello", p.Headers["x-custom"])
	assert.Equal(t, uint8(2), p.DeliveryMode)
}

func TestDecodeBasicProperties_NoHeadersFlagLeavesHeadersNil(t *testing.T) {
	// flags with only content-type set, no headers field present.
	var b []byte
	b = append(b, 0x80, 0x00) // propFlagContentType only
	ct := []byte("application/json")
	b = append(b, byte(len(ct))) //nolint:gosec
	b = append(b, ct...)

	p, err := decodeBasicProperties(b)
	require.NoError(t, err)
	assert.Equal(t, "application/json", p.ContentType)
	assert.Nil(t, p.Headers)
}

// buildEncodedProperties constructs a RabbitMQ "encoded properties" binary
// (flags + content-type + a single-entry headers table + delivery-mode),
// matching the wire format decodeBasicProperties parses.
func buildEncodedProperties(t *testing.T, contentType, headerKey, headerVal string, deliveryMode byte) []byte {
	t.Helper()

	var b []byte
	flags := uint16(propFlagContentType | propFlagHeaders | propFlagDeliveryMode)
	b = append(b, byte(flags>>8), byte(flags)) //nolint:gosec

	// content-type: shortstr
	b = append(b, byte(len(contentType))) //nolint:gosec
	b = append(b, contentType...)

	// headers: longstr containing one packed key/value pair (type 'S' longstr)
	var table []byte
	table = append(table, byte(len(headerKey))) //nolint:gosec
	table = append(table, headerKey...)
	table = append(table, 'S')
	table = appendUint32(table, uint32(len(headerVal))) //nolint:gosec
	table = append(table, headerVal...)
	b = appendUint32(b, uint32(len(table))) //nolint:gosec
	b = append(b, table...)

	// delivery-mode: octet
	b = append(b, deliveryMode)

	return b
}

func TestDecodeBasicProperties_AllFlags(t *testing.T) {
	var b []byte
	flags := uint16(propFlagContentType | propFlagContentEncoding | propFlagHeaders |
		propFlagDeliveryMode | propFlagPriority | propFlagCorrelationID | propFlagReplyTo |
		propFlagExpiration | propFlagMessageID | propFlagTimestamp | propFlagType |
		propFlagUserID | propFlagAppID)
	b = append(b, byte(flags>>8), byte(flags)) //nolint:gosec

	b = appendShortstr(b, "text/plain") // content-type
	b = appendShortstr(b, "identity")   // content-encoding
	b = appendUint32(b, 0)              // headers: empty table
	b = append(b, 2)                    // delivery-mode
	b = append(b, 5)                    // priority
	b = appendShortstr(b, "corr-1")     // correlation-id
	b = appendShortstr(b, "reply-q")    // reply-to
	b = appendShortstr(b, "60000")      // expiration
	b = appendShortstr(b, "msg-1")      // message-id
	ts := time.Unix(1700000000, 0)
	tsBuf := make([]byte, 8)
	for i := range tsBuf {
		tsBuf[len(tsBuf)-1-i] = byte(ts.Unix() >> (8 * i)) //nolint:gosec
	}
	b = append(b, tsBuf...)                // timestamp
	b = appendShortstr(b, "order.created") // type
	b = appendShortstr(b, "alice")         // user-id
	b = appendShortstr(b, "svc-1")         // app-id

	p, err := decodeBasicProperties(b)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", p.ContentType)
	assert.Equal(t, "identity", p.ContentEncoding)
	assert.NotNil(t, p.Headers)
	assert.Equal(t, uint8(2), p.DeliveryMode)
	assert.Equal(t, uint8(5), p.Priority)
	assert.Equal(t, "corr-1", p.CorrelationId)
	assert.Equal(t, "reply-q", p.ReplyTo)
	assert.Equal(t, "60000", p.Expiration)
	assert.Equal(t, "msg-1", p.MessageId)
	assert.True(t, p.Timestamp.Equal(ts))
	assert.Equal(t, "order.created", p.Type)
	assert.Equal(t, "alice", p.UserId)
	assert.Equal(t, "svc-1", p.AppId)
}

func TestDecodeBasicProperties_TruncatedFieldReturnsError(t *testing.T) {
	tests := []struct {
		name  string
		flag  uint16
		extra []byte // valid bytes for a *different* flagged field, to make this one look short
	}{
		{"content-type", propFlagContentType, nil},
		{"content-encoding", propFlagContentEncoding, nil},
		{"headers", propFlagHeaders, nil},
		{"delivery-mode", propFlagDeliveryMode, nil},
		{"priority", propFlagPriority, nil},
		{"correlation-id", propFlagCorrelationID, nil},
		{"reply-to", propFlagReplyTo, nil},
		{"expiration", propFlagExpiration, nil},
		{"message-id", propFlagMessageID, nil},
		{"timestamp", propFlagTimestamp, nil},
		{"type", propFlagType, nil},
		{"user-id", propFlagUserID, nil},
		{"app-id", propFlagAppID, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := []byte{byte(tc.flag >> 8), byte(tc.flag)} //nolint:gosec
			_, err := decodeBasicProperties(b)
			assert.Error(t, err)
		})
	}
}

func appendShortstr(b []byte, s string) []byte {
	b = append(b, byte(len(s))) //nolint:gosec
	return append(b, s...)
}

func TestDecodeBasicProperties_TruncatedShortstrBodyErrors(t *testing.T) {
	// content-type flag set, length byte says 5 bytes follow but none do.
	b := []byte{byte(propFlagContentType >> 8), byte(propFlagContentType & 0xFF), 5}
	_, err := decodeBasicProperties(b)
	assert.Error(t, err)
}

func TestDecodeBasicProperties_TruncatedLongstrBodyErrors(t *testing.T) {
	// headers flag set, longstr length says 10 bytes but none follow.
	b := []byte{byte(propFlagHeaders >> 8), byte(propFlagHeaders & 0xFF)}
	b = appendUint32(b, 10)
	_, err := decodeBasicProperties(b)
	assert.Error(t, err)
}

func TestDecodeBasicProperties_HeadersTableKeyReadErrors(t *testing.T) {
	// headers flag set, table longstr declares 1 byte but the key's own
	// length prefix then demands more than is present.
	b := []byte{byte(propFlagHeaders >> 8), byte(propFlagHeaders & 0xFF)}
	b = appendUint32(b, 1)
	b = append(b, 5) // shortstr key length=5, but 0 bytes of key follow
	_, err := decodeBasicProperties(b)
	assert.Error(t, err)
}

func TestDecodeBasicProperties_HeadersTableValueReadErrors(t *testing.T) {
	// headers flag set, table has a valid key but an unsupported value type tag.
	var table []byte
	table = appendShortstr(table, "k")
	table = append(table, '?')

	b := []byte{byte(propFlagHeaders >> 8), byte(propFlagHeaders & 0xFF)}
	b = appendUint32(b, uint32(len(table))) //nolint:gosec
	b = append(b, table...)
	_, err := decodeBasicProperties(b)
	assert.Error(t, err)
}

func TestReadAMQPFieldValue_AllTypes(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want any
	}{
		{"bool-true", []byte{'t', 1}, true},
		{"bool-false", []byte{'t', 0}, false},
		{"byte", []byte{'B', 42}, byte(42)},
		{"signed-byte", []byte{'b', 0xFF}, int8(-1)},
		{"short", append([]byte{'s'}, 0x00, 0x05), int16(5)},
		{"unsigned-short", append([]byte{'u'}, 0xFF, 0xFF), uint16(65535)},
		{"long", append([]byte{'I'}, 0xFF, 0xFF, 0xFF, 0xFF), int32(-1)},
		{"unsigned-long", append([]byte{'i'}, 0x00, 0x00, 0x00, 0x07), uint32(7)},
		{"longlong", append([]byte{'l'}, 0, 0, 0, 0, 0, 0, 0, 9), int64(9)},
		{"float", append([]byte{'f'}, 0x3F, 0x80, 0x00, 0x00), float32(1)},
		{"double", append([]byte{'d'}, 0x3F, 0xF0, 0, 0, 0, 0, 0, 0), float64(1)},
		{"void", []byte{'V'}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(tc.data)
			got, err := readAMQPFieldValue(r)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestReadAMQPFieldValue_Decimal(t *testing.T) {
	data := []byte{'D', 2, 0, 0, 0, 100} // scale=2, value=100
	r := bytes.NewReader(data)
	got, err := readAMQPFieldValue(r)
	require.NoError(t, err)
	dec, ok := got.(amqp.Decimal)
	require.True(t, ok)
	assert.Equal(t, uint8(2), dec.Scale)
	assert.Equal(t, int32(100), dec.Value)
}

func TestReadAMQPFieldValue_LongString(t *testing.T) {
	var data []byte
	data = append(data, 'S')
	data = appendUint32(data, 5)
	data = append(data, "hello"...)
	r := bytes.NewReader(data)
	got, err := readAMQPFieldValue(r)
	require.NoError(t, err)
	assert.Equal(t, "hello", got)
}

func TestReadAMQPFieldValue_ByteArray(t *testing.T) {
	var data []byte
	data = append(data, 'x')
	data = appendUint32(data, 3)
	data = append(data, 1, 2, 3)
	r := bytes.NewReader(data)
	got, err := readAMQPFieldValue(r)
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3}, got)
}

func TestReadAMQPFieldValue_ByteArrayNegativeLengthErrors(t *testing.T) {
	var data []byte
	data = append(data, 'x')
	data = appendUint32(data, 0x80000000) // top bit set -> negative int32
	r := bytes.NewReader(data)
	_, err := readAMQPFieldValue(r)
	assert.Error(t, err)
}

func TestReadAMQPFieldValue_Timestamp(t *testing.T) {
	var data []byte
	data = append(data, 'T')
	data = appendUint64(data, 1700000000)
	r := bytes.NewReader(data)
	got, err := readAMQPFieldValue(r)
	require.NoError(t, err)
	ts, ok := got.(time.Time)
	require.True(t, ok)
	assert.Equal(t, int64(1700000000), ts.Unix())
}

func TestReadAMQPFieldValue_NestedTable(t *testing.T) {
	var inner []byte
	inner = appendShortstr(inner, "k")
	inner = append(inner, 'B', 9)

	var data []byte
	data = append(data, 'F')
	data = appendUint32(data, uint32(len(inner))) //nolint:gosec
	data = append(data, inner...)

	r := bytes.NewReader(data)
	got, err := readAMQPFieldValue(r)
	require.NoError(t, err)
	table, ok := got.(amqp.Table)
	require.True(t, ok)
	assert.Equal(t, byte(9), table["k"])
}

func TestReadAMQPFieldValue_UnsupportedTypeErrors(t *testing.T) {
	r := bytes.NewReader([]byte{'?'})
	_, err := readAMQPFieldValue(r)
	assert.Error(t, err)
}

func TestReadAMQPFieldValue_EmptyReaderErrors(t *testing.T) {
	r := bytes.NewReader(nil)
	_, err := readAMQPFieldValue(r)
	assert.Error(t, err)
}

func TestReadAMQPFieldValue_TruncatedValueErrors(t *testing.T) {
	// 'I' (int32) declares 4 bytes of payload but only 1 is present.
	r := bytes.NewReader([]byte{'I', 0x00})
	_, err := readAMQPFieldValue(r)
	assert.Error(t, err)
}

func TestReadAMQPFieldArray(t *testing.T) {
	var elems []byte
	elems = append(elems, 't', 1) // bool true
	elems = append(elems, 'B', 7) // byte 7

	var data []byte
	data = appendUint32(data, uint32(len(elems))) //nolint:gosec
	data = append(data, elems...)

	r := bytes.NewReader(data)
	got, err := readAMQPFieldArray(r)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, true, got[0])
	assert.Equal(t, byte(7), got[1])
}

func TestReadAMQPFieldArray_EmptyArray(t *testing.T) {
	data := appendUint32(nil, 0)
	r := bytes.NewReader(data)
	got, err := readAMQPFieldArray(r)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestReadAMQPFieldArray_TruncatedSizePrefixErrors(t *testing.T) {
	r := bytes.NewReader([]byte{0x00, 0x00}) // only 2 of 4 length bytes
	_, err := readAMQPFieldArray(r)
	assert.Error(t, err)
}

func TestReadAMQPFieldArray_TruncatedBodyErrors(t *testing.T) {
	data := appendUint32(nil, 10) // claims 10 bytes but body is empty
	r := bytes.NewReader(data)
	_, err := readAMQPFieldArray(r)
	assert.Error(t, err)
}

func TestReadAMQPFieldArray_InvalidElementErrors(t *testing.T) {
	elems := []byte{'?'}                          // unsupported type tag
	data := appendUint32(nil, uint32(len(elems))) //nolint:gosec
	data = append(data, elems...)
	r := bytes.NewReader(data)
	_, err := readAMQPFieldArray(r)
	assert.Error(t, err)
}
