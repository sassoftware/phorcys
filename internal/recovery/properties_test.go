// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"testing"

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
	b = append(b, byte(len(ct)))
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
	b = append(b, byte(flags>>8), byte(flags))

	// content-type: shortstr
	b = append(b, byte(len(contentType)))
	b = append(b, contentType...)

	// headers: longstr containing one packed key/value pair (type 'S' longstr)
	var table []byte
	table = append(table, byte(len(headerKey)))
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
