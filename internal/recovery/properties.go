// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Property flag bits for the AMQP 0-9-1 basic-properties wire format. This is
// the exact layout RabbitMQ uses both on the wire (content-header frame) and
// for the "encoded properties" binary stored alongside each message in a
// content tuple, so decoding it recovers headers and the rest of the original
// message properties (content-type, delivery-mode, correlation-id, etc.).
const (
	propFlagContentType     = 0x8000
	propFlagContentEncoding = 0x4000
	propFlagHeaders         = 0x2000
	propFlagDeliveryMode    = 0x1000
	propFlagPriority        = 0x0800
	propFlagCorrelationID   = 0x0400
	propFlagReplyTo         = 0x0200
	propFlagExpiration      = 0x0100
	propFlagMessageID       = 0x0080
	propFlagTimestamp       = 0x0040
	propFlagType            = 0x0020
	propFlagUserID          = 0x0010
	propFlagAppID           = 0x0008
	propFlagReserved1       = 0x0004 // deprecated cluster-id; not surfaced
)

// decodeBasicProperties parses a RabbitMQ "encoded properties" binary (the
// value carved out of field 4 of a {content,...} tuple) into an amqp.Publishing
// template with Headers and the other basic-properties populated; Body is left
// unset. A nil/short input (no properties present) yields a zero-value result.
func decodeBasicProperties(data []byte) (amqp.Publishing, error) {
	var p amqp.Publishing
	if len(data) < 2 {
		return p, nil
	}

	r := bytes.NewReader(data)
	var flags uint16
	if err := binary.Read(r, binary.BigEndian, &flags); err != nil {
		return p, fmt.Errorf("reading property flags: %w", err)
	}

	var err error
	if flags&propFlagContentType != 0 {
		if p.ContentType, err = readShortstr(r); err != nil {
			return p, fmt.Errorf("content-type: %w", err)
		}
	}
	if flags&propFlagContentEncoding != 0 {
		if p.ContentEncoding, err = readShortstr(r); err != nil {
			return p, fmt.Errorf("content-encoding: %w", err)
		}
	}
	if flags&propFlagHeaders != 0 {
		if p.Headers, err = readAMQPTable(r); err != nil {
			return p, fmt.Errorf("headers: %w", err)
		}
	}
	if flags&propFlagDeliveryMode != 0 {
		if err = binary.Read(r, binary.BigEndian, &p.DeliveryMode); err != nil {
			return p, fmt.Errorf("delivery-mode: %w", err)
		}
	}
	if flags&propFlagPriority != 0 {
		if err = binary.Read(r, binary.BigEndian, &p.Priority); err != nil {
			return p, fmt.Errorf("priority: %w", err)
		}
	}
	if flags&propFlagCorrelationID != 0 {
		if p.CorrelationId, err = readShortstr(r); err != nil {
			return p, fmt.Errorf("correlation-id: %w", err)
		}
	}
	if flags&propFlagReplyTo != 0 {
		if p.ReplyTo, err = readShortstr(r); err != nil {
			return p, fmt.Errorf("reply-to: %w", err)
		}
	}
	if flags&propFlagExpiration != 0 {
		if p.Expiration, err = readShortstr(r); err != nil {
			return p, fmt.Errorf("expiration: %w", err)
		}
	}
	if flags&propFlagMessageID != 0 {
		if p.MessageId, err = readShortstr(r); err != nil {
			return p, fmt.Errorf("message-id: %w", err)
		}
	}
	if flags&propFlagTimestamp != 0 {
		var sec int64
		if err = binary.Read(r, binary.BigEndian, &sec); err != nil {
			return p, fmt.Errorf("timestamp: %w", err)
		}
		p.Timestamp = time.Unix(sec, 0)
	}
	if flags&propFlagType != 0 {
		if p.Type, err = readShortstr(r); err != nil {
			return p, fmt.Errorf("type: %w", err)
		}
	}
	if flags&propFlagUserID != 0 {
		if p.UserId, err = readShortstr(r); err != nil {
			return p, fmt.Errorf("user-id: %w", err)
		}
	}
	if flags&propFlagAppID != 0 {
		if p.AppId, err = readShortstr(r); err != nil {
			return p, fmt.Errorf("app-id: %w", err)
		}
	}

	return p, nil
}

func readShortstr(r *bytes.Reader) (string, error) {
	length, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readLongstrBytes(r *bytes.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// readAMQPTable decodes an AMQP 0-9-1 field-table (used for both the top-level
// headers property and nested 'F' field values).
func readAMQPTable(r *bytes.Reader) (amqp.Table, error) {
	raw, err := readLongstrBytes(r)
	if err != nil {
		return nil, err
	}
	nested := bytes.NewReader(raw)
	table := make(amqp.Table)
	for nested.Len() > 0 {
		key, err := readShortstr(nested)
		if err != nil {
			return nil, err
		}
		value, err := readAMQPFieldValue(nested)
		if err != nil {
			return nil, err
		}
		table[key] = value
	}
	return table, nil
}

// readAMQPFieldValue decodes a single typed field-table value. The type tags
// and layouts mirror the AMQP 0-9-1 spec (and amqp091-go's read.go, which
// decodes the same wire format for live connections).
func readAMQPFieldValue(r *bytes.Reader) (any, error) {
	typ, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch typ {
	case 't':
		v, err := r.ReadByte()
		return v != 0, err
	case 'B':
		return r.ReadByte()
	case 'b':
		v, err := r.ReadByte()
		return int8(v), err //nolint:gosec
	case 's':
		var v int16
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	case 'u':
		var v uint16
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	case 'I':
		var v int32
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	case 'i':
		var v uint32
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	case 'l':
		var v int64
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	case 'f':
		var v float32
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	case 'd':
		var v float64
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	case 'D':
		var dec amqp.Decimal
		if err := binary.Read(r, binary.BigEndian, &dec.Scale); err != nil {
			return nil, err
		}
		err := binary.Read(r, binary.BigEndian, &dec.Value)
		return dec, err
	case 'S':
		b, err := readLongstrBytes(r)
		return string(b), err
	case 'x':
		var n int32
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, fmt.Errorf("negative byte-array length: %d", n)
		}
		buf := make([]byte, n)
		_, err := io.ReadFull(r, buf)
		return buf, err
	case 'T':
		var sec int64
		err := binary.Read(r, binary.BigEndian, &sec)
		return time.Unix(sec, 0), err
	case 'F':
		return readAMQPTable(r)
	case 'A':
		return readAMQPFieldArray(r)
	case 'V':
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported field-table type %q", typ)
	}
}

// readAMQPFieldArray decodes an 'A' field-table array: a 4-byte byte-length
// prefix followed by that many bytes of back-to-back field values.
func readAMQPFieldArray(r *bytes.Reader) ([]any, error) {
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	nested := bytes.NewReader(buf)
	var out []any
	for nested.Len() > 0 {
		v, err := readAMQPFieldValue(nested)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
