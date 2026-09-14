package main

import (
	"bytes"

	"github.com/DeedleFake/etf"
)

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

// makeQuorumQueueTerm returns the ETF structure that RabbitMQ stores in a
// quorum queue 'meta' file for the given vhost and queue name.
func makeQuorumQueueTerm(vhost, queueName string) etf.Tuple {
	return etf.Tuple{
		etf.Atom("rabbit_quorum_queue"),
		etf.Tuple{
			etf.Atom("resource"),
			[]byte(vhost),
			etf.Atom("queue"),
			[]byte(queueName),
		},
	}
}

// makeBasicMessageTerm returns the ETF structure matching the RabbitMQ 4.x mc_amqpl format:
//
//	{'$usr', Meta, {e, Seq, {mc, mc_amqpl, {content, ClassId, none, Props, Module, [Body]}, Annots}}}
func makeBasicMessageTerm(body []byte) etf.Tuple {
	contentTuple := etf.Tuple{
		etf.Atom("content"),
		int64(60), // class ID for basic
		etf.Atom("none"),
		[]byte{},                           // encoded properties binary
		etf.Atom("rabbit_framing_amqp_0_9_1"),
		etf.List{body},                     // payload list
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
