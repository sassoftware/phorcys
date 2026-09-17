package testware

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"github.com/DeedleFake/etf"
	"github.com/sassoftware/argus/internal/rabbitmq"
)

// EncodeETFTerm serialises an ETF value to a byte slice using the same encoding
// the RabbitMQ broker uses when writing quorum queue meta files and WAL segments.
func EncodeETFTerm(term any) ([]byte, error) {
	var ctx etf.Context
	var buf bytes.Buffer
	if err := ctx.Encoder(&buf).Encode(term); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MakeQuorumQueueTerm returns the ETF structure that RabbitMQ stores in a
// quorum queue 'meta' file for the given vhost and queue name.
func MakeQuorumQueueTerm(vhost, queueName string) etf.Tuple {
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

// MakeBasicMessageTerm returns the ETF structure matching the RabbitMQ 4.x mc_amqpl format:
//
//	{'$usr', Meta, {e, Seq, {mc, mc_amqpl, {content, ClassId, none, Props, Module, [Body]}, Annots}}}
func MakeBasicMessageTerm(body []byte) etf.Tuple {
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

// TestHealthServer creates a test server that routes the three management API
// endpoints used by EvaluateQueueHealth.
func TestHealthServer(
	nodes []rabbitmq.Node,
	queue *rabbitmq.Queue,
	queueStatus int, // HTTP status for the specific-queue endpoint; 0 means use queue value
	allQueues []rabbitmq.Queue,
) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := EffectivePath(r)
		switch {
		case path == "/api/nodes":
			json.NewEncoder(w).Encode(nodes)
		case path == "/api/queues":
			json.NewEncoder(w).Encode(allQueues)
		default:
			// Specific queue endpoint
			if queueStatus != 0 {
				w.WriteHeader(queueStatus)
				return
			}
			if queue != nil {
				json.NewEncoder(w).Encode(queue)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		}
	}))
}

// EffectivePath returns the raw (percent-encoded) path when available,
// falling back to the decoded URL.Path. This lets test handlers match
// the actual endpoint strings built by fetchSpecificQueue.
func EffectivePath(r *http.Request) string {
	if r.URL.RawPath != "" {
		return r.URL.RawPath
	}
	return r.URL.Path
}
