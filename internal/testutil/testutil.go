// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"github.com/sassoftware/argus/internal/broker"
)

// TestHealthServer creates a test server that routes the three management API
// endpoints used by EvaluateQueueHealth. It is shared by the broker and monitor
// packages so both can exercise the health-check logic against a fake
// RabbitMQ Management API without duplicating the HTTP test double.
func TestHealthServer(
	nodes []broker.RabbitNode,
	queue *broker.RabbitQueue,
	queueStatus int, // HTTP status for the specific-queue endpoint; 0 means use queue value
	allQueues []broker.RabbitQueue,
) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := effectivePath(r)
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

// effectivePath returns the raw (percent-encoded) path when available,
// falling back to the decoded URL.Path. This lets the handler above match
// the actual endpoint strings built by the broker package's HTTP client.
func effectivePath(r *http.Request) string {
	if r.URL.RawPath != "" {
		return r.URL.RawPath
	}
	return r.URL.Path
}
