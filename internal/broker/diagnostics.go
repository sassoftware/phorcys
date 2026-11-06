// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type QueueHealth string

const (
	HealthGreen         QueueHealth = "GREEN"
	HealthTransient     QueueHealth = "TRANSIENT_DOWN" // Throttled or recovering; do not delete!
	HealthUnrecoverable QueueHealth = "UNRECOVERABLE"  // Isolated Raft failure; safe to run recovery
)

const QuorumType = "quorum"

type DiagnosticsManager struct {
	APIURL   string // e.g., "http://localhost:15672"
	Username string
	Password string
	Client   *http.Client
}

// NewDiagnosticsManager returns a client for the Management API. tlsCfg applies to
// https:// URLs only;
func NewDiagnosticsManager(apiURL, username, password string, tlsCfg *tls.Config) *DiagnosticsManager {
	client := &http.Client{Timeout: 10 * time.Second}
	if tlsCfg != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsCfg.Clone()
		client.Transport = transport
	}
	return &DiagnosticsManager{
		APIURL:   apiURL,
		Username: username,
		Password: password,
		Client:   client,
	}
}

// EvaluateQueueHealth executes the full differential check to verify if a queue is dead
//
//nolint:gocognit
func (dm *DiagnosticsManager) EvaluateQueueHealth(ctx context.Context, vhost, queueName string) (QueueHealth, error) {
	// 1. Fetch the general health state of all cluster nodes
	nodes, err := dm.fetchNodes(ctx)
	if err != nil {
		return "", fmt.Errorf("failed cluster node check: %w", err)
	}

	// Safety Gate A: If any live node has a memory or disk alarm active, the broker
	// intentionally halts queues. This is transient resource pressure.
	for _, node := range nodes {
		if node.MemAlarm || node.DiskFreeAlarm {
			return HealthTransient, nil
		}
	}

	// 2. Target the specific queue endpoint. If a quorum queue's internal state machine
	// is completely corrupted, the management API will frequently panic and throw a 500 error.
	queue, is500, err := dm.fetchSpecificQueue(ctx, vhost, queueName)
	if is500 {
		// If the cluster nodes are healthy but this specific queue process causes an internal 500,
		// it is almost certainly a fatal Raft supervisor crash loop.
		return HealthUnrecoverable, nil
	}
	if err != nil {
		return "", fmt.Errorf("failed targeted queue check: %w", err)
	}

	// Safety Gate B: Verify if the node currently hosting the target queue is actually alive
	var hostingNodeAlive bool
	for _, node := range nodes {
		if node.Name == queue.Node && node.Running {
			hostingNodeAlive = true
			break
		}
	}

	// If the host node is down or restarting, the queue's state is just waiting for the infrastructure
	if !hostingNodeAlive {
		return HealthTransient, nil
	}

	// Safety Gate C: If the queue has no Raft leader it is broken regardless of what the
	// management API reports for "status". A crashed ra process returns noproc on any
	// consumer operation while still showing status="running" in Mnesia metadata.
	if queue.Leader == "" || queue.Leader == "none" {
		// Let's verify if neighbor quorum queues on the same node are running fine.
		// Fetching the global queue list lets us check isolated health comparisons.
		allQueues, err := dm.FetchAllQueues(ctx)
		if err == nil {
			neighborsDownCount := 0
			neighborsTotal := 0

			for _, q := range allQueues {
				if q.Node == queue.Node && q.Name != queue.Name && q.Type == QuorumType {
					neighborsTotal++
					if q.Status == "down" {
						neighborsDownCount++
					}
				}
			}

			// If all other quorum queues on this node are down, the entire Raft storage sub-system
			// on the host node has likely locked up. Do not delete the queue yet.
			if neighborsTotal > 0 && neighborsDownCount == neighborsTotal {
				return HealthTransient, nil
			}
		}

		// The cluster is fine, the host node is fine, neighbor queues are fine,
		// but THIS queue has lost its leader entirely. It is corrupted.
		return HealthUnrecoverable, nil
	}

	// Safety Gate D: Active probe. The ra process may still appear registered to the management
	// API while being unable to serve requests (e.g. segment header corruption causes a crash
	// only on the first read attempt, returning noproc to the caller). POST a peek-get with
	// requeue=true so no messages are consumed. A 500 response confirms the queue is broken.
	if probeIs500 := dm.probeQueueGet(ctx, vhost, queueName); probeIs500 {
		return HealthUnrecoverable, nil
	}

	return HealthGreen, nil
}

// ============================================================================
// HTTP API HELPER METHODS
// ============================================================================

// ResolveQuorumBasePath fetches the name of the node serving the management API request
// (/api/overview "node") and derives the standard quorum storage path from it. /api/nodes is
// not used because it lists every cluster member in arbitrary order. Falls back to
// rabbit@localhost if the API is unreachable.
func (dm *DiagnosticsManager) ResolveQuorumBasePath(ctx context.Context) string {
	const baseMnesia = "/var/lib/rabbitmq/mnesia"
	overview, err := fetchJSON[struct {
		Node string `json:"node"`
	}](ctx, dm, "/api/overview", false)
	if err != nil || overview.Node == "" {
		log.Printf("Warning: could not resolve node name from API (%v); falling back to rabbit@localhost", err)
		return baseMnesia + "/rabbit@localhost/quorum/rabbit@localhost"
	}
	nodeName := overview.Node
	path := fmt.Sprintf("%s/%s/quorum/%s", baseMnesia, nodeName, nodeName)
	log.Printf("Resolved quorum base path: %s", path)
	return path
}

// probeQueueGet issues a management API peek-get (requeue=true, count=1) and returns true only if
// the server responds with HTTP 500, which indicates the ra process crashed during the read attempt.
func (dm *DiagnosticsManager) probeQueueGet(ctx context.Context, vhost, queueName string) bool {
	escapedVHost := url.PathEscape(vhost)
	escapedQueue := url.PathEscape(queueName)
	endpoint := fmt.Sprintf("/api/queues/%s/%s/get", escapedVHost, escapedQueue)

	body := bytes.NewBufferString(`{"count":1,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}`)
	req, err := http.NewRequestWithContext(ctx, "POST", dm.APIURL+endpoint, body)
	if err != nil {
		return false
	}
	req.SetBasicAuth(dm.Username, dm.Password)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := dm.Client.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()

	// A healthy queue returns 200. An unrecoverable queue returns 400 or 500 with
	// "noproc" in the body — the ra process crashed during the read attempt.
	if resp.StatusCode == http.StatusOK {
		return false
	}
	errBody, _ := io.ReadAll(resp.Body)
	return strings.Contains(string(errBody), "noproc")
}

func (dm *DiagnosticsManager) newRequest(ctx context.Context, method, endpoint string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, dm.APIURL+endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(dm.Username, dm.Password)
	req.Header.Set("Accept", "application/json")
	return req, nil
}

// fetchJSON issues an authenticated GET to endpoint and JSON-decodes the
// response body into a new T. useNumber preserves the int-vs-float
// distinction in numeric fields (e.g. x-max-length) instead of silently
// decoding them all as float64.
func fetchJSON[T any](ctx context.Context, dm *DiagnosticsManager, endpoint string, useNumber bool) (T, error) {
	var out T
	req, err := dm.newRequest(ctx, http.MethodGet, endpoint)
	if err != nil {
		return out, err
	}

	resp, err := dm.Client.Do(req)
	if err != nil {
		return out, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	dec := json.NewDecoder(resp.Body)
	if useNumber {
		dec.UseNumber()
	}
	if err := dec.Decode(&out); err != nil {
		return out, fmt.Errorf("decoding response: %w", err)
	}
	return out, nil
}

func (dm *DiagnosticsManager) fetchNodes(ctx context.Context) ([]RabbitNode, error) {
	return fetchJSON[[]RabbitNode](ctx, dm, "/api/nodes", false)
}

func (dm *DiagnosticsManager) FetchAllQueues(ctx context.Context) ([]RabbitQueue, error) {
	return fetchJSON[[]RabbitQueue](ctx, dm, "/api/queues", false)
}

// FetchQueueMembers returns the nodes hosting a replica of the queue, per
// /api/queues/{vhost}/{queue}. It returns an error if the endpoint fails,
// returns 500, or reports no members.
func (dm *DiagnosticsManager) FetchQueueMembers(ctx context.Context, vhost, queueName string) ([]string, error) {
	queue, is500, err := dm.fetchSpecificQueue(ctx, vhost, queueName)
	if err != nil {
		return nil, fmt.Errorf("failed queue members check: %w", err)
	}
	if is500 {
		return nil, fmt.Errorf("failed queue members check: management API returned 500 for %s/%s", vhost, queueName)
	}
	if len(queue.Members) == 0 {
		return nil, fmt.Errorf("failed queue members check: no members reported for %s/%s", vhost, queueName)
	}
	return queue.Members, nil
}

// fetchSpecificQueue targets one precise endpoint and safely handles internal 500 crashes
func (dm *DiagnosticsManager) fetchSpecificQueue(ctx context.Context, vhost, queueName string) (*RabbitQueue, bool, error) {
	// Virtual hosts must be URL encoded safely (e.g., "/" becomes "%2F")
	escapedVHost := url.PathEscape(vhost)
	escapedQueue := url.PathEscape(queueName)
	endpoint := fmt.Sprintf("/api/queues/%s/%s", escapedVHost, escapedQueue)

	req, err := dm.newRequest(ctx, "GET", endpoint)
	if err != nil {
		return nil, false, err
	}

	resp, err := dm.Client.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = resp.Body.Close() }()

	// Capture explicit 500 status codes caused by internal Erlang process panics
	if resp.StatusCode == http.StatusInternalServerError {
		return nil, true, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, false, fmt.Errorf("api error code %d: %s", resp.StatusCode, string(body))
	}

	var queue RabbitQueue
	if err := json.NewDecoder(resp.Body).Decode(&queue); err != nil {
		return nil, false, err
	}

	return &queue, false, nil
}
