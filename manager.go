package main

import (
	"bytes"
	"context"
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

// RabbitNode represents the health metrics returned by /api/nodes
type RabbitNode struct {
	Name          string `json:"name"`
	Running       bool   `json:"running"`
	MemAlarm      bool   `json:"mem_alarm"`
	DiskFreeAlarm bool   `json:"disk_free_alarm"`
}

// RabbitQueue represents the state metrics returned by /api/queues
type RabbitQueue struct {
	Name    string   `json:"name"`
	VHost   string   `json:"vhost"`
	Type    string   `json:"type"`
	Status  string   `json:"status"`
	Leader  string   `json:"leader"`  // Specific to Quorum/Streams (Raft leader)
	Node    string   `json:"node"`    // Primary node hosting the process coordinator
	Members []string `json:"members"` // Raft cluster cluster nodes for this queue
}

type DiagnosticsManager struct {
	APIURL   string // e.g., "http://localhost:15672"
	Username string
	Password string
	Client   *http.Client
}

func NewDiagnosticsManager(apiURL, username, password string) *DiagnosticsManager {
	return &DiagnosticsManager{
		APIURL:   apiURL,
		Username: username,
		Password: password,
		Client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// EvaluateQueueHealth executes the full differential check to verify if a queue is dead
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
		allQueues, err := dm.fetchAllQueues(ctx)
		if err == nil {
			neighborsDownCount := 0
			neighborsTotal := 0

			for _, q := range allQueues {
				if q.Node == queue.Node && q.Name != queue.Name && q.Type == "quorum" {
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

// resolveQuorumBasePath fetches the first node name from the management API and derives the
// standard quorum storage path from it. Falls back to rabbit@localhost if the API is unreachable.
func (dm *DiagnosticsManager) resolveQuorumBasePath(ctx context.Context) string {
	const baseMnesia = "/var/lib/rabbitmq/mnesia"
	nodes, err := dm.fetchNodes(ctx)
	if err != nil || len(nodes) == 0 {
		log.Printf("Warning: could not resolve node name from API (%v); falling back to rabbit@localhost", err)
		return baseMnesia + "/rabbit@localhost/quorum/rabbit@localhost"
	}
	nodeName := nodes[0].Name
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
	defer resp.Body.Close()

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

func (dm *DiagnosticsManager) fetchNodes(ctx context.Context) ([]RabbitNode, error) {
	req, err := dm.newRequest(ctx, "GET", "/api/nodes")
	if err != nil {
		return nil, err
	}

	resp, err := dm.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var nodes []RabbitNode
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (dm *DiagnosticsManager) fetchAllQueues(ctx context.Context) ([]RabbitQueue, error) {
	req, err := dm.newRequest(ctx, "GET", "/api/queues")
	if err != nil {
		return nil, err
	}

	resp, err := dm.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var queues []RabbitQueue
	if err := json.NewDecoder(resp.Body).Decode(&queues); err != nil {
		return nil, err
	}
	return queues, nil
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
	defer resp.Body.Close()

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
