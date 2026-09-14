package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// effectivePath returns the raw (percent-encoded) path when available,
// falling back to the decoded URL.Path. This lets test handlers match
// the actual endpoint strings built by fetchSpecificQueue.
func effectivePath(r *http.Request) string {
	if r.URL.RawPath != "" {
		return r.URL.RawPath
	}
	return r.URL.Path
}

func newTestManager(server *httptest.Server) *DiagnosticsManager {
	return &DiagnosticsManager{
		APIURL:   server.URL,
		Username: "test",
		Password: "test",
		Client:   server.Client(),
	}
}

// ---------------------------------------------------------------------------
// NewDiagnosticsManager
// ---------------------------------------------------------------------------

func TestNewDiagnosticsManager_SetsFields(t *testing.T) {
	dm := NewDiagnosticsManager("http://host:15672", "admin", "secret")
	if dm.APIURL != "http://host:15672" {
		t.Errorf("APIURL = %q", dm.APIURL)
	}
	if dm.Username != "admin" || dm.Password != "secret" {
		t.Errorf("credentials not set correctly")
	}
	if dm.Client == nil {
		t.Error("Client must not be nil")
	}
	if dm.Client.Timeout != 10*time.Second {
		t.Errorf("Client.Timeout = %v, want 10s", dm.Client.Timeout)
	}
}

// ---------------------------------------------------------------------------
// fetchNodes
// ---------------------------------------------------------------------------

func TestFetchNodes_Success(t *testing.T) {
	nodes := []RabbitNode{
		{Name: "rabbit@n1", Running: true},
		{Name: "rabbit@n2", Running: true},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(nodes)
	}))
	defer server.Close()

	dm := newTestManager(server)
	got, err := dm.fetchNodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(got))
	}
}

func TestFetchNodes_NonOKStatusReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	dm := newTestManager(server)
	_, err := dm.fetchNodes(context.Background())
	if err == nil {
		t.Error("expected error for non-200 status")
	}
}

// ---------------------------------------------------------------------------
// fetchAllQueues
// ---------------------------------------------------------------------------

func TestFetchAllQueues_Success(t *testing.T) {
	queues := []RabbitQueue{
		{Name: "q1", VHost: "/", Type: "quorum"},
		{Name: "q2", VHost: "/", Type: "classic"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(queues)
	}))
	defer server.Close()

	dm := newTestManager(server)
	got, err := dm.fetchAllQueues(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 queues, got %d", len(got))
	}
}

func TestFetchAllQueues_NonOKStatusReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	dm := newTestManager(server)
	_, err := dm.fetchAllQueues(context.Background())
	if err == nil {
		t.Error("expected error for non-200 status")
	}
}

// ---------------------------------------------------------------------------
// fetchSpecificQueue
// ---------------------------------------------------------------------------

func TestFetchSpecificQueue_Success(t *testing.T) {
	queue := RabbitQueue{Name: "target.q", VHost: "/", Type: "quorum", Status: "running", Node: "rabbit@n1"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(queue)
	}))
	defer server.Close()

	dm := newTestManager(server)
	got, is500, err := dm.fetchSpecificQueue(context.Background(), "/", "target.q")
	if err != nil || is500 {
		t.Fatalf("unexpected error=%v is500=%v", err, is500)
	}
	if got.Name != "target.q" {
		t.Errorf("Name = %q, want %q", got.Name, "target.q")
	}
}

func TestFetchSpecificQueue_Returns500Flag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dm := newTestManager(server)
	got, is500, err := dm.fetchSpecificQueue(context.Background(), "/", "broken.q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !is500 {
		t.Error("expected is500=true")
	}
	if got != nil {
		t.Error("expected nil queue on 500")
	}
}

func TestFetchSpecificQueue_NonOKNon500ReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	dm := newTestManager(server)
	_, is500, err := dm.fetchSpecificQueue(context.Background(), "/", "gone.q")
	if err == nil {
		t.Error("expected error for 404")
	}
	if is500 {
		t.Error("is500 must be false for 404")
	}
}

// ---------------------------------------------------------------------------
// EvaluateQueueHealth
// ---------------------------------------------------------------------------

// testHealthServer creates a test server that routes the three management API
// endpoints used by EvaluateQueueHealth.
func testHealthServer(
	nodes []RabbitNode,
	queue *RabbitQueue,
	queueStatus int, // HTTP status for the specific-queue endpoint; 0 means use queue value
	allQueues []RabbitQueue,
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

func TestEvaluateQueueHealth_Green(t *testing.T) {
	nodes := []RabbitNode{{Name: "rabbit@n1", Running: true}}
	queue := &RabbitQueue{
		Name: "healthy.q", VHost: "/", Type: "quorum",
		Status: "running", Leader: "rabbit@n1", Node: "rabbit@n1",
	}
	server := testHealthServer(nodes, queue, 0, nil)
	defer server.Close()

	health, err := newTestManager(server).EvaluateQueueHealth(context.Background(), "/", "healthy.q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != HealthGreen {
		t.Errorf("health = %v, want %v", health, HealthGreen)
	}
}

func TestEvaluateQueueHealth_Transient_MemAlarm(t *testing.T) {
	nodes := []RabbitNode{{Name: "rabbit@n1", Running: true, MemAlarm: true}}
	server := testHealthServer(nodes, nil, 0, nil)
	defer server.Close()

	health, err := newTestManager(server).EvaluateQueueHealth(context.Background(), "/", "any.q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != HealthTransient {
		t.Errorf("health = %v, want %v", health, HealthTransient)
	}
}

func TestEvaluateQueueHealth_Transient_DiskAlarm(t *testing.T) {
	nodes := []RabbitNode{{Name: "rabbit@n1", Running: true, DiskFreeAlarm: true}}
	server := testHealthServer(nodes, nil, 0, nil)
	defer server.Close()

	health, err := newTestManager(server).EvaluateQueueHealth(context.Background(), "/", "any.q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != HealthTransient {
		t.Errorf("health = %v, want %v", health, HealthTransient)
	}
}

func TestEvaluateQueueHealth_Unrecoverable_Queue500(t *testing.T) {
	nodes := []RabbitNode{{Name: "rabbit@n1", Running: true}}
	server := testHealthServer(nodes, nil, http.StatusInternalServerError, nil)
	defer server.Close()

	health, err := newTestManager(server).EvaluateQueueHealth(context.Background(), "/", "crashed.q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != HealthUnrecoverable {
		t.Errorf("health = %v, want %v", health, HealthUnrecoverable)
	}
}

func TestEvaluateQueueHealth_Transient_HostNodeDown(t *testing.T) {
	nodes := []RabbitNode{{Name: "rabbit@n1", Running: false}}
	queue := &RabbitQueue{
		Name: "q1", VHost: "/", Type: "quorum",
		Status: "running", Node: "rabbit@n1",
	}
	server := testHealthServer(nodes, queue, 0, nil)
	defer server.Close()

	health, err := newTestManager(server).EvaluateQueueHealth(context.Background(), "/", "q1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != HealthTransient {
		t.Errorf("health = %v, want %v", health, HealthTransient)
	}
}

func TestEvaluateQueueHealth_Unrecoverable_NoLeaderHealthyNeighbors(t *testing.T) {
	nodes := []RabbitNode{{Name: "rabbit@n1", Running: true}}
	queue := &RabbitQueue{
		Name: "dead.q", VHost: "/", Type: "quorum",
		Status: "down", Leader: "", Node: "rabbit@n1",
	}
	allQueues := []RabbitQueue{
		*queue,
		{Name: "alive.q", VHost: "/", Type: "quorum", Status: "running", Node: "rabbit@n1"},
	}
	server := testHealthServer(nodes, queue, 0, allQueues)
	defer server.Close()

	health, err := newTestManager(server).EvaluateQueueHealth(context.Background(), "/", "dead.q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != HealthUnrecoverable {
		t.Errorf("health = %v, want %v", health, HealthUnrecoverable)
	}
}

func TestEvaluateQueueHealth_Transient_AllNeighborsDown(t *testing.T) {
	nodes := []RabbitNode{{Name: "rabbit@n1", Running: true}}
	queue := &RabbitQueue{
		Name: "q1", VHost: "/", Type: "quorum",
		Status: "down", Leader: "", Node: "rabbit@n1",
	}
	allQueues := []RabbitQueue{
		*queue,
		{Name: "q2", VHost: "/", Type: "quorum", Status: "down", Node: "rabbit@n1"},
	}
	server := testHealthServer(nodes, queue, 0, allQueues)
	defer server.Close()

	health, err := newTestManager(server).EvaluateQueueHealth(context.Background(), "/", "q1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != HealthTransient {
		t.Errorf("health = %v, want %v", health, HealthTransient)
	}
}

func TestEvaluateQueueHealth_Unrecoverable_LeaderNone(t *testing.T) {
	nodes := []RabbitNode{{Name: "rabbit@n1", Running: true}}
	queue := &RabbitQueue{
		Name: "q1", VHost: "/", Type: "quorum",
		Status: "down", Leader: "none", Node: "rabbit@n1",
	}
	// no neighbors → neighborsTotal=0, so goes straight to Unrecoverable
	allQueues := []RabbitQueue{*queue}
	server := testHealthServer(nodes, queue, 0, allQueues)
	defer server.Close()

	health, err := newTestManager(server).EvaluateQueueHealth(context.Background(), "/", "q1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != HealthUnrecoverable {
		t.Errorf("health = %v, want %v", health, HealthUnrecoverable)
	}
}

func TestEvaluateQueueHealth_FetchNodesError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("boom"))
	}))
	defer server.Close()

	// /api/nodes returns 500 → fetchNodes returns error → EvaluateQueueHealth returns error
	_, err := newTestManager(server).EvaluateQueueHealth(context.Background(), "/", "q")
	if err == nil {
		t.Error("expected error when fetchNodes fails")
	}
	if !strings.Contains(err.Error(), "failed cluster node check") {
		t.Errorf("unexpected error message: %v", err)
	}
}
