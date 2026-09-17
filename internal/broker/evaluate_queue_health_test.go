package broker_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sassoftware/argus/internal/broker"
	"github.com/sassoftware/argus/internal/testutil"
)

// ---------------------------------------------------------------------------
// EvaluateQueueHealth
// ---------------------------------------------------------------------------

func TestEvaluateQueueHealth_Green(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: "rabbit@n1", Running: true}}
	queue := &broker.RabbitQueue{
		Name: "healthy.q", VHost: "/", Type: "quorum",
		Status: "running", Leader: "rabbit@n1", Node: "rabbit@n1",
	}
	server := testutil.TestHealthServer(nodes, queue, 0, nil)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "healthy.q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != broker.HealthGreen {
		t.Errorf("health = %v, want %v", health, broker.HealthGreen)
	}
}

func TestEvaluateQueueHealth_Transient_MemAlarm(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: "rabbit@n1", Running: true, MemAlarm: true}}
	server := testutil.TestHealthServer(nodes, nil, 0, nil)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "any.q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != broker.HealthTransient {
		t.Errorf("health = %v, want %v", health, broker.HealthTransient)
	}
}

func TestEvaluateQueueHealth_Transient_DiskAlarm(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: "rabbit@n1", Running: true, DiskFreeAlarm: true}}
	server := testutil.TestHealthServer(nodes, nil, 0, nil)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "any.q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != broker.HealthTransient {
		t.Errorf("health = %v, want %v", health, broker.HealthTransient)
	}
}

func TestEvaluateQueueHealth_Unrecoverable_Queue500(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: "rabbit@n1", Running: true}}
	server := testutil.TestHealthServer(nodes, nil, http.StatusInternalServerError, nil)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "crashed.q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != broker.HealthUnrecoverable {
		t.Errorf("health = %v, want %v", health, broker.HealthUnrecoverable)
	}
}

func TestEvaluateQueueHealth_Transient_HostNodeDown(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: "rabbit@n1", Running: false}}
	queue := &broker.RabbitQueue{
		Name: "q1", VHost: "/", Type: "quorum",
		Status: "running", Node: "rabbit@n1",
	}
	server := testutil.TestHealthServer(nodes, queue, 0, nil)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "q1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != broker.HealthTransient {
		t.Errorf("health = %v, want %v", health, broker.HealthTransient)
	}
}

func TestEvaluateQueueHealth_Unrecoverable_NoLeaderHealthyNeighbors(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: "rabbit@n1", Running: true}}
	queue := &broker.RabbitQueue{
		Name: "dead.q", VHost: "/", Type: "quorum",
		Status: "down", Leader: "", Node: "rabbit@n1",
	}
	allQueues := []broker.RabbitQueue{
		*queue,
		{Name: "alive.q", VHost: "/", Type: "quorum", Status: "running", Node: "rabbit@n1"},
	}
	server := testutil.TestHealthServer(nodes, queue, 0, allQueues)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "dead.q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != broker.HealthUnrecoverable {
		t.Errorf("health = %v, want %v", health, broker.HealthUnrecoverable)
	}
}

func TestEvaluateQueueHealth_Transient_AllNeighborsDown(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: "rabbit@n1", Running: true}}
	queue := &broker.RabbitQueue{
		Name: "q1", VHost: "/", Type: "quorum",
		Status: "down", Leader: "", Node: "rabbit@n1",
	}
	allQueues := []broker.RabbitQueue{
		*queue,
		{Name: "q2", VHost: "/", Type: "quorum", Status: "down", Node: "rabbit@n1"},
	}
	server := testutil.TestHealthServer(nodes, queue, 0, allQueues)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "q1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != broker.HealthTransient {
		t.Errorf("health = %v, want %v", health, broker.HealthTransient)
	}
}

func TestEvaluateQueueHealth_Unrecoverable_LeaderNone(t *testing.T) {
	nodes := []broker.RabbitNode{{Name: "rabbit@n1", Running: true}}
	queue := &broker.RabbitQueue{
		Name: "q1", VHost: "/", Type: "quorum",
		Status: "down", Leader: "none", Node: "rabbit@n1",
	}
	// no neighbors → neighborsTotal=0, so goes straight to Unrecoverable
	allQueues := []broker.RabbitQueue{*queue}
	server := testutil.TestHealthServer(nodes, queue, 0, allQueues)
	defer server.Close()

	health, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "q1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health != broker.HealthUnrecoverable {
		t.Errorf("health = %v, want %v", health, broker.HealthUnrecoverable)
	}
}

func TestEvaluateQueueHealth_FetchNodesError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("boom"))
	}))
	defer server.Close()

	// /api/nodes returns 500 → fetchNodes returns error → EvaluateQueueHealth returns error
	_, err := newDiagnosticsManager(server).EvaluateQueueHealth(context.Background(), "/", "q")
	if err == nil {
		t.Error("expected error when fetchNodes fails")
	}
	if !strings.Contains(err.Error(), "failed cluster node check") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func newDiagnosticsManager(server *httptest.Server) *broker.DiagnosticsManager {
	return &broker.DiagnosticsManager{
		APIURL:   server.URL,
		Username: "test",
		Password: "test",
		Client:   server.Client(),
	}
}
