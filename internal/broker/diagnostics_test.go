package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

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
	got, err := dm.FetchAllQueues(context.Background())
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
	_, err := dm.FetchAllQueues(context.Background())
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

func newTestManager(server *httptest.Server) *DiagnosticsManager {
	return &DiagnosticsManager{
		APIURL:   server.URL,
		Username: "test",
		Password: "test",
		Client:   server.Client(),
	}
}
