// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cluster node names used by the fake management API responses below.
const (
	testNode1 = "rabbit@n1"
	testNode2 = "rabbit@n2"
)

// ---------------------------------------------------------------------------
// NewDiagnosticsManager
// ---------------------------------------------------------------------------

func TestNewDiagnosticsManager_SetsFields(t *testing.T) {
	dm := NewDiagnosticsManager("http://host:15672", "admin", "secret")
	if dm.APIURL != "http://host:15672" {
		assert.Equal(t, "http://host:15672", dm.APIURL, "APIURL = %q", dm.APIURL)
	}
	if dm.Username != "admin" || dm.Password != "secret" {
		assert.Equal(t, "admin", dm.Username, "credentials not set correctly")
		assert.Equal(t, "secret", dm.Password, "credentials not set correctly")
	}
	assert.NotNil(t, dm.Client, "Client must not be nil")
	if dm.Client.Timeout != 10*time.Second {
		assert.Equal(t, 10*time.Second, dm.Client.Timeout, "Client.Timeout = %v, want 10s", dm.Client.Timeout)
	}
}

// ---------------------------------------------------------------------------
// fetchNodes
// ---------------------------------------------------------------------------

func TestFetchNodes_Success(t *testing.T) {
	nodes := []RabbitNode{
		{Name: testNode1, Running: true},
		{Name: testNode2, Running: true},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode(nodes)
		require.NoError(t, err)
	}))
	defer server.Close()

	dm := newTestManager(server)
	got, err := dm.fetchNodes(context.Background())
	require.NoError(t, err, "unexpected error: %v", err)
	if len(got) != 2 {
		assert.Len(t, got, 2, "expected 2 nodes, got %d", len(got))
	}
}

func TestFetchNodes_NonOKStatusReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	dm := newTestManager(server)
	_, err := dm.fetchNodes(context.Background())
	require.Error(t, err, "expected error for non-200 status")
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
		err := json.NewEncoder(w).Encode(queues)
		require.NoError(t, err)
	}))
	defer server.Close()

	dm := newTestManager(server)
	got, err := dm.FetchAllQueues(context.Background())
	require.NoError(t, err, "unexpected error: %v", err)
	if len(got) != 2 {
		assert.Len(t, got, 2, "expected 2 queues, got %d", len(got))
	}
}

func TestFetchAllQueues_NonOKStatusReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	dm := newTestManager(server)
	_, err := dm.FetchAllQueues(context.Background())
	require.Error(t, err, "expected error for non-200 status")
}

// ---------------------------------------------------------------------------
// fetchSpecificQueue
// ---------------------------------------------------------------------------

func TestFetchSpecificQueue_Success(t *testing.T) {
	queue := RabbitQueue{Name: "target.q", VHost: "/", Type: "quorum", Status: "running", Node: testNode1}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := json.NewEncoder(w).Encode(queue)
		require.NoError(t, err)
	}))
	defer server.Close()

	dm := newTestManager(server)
	got, is500, err := dm.fetchSpecificQueue(context.Background(), "/", "target.q")
	require.NoError(t, err, "unexpected error=%v is500=%v", err, is500)
	require.False(t, is500, "unexpected error=%v is500=%v", err, is500)
	if got.Name != "target.q" {
		assert.Equal(t, "target.q", got.Name, "Name = %q, want %q", got.Name, "target.q")
	}
}

func TestFetchSpecificQueue_Returns500Flag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dm := newTestManager(server)
	got, is500, err := dm.fetchSpecificQueue(context.Background(), "/", "broken.q")
	require.NoError(t, err, "unexpected error: %v", err)
	assert.True(t, is500, "expected is500=true")
	assert.Nil(t, got, "expected nil queue on 500")
}

func TestFetchSpecificQueue_NonOKNon500ReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	dm := newTestManager(server)
	_, is500, err := dm.fetchSpecificQueue(context.Background(), "/", "gone.q")
	require.Error(t, err, "expected error for 404")
	assert.False(t, is500, "is500 must be false for 404")
}

// ---------------------------------------------------------------------------
// ResolveQuorumBasePath
// ---------------------------------------------------------------------------

func TestResolveQuorumBasePath_UsesFirstNodeName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]RabbitNode{{Name: testNode1}, {Name: testNode2}}))
	}))
	defer server.Close()

	got := newTestManager(server).ResolveQuorumBasePath(context.Background())

	assert.Equal(t, "/var/lib/rabbitmq/mnesia/rabbit@n1/quorum/rabbit@n1", got)
}

func TestResolveQuorumBasePath_FallsBackOnAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	got := newTestManager(server).ResolveQuorumBasePath(context.Background())

	assert.Equal(t, "/var/lib/rabbitmq/mnesia/rabbit@localhost/quorum/rabbit@localhost", got)
}

func TestResolveQuorumBasePath_FallsBackOnEmptyNodeList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("[]"))
	}))
	defer server.Close()

	got := newTestManager(server).ResolveQuorumBasePath(context.Background())

	assert.Equal(t, "/var/lib/rabbitmq/mnesia/rabbit@localhost/quorum/rabbit@localhost", got)
}

// ---------------------------------------------------------------------------
// probeQueueGet
// ---------------------------------------------------------------------------

func TestProbeQueueGet(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"healthy 200", http.StatusOK, "[]", false},
		{"500 with noproc", http.StatusInternalServerError, `{"reason":"noproc"}`, true},
		{"400 with noproc", http.StatusBadRequest, `{"reason":"noproc"}`, true},
		{"500 without noproc", http.StatusInternalServerError, "boom", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/api/queues/%2F/q1/get", r.URL.EscapedPath())
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			assert.Equal(t, tt.want, newTestManager(server).probeQueueGet(context.Background(), "/", "q1"))
		})
	}
}

func TestProbeQueueGet_TransportAndRequestErrorsReturnFalse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	dm := newTestManager(server)
	server.Close() // connection refused

	assert.False(t, dm.probeQueueGet(context.Background(), "/", "q1"))

	dm.APIURL = "http://bad host/" // invalid URL -> NewRequest error
	assert.False(t, dm.probeQueueGet(context.Background(), "/", "q1"))
}

// ---------------------------------------------------------------------------
// request / transport / decode error paths
// ---------------------------------------------------------------------------

func TestFetchers_InvalidJSONReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer server.Close()
	dm := newTestManager(server)

	_, err := dm.fetchNodes(context.Background())
	require.Error(t, err)
	_, err = dm.FetchAllQueues(context.Background())
	require.Error(t, err)
	_, _, err = dm.fetchSpecificQueue(context.Background(), "/", "q")
	require.Error(t, err)
}

func TestFetchers_TransportErrorReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	dm := newTestManager(server)
	server.Close()

	_, err := dm.fetchNodes(context.Background())
	require.Error(t, err)
	_, err = dm.FetchAllQueues(context.Background())
	require.Error(t, err)
	_, is500, err := dm.fetchSpecificQueue(context.Background(), "/", "q")
	require.Error(t, err)
	assert.False(t, is500)
}

func TestFetchers_InvalidURLReturnsError(t *testing.T) {
	dm := &DiagnosticsManager{APIURL: "http://bad host/", Client: http.DefaultClient}

	_, err := dm.fetchNodes(context.Background())
	require.Error(t, err)
	_, err = dm.FetchAllQueues(context.Background())
	require.Error(t, err)
	_, _, err = dm.fetchSpecificQueue(context.Background(), "/", "q")
	require.Error(t, err)
}

func newTestManager(server *httptest.Server) *DiagnosticsManager {
	return &DiagnosticsManager{
		APIURL:   server.URL,
		Username: "test",
		Password: "test",
		Client:   server.Client(),
	}
}
