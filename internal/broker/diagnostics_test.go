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
		{Name: "rabbit@n1", Running: true},
		{Name: "rabbit@n2", Running: true},
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
	queue := RabbitQueue{Name: "target.q", VHost: "/", Type: "quorum", Status: "running", Node: "rabbit@n1"}
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

func newTestManager(server *httptest.Server) *DiagnosticsManager {
	return &DiagnosticsManager{
		APIURL:   server.URL,
		Username: "test",
		Password: "test",
		Client:   server.Client(),
	}
}
