// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// deleteQueue (HTTP DELETE helper)
// ---------------------------------------------------------------------------

func TestDeleteQueue_NoContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			assert.Equal(t, http.MethodDelete, r.Method, "expected DELETE, got %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	if err := dm.deleteQueue(context.Background(), "/api/queues/%2F/q"); err != nil {
		assert.NoError(t, err, "unexpected error: %v", err)
	}
}

func TestDeleteQueue_OKStatusAlsoSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	if err := dm.deleteQueue(context.Background(), "/api/queues/%2F/q"); err != nil {
		assert.NoError(t, err, "unexpected error: %v", err)
	}
}

func TestDeleteQueue_ServerErrorReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	err := dm.deleteQueue(context.Background(), "/api/queues/%2F/q")
	require.Error(t, err, "expected error for 500 response")
	if !strings.Contains(err.Error(), "500") {
		assert.Contains(t, err.Error(), "500", "error should mention status code, got: %v", err)
	}
}

func TestDeleteQueue_ForbiddenReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	err := dm.deleteQueue(context.Background(), "/api/queues/%2F/q")
	require.Error(t, err, "expected error for 403 response")
}

func TestDeleteQueue_SetsBasicAuth(t *testing.T) {
	var receivedUser, receivedPass string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUser, receivedPass, _ = r.BasicAuth()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{
		APIURL:   server.URL,
		Username: "myuser",
		Password: "mypass",
		Client:   server.Client(),
	}
	err := dm.deleteQueue(context.Background(), "/api/queues/%2F/q")
	assert.NoError(t, err)
	if receivedUser != "myuser" || receivedPass != "mypass" {
		assert.Equal(t, "myuser", receivedUser, "basic auth: got user=%q pass=%q", receivedUser, receivedPass)
		assert.Equal(t, "mypass", receivedPass, "basic auth: got user=%q pass=%q", receivedUser, receivedPass)
	}
}

// ---------------------------------------------------------------------------
// DeleteOrForceEvict
// ---------------------------------------------------------------------------

func TestDeleteOrForceEvict_HTTPDeleteSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	err := dm.DeleteOrForceEvict(context.Background(), "/", "q")
	assert.NoError(t, err)
}

func TestDeleteOrForceEvict_FallsBackToErlangWhenHTTPFails(t *testing.T) {
	// HTTP always returns 500, so deleteQueue will fail.
	// forceDeleteQueue then runs rabbitmqctl which is not installed in the test
	// environment — we just verify that the error message clearly indicates
	// both strategies were attempted.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	err := dm.DeleteOrForceEvict(context.Background(), "/", "q")
	// We expect an error because rabbitmqctl is not available in the test env.
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "both standard and force-deletion failed")
}

func TestDeleteOrForceEvict_URLEncodesVhostAndQueue(t *testing.T) {
	var receivedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		if r.URL.RawPath != "" {
			receivedPath = r.URL.RawPath
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	err := dm.DeleteOrForceEvict(context.Background(), "/", "my queue")
	assert.NoError(t, err)

	if !strings.Contains(receivedPath, "%2F") {
		assert.Contains(t, receivedPath, "%2F", "expected %%2F for vhost '/', got path: %q", receivedPath)
	}
	if !strings.Contains(receivedPath, "my+queue") && !strings.Contains(receivedPath, "my%20queue") {
		assert.True(t, strings.Contains(receivedPath, "my+queue") || strings.Contains(receivedPath, "my%20queue"), "expected encoded space in queue name, got path: %q", receivedPath)
	}
}
