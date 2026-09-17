package broker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// deleteQueue (HTTP DELETE helper)
// ---------------------------------------------------------------------------

func TestDeleteQueue_NoContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	if err := dm.deleteQueue(context.Background(), "/api/queues/%2F/q"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDeleteQueue_OKStatusAlsoSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	if err := dm.deleteQueue(context.Background(), "/api/queues/%2F/q"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDeleteQueue_ServerErrorReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	err := dm.deleteQueue(context.Background(), "/api/queues/%2F/q")
	if err == nil {
		t.Error("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should mention status code, got: %v", err)
	}
}

func TestDeleteQueue_ForbiddenReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	dm := &DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	err := dm.deleteQueue(context.Background(), "/api/queues/%2F/q")
	if err == nil {
		t.Error("expected error for 403 response")
	}
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
	dm.deleteQueue(context.Background(), "/api/queues/%2F/q")
	if receivedUser != "myuser" || receivedPass != "mypass" {
		t.Errorf("basic auth: got user=%q pass=%q", receivedUser, receivedPass)
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
	if err := dm.DeleteOrForceEvict(context.Background(), "/", "q"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
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
	if err == nil {
		t.Error("expected error when both HTTP and Erlang eviction fail")
	}
	if !strings.Contains(err.Error(), "both standard and force-deletion failed") {
		t.Errorf("unexpected error message: %v", err)
	}
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
	dm.DeleteOrForceEvict(context.Background(), "/", "my queue")

	if !strings.Contains(receivedPath, "%2F") {
		t.Errorf("expected %%2F for vhost '/', got path: %q", receivedPath)
	}
	if !strings.Contains(receivedPath, "my+queue") && !strings.Contains(receivedPath, "my%20queue") {
		t.Errorf("expected encoded space in queue name, got path: %q", receivedPath)
	}
}
