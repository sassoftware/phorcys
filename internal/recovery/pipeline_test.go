// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unreachableAMQPURL is deliberately unroutable so amqp.Dial fails fast
// without a live broker, letting tests assert on non-fatal publish/republish
// failures without a network timeout.
const unreachableAMQPURL = "amqp://127.0.0.1:1/"

// noContentServer returns an httptest server that answers every request with
// 204, satisfying DeleteOrForceEvict's HTTP deletion path.
func noContentServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
}

// ---------------------------------------------------------------------------
// Prepare
// ---------------------------------------------------------------------------

func TestPrepare_LocatePhaseFailsReturnsError(t *testing.T) {
	cfg := runtime.Config{
		QuorumBasePath: t.TempDir(),
		BackupBaseDir:  t.TempDir(),
		AMQPURL:        unreachableAMQPURL,
	}

	_, err := Prepare(context.Background(), cfg, "/", "missing-queue")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "locate phase")
}

func TestPrepare_SuccessFindsPositionAndSurvivesPublishFailure(t *testing.T) {
	quorumBase := t.TempDir()
	writeConfigFile(t, quorumBase, "queue-uid-123", "/", "orders")
	queueDir := filepath.Join(quorumBase, "queue-uid-123")
	writeSegmentBackup(t, queueDir, "0000000000000001.segment", []segmentEntry{
		{idx: 5, term: 2, data: []byte("x")},
	})

	cfg := runtime.Config{
		QuorumBasePath: quorumBase,
		BackupBaseDir:  t.TempDir(),
		AMQPURL:        unreachableAMQPURL, // announce publish must be non-fatal
	}

	prep, err := Prepare(context.Background(), cfg, "/", "orders")
	require.NoError(t, err, "Prepare should succeed even when the announcement publish fails")
	require.NotNil(t, prep)
	assert.Equal(t, "queue-uid-123", prep.QueueUID)
	assert.DirExists(t, prep.BackupDir)
	assert.True(t, prep.PositionFound)
	assert.Equal(t, RaftPosition{Idx: 5, Term: 2}, prep.Position)
}

func TestPrepare_NoBackupDataLeavesPositionNotFound(t *testing.T) {
	quorumBase := t.TempDir()
	writeConfigFile(t, quorumBase, "queue-uid-456", "/", "empty-queue")

	cfg := runtime.Config{
		QuorumBasePath: quorumBase,
		BackupBaseDir:  t.TempDir(),
		AMQPURL:        unreachableAMQPURL,
	}

	prep, err := Prepare(context.Background(), cfg, "/", "empty-queue")
	require.NoError(t, err)
	assert.False(t, prep.PositionFound)
	assert.Equal(t, RaftPosition{}, prep.Position)
}

// ---------------------------------------------------------------------------
// Execute
// ---------------------------------------------------------------------------

func TestExecute_DeletePhaseFailsReturnsError(t *testing.T) {
	// Always-500 server: HTTP delete fails, and rabbitmqctl isn't installed in
	// the test environment, so DeleteOrForceEvict is guaranteed to fail too.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	cfg := runtime.Config{AMQPURL: unreachableAMQPURL}
	prep := &PreparedRecovery{Queue: "q", VHost: "/", BackupDir: t.TempDir(), WALBackupDir: t.TempDir()}

	err := Execute(context.Background(), cfg, dm, prep)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete phase")
}

func TestExecute_SegmentCarveFailsReturnsError(t *testing.T) {
	server := noContentServer(t)
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	cfg := runtime.Config{AMQPURL: unreachableAMQPURL}
	prep := &PreparedRecovery{
		Queue:        "q",
		VHost:        "/",
		BackupDir:    filepath.Join(t.TempDir(), "does-not-exist"),
		WALBackupDir: t.TempDir(),
	}

	err := Execute(context.Background(), cfg, dm, prep)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "segment carve phase")
}

func TestExecute_NoMessagesFoundSkipsRepublishAndSucceeds(t *testing.T) {
	server := noContentServer(t)
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}
	// AMQPURL is unreachable; if Execute tried to republish it would fail, so
	// a nil result here proves the zero-message republish phase was skipped.
	cfg := runtime.Config{AMQPURL: unreachableAMQPURL}
	prep := &PreparedRecovery{Queue: "q", VHost: "/", BackupDir: t.TempDir(), WALBackupDir: t.TempDir(), QueueUID: "uid"}

	err := Execute(context.Background(), cfg, dm, prep)
	assert.NoError(t, err)
}

func TestExecute_MessagesFoundAttemptsRepublish(t *testing.T) {
	server := noContentServer(t)
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}

	backupDir := t.TempDir()
	msg, err := encodeETFTerm(makeBasicMessageTerm([]byte("recovered payload")))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "0000000000000001.segment"), msg, 0600))

	cfg := runtime.Config{AMQPURL: unreachableAMQPURL} // republish must fail
	prep := &PreparedRecovery{Queue: "q", VHost: "/", BackupDir: backupDir, WALBackupDir: t.TempDir(), QueueUID: "uid"}

	err = Execute(context.Background(), cfg, dm, prep)
	require.Error(t, err, "carved messages should trigger a republish attempt against the unreachable broker")
	assert.Contains(t, err.Error(), "republish phase")
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

func TestRun_PropagatesPrepareError(t *testing.T) {
	cfg := runtime.Config{
		QuorumBasePath: t.TempDir(),
		BackupBaseDir:  t.TempDir(),
		AMQPURL:        unreachableAMQPURL,
	}

	err := Run(context.Background(), cfg, &broker.DiagnosticsManager{}, "/", "missing-queue")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "locate phase")
}

func TestRun_FullPipelineWithNoBackupDataSucceeds(t *testing.T) {
	quorumBase := t.TempDir()
	writeConfigFile(t, quorumBase, "queue-uid-789", "/", "orders")

	server := noContentServer(t)
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}

	cfg := runtime.Config{
		QuorumBasePath: quorumBase,
		BackupBaseDir:  t.TempDir(),
		AMQPURL:        unreachableAMQPURL, // announce + (skipped) republish must not block success
	}

	err := Run(context.Background(), cfg, dm, "/", "orders")
	assert.NoError(t, err)
}
