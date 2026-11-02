// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sassoftware/phorcys/internal/amqpx/amqptest"
	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unreachableAMQPURL is deliberately unroutable so amqp.Dial fails fast
// without a live broker, letting tests assert on non-fatal publish/republish
// failures without a network timeout.
const unreachableAMQPURL = "amqp://127.0.0.1:1/"

const quorumType = "quorum"
const queueArg = "queue"
const testExchange = "orders-exchange"
const testRK = "orders.rk"

// noContentServer returns an httptest server that answers every request with
// 204, satisfying DeleteOrForceEvict's HTTP deletion path.
func noContentServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
}

// emptyDefinitionsServer returns an httptest server that answers every request
// with an empty /api/definitions/{vhost} export, satisfying Prepare's
// definition-fetch phase when a test doesn't care about its content.
func emptyDefinitionsServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"queues":[],"bindings":[]}`))
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

	_, err := Prepare(context.Background(), cfg, nil, "/", "missing-queue")
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

	server := emptyDefinitionsServer(t)
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}

	cfg := runtime.Config{
		QuorumBasePath: quorumBase,
		BackupBaseDir:  t.TempDir(),
		AMQPURL:        unreachableAMQPURL, // announce publish must be non-fatal
	}

	prep, err := Prepare(context.Background(), cfg, dm, "/", "orders")
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

	server := emptyDefinitionsServer(t)
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}

	cfg := runtime.Config{
		QuorumBasePath: quorumBase,
		BackupBaseDir:  t.TempDir(),
		AMQPURL:        unreachableAMQPURL,
	}

	prep, err := Prepare(context.Background(), cfg, dm, "/", "empty-queue")
	require.NoError(t, err)
	assert.False(t, prep.PositionFound)
	assert.Equal(t, RaftPosition{}, prep.Position)
}

func TestPrepare_DefinitionFetchFailureIsNonFatal(t *testing.T) {
	quorumBase := t.TempDir()
	writeConfigFile(t, quorumBase, "queue-uid-999", "/", "orders")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}

	cfg := runtime.Config{
		QuorumBasePath: quorumBase,
		BackupBaseDir:  t.TempDir(),
		AMQPURL:        unreachableAMQPURL,
	}

	prep, err := Prepare(context.Background(), cfg, dm, "/", "orders")
	require.NoError(t, err, "definition fetch failure must not fail Prepare")
	assert.Nil(t, prep.Definition)
	assert.NoFileExists(t, filepath.Join(prep.BackupDir, "definition.json"))
}

func TestPrepare_CapturesAndPersistsDefinition(t *testing.T) {
	quorumBase := t.TempDir()
	writeConfigFile(t, quorumBase, "queue-uid-def", "/", "orders")

	server := ordersDefinitionsServer(t)
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}

	cfg := runtime.Config{
		QuorumBasePath: quorumBase,
		BackupBaseDir:  t.TempDir(),
		AMQPURL:        unreachableAMQPURL,
	}

	prep, err := Prepare(context.Background(), cfg, dm, "/", "orders")
	require.NoError(t, err)
	require.NotNil(t, prep.Definition)
	assert.True(t, prep.Definition.Durable)
	assert.False(t, prep.Definition.AutoDelete)
	require.Len(t, prep.Definition.Bindings, 1)
	assert.Equal(t, testExchange, prep.Definition.Bindings[0].Source)

	loaded, err := LoadDefinition(prep.BackupDir)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, prep.Definition.Durable, loaded.Durable)
	assert.Equal(t, prep.Definition.Bindings, loaded.Bindings)
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
	prep := &PreparedRecovery{Queue: "q", VHost: "/", BackupDir: t.TempDir(), WALBackupDir: t.TempDir(), QueueUID: "789"}

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
	prep := &PreparedRecovery{Queue: "q", VHost: "/", BackupDir: backupDir, WALBackupDir: t.TempDir(), QueueUID: "111"}

	err = Execute(context.Background(), cfg, dm, prep)
	require.Error(t, err, "carved messages should trigger a republish attempt against the unreachable broker")
	assert.Contains(t, err.Error(), "republish phase")
}

func TestExecute_DeclaresQueueAndBindingsFromSavedDefinition(t *testing.T) {
	server := noContentServer(t)
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}

	backupDir := t.TempDir()
	msg, err := encodeETFTerm(makeBasicMessageTerm([]byte("recovered payload")))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "0000000000000001.segment"), msg, 0600))

	qd := &QueueDefinition{
		Durable:    true,
		AutoDelete: false,
		Arguments:  map[string]any{queueType: quorumType, "x-max-length": json.Number("1000")},
		Bindings: []broker.QueueBinding{
			{Source: testExchange, Destination: "q", DestinationType: queueArg, RoutingKey: testRK},
		},
	}
	require.NoError(t, SaveDefinition(backupDir, qd))

	ch := amqptest.NewChannel()
	conn := amqptest.NewConnection(ch)
	useFakeBroker(t, conn, nil)

	cfg := runtime.Config{AMQPURL: "amqp://fake/"}
	prep := &PreparedRecovery{Queue: "q", VHost: "/", BackupDir: backupDir, WALBackupDir: t.TempDir(), QueueUID: "456"}

	err = Execute(context.Background(), cfg, dm, prep)
	require.NoError(t, err)

	declared := ch.Declared()
	require.Len(t, declared, 1)
	assert.Equal(t, "q", declared[0].Name)
	assert.True(t, declared[0].Durable)
	assert.False(t, declared[0].NoWait)
	assert.Equal(t, int64(1000), declared[0].Args["x-max-length"])

	bindings := ch.Bindings()
	require.Len(t, bindings, 1)
	assert.Equal(t, testExchange, bindings[0].Exchange)
	assert.Equal(t, testRK, bindings[0].Key)
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
