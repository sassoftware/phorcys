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

	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// FetchDefinition
// ---------------------------------------------------------------------------

// ordersDefinitionsJSON is a /api/definitions/{vhost} export containing a
// single durable "orders" queue and its one binding to "orders-exchange",
// shared by tests exercising the FetchDefinition -> SaveDefinition ->
// LoadDefinition path.
const ordersDefinitionsJSON = `{
	"queues": [{"name":"orders","vhost":"/","durable":true,"auto_delete":false,
	            "arguments":{"x-queue-type":"quorum","x-max-length":1000}}],
	"bindings": [{"source":"orders-exchange","vhost":"/","destination":"orders",
	              "destination_type":"queue","routing_key":"orders.rk","arguments":{}}]
}`

// ordersDefinitionsServer returns an httptest server that answers every
// request with ordersDefinitionsJSON.
func ordersDefinitionsServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(ordersDefinitionsJSON))
	}))
}

func TestFetchDefinition_ReturnsDefinitionWithBindings(t *testing.T) {
	server := ordersDefinitionsServer(t)
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}

	def, err := FetchDefinition(context.Background(), dm, "/", testQueue)
	require.NoError(t, err)
	require.NotNil(t, def)
	assert.True(t, def.Durable)
	assert.False(t, def.AutoDelete)
	require.Len(t, def.Bindings, 1)
	assert.Equal(t, "orders-exchange", def.Bindings[0].Source)

	n, ok := def.Arguments["x-max-length"].(json.Number)
	require.True(t, ok)
	i, err := n.Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(1000), i)
}

func TestFetchDefinition_QueueNotFoundReturnsNilNoError(t *testing.T) {
	server := emptyDefinitionsServer(t)
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}

	def, err := FetchDefinition(context.Background(), dm, "/", "missing")
	require.NoError(t, err)
	assert.Nil(t, def)
}

func TestFetchDefinition_PropagatesHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	dm := &broker.DiagnosticsManager{APIURL: server.URL, Client: server.Client()}

	_, err := FetchDefinition(context.Background(), dm, "/", testQueue)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// SaveDefinition / LoadDefinition
// ---------------------------------------------------------------------------

func TestSaveDefinition_NilIsNoOp(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, SaveDefinition(dir, nil))
	assert.NoFileExists(t, filepath.Join(dir, definitionFileName))
}

func TestSaveDefinition_WriteErrorReturnsError(t *testing.T) {
	err := SaveDefinition(filepath.Join(t.TempDir(), "does-not-exist"), &QueueDefinition{Durable: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "writing definition file")
}

func TestLoadDefinition_ReturnsNilWhenAbsent(t *testing.T) {
	def, err := LoadDefinition(t.TempDir())
	require.NoError(t, err)
	assert.Nil(t, def)
}

func TestLoadDefinition_CorruptFileReturnsError(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, definitionFileName), []byte("{not valid json"), 0600))

	_, err := LoadDefinition(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshaling definition file")
}

func TestSaveAndLoadDefinition_RoundTripPreservesIntegerArguments(t *testing.T) {
	dir := t.TempDir()
	qd := &QueueDefinition{
		Durable:    true,
		AutoDelete: false,
		Arguments:  map[string]any{queueType: "quorum", "x-max-length": json.Number("1000")},
		Bindings: []broker.QueueBinding{
			{Source: "orders-exchange", Destination: testQueue, DestinationType: queueArg, RoutingKey: testRK},
		},
	}
	require.NoError(t, SaveDefinition(dir, qd))

	loaded, err := LoadDefinition(dir)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, qd.Durable, loaded.Durable)
	assert.Equal(t, qd.Bindings, loaded.Bindings)

	n, ok := loaded.Arguments["x-max-length"].(json.Number)
	require.True(t, ok, "x-max-length should round-trip as json.Number, got %T", loaded.Arguments["x-max-length"])
	i, err := n.Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(1000), i)
}
