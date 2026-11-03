// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const definitionsFixture = `{
	"queues": [
		{"name": "orders", "vhost": "/", "durable": true, "auto_delete": false,
		 "arguments": {"x-queue-type": "quorum", "x-max-length": 1000}},
		{"name": "other", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {}}
	],
	"bindings": [
		{"source": "orders-exchange", "vhost": "/", "destination": "orders",
		 "destination_type": "queue", "routing_key": "orders.rk", "arguments": {}},
		{"source": "other-exchange", "vhost": "/", "destination": "other",
		 "destination_type": "queue", "routing_key": "other.rk", "arguments": {}},
		{"source": "orders-exchange", "vhost": "/", "destination": "orders-dlx",
		 "destination_type": "exchange", "routing_key": "", "arguments": {}}
	]
}`

const badHost = "http://bad host/"
const testRK = "orders.rk"

func definitionsServer(t *testing.T, body string, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func TestFetchQueueDefinition_FindsQueueAndFiltersBindings(t *testing.T) {
	server := definitionsServer(t, definitionsFixture, http.StatusOK)
	defer server.Close()
	dm := newTestManager(server)

	def, bindings, err := dm.FetchQueueDefinition(context.Background(), "/", "orders")
	require.NoError(t, err)
	require.NotNil(t, def)
	assert.Equal(t, "orders", def.Name)
	assert.True(t, def.Durable)
	assert.False(t, def.AutoDelete)

	require.Len(t, bindings, 1, "bindings for other queues/exchanges must be filtered out")
	assert.Equal(t, "orders-exchange", bindings[0].Source)
	assert.Equal(t, testRK, bindings[0].RoutingKey)
}

func TestFetchQueueDefinition_PreservesIntegerArguments(t *testing.T) {
	server := definitionsServer(t, definitionsFixture, http.StatusOK)
	defer server.Close()
	dm := newTestManager(server)

	def, _, err := dm.FetchQueueDefinition(context.Background(), "/", "orders")
	require.NoError(t, err)
	require.NotNil(t, def)

	n, ok := def.Arguments["x-max-length"].(json.Number)
	require.True(t, ok, "x-max-length should decode as json.Number, got %T", def.Arguments["x-max-length"])
	i, err := n.Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(1000), i)
}

func TestFetchQueueDefinition_QueueNotFoundReturnsNil(t *testing.T) {
	server := definitionsServer(t, `{"queues":[],"bindings":[]}`, http.StatusOK)
	defer server.Close()
	dm := newTestManager(server)

	def, bindings, err := dm.FetchQueueDefinition(context.Background(), "/", "missing")
	require.NoError(t, err)
	assert.Nil(t, def)
	assert.Nil(t, bindings)
}

func TestFetchQueueDefinition_HTTPErrorStatus(t *testing.T) {
	server := definitionsServer(t, "", http.StatusInternalServerError)
	defer server.Close()
	dm := newTestManager(server)

	_, _, err := dm.FetchQueueDefinition(context.Background(), "/", "orders")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status code: 500")
}

func TestFetchQueueDefinition_DecodeErrorReturnsError(t *testing.T) {
	server := definitionsServer(t, `{not valid json`, http.StatusOK)
	defer server.Close()
	dm := newTestManager(server)

	_, _, err := dm.FetchQueueDefinition(context.Background(), "/", "orders")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decoding response")
}

func TestFetchQueueDefinition_TransportErrorReturnsError(t *testing.T) {
	server := definitionsServer(t, definitionsFixture, http.StatusOK)
	dm := newTestManager(server)
	server.Close()

	_, _, err := dm.FetchQueueDefinition(context.Background(), "/", "orders")
	require.Error(t, err)
}

func TestFetchQueueDefinition_InvalidURLReturnsError(t *testing.T) {
	dm := &DiagnosticsManager{APIURL: badHost, Client: http.DefaultClient}

	_, _, err := dm.FetchQueueDefinition(context.Background(), "/", "orders")
	require.Error(t, err)
}
