// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recover

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/runtime"
)

type call struct {
	cfg          runtime.Config
	vhost, queue string
}

func stub(t *testing.T, cfg runtime.Config, pipelineErr error) *[]call {
	t.Helper()
	calls := &[]call{}
	origRun, origLoad, origEvaluateQueueHealth := runPipeline, loadConfig, evaluateQueueHealth
	t.Cleanup(func() {
		runPipeline, loadConfig, evaluateQueueHealth = origRun, origLoad, origEvaluateQueueHealth
	})
	loadConfig = func() runtime.Config { return cfg }
	evaluateQueueHealth = func(_ context.Context, _ *broker.DiagnosticsManager, _, _ string) (broker.QueueHealth, error) {
		return broker.HealthGreen, nil
	}
	runPipeline = func(_ context.Context, c runtime.Config, dm *broker.DiagnosticsManager, vhost, queue string) error {
		require.NotNil(t, dm)
		*calls = append(*calls, call{c, vhost, queue})
		return pipelineErr
	}
	return calls
}

func baseCfg() runtime.Config {
	return runtime.Config{QuorumBasePath: "/q", BackupBaseDir: "/b"}
}

func TestRun_WithYesRunsPipeline(t *testing.T) {
	calls := stub(t, baseCfg(), nil)
	var out bytes.Buffer

	err := RunCommand(context.Background(), []string{"-queue", "orders", "-vhost", "prod", "-yes"}, strings.NewReader(""), &out)

	require.NoError(t, err)
	require.Len(t, *calls, 1)
	assert.Equal(t, "prod", (*calls)[0].vhost)
	assert.Equal(t, "orders", (*calls)[0].queue)
	assert.Equal(t, "/q", (*calls)[0].cfg.QuorumBasePath)
	assert.Contains(t, out.String(), "complete")
}

func TestRun_DefaultVhost(t *testing.T) {
	calls := stub(t, baseCfg(), nil)

	err := RunCommand(context.Background(), []string{"-queue", "orders", "-yes"}, nil, &bytes.Buffer{})

	require.NoError(t, err)
	require.Len(t, *calls, 1)
	assert.Equal(t, "/", (*calls)[0].vhost)
}

func TestRun_ConfirmYes(t *testing.T) {
	for _, answer := range []string{"y\n", "YES\n", " yes"} {
		calls := stub(t, baseCfg(), nil)
		var out bytes.Buffer

		err := RunCommand(context.Background(), []string{"-queue", "orders"}, strings.NewReader(answer), &out)

		require.NoError(t, err)
		assert.Len(t, *calls, 1, "answer %q", answer)
		assert.Contains(t, out.String(), "Continue? [y/N]")
		assert.Contains(t, out.String(), "/q")
		assert.Contains(t, out.String(), "/b")
	}
}

func TestRun_ConfirmDeclined(t *testing.T) {
	for _, answer := range []string{"n\n", "\n", ""} {
		calls := stub(t, baseCfg(), nil)
		var out bytes.Buffer

		err := RunCommand(context.Background(), []string{"-queue", "orders"}, strings.NewReader(answer), &out)

		require.NoError(t, err)
		assert.Empty(t, *calls, "answer %q", answer)
		assert.Contains(t, out.String(), "Aborted.")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

func TestRun_ConfirmReadError(t *testing.T) {
	calls := stub(t, baseCfg(), nil)

	err := RunCommand(context.Background(), []string{"-queue", "orders"}, errReader{}, &bytes.Buffer{})

	require.ErrorContains(t, err, "reading confirmation")
	assert.Empty(t, *calls)
}

func TestRun_MissingQueue(t *testing.T) {
	calls := stub(t, baseCfg(), nil)
	var out bytes.Buffer

	err := RunCommand(context.Background(), []string{"-yes"}, nil, &out)

	require.ErrorContains(t, err, "-queue is required")
	assert.Empty(t, *calls)
	assert.Contains(t, out.String(), "Usage: phorcys recover")
}

func TestRun_UnexpectedArgs(t *testing.T) {
	calls := stub(t, baseCfg(), nil)

	err := RunCommand(context.Background(), []string{"-queue", "orders", "extra"}, nil, &bytes.Buffer{})

	require.ErrorContains(t, err, "unexpected arguments: extra")
	assert.Empty(t, *calls)
}

func TestRun_Help(t *testing.T) {
	stub(t, baseCfg(), nil)
	var out bytes.Buffer

	err := RunCommand(context.Background(), []string{"-h"}, nil, &out)

	require.ErrorIs(t, err, flag.ErrHelp)
	assert.Contains(t, out.String(), "-queue")
}

func TestRun_PipelineError(t *testing.T) {
	stub(t, baseCfg(), errors.New("locate failed"))

	err := RunCommand(context.Background(), []string{"-queue", "orders", "-yes"}, nil, &bytes.Buffer{})

	require.ErrorContains(t, err, "recovery of //orders failed: locate failed")
}

func TestRun_ResolvesQuorumPathWhenUnset(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"name":"rabbit@node1","running":true}]`))
	}))
	defer srv.Close()

	cfg := baseCfg()
	cfg.QuorumBasePath = ""
	cfg.ManagementURL = srv.URL
	calls := stub(t, cfg, nil)

	err := RunCommand(context.Background(), []string{"-queue", "orders", "-yes"}, nil, &bytes.Buffer{})

	require.NoError(t, err)
	require.Len(t, *calls, 1)
	assert.Contains(t, (*calls)[0].cfg.QuorumBasePath, "rabbit@node1")
}
