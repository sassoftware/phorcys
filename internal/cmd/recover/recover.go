// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package recover implements the "recover" CLI subcommand, which manually runs
// the recovery pipeline against a quorum queue that the log monitor did not
// detect as corrupted.
package recover

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/recovery"
	"github.com/sassoftware/phorcys/internal/runtime"
)

// Overridable for tests.
var (
	runPipeline         = recovery.Run
	loadConfig          = runtime.LoadConfig
	evaluateQueueHealth = func(ctx context.Context, dm *broker.DiagnosticsManager, vhost, queue string) (broker.QueueHealth, error) {
		return dm.EvaluateQueueHealth(ctx, vhost, queue)
	}
)

const Command = "recover"

const usageHeader = `Usage: phorcys recover -queue <name> [-vhost <vhost>] [-yes]

Manually runs the recovery pipeline for a corrupted quorum queue: backs up its
Raft segment/WAL data, deletes the queue from the broker, carves recoverable
messages from the backup and republishes them to a new queue of the same name.

Connection settings are read from the same PHORCYS_* environment variables
used by the monitor.

Flags:
`

// RunCommand parses args and executes the recovery pipeline for the requested queue.
// Unless -yes is given, the user is asked to confirm on in before the
// (destructive) pipeline starts. Returns flag.ErrHelp when -h is requested.
func RunCommand(ctx context.Context, args []string, in io.Reader, out io.Writer) error {
	// Parse flags and validate required arguments.
	fs := flag.NewFlagSet(Command, flag.ContinueOnError)
	fs.SetOutput(out)
	queue := fs.String("queue", "", "name of the quorum queue to recover (required)")
	vhost := fs.String("vhost", "/", "virtual host of the queue")
	yes := fs.Bool("yes", false, "skip the interactive confirmation prompt")
	fs.Usage = func() {
		_, _ = fmt.Fprint(out, usageHeader)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		fs.Usage()
		return fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if strings.TrimSpace(*queue) == "" {
		fs.Usage()
		return errors.New("-queue is required")
	}

	// Load configuration and initialize diagnostics manager.
	cfg := loadConfig()
	dm := broker.NewDiagnosticsManager(cfg.ManagementURL, cfg.ManagementUser, cfg.ManagementPassword)
	if cfg.QuorumBasePath == "" {
		cfg.QuorumBasePath = dm.ResolveQuorumBasePath(ctx)
	}

	// Ask for confirmation unless -yes is given.
	if !*yes {
		qh, err := evaluateQueueHealth(ctx, dm, *vhost, *queue)
		if err != nil {
			return fmt.Errorf("failed to evaluate queue health: %w", err)
		}
		ok, err := confirm(in, out, *vhost, *queue, qh, cfg)
		if err != nil {
			return err
		}
		if !ok {
			_, _ = fmt.Fprintln(out, "Aborted.")
			return nil
		}
	}

	// Run the recovery pipeline.
	if err := runPipeline(ctx, cfg, dm, *vhost, *queue); err != nil {
		return fmt.Errorf("recovery of %s/%s failed: %w", *vhost, *queue, err)
	}
	_, _ = fmt.Fprintf(out, "Recovery of %s/%s complete.\n", *vhost, *queue)
	return nil
}

func confirm(in io.Reader, out io.Writer, vhost, queue string, qh broker.QueueHealth, cfg runtime.Config) (bool, error) {
	_, _ = fmt.Fprintf(out,
		"About to recover queue %q in vhost %q.\n"+
			"  Queue health: %s\n"+
			"  Quorum data path: %s\n"+
			"  Backup directory: %s\n"+
			"The queue will be DELETED from the broker and its recovered messages republished.\n"+
			"Continue? [y/N]: ",
		queue, vhost, qh, cfg.QuorumBasePath, cfg.BackupBaseDir)

	line, err := bufio.NewReader(in).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("reading confirmation: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}
