// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sassoftware/phorcys/internal/amqpx"
	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/cmd/recover"
	"github.com/sassoftware/phorcys/internal/jobqueue"
	"github.com/sassoftware/phorcys/internal/monitor"
	"github.com/sassoftware/phorcys/internal/recovery"
	"github.com/sassoftware/phorcys/internal/runtime"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == recover.Command {
		if err := runRecover(os.Args[2:]); err != nil && !errors.Is(err, flag.ErrHelp) {
			log.Fatalf("FATAL: %v", err)
		}
		return
	}

	cfg, err := runtime.LoadConfig()
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}

	// If the quorum path is not explicitly set, derive it from the node name reported
	// by the management API so the default works for any node name (e.g. rabbit@rabbitmq).
	if cfg.QuorumBasePath == "" {
		dm0 := broker.NewDiagnosticsManager(cfg.ManagementURL, cfg.ManagementUser, cfg.ManagementPassword, cfg.TLS)
		cfg.QuorumBasePath = dm0.ResolveQuorumBasePath(context.Background())
	}

	conn, err := amqpx.Dial(cfg.AMQPURL, cfg.TLS)
	if err != nil {
		log.Fatalf("FATAL: Cannot connect to broker: %v", err)
	}
	defer func() { _ = conn.Close() }()

	dm := broker.NewDiagnosticsManager(cfg.ManagementURL, cfg.ManagementUser, cfg.ManagementPassword, cfg.TLS)
	coordinator := recovery.NewCoordinator(conn, dm, cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := coordinator.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("ERROR: Consensus consumer exited: %v", err)
		}
	}()

	// Initialize the job queue for health check jobs.
	jq := jobqueue.New()

	// Search for existing backup files and publish any recoverable messages to a new queue before starting the log monitor.
	locations, err := recovery.ScanBackupDirectories(cfg.BackupBaseDir)
	if err != nil {
		log.Printf("FATAL: Failed to parse recoveries: %v", err)
		return
	}
	for path, qd := range locations {
		log.Printf("[Recovery] Found recoverable queue %s in backup directory %s", qd.Name, path)
		jq.Add(qd.VHost, qd.Name)
	}

	// Start the log monitor to watch for new errors and trigger recovery.
	m := monitor.NewLogMonitorWorker(conn, dm, cfg, jq, cfg.WorkerCount)
	m.UseCoordinator(coordinator)
	log.Println("Phorcys started — monitoring RabbitMQ log exchange for errors...")
	if err := m.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("FATAL: Monitor exited unexpectedly: %v", err)
		return
	}
	log.Println("Phorcys shutdown complete.")
}

// runRecover executes the "recover" subcommand, cancelling on SIGINT/SIGTERM.
func runRecover(args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	return recover.RunCommand(ctx, args, os.Stdin, os.Stdout)
}
