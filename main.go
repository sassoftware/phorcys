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
	"github.com/sassoftware/phorcys/internal/monitor"
	"github.com/sassoftware/phorcys/internal/runtime"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == recover.Command {
		if err := runRecover(os.Args[2:]); err != nil && !errors.Is(err, flag.ErrHelp) {
			log.Fatalf("FATAL: %v", err)
		}
		return
	}

	cfg := runtime.LoadConfig()

	// If the quorum path is not explicitly set, derive it from the node name reported
	// by the management API so the default works for any node name (e.g. rabbit@rabbitmq).
	if cfg.QuorumBasePath == "" {
		dm0 := broker.NewDiagnosticsManager(cfg.ManagementURL, cfg.ManagementUser, cfg.ManagementPassword)
		cfg.QuorumBasePath = dm0.ResolveQuorumBasePath(context.Background())
	}

	conn, err := amqpx.Dial(cfg.AMQPURL)
	if err != nil {
		log.Fatalf("FATAL: Cannot connect to broker: %v", err)
	}
	defer func() { _ = conn.Close() }()

	dm := broker.NewDiagnosticsManager(cfg.ManagementURL, cfg.ManagementUser, cfg.ManagementPassword)
	m := monitor.NewLogMonitorWorker(conn, dm, cfg, cfg.WorkerCount)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

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
