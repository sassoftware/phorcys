// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sassoftware/phorcys/internal/amqpx"
	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/monitor"
	"github.com/sassoftware/phorcys/internal/runtime"
)

func main() {
	cfg := runtime.Config{
		AMQPURL:            runtime.GetEnv(runtime.EnvVarAMQPURL, "amqp://guest:guest@localhost:5672/"),
		ManagementURL:      runtime.GetEnv(runtime.EnvVarMgmtURL, "http://localhost:15672"),
		ManagementUser:     runtime.GetEnv(runtime.EnvVarMgmtUser, "guest"),
		ManagementPassword: runtime.GetEnv(runtime.EnvVarMgmtPass, "guest"),
		BackupBaseDir:      runtime.GetEnv(runtime.EnvVarBackupDir, "/var/lib/rabbitmq/phorcys-backups"),
		WorkerCount:        3,
	}

	// If the quorum path is not explicitly set, derive it from the node name reported
	// by the management API so the default works for any node name (e.g. rabbit@rabbitmq).
	if path := os.Getenv(runtime.EnvVarQuorumPath); path != "" {
		cfg.QuorumBasePath = path
	} else {
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
