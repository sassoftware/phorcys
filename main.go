package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sassoftware/argus/internal/manager"
	"github.com/sassoftware/argus/internal/monitor"
	"github.com/sassoftware/argus/internal/runtime"
	"github.com/sassoftware/argus/internal/util"
)

func main() {
	cfg := runtime.Config{
		AMQPURL:            util.GetEnv(runtime.EnvVarAMQPURL, "amqp://guest:guest@localhost:5672/"),
		ManagementURL:      util.GetEnv(runtime.EnvVarMgmtURL, "http://localhost:15672"),
		ManagementUser:     util.GetEnv(runtime.EnvVarMgmtUser, "guest"),
		ManagementPassword: util.GetEnv(runtime.EnvVarMgmtPass, "guest"),
		BackupBaseDir:      util.GetEnv(runtime.EnvVarBackupDir, "/var/lib/rabbitmq/argus-backups"),
		WorkerCount:        3,
	}

	// If the quorum path is not explicitly set, derive it from the node name reported
	// by the management API so the default works for any node name (e.g. rabbit@rabbitmq).
	if path := os.Getenv(runtime.EnvVarQuorumPath); path != "" {
		cfg.QuorumBasePath = path
	} else {
		dm0 := manager.NewDiagnosticsManager(cfg.ManagementURL, cfg.ManagementUser, cfg.ManagementPassword)
		cfg.QuorumBasePath = dm0.ResolveQuorumBasePath(context.Background())
	}

	conn, err := amqp.Dial(cfg.AMQPURL)
	if err != nil {
		log.Fatalf("FATAL: Cannot connect to broker: %v", err)
	}
	defer conn.Close()

	dm := manager.NewDiagnosticsManager(cfg.ManagementURL, cfg.ManagementUser, cfg.ManagementPassword)
	monitor := monitor.NewLogMonitorWorker(conn, dm, cfg, cfg.WorkerCount)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Println("Argus started — monitoring RabbitMQ log exchange for errors...")
	if err := monitor.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("FATAL: Monitor exited unexpectedly: %v", err)
	}
	log.Println("Argus shutdown complete.")
}
