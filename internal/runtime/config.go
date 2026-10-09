// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package runtime

import "os"

const (
	EnvVarAMQPURL    = "PHORCYS_AMQP_URL"
	EnvVarMgmtURL    = "PHORCYS_MGMT_URL"
	EnvVarMgmtUser   = "PHORCYS_MGMT_USER"
	EnvVarMgmtPass   = "PHORCYS_MGMT_PASS" //nolint:gosec
	EnvVarBackupDir  = "PHORCYS_BACKUP_DIR"
	EnvVarQuorumPath = "PHORCYS_QUORUM_PATH"

	amqpURLDefault        = "amqp://guest:guest@localhost:5672/" // #nosec G101 -- local-development default
	managementURLDefault  = "http://localhost:15672"
	managementUserDefault = "guest"
	managementPassDefault = "guest" // #nosec G101 -- local-development default
	backupBaseDirDefault  = "/var/lib/rabbitmq/phorcys-backups"
	workerCountDefault    = 3
)

// Config holds all runtime configuration for Phorcys.
type Config struct {
	// AMQP broker connection URL
	AMQPURL string
	// RabbitMQ Management API base URL (e.g., "http://localhost:15672")
	ManagementURL string
	// Management API credentials
	ManagementUser     string
	ManagementPassword string
	// Base filesystem path for quorum queue Raft storage
	QuorumBasePath string
	// Directory where queue data will be backed up before deletion
	BackupBaseDir string
	// Number of concurrent health-check workers
	WorkerCount int
}

// LoadConfig builds a Config from environment variables, falling back to
// local-development defaults. QuorumBasePath is left empty when
// PHORCYS_QUORUM_PATH is unset so callers can resolve it from the broker.
func LoadConfig() Config {
	return Config{
		AMQPURL:            GetEnv(EnvVarAMQPURL, amqpURLDefault),
		ManagementURL:      GetEnv(EnvVarMgmtURL, managementURLDefault),
		ManagementUser:     GetEnv(EnvVarMgmtUser, managementUserDefault),
		ManagementPassword: GetEnv(EnvVarMgmtPass, managementPassDefault),
		BackupBaseDir:      GetEnv(EnvVarBackupDir, backupBaseDirDefault),
		QuorumBasePath:     os.Getenv(EnvVarQuorumPath),
		WorkerCount:        workerCountDefault,
	}
}

// GetEnv returns the value of the environment variable named by key, or
// fallback if the variable is unset or empty.
func GetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
