// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package runtime

import "os"

const (
	EnvVarAMQPURL    = "ARGUS_AMQP_URL"
	EnvVarMgmtURL    = "ARGUS_MGMT_URL"
	EnvVarMgmtUser   = "ARGUS_MGMT_USER"
	EnvVarMgmtPass   = "ARGUS_MGMT_PASS" //nolint:gosec
	EnvVarBackupDir  = "ARGUS_BACKUP_DIR"
	EnvVarQuorumPath = "ARGUS_QUORUM_PATH"
)

// Config holds all runtime configuration for Argus.
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

// GetEnv returns the value of the environment variable named by key, or
// fallback if the variable is unset or empty.
func GetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
