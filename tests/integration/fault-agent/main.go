// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	triggerQueuePrefix = "test-fault-trigger."
	defaultMnesiaPath  = "/var/lib/rabbitmq/mnesia"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// tlsConfigFromEnv trusts the CA in RABBITMQ_TLS_CA_FILE when set; it is only used for amqps:// URLs.
func tlsConfigFromEnv() (*tls.Config, error) {
	caFile := os.Getenv("RABBITMQ_TLS_CA_FILE")
	if caFile == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(caFile) //nolint:gosec // operator-supplied container config
	if err != nil {
		return nil, fmt.Errorf("reading RABBITMQ_TLS_CA_FILE: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("RABBITMQ_TLS_CA_FILE %s contains no valid certificates", caFile)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}, nil
}

func run() error {
	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/" // betterleaks:allow // local docker-compose default, not a real credential
	}

	mnesiaPath := os.Getenv("MNESIA_PATH")
	if mnesiaPath == "" {
		mnesiaPath = defaultMnesiaPath
	}
	hostname, hErr := os.Hostname()
	if hErr != nil {
		log.Printf("WARNING: failed to determine hostname (non-fatal): %v", hErr)
		hostname = "localhost"
	}

	log.Printf("Starting fault injection agent. Target path: %s", mnesiaPath) // mnesiaPath is operator-supplied container config, not attacker input

	tlsCfg, err := tlsConfigFromEnv()
	if err != nil {
		return err
	}

	// Retry loop to wait for RabbitMQ to fully boot inside the container
	var conn *amqp.Connection
	for i := 0; i < 10; i++ {
		conn, err = amqp.DialConfig(amqpURL, amqp.Config{TLSClientConfig: tlsCfg})
		if err == nil {
			break
		}
		log.Printf("Waiting for RabbitMQ to become available... (%d/10)", i+1)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		return fmt.Errorf("could not connect to RabbitMQ: %w", err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	triggerQueue := triggerQueuePrefix + hostname
	// Declare a queue specifically for the trigger signal
	_, err = ch.QueueDeclare(
		triggerQueue,
		false, // durable
		true,  // auto-delete
		true,  // exclusive
		false, // no-wait
		amqp.Table{},
	)
	if err != nil {
		return fmt.Errorf("failed to declare classic trigger queue: %w", err)
	}

	err = ch.QueueBind(triggerQueue, "", "amq.fanout", false, nil)
	if err != nil {
		return fmt.Errorf("failed to bind trigger queue to fanout exchange: %w", err)
	}

	msgs, err := ch.Consume(
		triggerQueue,
		"",    // consumer tags
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	log.Printf("Agent successfully listening on queue: %s", triggerQueue)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Wait for a message on the trigger queue
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-msgs:
			log.Println("Received fault injection signal from integration test! Corrupting logs...")

			if err := CorruptQuorumQueueData(mnesiaPath); err != nil {
				log.Printf("[Error] Fault injection failed: %v", err)
			} else {
				log.Println("SUCCESS: Data directories corrupted.")
			}
		}
	}
}

// CorruptQuorumQueueData overwrites the header bytes of each quorum queue .segment file with random
// garbage. This simulates partial storage corruption: ra validates the header on open and refuses to
// load the file (making the queue unrecoverable), while phorcys's raw ETF byte-scanner can still locate
// message payloads deeper in the file body.
func CorruptQuorumQueueData(dataDir string) error {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) { // dataDir is operator-supplied container config, not attacker input
		return fmt.Errorf("data directory does not exist: %s", dataDir)
	}

	return filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error { // dataDir is operator-supplied container config, not attacker input
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.Contains(path, "/quorum/") && filepath.Ext(path) == ".segment" {
			file, err := os.OpenFile(path, os.O_WRONLY, 0600) //nolint:gosec // test-only fault injection in an ephemeral container, not attacker-reachable
			if err != nil {
				return fmt.Errorf("failed to open %s: %w", path, err)
			}

			// 64 bytes is enough to destroy the segment header without touching message data further in.
			const headerSize = 64
			garbage := make([]byte, headerSize)
			if _, err := rand.Read(garbage); err != nil {
				_ = file.Close()
				return fmt.Errorf("failed to read random bytes: %w", err)
			}

			// Overwrite only the header so ra refuses to load the file, but message payloads
			// deeper in the file body remain intact for phorcys's byte-scanner.
			if _, err := file.WriteAt(garbage, 0); err != nil {
				_ = file.Close()
				return fmt.Errorf("failed to overwrite data in %s: %w", path, err)
			}

			if err := file.Sync(); err != nil {
				_ = file.Close()
				return fmt.Errorf("failed to sync %s: %w", path, err)
			}
			_ = file.Close()
			log.Printf("Corrupted segment header: %s", path)
		}
		return nil
	})
}
