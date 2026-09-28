// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/rand"
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
	triggerQueue      = "test-fault-trigger"
	defaultMnesiaPath = "/var/lib/rabbitmq/mnesia"
)

func main() {
	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}

	mnesiaPath := os.Getenv("MNESIA_PATH")
	if mnesiaPath == "" {
		mnesiaPath = defaultMnesiaPath
	}

	log.Printf("Starting fault injection agent. Target path: %s", mnesiaPath)

	// Retry loop to wait for RabbitMQ to fully boot inside the container
	var conn *amqp.Connection
	var err error
	for i := 0; i < 10; i++ {
		conn, err = amqp.Dial(amqpURL)
		if err == nil {
			break
		}
		log.Printf("Waiting for RabbitMQ to become available... (%d/10)", i+1)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

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
		log.Fatalf("Failed to declare classic trigger queue: %v", err)
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
		log.Fatalf("Failed to register consumer: %v", err)
	}

	log.Printf("Agent successfully listening on queue: %s", triggerQueue)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Wait for a message on the trigger queue
	for {
		select {
		case <-ctx.Done():
			return
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
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return fmt.Errorf("data directory does not exist: %s", dataDir)
	}

	return filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.Contains(path, "/quorum/") && filepath.Ext(path) == ".segment" {
			file, err := os.OpenFile(path, os.O_WRONLY, 0600)
			if err != nil {
				return fmt.Errorf("failed to open %s: %w", path, err)
			}

			// 64 bytes is enough to destroy the segment header without touching message data further in.
			const headerSize = 64
			garbage := make([]byte, headerSize)
			if _, err := rand.Read(garbage); err != nil {
				file.Close()
				return fmt.Errorf("failed to read random bytes: %w", err)
			}

			// Overwrite only the header so ra refuses to load the file, but message payloads
			// deeper in the file body remain intact for phorcys's byte-scanner.
			if _, err := file.WriteAt(garbage, 0); err != nil {
				file.Close()
				return fmt.Errorf("failed to overwrite data in %s: %w", path, err)
			}

			if err := file.Sync(); err != nil {
				file.Close()
				return fmt.Errorf("failed to sync %s: %w", path, err)
			}
			file.Close()
			log.Printf("Corrupted segment header: %s", path)
		}
		return nil
	})
}
