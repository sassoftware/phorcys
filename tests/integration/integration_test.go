// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// dialCluster connects to RABBITMQ_URL (default: plain AMQP on localhost). For the TLS
// compose overlay set RABBITMQ_URL to the amqps:// URL on port 5671 and
// RABBITMQ_TLS_CA_FILE=tests/integration/certs/ca.pem.
func dialCluster(t *testing.T) *amqp.Connection {
	t.Helper()
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/" // betterleaks:allow
	}

	var tlsCfg *tls.Config
	if caFile := os.Getenv("RABBITMQ_TLS_CA_FILE"); caFile != "" {
		pem, err := os.ReadFile(caFile)
		require.NoError(t, err, "Failed to read RABBITMQ_TLS_CA_FILE")
		pool := x509.NewCertPool()
		require.True(t, pool.AppendCertsFromPEM(pem), "No certificates in %s", caFile)
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}
	}

	conn, err := amqp.DialConfig(url, amqp.Config{TLSClientConfig: tlsCfg})
	require.NoError(t, err, "Failed to connect to cluster: %v", err)
	return conn
}

func Test_QuorumQueueRecovery(t *testing.T) {
	ctx := context.Background()

	conn := dialCluster(t)
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel: %v", err)
	defer func() { _ = ch.Close() }()

	// 1. Declare the quorum queue
	// Note: Quorum queues must always be declared as durable
	quorumQueue := "test-target-quorum-queue"
	_, err = ch.QueueDeclare(
		quorumQueue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		amqp.Table{"x-queue-type": "quorum"},
	)
	require.NoError(t, err, "Failed to declare quorum queue: %v", err)
	log.Printf("Declared quorum queue: %s", quorumQueue)

	// 2. Publish 20000 messages
	for i := 1; i <= 20000; i++ {
		messageBody := fmt.Sprintf("payload-data-packet-%d", i)
		err = ch.PublishWithContext(
			ctx,
			"",          // default exchange
			quorumQueue, // routing key matches queue name
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				Headers: amqp.Table{
					"x-packet-index": i,
					"x-source":       "integration-test",
				},
				ContentType:     "text/plain",
				ContentEncoding: "utf-8",
				Body:            []byte(messageBody),
				DeliveryMode:    amqp.Persistent, // Forces Raft to write to disk segments
			},
		)
		require.NoError(t, err, "Failed to publish message %d: %v", i, err)
	}
	log.Println("Successfully published 20000 persistent messages to quorum queue.")

	// 3. Sleep for 1 second
	// Gives the Erlang VM and OS cache time to sync the state down to the WAL/segments
	log.Println("Sleeping for 1 second to ensure disk synchronization...")
	time.Sleep(1 * time.Second)

	// 4. Send the trigger to the sidecar agent's classic queue
	err = ch.PublishWithContext(
		ctx,
		"amq.fanout", // built-in fanout exchange
		"",
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(quorumQueue),
		},
	)
	require.NoError(t, err, "Failed to dispatch fault trigger to sidecar: %v", err)

	log.Println("Sent corruption signal. Sidecar is corrupting the logs now.")
}
