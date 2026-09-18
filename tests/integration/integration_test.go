// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func Test_QuorumQueueRecovery(t *testing.T) {
	amqpClusterURL := "amqp://guest:guest@localhost:5672/" //nolint:gosec
	ctx := context.Background()

	conn, err := amqp.Dial(amqpClusterURL)
	require.NoError(t, err, "Failed to connect to cluster: %v", err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "Failed to open channel: %v", err)
	defer ch.Close()

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
				ContentType:  "text/plain",
				Body:         []byte(messageBody),
				DeliveryMode: amqp.Persistent, // Forces Raft to write to disk segments
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
	triggerQueue := "test-fault-trigger"
	err = ch.PublishWithContext(
		ctx,
		"",           // default exchange
		triggerQueue, // routing key matching the agent configuration
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("TRIGGER_APPEND_CORRUPTION"),
		},
	)
	require.NoError(t, err, "Failed to dispatch fault trigger to sidecar: %v", err)

	log.Println("Sent corruption signal. Sidecar is corrupting the logs now.")
}
