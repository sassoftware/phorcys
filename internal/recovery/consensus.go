// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsensusExchange is the fanout exchange every Phorcys instance publishes
// RecoveryAnnouncements to. Fanout ensures every node subscribed to it — i.e.
// every other node hosting a replica of the announced queue — receives every
// announcement regardless of routing key.
const ConsensusExchange = "phorcys.consensus"

// RecoveryAnnouncement is what a node publishes to ConsensusExchange after
// Prepare backs up a queue, so other nodes hosting that queue can compare
// Term/Index (via RaftPosition.After) and agree on which one calls Execute.
type RecoveryAnnouncement struct {
	Hostname  string    `json:"hostname"`
	VHost     string    `json:"vhost"`
	Queue     string    `json:"queue"`
	Timestamp time.Time `json:"timestamp"`
	Term      uint64    `json:"term"`
	Index     uint64    `json:"index"`
}

// PublishRecoveryAnnouncement declares ConsensusExchange and publishes ann to
// it as JSON.
func PublishRecoveryAnnouncement(ctx context.Context, amqpURL string, ann RecoveryAnnouncement) error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.ExchangeDeclare(ConsensusExchange, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("failed to declare consensus exchange: %w", err)
	}

	body, err := json.Marshal(ann)
	if err != nil {
		return fmt.Errorf("failed to marshal announcement: %w", err)
	}

	pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = ch.PublishWithContext(pubCtx,
		ConsensusExchange,
		"", // fanout ignores routing key
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Timestamp:   ann.Timestamp,
			Body:        body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish announcement: %w", err)
	}

	log.Printf("[Consensus] Announced %s/%s: term=%d idx=%d", ann.VHost, ann.Queue, ann.Term, ann.Index)
	return nil
}
