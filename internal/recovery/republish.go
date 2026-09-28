// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RepublishMessages connects to the broker and publishes all recovered messages to the
// default exchange using targetQueue as the routing key, so each message is delivered
// back to the original queue. Original properties (headers, content-type, delivery-mode,
// etc.) recovered from the queue's data files are preserved; DeliveryMode and ContentType
// fall back to sane defaults when they could not be recovered.
func RepublishMessages(ctx context.Context, amqpURL, targetQueue string, messages []amqp.Publishing) error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	pubCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// TODO: This is temporary
	_, qErr := ch.QueueDeclare(targetQueue,
		true,
		false,
		false,
		true,
		amqp.Table{})
	if qErr != nil {
		return fmt.Errorf("failed to recreate queue : %w", qErr)
	}

	for i, msg := range messages {
		if msg.DeliveryMode == 0 {
			msg.DeliveryMode = amqp.Persistent
		}
		if msg.ContentType == "" {
			msg.ContentType = "application/octet-stream"
		}
		err = ch.PublishWithContext(pubCtx,
			"",          // default exchange
			targetQueue, // routing key = queue name
			false,
			false,
			msg,
		)
		if err != nil {
			return fmt.Errorf("failed publishing message %d: %w", i, err)
		}
	}

	log.Printf("[Republish] Published %d messages to queue %s", len(messages), targetQueue)
	return nil
}
