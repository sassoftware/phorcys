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

// amqpChannel is the subset of *amqp.Channel used for republishing.
type amqpChannel interface {
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

// amqpConnection is the subset of *amqp.Connection used for republishing.
type amqpConnection interface {
	Channel() (amqpChannel, error)
	Close() error
}

type amqpConnectionAdapter struct {
	conn *amqp.Connection
}

func (a *amqpConnectionAdapter) Channel() (amqpChannel, error) {
	return a.conn.Channel()
}

func (a *amqpConnectionAdapter) Close() error {
	return a.conn.Close()
}

// dialAMQP is a variable so tests can substitute a fake broker connection.
var dialAMQP = func(url string) (amqpConnection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return &amqpConnectionAdapter{conn: conn}, nil
}

// RepublishMessages connects to the broker and publishes all recovered messages to the
// default exchange using targetQueue as the routing key, so each message is delivered
// back to the original queue. Original properties (headers, content-type, delivery-mode,
// etc.) recovered from the queue's data files are preserved; DeliveryMode and ContentType
// fall back to sane defaults when they could not be recovered.
func RepublishMessages(ctx context.Context, amqpURL, targetQueue string, messages []amqp.Publishing) error {
	conn, err := dialAMQP(amqpURL)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err = ch.Confirm(false); err != nil {
		return fmt.Errorf("failed to enable publisher confirmations: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

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
		if err = waitForPublishConfirmation(pubCtx, confirms, i); err != nil {
			return err
		}
	}

	log.Printf("[Republish] Published %d messages to queue %s", len(messages), targetQueue)
	return nil
}

func waitForPublishConfirmation(ctx context.Context, confirms <-chan amqp.Confirmation, index int) error {
	select {
	case confirmation, ok := <-confirms:
		if !ok {
			return fmt.Errorf("publisher confirmation channel closed while waiting for message %d", index)
		}
		if !confirmation.Ack {
			return fmt.Errorf("broker negatively acknowledged message %d", index)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for publisher confirmation for message %d: %w", index, ctx.Err())
	}
}
