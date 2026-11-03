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

	"github.com/sassoftware/phorcys/internal/amqpx"
)

// dialAMQP is a variable so tests can substitute a fake broker connection.
var dialAMQP = amqpx.Dial

// minimalQueueArgs is the last-resort declare argument set used when no queue
// definition could be recovered: just enough to recreate it as a quorum
// queue. Callers must log a loud warning that the original arguments
// (delivery limits, dead-letter-exchange, etc.) were lost.
func minimalQueueArgs() amqp.Table {
	return amqp.Table{"x-queue-type": "quorum"}
}

// toAMQPTable converts JSON-decoded arguments (map[string]any, with
// json.Number for numeric fields — see broker.FetchQueueDefinition) into an
// amqp.Table whose value types amqp091-go's wire encoder understands: nested
// maps become amqp.Table, json.Number becomes int64 (or float64 if it has a
// fractional/exponent part), and everything else passes through unchanged.
func toAMQPTable(args map[string]any) amqp.Table {
	table := amqp.Table{}
	for k, v := range args {
		table[k] = toAMQPValue(v)
	}
	return table
}

func toAMQPValue(v any) any {
	switch val := v.(type) {
	case json.Number:
		if i, err := val.Int64(); err == nil {
			return i
		}
		if f, err := val.Float64(); err == nil {
			return f
		}
		return val.String()
	case map[string]any:
		return toAMQPTable(val)
	case []any:
		out := make([]any, len(val))
		for i, e := range val {
			out[i] = toAMQPValue(e)
		}
		return out
	default:
		return v
	}
}

// RepublishMessages connects to the broker, recreates targetQueue (and its
// bindings) from def, and publishes all recovered messages to the default
// exchange using targetQueue as the routing key, so each message is delivered
// back to the original queue. Original properties (headers, content-type,
// delivery-mode, etc.) recovered from the queue's data files are preserved;
// DeliveryMode and ContentType fall back to sane defaults when they could not
// be recovered. A nil def means no definition could be recovered: the queue
// is declared as quorum-only and a warning is logged, since the original
// arguments and bindings are lost.
func RepublishMessages(ctx context.Context, amqpURL, targetQueue string, def *QueueDefinition, messages []amqp.Publishing) error {
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

	durable, autoDelete := true, false
	args := minimalQueueArgs()
	if def != nil {
		durable = def.Durable
		autoDelete = def.AutoDelete
		args = toAMQPTable(def.Arguments)
		if _, ok := args["x-queue-type"]; !ok {
			args["x-queue-type"] = "quorum"
		}
	} else {
		log.Printf("[Republish] WARNING: no queue definition recovered for %s — declaring with x-queue-type=quorum only; original arguments were lost", targetQueue)
	}

	if _, err = ch.QueueDeclare(targetQueue, durable, autoDelete, false, false, args); err != nil {
		return fmt.Errorf("failed to recreate queue: %w", err)
	}

	if def != nil {
		for _, b := range def.Bindings {
			if err := ch.QueueBind(targetQueue, b.RoutingKey, b.Source, false, toAMQPTable(b.Arguments)); err != nil {
				return fmt.Errorf("failed to recreate binding from exchange %q: %w", b.Source, err)
			}
		}
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
