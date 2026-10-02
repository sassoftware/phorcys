// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package amqpx defines the narrow AMQP connection and channel interfaces used
// by Phorcys, so broker interactions can be substituted with fakes in tests.
// See the amqptest subpackage for test doubles.
package amqpx

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Channel is the subset of *amqp.Channel used by Phorcys.
type Channel interface {
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

// Connection is the subset of *amqp.Connection used by Phorcys.
type Connection interface {
	Channel() (Channel, error)
	Close() error
}

// Ensure that *amqp.Channel implements the Channel interface.
var _ Channel = (*amqp.Channel)(nil)
