// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqpx

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// Dial connects to a live broker and returns it as a Connection.
func Dial(url string) (Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return Wrap(conn), nil
}

// Wrap adapts an existing *amqp.Connection to the Connection interface.
func Wrap(conn *amqp.Connection) Connection {
	return &connection{conn: conn}
}

type connection struct {
	conn *amqp.Connection
}

func (c *connection) Channel() (Channel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		// Avoid returning a non-nil interface wrapping a nil *amqp.Channel.
		return nil, err
	}
	return ch, nil
}

func (c *connection) Close() error {
	return c.conn.Close()
}
