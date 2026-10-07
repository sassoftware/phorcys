// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package amqptest provides in-memory fakes of the amqpx interfaces for unit
// tests, in the spirit of net/http/httptest.
package amqptest

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/sassoftware/phorcys/internal/amqpx"
)

var (
	// Ensure that *Channel implements the amqpx.Channel interface.
	_ amqpx.Channel = (*Channel)(nil)
	// Ensure that *Connection implements the amqpx.Connection interface.
	_ amqpx.Connection = (*Connection)(nil)
)

// DeclaredQueue records a QueueDeclare call.
type DeclaredQueue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

// Binding records a QueueBind call.
type Binding struct {
	Queue    string
	Key      string
	Exchange string
	NoWait   bool
	Args     amqp.Table
}

// Consumer records a Consume call.
type Consumer struct {
	Queue     string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

// Publication records a PublishWithContext call that was accepted.
type Publication struct {
	Exchange  string
	Key       string
	Mandatory bool
	Immediate bool
	Msg       amqp.Publishing
}

// Channel is a fake amqpx.Channel. Configure its exported fields before use;
// recorded calls are available through its accessor methods. It is safe for
// concurrent use once configured.
//
// After Confirm succeeds, every accepted publish is acknowledged unless
// overridden per zero-based publish index via NackAt, NoConfirmAt, or
// CloseConfirmsAt.
type Channel struct {
	ConfirmErr error
	DeclareErr error
	BindErr    error
	ConsumeErr error
	// PublishErrAt fails the publish at the given index; the message is not recorded.
	PublishErrAt map[int]error
	// NackAt negatively acknowledges the publish at the given index.
	NackAt map[int]bool
	// NoConfirmAt sends no confirmation for the publish at the given index.
	NoConfirmAt map[int]bool
	// CloseConfirmsAt closes the confirmation channel instead of confirming the
	// publish at the given index.
	CloseConfirmsAt map[int]bool
	// Deliveries is returned from Consume. If nil, Consume creates an unbuffered
	// channel, which is retrievable via the Deliveries field afterwards.
	Deliveries chan amqp.Delivery

	mu            sync.Mutex
	confirmMode   bool
	confirmNoWait bool
	confirms      chan amqp.Confirmation
	declared      []DeclaredQueue
	bindings      []Binding
	consumers     []Consumer
	published     []Publication
	closed        bool
}

// NewChannel returns a Channel whose Deliveries channel is ready for tests to feed.
func NewChannel() *Channel {
	return &Channel{Deliveries: make(chan amqp.Delivery)}
}

// Confirm implements amqpx.Channel.
func (c *Channel) Confirm(noWait bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.confirmNoWait = noWait
	if c.ConfirmErr != nil {
		return c.ConfirmErr
	}
	c.confirmMode = true
	return nil
}

// NotifyPublish implements amqpx.Channel.
func (c *Channel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.confirms = confirm
	return confirm
}

// QueueDeclare implements amqpx.Channel.
func (c *Channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.declared = append(c.declared, DeclaredQueue{name, durable, autoDelete, exclusive, noWait, args})
	if c.DeclareErr != nil {
		return amqp.Queue{}, c.DeclareErr
	}
	return amqp.Queue{Name: name}, nil
}

// QueueBind implements amqpx.Channel.
func (c *Channel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bindings = append(c.bindings, Binding{name, key, exchange, noWait, args})
	return c.BindErr
}

// Consume implements amqpx.Channel.
func (c *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumers = append(c.consumers, Consumer{queue, consumer, autoAck, exclusive, noLocal, noWait, args})
	if c.ConsumeErr != nil {
		return nil, c.ConsumeErr
	}
	if c.Deliveries == nil {
		c.Deliveries = make(chan amqp.Delivery)
	}
	return c.Deliveries, nil
}

// PublishWithContext implements amqpx.Channel. The confirmation, if any, is
// sent synchronously, so callers must supply a buffered NotifyPublish channel
// or be ready to receive.
func (c *Channel) PublishWithContext(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	c.mu.Lock()
	i := len(c.published)
	if err := c.PublishErrAt[i]; err != nil {
		c.mu.Unlock()
		return err
	}
	c.published = append(c.published, Publication{exchange, key, mandatory, immediate, msg})
	confirms := c.confirms
	if !c.confirmMode || confirms == nil {
		c.mu.Unlock()
		return nil
	}
	switch {
	case c.CloseConfirmsAt[i]:
		close(confirms)
		c.confirms = nil
		c.mu.Unlock()
	case c.NoConfirmAt[i]:
		c.mu.Unlock()
	default:
		ack := !c.NackAt[i]
		c.mu.Unlock()
		confirms <- amqp.Confirmation{DeliveryTag: uint64(i + 1), Ack: ack}
	}
	return nil
}

// Close implements amqpx.Channel.
func (c *Channel) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

// ConfirmMode reports whether Confirm was called successfully.
func (c *Channel) ConfirmMode() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.confirmMode
}

// ConfirmNoWait reports the noWait argument of the last Confirm call.
func (c *Channel) ConfirmNoWait() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.confirmNoWait
}

// Declared returns all recorded QueueDeclare calls.
func (c *Channel) Declared() []DeclaredQueue {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]DeclaredQueue(nil), c.declared...)
}

// Bindings returns all recorded QueueBind calls.
func (c *Channel) Bindings() []Binding {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Binding(nil), c.bindings...)
}

// Consumers returns all recorded Consume calls.
func (c *Channel) Consumers() []Consumer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Consumer(nil), c.consumers...)
}

// Published returns all accepted publishes, in order.
func (c *Channel) Published() []Publication {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Publication(nil), c.published...)
}

// Closed reports whether Close was called.
func (c *Channel) Closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Connection is a fake amqpx.Connection that hands out a single Channel.
type Connection struct {
	Ch         *Channel
	ChannelErr error

	mu     sync.Mutex
	closed bool
}

// NewConnection returns a Connection that serves ch.
func NewConnection(ch *Channel) *Connection {
	return &Connection{Ch: ch}
}

// Channel implements amqpx.Connection.
func (c *Connection) Channel() (amqpx.Channel, error) {
	if c.ChannelErr != nil {
		return nil, c.ChannelErr
	}
	return c.Ch, nil
}

// Close implements amqpx.Connection.
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

// Closed reports whether Close was called.
func (c *Connection) Closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
