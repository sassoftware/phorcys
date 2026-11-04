// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/sassoftware/phorcys/internal/amqpx"
	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/runtime"
)

const (
	// consensusQueuePrefix + hostname names the queue each Phorcys instance
	// uses to receive other instances' announcements from ConsensusExchange.
	consensusQueuePrefix = "phorcys.consensus."

	// announcementMaxAge bounds how long an announcement is trusted, so a
	// previous incident's positions never influence a new round for the same queue.
	announcementMaxAge = 10 * time.Minute

	defaultConsensusTimeout = 2 * time.Minute
)

// errNoQuorum means too few of a queue's members announced for it to be safe
// to pick the node with the newest data.
var errNoQuorum = errors.New("no majority of queue members announced")

// Coordinator runs the node selection/consensus protocol: it consumes other
// nodes' RecoveryAnnouncements, responds to queues it has not yet prepared,
// and runs Execute only on the node holding the most recent Raft position.
type Coordinator struct {
	conn     amqpx.Connection
	dm       *broker.DiagnosticsManager
	cfg      runtime.Config
	hostname string
	timeout  time.Duration

	mu            sync.Mutex
	announcements map[string]map[string]RecoveryAnnouncement // queue key -> hostname -> newest
	inFlight      map[string]struct{}
	changed       chan struct{} // closed and replaced whenever an announcement is recorded
}

// NewCoordinator returns a Coordinator that consumes announcements over conn.
func NewCoordinator(conn amqpx.Connection, dm *broker.DiagnosticsManager, cfg runtime.Config) *Coordinator {
	timeout := cfg.ConsensusTimeout
	if timeout <= 0 {
		timeout = defaultConsensusTimeout
	}
	return &Coordinator{
		conn:          conn,
		dm:            dm,
		cfg:           cfg,
		hostname:      cfg.Hostname,
		timeout:       timeout,
		announcements: make(map[string]map[string]RecoveryAnnouncement),
		inFlight:      make(map[string]struct{}),
		changed:       make(chan struct{}),
	}
}

// Start declares this node's announcement queue, binds it to ConsensusExchange
// and consumes announcements until ctx is cancelled or the channel closes.
func (c *Coordinator) Start(ctx context.Context) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.ExchangeDeclare(ConsensusExchange, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("failed to declare consensus exchange: %w", err)
	}

	// Durable and kept while no consumer is attached, so announcements made
	// while Phorcys was down or the broker restarted are still delivered; the
	// TTL bounds how stale they can be.
	q, err := ch.QueueDeclare(
		consensusQueuePrefix+c.hostname,
		true,  // durable
		false, // keep when the consumer detaches
		false, // not exclusive, which would delete it with the connection
		false,
		amqp.Table{"x-message-ttl": int32(announcementMaxAge / time.Millisecond)},
	)
	if err != nil {
		return fmt.Errorf("failed to declare consensus queue: %w", err)
	}
	if err := ch.QueueBind(q.Name, "", ConsensusExchange, false, nil); err != nil {
		return fmt.Errorf("failed to bind consensus queue: %w", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume consensus queue: %w", err)
	}

	log.Printf("[Consensus] Listening for announcements on %s as %s", q.Name, c.hostname)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}
			c.handleAnnouncement(ctx, msg.Body)
			// Acked even if malformed: requeueing a poison message would redeliver it forever.
			if err := msg.Ack(false); err != nil {
				log.Printf("[Consensus] WARNING: failed to ack announcement: %v", err)
			}
		}
	}
}

// handleAnnouncement records a peer's announcement and, if this node isn't
// already handling the queue, responds to it so the peer can compare positions.
func (c *Coordinator) handleAnnouncement(ctx context.Context, body []byte) {
	var ann RecoveryAnnouncement
	if err := json.Unmarshal(body, &ann); err != nil {
		log.Printf("[Consensus] WARNING: ignoring malformed announcement: %v", err)
		return
	}
	if ann.Hostname == "" || ann.Queue == "" {
		log.Printf("[Consensus] WARNING: ignoring announcement without hostname or queue")
		return
	}

	c.record(ann)
	if ann.Hostname == c.hostname || time.Since(ann.Timestamp) > announcementMaxAge {
		return
	}

	key := queueKey(ann.VHost, ann.Queue)
	if c.busy(key) {
		return
	}
	log.Printf("[Consensus] %s announced %s: term=%d idx=%d", ann.Hostname, key, ann.Term, ann.Index)
	go c.respond(ctx, ann.VHost, ann.Queue)
}

// respond runs this node's side of a peer's announcement: if the queue is also
// unrecoverable from here, back up local data and join the consensus round.
// The health check guards against acting on announcements for queues that
// have since been recovered or deleted.
func (c *Coordinator) respond(ctx context.Context, vhost, queueName string) {
	key := queueKey(vhost, queueName)
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Consensus] Response PANIC for %s: %v", key, r)
		}
	}()

	health, err := c.dm.EvaluateQueueHealth(ctx, vhost, queueName)
	if err != nil {
		log.Printf("[Consensus] Health evaluation failed for %s: %v", key, err)
		return
	}
	if health != broker.HealthUnrecoverable {
		log.Printf("[Consensus] %s is %v from this node — not joining recovery", key, health)
		return
	}
	if err := c.Recover(ctx, vhost, queueName); err != nil {
		log.Printf("[Consensus] Recovery FAILED for %s: %v", key, err)
	}
}

// Recover runs Prepare, announces this node's position, waits for the queue's
// other replicas to announce theirs, and runs Execute only if this node holds
// the most recent data. It is a no-op if a round for the queue is already
// running on this node.
func (c *Coordinator) Recover(ctx context.Context, vhost, queueName string) error {
	key := queueKey(vhost, queueName)
	if !c.begin(key) {
		log.Printf("[Consensus] Round for %s already in progress", key)
		return nil
	}
	defer c.end(key)

	prep, err := Prepare(ctx, c.cfg, c.dm, vhost, queueName)
	if err != nil {
		return err
	}
	// Don't rely on our own announcement looping back through the broker.
	c.record(RecoveryAnnouncement{
		Hostname:  c.hostname,
		VHost:     vhost,
		Queue:     queueName,
		Timestamp: time.Now().UTC(),
		Term:      prep.Position.Term,
		Index:     prep.Position.Idx,
	})

	members, err := c.dm.FetchQueueMembers(ctx, vhost, queueName)
	if err != nil {
		return fmt.Errorf("consensus: determining queue members: %w", err)
	}

	anns, err := c.awaitMembers(ctx, key, members)
	if err := ctx.Err(); err != nil {
		return err
	}
	if errors.Is(err, errNoQuorum) {
		return fmt.Errorf("consensus: %s: %w", key, err)
	}
	if err != nil {
		log.Printf("[Consensus] WARNING: %s: %v — deciding without them", key, err)
	}

	winner, ok := SelectRecoveryNode(anns)
	if !ok || winner != c.hostname {
		log.Printf("[Consensus] %s: %q holds the most recent data — leaving recovery to it", key, winner)
		return nil
	}
	log.Printf("[Consensus] %s: this node (%s) holds the most recent data — running recovery", key, c.hostname)
	return Execute(ctx, c.cfg, c.dm, prep)
}

// awaitMembers returns the fresh announcements for key once every member has
// announced. If the timeout expires first it returns what has arrived and an
// error naming the silent members, wrapping errNoQuorum if they are a majority.
// If ctx expires it returns ctx's error.
func (c *Coordinator) awaitMembers(ctx context.Context, key string, members []string) ([]RecoveryAnnouncement, error) {
	timer := time.NewTimer(c.timeout)
	defer timer.Stop()

	for {
		c.mu.Lock()
		anns := c.fresh(key)
		changed := c.changed
		c.mu.Unlock()

		missing := missingMembers(members, anns)
		if len(missing) == 0 {
			return anns, nil
		}

		select {
		case <-changed:
		case <-timer.C:
			if len(members)-len(missing) < quorum(len(members)) {
				return anns, fmt.Errorf("%w: no announcement from %v within %v", errNoQuorum, missing, c.timeout)
			}
			return anns, fmt.Errorf("no announcement from %v within %v", missing, c.timeout)
		case <-ctx.Done():
			return anns, ctx.Err()
		}
	}
}

// record stores ann, keeping only the newest announcement per node and queue.
func (c *Coordinator) record(ann RecoveryAnnouncement) {
	key := queueKey(ann.VHost, ann.Queue)

	c.mu.Lock()
	defer c.mu.Unlock()

	byHost := c.announcements[key]
	if byHost == nil {
		byHost = make(map[string]RecoveryAnnouncement)
		c.announcements[key] = byHost
	}
	if prev, ok := byHost[ann.Hostname]; ok && prev.Timestamp.After(ann.Timestamp) {
		return
	}
	byHost[ann.Hostname] = ann

	close(c.changed)
	c.changed = make(chan struct{})
}

// fresh returns the unexpired announcements for key. c.mu must be held.
func (c *Coordinator) fresh(key string) []RecoveryAnnouncement {
	var out []RecoveryAnnouncement
	for _, a := range c.announcements[key] {
		if time.Since(a.Timestamp) <= announcementMaxAge {
			out = append(out, a)
		}
	}
	return out
}

func (c *Coordinator) busy(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.inFlight[key]
	return ok
}

func (c *Coordinator) begin(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.inFlight[key]; ok {
		return false
	}
	c.inFlight[key] = struct{}{}
	return true
}

// end marks the round over and discards its announcements so they cannot
// leak into a later incident on the same queue.
func (c *Coordinator) end(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inFlight, key)
	delete(c.announcements, key)
}

func queueKey(vhost, queueName string) string {
	return vhost + "/" + queueName
}

// quorum is the smallest majority of n members. Raft commits an entry once a
// majority hold it, so the newest position among a majority covers every
// confirmed message.
func quorum(n int) int {
	return n/2 + 1
}

// missingMembers returns the members (RabbitMQ node names such as
// "rabbit@host") that no announcement in anns came from.
func missingMembers(members []string, anns []RecoveryAnnouncement) []string {
	var missing []string
	for _, m := range members {
		heard := false
		for _, a := range anns {
			if hostMatchesMember(m, a.Hostname) {
				heard = true
				break
			}
		}
		if !heard {
			missing = append(missing, m)
		}
	}
	return missing
}

// hostMatchesMember reports whether hostname (as announced) is the host part
// of a RabbitMQ node name. Short and fully-qualified forms of the same host
// match, since the node name and os.Hostname often differ in that respect.
func hostMatchesMember(member, hostname string) bool {
	if i := strings.LastIndex(member, "@"); i >= 0 {
		member = member[i+1:]
	}
	return strings.EqualFold(member, hostname) || strings.EqualFold(shortHost(member), shortHost(hostname))
}

// shortHost strips the domain from a DNS name, leaving IP addresses intact.
func shortHost(h string) string {
	if net.ParseIP(h) != nil {
		return h
	}
	if i := strings.IndexByte(h, '.'); i >= 0 {
		return h[:i]
	}
	return h
}
