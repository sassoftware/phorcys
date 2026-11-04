// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sassoftware/phorcys/internal/amqpx/amqptest"
	"github.com/sassoftware/phorcys/internal/broker"
	"github.com/sassoftware/phorcys/internal/runtime"
)

const peerHost = "peer-node"
const selfNode = "rabbit@self-node"

// ---------------------------------------------------------------------------
// SelectRecoveryNode
// ---------------------------------------------------------------------------

func TestSelectRecoveryNode(t *testing.T) {
	ann := func(host string, term, idx uint64) RecoveryAnnouncement {
		return RecoveryAnnouncement{Hostname: host, Term: term, Index: idx}
	}
	tests := []struct {
		name string
		anns []RecoveryAnnouncement
		want string
	}{
		{"higher term wins despite lower index", []RecoveryAnnouncement{ann("a", 3, 10), ann("b", 4, 1)}, "b"},
		{"same term, higher index wins", []RecoveryAnnouncement{ann("a", 4, 10), ann("b", 4, 11)}, "b"},
		{"full tie goes to alphabetically first", []RecoveryAnnouncement{ann("c", 4, 10), ann("a", 4, 10), ann("b", 4, 10)}, "a"},
		{"tie-break only among the best position", []RecoveryAnnouncement{ann("a", 1, 1), ann("c", 4, 10), ann("b", 4, 10)}, "b"},
		{"single node", []RecoveryAnnouncement{ann("only", 0, 0)}, "only"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := SelectRecoveryNode(tt.anns)
			require.True(t, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSelectRecoveryNode_Empty(t *testing.T) {
	_, ok := SelectRecoveryNode(nil)
	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// hostMatchesMember / missingMembers
// ---------------------------------------------------------------------------

func TestHostMatchesMember(t *testing.T) {
	assert.True(t, hostMatchesMember("rabbit@node1", "node1"))
	assert.True(t, hostMatchesMember("rabbit@NODE1", "node1"))
	assert.True(t, hostMatchesMember("rabbit@node1.example.com", "node1"))
	assert.True(t, hostMatchesMember("rabbit@node1", "node1.example.com"))
	assert.True(t, hostMatchesMember("node1", "node1"))
	assert.False(t, hostMatchesMember("rabbit@node1", "node2"))
	assert.False(t, hostMatchesMember("rabbit@10.0.0.1", "10.0.0.2"))
}

func TestMissingMembers(t *testing.T) {
	anns := []RecoveryAnnouncement{{Hostname: "n1"}, {Hostname: "n3.example.com"}}
	assert.Equal(t, []string{"rabbit@n2"}, missingMembers([]string{"rabbit@n1", "rabbit@n2", "rabbit@n3"}, anns))
	assert.Empty(t, missingMembers(nil, anns))
}

// ---------------------------------------------------------------------------
// Start / handleAnnouncement
// ---------------------------------------------------------------------------

func newTestCoordinator(conn *amqptest.Connection, dm *broker.DiagnosticsManager, cfg runtime.Config) *Coordinator {
	c := NewCoordinator(conn, dm, cfg)
	c.hostname = "self-node"
	return c
}

func TestNewCoordinator_Timeout(t *testing.T) {
	assert.Equal(t, defaultConsensusTimeout, NewCoordinator(nil, nil, runtime.Config{}).timeout)
	assert.Equal(t, 5*time.Second, NewCoordinator(nil, nil, runtime.Config{ConsensusTimeout: 5 * time.Second}).timeout)
}

func TestCoordinator_Start_DeclaresBindsAndRecordsAnnouncements(t *testing.T) {
	ch := amqptest.NewChannel()
	c := newTestCoordinator(amqptest.NewConnection(ch), nil, runtime.Config{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()

	ann := RecoveryAnnouncement{Hostname: c.hostname, VHost: "/", Queue: testQueue, Timestamp: time.Now(), Term: 2, Index: 9}
	body, err := json.Marshal(ann)
	require.NoError(t, err)
	ch.Deliveries <- amqp.Delivery{Body: body}
	ch.Deliveries <- amqp.Delivery{Body: []byte("not json")}
	ch.Deliveries <- amqp.Delivery{Body: []byte(`{"hostname":"","queue":""}`)}

	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.announcements[queueKey("/", testQueue)]) == 1
	}, time.Second, 5*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	exchanges := ch.DeclaredExchanges()
	require.Len(t, exchanges, 1)
	assert.Equal(t, ConsensusExchange, exchanges[0].Name)
	assert.Equal(t, "fanout", exchanges[0].Kind)

	declared := ch.Declared()
	require.Len(t, declared, 1)
	assert.Equal(t, consensusQueuePrefix+"self-node", declared[0].Name)
	assert.False(t, declared[0].Exclusive)
	assert.False(t, declared[0].AutoDelete)
	assert.True(t, declared[0].Durable)
	assert.Contains(t, declared[0].Args, "x-message-ttl")

	bindings := ch.Bindings()
	require.Len(t, bindings, 1)
	assert.Equal(t, ConsensusExchange, bindings[0].Exchange)
	assert.Equal(t, consensusQueuePrefix+"self-node", bindings[0].Queue)

	require.Len(t, ch.Consumers(), 1)
	assert.True(t, ch.Closed())
}

func TestCoordinator_Start_Errors(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name string
		conn *amqptest.Connection
		want string
	}{
		{"channel", &amqptest.Connection{ChannelErr: boom}, "failed to open channel"},
		{"exchange", amqptest.NewConnection(&amqptest.Channel{ExchangeDeclareErr: boom}), "failed to declare consensus exchange"},
		{"queue", amqptest.NewConnection(&amqptest.Channel{DeclareErr: boom}), "failed to declare consensus queue"},
		{"bind", amqptest.NewConnection(&amqptest.Channel{BindErr: boom}), "failed to bind consensus queue"},
		{"consume", amqptest.NewConnection(&amqptest.Channel{ConsumeErr: boom}), "failed to consume consensus queue"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newTestCoordinator(tt.conn, nil, runtime.Config{}).Start(context.Background())
			require.ErrorIs(t, err, boom)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestCoordinator_Start_ReturnsNilWhenDeliveriesClose(t *testing.T) {
	ch := amqptest.NewChannel()
	close(ch.Deliveries)
	err := newTestCoordinator(amqptest.NewConnection(ch), nil, runtime.Config{}).Start(context.Background())
	assert.NoError(t, err)
}

func TestCoordinator_HandleAnnouncement_IgnoresOwnAndStale(t *testing.T) {
	// A nil DiagnosticsManager would panic if respond ran.
	c := newTestCoordinator(nil, nil, runtime.Config{})

	own, _ := json.Marshal(RecoveryAnnouncement{Hostname: c.hostname, VHost: "/", Queue: testQueue, Timestamp: time.Now()})
	stale, _ := json.Marshal(RecoveryAnnouncement{Hostname: peerHost, VHost: "/", Queue: testQueue, Timestamp: time.Now().Add(-time.Hour)})
	c.handleAnnouncement(context.Background(), own)
	c.handleAnnouncement(context.Background(), stale)

	c.mu.Lock()
	defer c.mu.Unlock()
	fresh := c.fresh(queueKey("/", testQueue))
	require.Len(t, fresh, 1, "the stale peer announcement must not count")
	assert.Equal(t, c.hostname, fresh[0].Hostname)
}

func TestCoordinator_Record_KeepsNewestPerHost(t *testing.T) {
	c := newTestCoordinator(nil, nil, runtime.Config{})
	now := time.Now()
	c.record(RecoveryAnnouncement{Hostname: peerHost, VHost: "/", Queue: testQueue, Timestamp: now, Term: 5})
	c.record(RecoveryAnnouncement{Hostname: peerHost, VHost: "/", Queue: testQueue, Timestamp: now.Add(-time.Second), Term: 4})

	c.mu.Lock()
	defer c.mu.Unlock()
	anns := c.fresh(queueKey("/", testQueue))
	require.Len(t, anns, 1)
	assert.Equal(t, uint64(5), anns[0].Term)
}

// ---------------------------------------------------------------------------
// Recover / respond
// ---------------------------------------------------------------------------

// consensusServer fakes the management API for a cluster whose single queue
// is members-wide. It records DELETE requests and answers health probes as
// unrecoverable when queueStatus is 500.
type consensusServer struct {
	*httptest.Server
	mu      sync.Mutex
	deletes []string
}

func newConsensusServer(t *testing.T, members []string, queueStatus int) *consensusServer {
	t.Helper()
	cs := &consensusServer{}
	cs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodDelete:
			cs.mu.Lock()
			cs.deletes = append(cs.deletes, r.URL.EscapedPath())
			cs.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Path == "/api/nodes":
			nodes := make([]broker.RabbitNode, 0, len(members))
			for _, m := range members {
				nodes = append(nodes, broker.RabbitNode{Name: m, Running: true})
			}
			_ = json.NewEncoder(w).Encode(nodes)
		case strings.HasPrefix(r.URL.Path, "/api/definitions/"):
			_, _ = w.Write([]byte(`{"queues":[],"bindings":[]}`))
		case strings.HasPrefix(r.URL.Path, "/api/queues/"):
			if queueStatus != http.StatusOK {
				w.WriteHeader(queueStatus)
				return
			}
			_ = json.NewEncoder(w).Encode(broker.RabbitQueue{Name: testQueue, Members: members})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(cs.Close)
	return cs
}

func (cs *consensusServer) deleted() []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return append([]string(nil), cs.deletes...)
}

func recoverFixture(t *testing.T, cs *consensusServer, timeout time.Duration) *Coordinator {
	t.Helper()
	quorumBase := t.TempDir()
	writeConfigFile(t, quorumBase, "queue-uid-consensus", "/", testQueue)
	writeSegmentBackup(t, quorumBase+"/queue-uid-consensus", "0000000000000001.segment", []segmentEntry{
		{idx: 5, term: 2, data: []byte("x")},
	})
	cfg := runtime.Config{
		QuorumBasePath:   quorumBase,
		BackupBaseDir:    t.TempDir(),
		AMQPURL:          unreachableAMQPURL, // announcement publish is non-fatal; no messages to republish
		ConsensusTimeout: timeout,
	}
	dm := &broker.DiagnosticsManager{APIURL: cs.URL, Client: cs.Client()}
	return newTestCoordinator(nil, dm, cfg)
}

func TestCoordinator_Recover_SingleMemberExecutes(t *testing.T) {
	cs := newConsensusServer(t, []string{selfNode}, http.StatusOK)
	c := recoverFixture(t, cs, time.Minute)

	require.NoError(t, c.Recover(context.Background(), "/", testQueue))

	assert.Equal(t, []string{"/api/queues/%2F/" + testQueue}, cs.deleted())
	assert.False(t, c.busy(queueKey("/", testQueue)), "round must be released")
	assert.Empty(t, c.announcements, "round announcements must be discarded")
}

func TestCoordinator_Recover_LosesToPeerWithNewerPosition(t *testing.T) {
	cs := newConsensusServer(t, []string{selfNode, "rabbit@" + peerHost}, http.StatusOK)
	c := recoverFixture(t, cs, time.Minute)
	c.record(RecoveryAnnouncement{Hostname: peerHost, VHost: "/", Queue: testQueue, Timestamp: time.Now(), Term: 3, Index: 1})

	require.NoError(t, c.Recover(context.Background(), "/", testQueue))

	assert.Empty(t, cs.deleted(), "the node with the lower term must not run Execute")
}

func TestCoordinator_Recover_TieGoesToAlphabeticallyFirstHostname(t *testing.T) {
	// The fixture's own position is term 2 / index 5; the peer reports the same.
	tie := RecoveryAnnouncement{Hostname: peerHost, VHost: "/", Queue: testQueue, Timestamp: time.Now(), Term: 2, Index: 5}

	t.Run("later hostname defers", func(t *testing.T) {
		cs := newConsensusServer(t, []string{"rabbit@zulu-node", "rabbit@" + peerHost}, http.StatusOK)
		c := recoverFixture(t, cs, time.Minute)
		c.hostname = "zulu-node"
		c.record(tie)

		require.NoError(t, c.Recover(context.Background(), "/", testQueue))
		assert.Empty(t, cs.deleted())
	})

	t.Run("earlier hostname executes", func(t *testing.T) {
		cs := newConsensusServer(t, []string{"rabbit@alpha-node", "rabbit@" + peerHost}, http.StatusOK)
		c := recoverFixture(t, cs, time.Minute)
		c.hostname = "alpha-node"
		c.record(tie)

		require.NoError(t, c.Recover(context.Background(), "/", testQueue))
		assert.Len(t, cs.deleted(), 1)
	})
}

func TestCoordinator_Recover_WaitsForPeerAnnouncement(t *testing.T) {
	cs := newConsensusServer(t, []string{selfNode, "rabbit@" + peerHost}, http.StatusOK)
	c := recoverFixture(t, cs, time.Minute)

	go func() {
		time.Sleep(50 * time.Millisecond)
		c.record(RecoveryAnnouncement{Hostname: peerHost, VHost: "/", Queue: testQueue, Timestamp: time.Now(), Term: 1, Index: 1})
	}()

	require.NoError(t, c.Recover(context.Background(), "/", testQueue))

	assert.Len(t, cs.deleted(), 1, "self (term 2) beats the late peer announcement (term 1)")
}

func TestQuorum(t *testing.T) {
	for n, want := range map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3} {
		assert.Equalf(t, want, quorum(n), "quorum(%d)", n)
	}
}

func TestCoordinator_Recover_TimesOutAndDecidesWithMajority(t *testing.T) {
	cs := newConsensusServer(t, []string{selfNode, "rabbit@" + peerHost, "rabbit@silent-node"}, http.StatusOK)
	c := recoverFixture(t, cs, 50*time.Millisecond)
	c.record(RecoveryAnnouncement{Hostname: peerHost, VHost: "/", Queue: testQueue, Timestamp: time.Now(), Term: 1, Index: 1})

	require.NoError(t, c.Recover(context.Background(), "/", testQueue))

	assert.Len(t, cs.deleted(), 1)
}

func TestCoordinator_Recover_TimesOutWithoutMajorityDoesNotExecute(t *testing.T) {
	cs := newConsensusServer(t, []string{selfNode, "rabbit@" + peerHost, "rabbit@silent-node"}, http.StatusOK)
	c := recoverFixture(t, cs, 50*time.Millisecond)

	err := c.Recover(context.Background(), "/", testQueue)

	require.ErrorIs(t, err, errNoQuorum)
	assert.Empty(t, cs.deleted())
}

func TestCoordinator_Recover_ContextCancelledWhileWaiting(t *testing.T) {
	cs := newConsensusServer(t, []string{selfNode, "rabbit@" + peerHost}, http.StatusOK)
	c := recoverFixture(t, cs, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := c.Recover(ctx, "/", testQueue)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, cs.deleted())
}

func TestCoordinator_Recover_AlreadyInProgressIsNoOp(t *testing.T) {
	c := newTestCoordinator(nil, nil, runtime.Config{}) // Prepare would panic on a nil manager if reached
	require.True(t, c.begin(queueKey("/", testQueue)))

	assert.NoError(t, c.Recover(context.Background(), "/", testQueue))
}

func TestCoordinator_Recover_PrepareErrorIsReturned(t *testing.T) {
	cs := newConsensusServer(t, []string{selfNode}, http.StatusOK)
	c := newTestCoordinator(nil, &broker.DiagnosticsManager{APIURL: cs.URL, Client: cs.Client()},
		runtime.Config{QuorumBasePath: t.TempDir(), BackupBaseDir: t.TempDir()})

	err := c.Recover(context.Background(), "/", "missing")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "locate phase")
}

func TestCoordinator_Recover_MemberLookupFailureIsReturned(t *testing.T) {
	// Queue endpoint 404s, so members can't be determined and Execute must not run.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/definitions/") {
			_, _ = w.Write([]byte(`{"queues":[],"bindings":[]}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	quorumBase := t.TempDir()
	writeConfigFile(t, quorumBase, "uid", "/", testQueue)
	c := newTestCoordinator(nil, &broker.DiagnosticsManager{APIURL: srv.URL, Client: srv.Client()},
		runtime.Config{QuorumBasePath: quorumBase, BackupBaseDir: t.TempDir(), AMQPURL: unreachableAMQPURL})

	err := c.Recover(context.Background(), "/", testQueue)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "determining queue members")
}

func TestCoordinator_Respond_SkipsQueueThatIsNotUnrecoverable(t *testing.T) {
	// A node with a memory alarm makes EvaluateQueueHealth report a transient state.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]broker.RabbitNode{{Name: selfNode, Running: true, MemAlarm: true}})
	}))
	defer srv.Close()
	// Nil quorum config: Prepare would fail with "locate phase" if reached, and Recover would register in-flight.
	c := newTestCoordinator(nil, &broker.DiagnosticsManager{APIURL: srv.URL, Client: srv.Client()}, runtime.Config{})

	c.respond(context.Background(), "/", testQueue)

	assert.Empty(t, c.announcements, "no round should have started")
}

func TestCoordinator_Respond_HealthErrorIsLoggedNotFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	c := newTestCoordinator(nil, &broker.DiagnosticsManager{APIURL: srv.URL, Client: srv.Client()}, runtime.Config{})

	assert.NotPanics(t, func() { c.respond(context.Background(), "/", testQueue) })
}

func TestCoordinator_Respond_UnrecoverableQueueWithoutMembersDoesNotExecute(t *testing.T) {
	// The queue endpoint 500s: health is unrecoverable but members can't be determined.
	cs := newConsensusServer(t, []string{selfNode}, http.StatusInternalServerError)
	c := recoverFixture(t, cs, time.Minute)

	c.respond(context.Background(), "/", testQueue)

	assert.Empty(t, cs.deleted())
}
