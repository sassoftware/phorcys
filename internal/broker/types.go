// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package broker

// RabbitNode represents the health metrics returned by /api/nodes
type RabbitNode struct {
	Name          string `json:"name"`
	Running       bool   `json:"running"`
	MemAlarm      bool   `json:"mem_alarm"`
	DiskFreeAlarm bool   `json:"disk_free_alarm"`
}

// RabbitQueue represents the state metrics returned by /api/queues
type RabbitQueue struct {
	Name    string   `json:"name"`
	VHost   string   `json:"vhost"`
	Type    string   `json:"type"`
	Status  string   `json:"status"`
	Leader  string   `json:"leader"`  // Specific to Quorum/Streams (Raft leader)
	Node    string   `json:"node"`    // Primary node hosting the process coordinator
	Members []string `json:"members"` // Raft cluster cluster nodes for this queue
}

// QueueDefinition holds the subset of a queue's declaration fields needed to
// recreate it, as exported by /api/definitions/{vhost}.
type QueueDefinition struct {
	Name       string         `json:"name"`
	VHost      string         `json:"vhost"`
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Arguments  map[string]any `json:"arguments"`
}

// QueueBinding describes one binding exported by /api/definitions/{vhost} whose
// destination is a queue.
type QueueBinding struct {
	Source          string         `json:"source"`
	VHost           string         `json:"vhost"`
	Destination     string         `json:"destination"`
	DestinationType string         `json:"destination_type"`
	RoutingKey      string         `json:"routing_key"`
	Arguments       map[string]any `json:"arguments"`
}
