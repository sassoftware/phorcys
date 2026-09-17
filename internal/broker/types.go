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
