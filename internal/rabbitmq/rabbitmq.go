package rabbitmq

// Node represents the health metrics returned by /api/nodes
type Node struct {
	Name          string `json:"name"`
	Running       bool   `json:"running"`
	MemAlarm      bool   `json:"mem_alarm"`
	DiskFreeAlarm bool   `json:"disk_free_alarm"`
}

// Queue represents the state metrics returned by /api/queues
type Queue struct {
	Name    string   `json:"name"`
	VHost   string   `json:"vhost"`
	Type    string   `json:"type"`
	Status  string   `json:"status"`
	Leader  string   `json:"leader"`  // Specific to Quorum/Streams (Raft leader)
	Node    string   `json:"node"`    // Primary node hosting the process coordinator
	Members []string `json:"members"` // Raft cluster cluster nodes for this queue
}
