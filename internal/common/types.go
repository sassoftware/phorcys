package common

// QueueDef represents a queue that needs to be recovered, including its vhost and name.
type QueueDef struct {
	VHost string
	Name  string
}
