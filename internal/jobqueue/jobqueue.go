package jobqueue

import (
	"context"
	"errors"

	"github.com/sassoftware/phorcys/internal/common"
)

const jobQueueBufferSize = 1000

// JobQueue is a simple in-memory queue for storing jobs to be processed.
type JobQueue struct {
	queue chan *common.QueueDef
}

// New creates a new JobQueue with a buffered channel.
func New() *JobQueue {
	return &JobQueue{
		queue: make(chan *common.QueueDef, jobQueueBufferSize),
	}
}

// Add adds a new job to the queue.
// It blocks if the queue is full until space becomes available.
func (jq *JobQueue) Add(vhost, name string) {
	jq.queue <- &common.QueueDef{VHost: vhost, Name: name}
}

// GetNext retrieves the next job from the queue, blocking until a job is available or the context is canceled.
func (jq *JobQueue) GetNext(ctx context.Context) (*common.QueueDef, error) {
	select {
	case <-ctx.Done():
		return nil, errors.New("context canceled while waiting for next job")
	case job := <-jq.queue:
		return job, nil
	}
}

// Len returns the number of jobs currently in the queue.
func (jq *JobQueue) Len() int {
	return len(jq.queue)
}
