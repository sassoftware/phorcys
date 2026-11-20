package jobqueue

import (
	"context"
	"errors"
)

const jobQueueBufferSize = 1000

// Job is a single health-check work item dispatched to the worker pool.
type Job struct {
	VHost string
	Name  string
}

type JobQueue struct {
	queue chan Job
}

func New() *JobQueue {
	return &JobQueue{
		queue: make(chan Job, jobQueueBufferSize),
	}
}

func (jq *JobQueue) Add(vhost, name string) {
	jq.queue <- Job{VHost: vhost, Name: name}
}

func (jq *JobQueue) GetNext(ctx context.Context) (*Job, error) {
	select {
	case <-ctx.Done():
		return nil, errors.New("context canceled while waiting for next job")
	case job := <-jq.queue:
		return &job, nil
	}
}

func (jq *JobQueue) Size() int {
	return len(jq.queue)
}
