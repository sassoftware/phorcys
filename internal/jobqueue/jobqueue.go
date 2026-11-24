package jobqueue

import (
	"context"
	"errors"

	"github.com/sassoftware/phorcys/internal/common"
)

const jobQueueBufferSize = 1000

type JobQueue struct {
	queue chan common.QueueDef
}

func New() *JobQueue {
	return &JobQueue{
		queue: make(chan common.QueueDef, jobQueueBufferSize),
	}
}

func (jq *JobQueue) Add(vhost, name string) {
	jq.queue <- common.QueueDef{VHost: vhost, Name: name}
}

func (jq *JobQueue) GetNext(ctx context.Context) (*common.QueueDef, error) {
	select {
	case <-ctx.Done():
		return nil, errors.New("context canceled while waiting for next job")
	case job := <-jq.queue:
		return &job, nil
	}
}

func (jq *JobQueue) Len() int {
	return len(jq.queue)
}
