package jobqueue

import (
	"context"
	"testing"

	"github.com/sassoftware/phorcys/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	jq := New()
	assert.NotNil(t, jq.queue)
	assert.Equal(t, cap(jq.queue), jobQueueBufferSize)
}

func TestJobQueue_Add(t *testing.T) {
	jq := New()
	jq.Add("vhost1", "queue1")
	assert.Equal(t, 1, len(jq.queue))
	jq.Add("vhost2", "queue2")
	assert.Equal(t, 2, len(jq.queue))

	job1 := <-jq.queue
	assert.Equal(t, "vhost1", job1.VHost)
	assert.Equal(t, "queue1", job1.Name)
	assert.Equal(t, 1, len(jq.queue))

	job2 := <-jq.queue
	assert.Equal(t, "vhost2", job2.VHost)
	assert.Equal(t, "queue2", job2.Name)
	assert.Equal(t, 0, len(jq.queue))
}

func TestJobQueue_GetNext(t *testing.T) {
	ctx := context.Background()
	jq := New()
	jq.queue <- common.QueueDef{VHost: "vhost1", Name: "queue1"}
	jq.queue <- common.QueueDef{VHost: "vhost2", Name: "queue2"}

	job, err := jq.GetNext(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "vhost1", job.VHost)
	assert.Equal(t, "queue1", job.Name)
	assert.Equal(t, 1, len(jq.queue))

	job, err = jq.GetNext(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "vhost2", job.VHost)
	assert.Equal(t, "queue2", job.Name)
	assert.Equal(t, 0, len(jq.queue))

	// Test context cancellation
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = jq.GetNext(ctx)
	assert.Error(t, err)
}

func TestJobQueue_Len(t *testing.T) {
	jq := New()
	assert.Equal(t, 0, jq.Len())

	jq.queue <- common.QueueDef{VHost: "vhost1", Name: "queue1"}
	assert.Equal(t, 1, jq.Len())

	jq.queue <- common.QueueDef{VHost: "vhost2", Name: "queue2"}
	assert.Equal(t, 2, jq.Len())

	<-jq.queue
	assert.Equal(t, 1, jq.Len())

	<-jq.queue
	assert.Equal(t, 0, jq.Len())
}
