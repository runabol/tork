package mq_test

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork/job"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/task"
	"github.com/stretchr/testify/assert"
)

func TestInMemoryPublishAndSubsribeForTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	processed := 0
	err := b.SubscribeForTasks("test-queue", func(t *task.Task) error {
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishTask(ctx, "test-queue", &task.Task{})
	// wait for task to be processed
	time.Sleep(time.Millisecond * 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, processed)
}

func TestInMemoryGetQueue(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	err := b.PublishTask(ctx, "test-queue", &task.Task{})
	assert.NoError(t, err)
	qi, err := b.Queue(ctx, "test-queue")
	assert.NoError(t, err)
	assert.Equal(t, 1, qi.Size)
}

func TestInMemoryGetQueues(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	err := b.PublishTask(ctx, "test-queue", &task.Task{})
	assert.NoError(t, err)
	qis, err := b.Queues(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(qis))
}

func TestInMemoryPublishAndSubsribeForHeartbeat(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	processed := 0
	err := b.SubscribeForHeartbeats(func(n node.Node) error {
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishHeartbeat(ctx, node.Node{})
	// wait for heartbeat to be processed
	time.Sleep(time.Millisecond * 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, processed)
}

func TestInMemoryPublishAndSubsribeForJob(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	processed := 0
	err := b.SubscribeForJobs(func(j *job.Job) error {
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishJob(ctx, &job.Job{})
	// wait for heartbeat to be processed
	time.Sleep(time.Millisecond * 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, processed)
}
