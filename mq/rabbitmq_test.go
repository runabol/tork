package mq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tork/job"
	"github.com/tork/mq"
	"github.com/tork/node"
	"github.com/tork/task"
	"github.com/tork/uuid"
)

func TestRabbitMQPublishAndSubsribeForTask(t *testing.T) {
	ctx := context.Background()
	b, err := mq.NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	processed := 0
	qname := fmt.Sprintf("%stest-%s", mq.QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	err = b.SubscribeForTasks(qname, func(t *task.Task) error {
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishTask(ctx, qname, &task.Task{})
	// wait for task to be processed
	time.Sleep(time.Millisecond * 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, processed)
}

func TestRabbitMQGetQueues(t *testing.T) {
	ctx := context.Background()
	b, err := mq.NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	qname := fmt.Sprintf("%stest-%s", mq.QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	err = b.PublishTask(ctx, qname, &task.Task{})
	assert.NoError(t, err)
	qis, err := b.Queues(ctx)
	assert.NoError(t, err)
	found := false
	for _, qi := range qis {
		if qi.Name == qname {
			found = true
		}
	}
	assert.True(t, found)
}

func TestRabbitMQPublishAndSubsribeForHeartbeat(t *testing.T) {
	ctx := context.Background()
	b, err := mq.NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	processed := 0
	err = b.SubscribeForHeartbeats(func(n node.Node) error {
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

func TestRabbitMQPublishAndSubsribeForJob(t *testing.T) {
	ctx := context.Background()
	b, err := mq.NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	processed := 0
	err = b.SubscribeForJobs(func(j *job.Job) error {
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
