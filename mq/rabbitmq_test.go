package mq_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/runabol/tork/job"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/task"
	"github.com/runabol/tork/uuid"
	"github.com/stretchr/testify/assert"
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

func TestRabbitMQPublisConcurrent(t *testing.T) {
	ctx := context.Background()
	b, err := mq.NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	testq := fmt.Sprintf("%s%s", mq.QUEUE_EXCLUSIVE_PREFIX, "test")
	processed := 0
	err = b.SubscribeForTasks(testq, func(t *task.Task) error {
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				err = b.PublishTask(ctx, testq, &task.Task{})
			}
		}()
	}
	wg.Wait()
	// wait for heartbeat to be processed
	time.Sleep(time.Millisecond * 100)
	assert.NoError(t, err)
	assert.Equal(t, 300, processed)
}

func TestRabbitMQShutdown(t *testing.T) {
	ctx := context.Background()
	b, err := mq.NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	mu := sync.Mutex{}
	processed := 0
	qname := fmt.Sprintf("%stest-%s", mq.QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	err = b.SubscribeForTasks(qname, func(j *task.Task) error {
		mu.Lock()
		defer mu.Unlock()
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		err = b.PublishTask(ctx, qname, &task.Task{})
		assert.NoError(t, err)
	}
	// cleanly shutdown
	err = b.Shutdown(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 10, processed)
	// there should be no more processing past the shutdown
	for i := 0; i < 10; i++ {
		err = b.PublishTask(ctx, qname, &task.Task{})
		assert.NoError(t, err)
	}
	assert.Equal(t, 10, processed)
}
