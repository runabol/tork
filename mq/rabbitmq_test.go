package mq_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/mq"
	"github.com/stretchr/testify/assert"
)

func TestRabbitMQPublishAndSubsribeForTask(t *testing.T) {
	ctx := context.Background()
	b, err := mq.NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	processed := 0
	qname := fmt.Sprintf("%stest-%s", mq.QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	err = b.SubscribeForTasks(qname, func(t *tork.Task) error {
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishTask(ctx, qname, &tork.Task{})
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
	err = b.PublishTask(ctx, qname, &tork.Task{})
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
	err = b.SubscribeForHeartbeats(func(n tork.Node) error {
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishHeartbeat(ctx, tork.Node{})
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
	err = b.SubscribeForJobs(func(j *tork.Job) error {
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishJob(ctx, &tork.Job{})
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
	err = b.SubscribeForTasks(testq, func(t *tork.Task) error {
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
				err = b.PublishTask(ctx, testq, &tork.Task{})
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
	err = b.SubscribeForTasks(qname, func(t1 *tork.Task) error {
		mu.Lock()
		defer mu.Unlock()
		processed = processed + 1
		// should not be able to block
		// the termination process
		time.Sleep(time.Hour)
		return nil
	})
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		err = b.PublishTask(ctx, qname, &tork.Task{})
		assert.NoError(t, err)
	}
	// cleanly shutdown
	err = b.Shutdown(ctx)
	assert.NoError(t, err)
	assert.Greater(t, processed, 0)
	// there should be no more processing past the shutdown
	err = b.PublishTask(ctx, qname, &tork.Task{})
	assert.Error(t, err)
}

func TestRabbitMQSubsribeForEvent(t *testing.T) {
	ctx := context.Background()
	b, err := mq.NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer func() {
		// cleanly shutdown
		err = b.Shutdown(ctx)
		assert.NoError(t, err)
	}()
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateCancelled,
	}
	processed1 := 0
	processed2 := 0
	testq := fmt.Sprintf("%s%s", mq.QUEUE_EXCLUSIVE_PREFIX, "test")
	err = b.SubscribeForEvents(ctx, testq, func(event any) {
		t2 := event.(*tork.Task)
		assert.Equal(t, t1.ID, t2.ID)
		processed1 = processed1 + 1
	})
	err = b.SubscribeForEvents(ctx, testq, func(event any) {
		t2 := event.(*tork.Task)
		assert.Equal(t, t1.ID, t2.ID)
		processed2 = processed2 + 1
	})
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = b.PublishTask(ctx, testq, t1)
		assert.NoError(t, err)
	}
	// wait for task to be processed
	time.Sleep(time.Millisecond * 500)
	assert.Equal(t, 10, processed1)
	assert.Equal(t, 10, processed2)
}
