package mq_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/types/job"
	"github.com/runabol/tork/types/node"
	"github.com/runabol/tork/types/task"
	"github.com/runabol/tork/uuid"
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

	t1 := &task.Task{
		ID:      uuid.NewUUID(),
		Volumes: []string{"/somevolume"},
	}
	err = b.PublishTask(ctx, "test-queue", t1)
	// wait for task to be processed
	time.Sleep(time.Millisecond * 100)
	assert.NoError(t, err)
	assert.Equal(t, 1, processed)
	assert.Equal(t, []string{"/somevolume"}, t1.Volumes)
}

func TestInMemoryGetQueues(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	qname := fmt.Sprintf("test-queue-%s", uuid.NewUUID())
	err := b.PublishTask(ctx, qname, &task.Task{})
	assert.NoError(t, err)
	qis, err := b.Queues(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(qis))
	assert.Equal(t, 0, qis[0].Subscribers)
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			err := b.SubscribeForTasks(qname, func(t *task.Task) error {
				return nil
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	qis, err = b.Queues(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(qis))
	assert.Equal(t, 10, qis[0].Subscribers)
}

func TestInMemoryGetQueuesUnacked(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	qname := fmt.Sprintf("test-queue-%s", uuid.NewUUID())
	err := b.PublishTask(ctx, qname, &task.Task{})
	assert.NoError(t, err)
	qis, err := b.Queues(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(qis))
	assert.Equal(t, 0, qis[0].Subscribers)
	wg1 := sync.WaitGroup{}
	wg1.Add(1)
	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	err = b.SubscribeForTasks(qname, func(t *task.Task) error {
		wg1.Done()
		wg2.Wait()
		return nil
	})
	assert.NoError(t, err)
	wg1.Wait()
	qis, err = b.Queues(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, qis[0].Unacked)
	wg2.Done()
	time.Sleep(time.Millisecond * 100)
	qis, err = b.Queues(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, qis[0].Unacked)
	assert.Equal(t, 1, len(qis))
	assert.Equal(t, 1, qis[0].Subscribers)
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

func TestMultipleSubsSubsribeForJob(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	processed := 0
	mu := sync.Mutex{}
	err := b.SubscribeForJobs(func(j *job.Job) error {
		mu.Lock()
		defer mu.Unlock()
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)
	err = b.SubscribeForJobs(func(j *job.Job) error {
		mu.Lock()
		defer mu.Unlock()
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			wg.Done()
			err = b.PublishJob(ctx, &job.Job{})
		}()
	}
	wg.Wait()
	// wait for heartbeat to be processed
	time.Sleep(time.Millisecond * 100)
	assert.NoError(t, err)
	assert.Equal(t, 10, processed)
}

func TestInMemoryShutdown(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	mu := sync.Mutex{}
	processed := 0
	qname1 := fmt.Sprintf("%stest-%s", mq.QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	qname2 := fmt.Sprintf("%stest-%s", mq.QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	err := b.SubscribeForTasks(qname1, func(j *task.Task) error {
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
		err = b.PublishTask(ctx, qname1, &task.Task{})
		assert.NoError(t, err)
		err = b.PublishTask(ctx, qname2, &task.Task{})
		assert.NoError(t, err)
	}
	time.Sleep(time.Millisecond * 100)
	// cleanly shutdown
	err = b.Shutdown(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, processed)
	// there should be no more processing past the shutdown
	err = b.PublishTask(ctx, qname1, &task.Task{})
	assert.NoError(t, err)
	assert.Equal(t, 1, processed)
}
