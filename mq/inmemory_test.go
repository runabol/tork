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

func TestInMemoryPublishAndSubsribeForTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	processed := make(chan any)
	err := b.SubscribeForTasks("test-queue", func(t *tork.Task) error {
		close(processed)
		return nil
	})
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID: uuid.NewUUID(),
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/somevolume",
			},
		},
	}
	err = b.PublishTask(ctx, "test-queue", t1)
	<-processed
	assert.NoError(t, err)
	assert.Equal(t, "/somevolume", t1.Mounts[0].Target)
}

func TestInMemoryGetQueues(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	qname := fmt.Sprintf("test-queue-%s", uuid.NewUUID())
	err := b.PublishTask(ctx, qname, &tork.Task{})
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
			err := b.SubscribeForTasks(qname, func(t *tork.Task) error {
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
	err := b.PublishTask(ctx, qname, &tork.Task{})
	assert.NoError(t, err)
	qis, err := b.Queues(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(qis))
	assert.Equal(t, 0, qis[0].Subscribers)
	wg1 := sync.WaitGroup{}
	wg1.Add(1)
	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	err = b.SubscribeForTasks(qname, func(t *tork.Task) error {
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
	processed := make(chan any)
	err := b.SubscribeForHeartbeats(func(n *tork.Node) error {
		close(processed)
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishHeartbeat(ctx, &tork.Node{})
	<-processed
	assert.NoError(t, err)
}

func TestInMemoryPublishAndSubsribeForJob(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	processed := make(chan any)
	err := b.SubscribeForJobs(func(j *tork.Job) error {
		close(processed)
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishJob(ctx, &tork.Job{})
	<-processed
	assert.NoError(t, err)
}

func TestMultipleSubsSubsribeForJob(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	processed := make(chan any, 10)
	mu := sync.Mutex{}
	err := b.SubscribeForJobs(func(j *tork.Job) error {
		mu.Lock()
		defer mu.Unlock()
		processed <- 1
		return nil
	})
	assert.NoError(t, err)
	err = b.SubscribeForJobs(func(j *tork.Job) error {
		mu.Lock()
		defer mu.Unlock()
		processed <- 1
		return nil
	})
	assert.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			wg.Done()
			err := b.PublishJob(ctx, &tork.Job{})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		<-processed
	}
}

func TestInMemoryShutdown(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	mu := sync.Mutex{}
	processed := make(chan any)
	qname1 := fmt.Sprintf("%stest-%s", mq.QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	qname2 := fmt.Sprintf("%stest-%s", mq.QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	err := b.SubscribeForTasks(qname1, func(j *tork.Task) error {
		mu.Lock()
		defer mu.Unlock()
		close(processed)
		// should not be able to block
		// the termination process
		time.Sleep(time.Hour)
		return nil
	})
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		err = b.PublishTask(ctx, qname1, &tork.Task{})
		assert.NoError(t, err)
		err = b.PublishTask(ctx, qname2, &tork.Task{})
		assert.NoError(t, err)
	}
	<-processed
	// cleanly shutdown
	err = b.Shutdown(ctx)
	assert.NoError(t, err)

	// there should be no more processing past the shutdown
	err = b.PublishTask(ctx, qname1, &tork.Task{})
	assert.NoError(t, err)
}

func TestInMemorSubsribeForEvent(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	processed1 := make(chan any, 20)
	processed2 := make(chan any, 10)
	j1 := &tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStateCompleted,
	}
	err := b.SubscribeForEvents(ctx, mq.TOPIC_JOB, func(event any) {
		j2 := event.(*tork.Job)
		assert.Equal(t, j1.ID, j2.ID)
		processed1 <- 1
	})
	assert.NoError(t, err)
	err = b.SubscribeForEvents(ctx, mq.TOPIC_JOB_COMPLETED, func(event any) {
		j2 := event.(*tork.Job)
		assert.Equal(t, j1.ID, j2.ID)
		processed2 <- 1
	})
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = b.PublishEvent(ctx, mq.TOPIC_JOB_COMPLETED, j1)
		assert.NoError(t, err)
		err = b.PublishEvent(ctx, mq.TOPIC_JOB_FAILED, j1)
		assert.NoError(t, err)
	}

	for i := 0; i < 20; i++ {
		<-processed1
	}
	close(processed1)
	for i := 0; i < 10; i++ {
		<-processed2
	}
	close(processed2)
}

func TestInMemoryHealthChech(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	assert.NoError(t, b.HealthCheck(ctx))
	assert.NoError(t, b.Shutdown(ctx))
	assert.Error(t, b.HealthCheck(ctx))
}

func TestInMemoryPublishAndSubsribeTaskLogPart(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()
	processed := make(chan any)
	err := b.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {
		close(processed)
	})
	assert.NoError(t, err)
	err = b.PublishTaskLogPart(ctx, &tork.TaskLogPart{})
	<-processed
	assert.NoError(t, err)
}
