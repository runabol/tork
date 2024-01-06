package mq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRabbitMQPublishAndSubsribeForTask(t *testing.T) {
	ctx := context.Background()
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	processed := make(chan any)
	qname := fmt.Sprintf("%stest-%s", QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	err = b.SubscribeForTasks(qname, func(t *tork.Task) error {
		processed <- 1
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishTask(ctx, qname, &tork.Task{})
	<-processed
	assert.NoError(t, err)
}

func TestRabbitMQGetQueues(t *testing.T) {
	ctx := context.Background()
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	qname := fmt.Sprintf("%stest-%s", QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	err = b.SubscribeForTasks(qname, func(t *tork.Task) error {
		return nil
	})
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

func TestRabbitMQGetQueuesMgmtURL(t *testing.T) {
	ctx := context.Background()
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/", WithManagementURL("http://localhost:15672"))
	assert.NoError(t, err)
	qname := fmt.Sprintf("%stest-%s", QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	err = b.SubscribeForTasks(qname, func(t *tork.Task) error {
		return nil
	})
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
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	processed := make(chan any)
	err = b.SubscribeForHeartbeats(func(n *tork.Node) error {
		processed <- 1
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishHeartbeat(ctx, &tork.Node{})
	assert.NoError(t, err)
	<-processed
	// second heartbeat should expire
	err = b.PublishHeartbeat(ctx, &tork.Node{})
	assert.NoError(t, err)
}

func TestRabbitMQPublishAndSubsribeForJob(t *testing.T) {
	ctx := context.Background()
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	processed := make(chan any)
	err = b.SubscribeForJobs(func(j *tork.Job) error {
		close(processed)
		return nil
	})
	assert.NoError(t, err)
	err = b.PublishJob(ctx, &tork.Job{})
	<-processed
	assert.NoError(t, err)
}

func TestRabbitMQPublishConcurrent(t *testing.T) {
	ctx := context.Background()
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	testq := fmt.Sprintf("%s%s", QUEUE_EXCLUSIVE_PREFIX, "test")
	processed := make(chan any, 300)
	err = b.SubscribeForTasks(testq, func(t *tork.Task) error {
		processed <- 1
		return nil
	})
	assert.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				err := b.PublishTask(ctx, testq, &tork.Task{})
				assert.NoError(t, err)
			}
		}()
	}
	wg.Wait()
	for i := 0; i < 300; i++ {
		<-processed
	}
	close(processed)
}

func TestRabbitMQShutdown(t *testing.T) {
	ctx := context.Background()
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	mu := sync.Mutex{}
	qname := fmt.Sprintf("%stest-%s", QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	err = b.SubscribeForTasks(qname, func(t1 *tork.Task) error {
		mu.Lock()
		defer mu.Unlock()
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
	// there should be no more processing past the shutdown
	err = b.PublishTask(ctx, qname, &tork.Task{})
	assert.Error(t, err)
}

func TestRabbitMQSubsribeForEvent(t *testing.T) {
	ctx := context.Background()
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer func() {
		// cleanly shutdown
		err = b.Shutdown(ctx)
		assert.NoError(t, err)
	}()
	j1 := &tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStateCompleted,
	}
	processed1 := make(chan any, 30)
	processed2 := make(chan any, 10)
	err = b.SubscribeForEvents(ctx, TOPIC_JOB, func(event any) {
		j2 := event.(*tork.Job)
		assert.Equal(t, j1.ID, j2.ID)
		processed1 <- 1
	})
	err = b.SubscribeForEvents(ctx, TOPIC_JOB_COMPLETED, func(event any) {
		j2 := event.(*tork.Job)
		assert.Equal(t, j1.ID, j2.ID)
		processed2 <- 1
	})
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = b.PublishEvent(ctx, TOPIC_JOB_COMPLETED, j1)
		err = b.PublishEvent(ctx, TOPIC_JOB_FAILED, j1)
		err = b.PublishEvent(ctx, "job.x.y.z", j1)
		assert.NoError(t, err)
	}

	for i := 0; i < 30; i++ {
		<-processed1
	}
	close(processed1)
	for i := 0; i < 10; i++ {
		<-processed2
	}
	close(processed2)
}

func TestRabbitMQPublishUnknownEvent(t *testing.T) {
	ctx := context.Background()
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	defer func() {
		// cleanly shutdown
		err = b.Shutdown(ctx)
		assert.NoError(t, err)
	}()
	err = b.SubscribeForEvents(ctx, TOPIC_JOB, func(event any) {})
	assert.NoError(t, err)

	err = b.PublishEvent(ctx, TOPIC_JOB_COMPLETED, "not a thing")
	assert.Error(t, err)
}

func TestRabbitMQHealthChech(t *testing.T) {
	ctx := context.Background()
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	assert.NoError(t, b.HealthCheck(ctx))
	assert.NoError(t, b.Shutdown(ctx))
	assert.Error(t, b.HealthCheck(ctx))
}

func TestRabbitMQPublishAndSubsribe(t *testing.T) {
	ctx := context.Background()
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/", WithConsumerTimeoutMS(time.Minute))
	assert.NoError(t, err)
	qname := fmt.Sprintf("%stest-%s", QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	err = b.SubscribeForTasks(qname, func(t *tork.Task) error {
		return nil
	})
	assert.NoError(t, err)
	qs, err := b.rabbitQueues(ctx)
	assert.NoError(t, err)
	found := false
	for _, q := range qs {
		if q.Name == qname {
			found = true
			timeout := q.Arguments["x-consumer-timeout"].(float64)
			assert.Equal(t, float64(60000), timeout)
			break
		}
	}
	assert.True(t, found)
}

func TestRabbitMQPublishAndSubsribeTaskLogPart(t *testing.T) {
	ctx := context.Background()
	b, err := NewRabbitMQBroker("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err)
	processed := make(chan any)
	err = b.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {
		processed <- 1
	})
	assert.NoError(t, err)
	err = b.PublishTaskLogPart(ctx, &tork.TaskLogPart{})
	assert.NoError(t, err)
	<-processed
}
