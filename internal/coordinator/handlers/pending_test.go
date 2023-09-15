package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/mq"
	"github.com/stretchr/testify/assert"
)

func Test_handlePendingTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := 0
	err := b.SubscribeForTasks("test-queue", func(t *tork.Task) error {
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)

	ds := datastore.NewInMemoryDatastore()
	handler := NewPendingHandler(ds, b)
	assert.NotNil(t, handler)

	j1 := &tork.Job{
		ID:   uuid.NewUUID(),
		Name: "test job",
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		Queue: "test-queue",
		JobID: j1.ID,
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = handler(ctx, tk)
	assert.NoError(t, err)

	// wait for the task to get processed
	time.Sleep(time.Millisecond * 100)

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, tk.State)
	// task should only be processed once
	assert.Equal(t, 1, processed)
}

func Test_handleConditionalTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	completed := 0
	err := b.SubscribeForTasks(mq.QUEUE_COMPLETED, func(t *tork.Task) error {
		completed = completed + 1
		return nil
	})
	assert.NoError(t, err)

	ds := datastore.NewInMemoryDatastore()
	handler := NewPendingHandler(ds, b)
	assert.NotNil(t, handler)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		Queue: "test-queue",
		If:    "false",
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = handler(ctx, tk)
	assert.NoError(t, err)

	// wait for the task to get processed
	time.Sleep(time.Millisecond * 100)

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, tk.State)
	// task should only be processed once
	assert.Equal(t, 1, completed)
}
