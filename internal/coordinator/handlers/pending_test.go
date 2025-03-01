package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/middleware/task"
	"github.com/stretchr/testify/assert"
)

func Test_handlePendingTask(t *testing.T) {
	ctx := context.Background()
	b := broker.NewInMemoryBroker()

	processed := make(chan any)
	err := b.SubscribeForTasks("test-queue", func(t *tork.Task) error {
		close(processed)
		return nil
	})
	assert.NoError(t, err)

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewPendingHandler(ds, b)
	assert.NotNil(t, handler)

	j1 := &tork.Job{
		ID:   uuid.NewUUID(),
		Name: "test job",
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tk := &tork.Task{
		ID:        uuid.NewUUID(),
		Queue:     "test-queue",
		JobID:     j1.ID,
		CreatedAt: &now,
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = handler(ctx, task.StateChange, tk)
	assert.NoError(t, err)

	// wait for the task to get processed
	<-processed

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, tk.State)
	assert.NoError(t, ds.Close())
}

func Test_handleConditionalTask(t *testing.T) {
	ctx := context.Background()
	b := broker.NewInMemoryBroker()

	completed := make(chan any)
	err := b.SubscribeForTasks(broker.QUEUE_COMPLETED, func(t *tork.Task) error {
		close(completed)
		return nil
	})
	assert.NoError(t, err)

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewPendingHandler(ds, b)
	assert.NotNil(t, handler)

	j1 := &tork.Job{
		ID:   uuid.NewUUID(),
		Name: "test job",
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tk := &tork.Task{
		ID:        uuid.NewUUID(),
		Queue:     "test-queue",
		If:        "false",
		CreatedAt: &now,
		JobID:     j1.ID,
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = handler(ctx, task.StateChange, tk)
	assert.NoError(t, err)

	// wait for the task to get processed
	<-completed

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateSkipped, tk.State)
	assert.NoError(t, ds.Close())
}
