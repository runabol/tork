package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/mq"
	"github.com/stretchr/testify/assert"
)

func Test_handleFailedTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	events := make(chan any)
	err := b.SubscribeForEvents(ctx, mq.TOPIC_JOB_FAILED, func(event any) {
		j, ok := event.(*tork.Job)
		assert.True(t, ok)
		assert.Equal(t, tork.JobStateFailed, j.State)
		close(events)
	})
	assert.NoError(t, err)

	ds := datastore.NewInMemoryDatastore()
	handler := NewErrorHandler(ds, b)
	assert.NotNil(t, handler)

	now := time.Now().UTC()

	node := &tork.Node{
		ID:    uuid.NewUUID(),
		Queue: uuid.NewUUID(),
	}
	err = ds.CreateNode(ctx, node)
	assert.NoError(t, err)

	j1 := &tork.Job{
		ID:       uuid.NewUUID(),
		State:    tork.JobStateRunning,
		Position: 1,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateRunning,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      node.ID,
		JobID:       j1.ID,
		Position:    1,
	}

	t2 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateRunning,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      node.ID,
		JobID:       j1.ID,
		Position:    1,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = ds.CreateTask(ctx, t2)
	assert.NoError(t, err)

	actives, err := ds.GetActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Len(t, actives, 2)

	err = handler(ctx, task.StateChange, t1)
	assert.NoError(t, err)

	<-events

	t11, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateFailed, t11.State)
	assert.Equal(t, t1.CompletedAt, t11.CompletedAt)

	// verify that the job was
	// marked as FAILED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateFailed, j2.State)

	actives, err = ds.GetActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Len(t, actives, 0)
}

func Test_handleFailedTaskRetry(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := make(chan any)
	err := b.SubscribeForTasks(mq.QUEUE_PENDING, func(t *tork.Task) error {
		close(processed)
		return nil
	})
	assert.NoError(t, err)

	ds := datastore.NewInMemoryDatastore()
	handler := NewErrorHandler(ds, b)
	assert.NotNil(t, handler)

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:       uuid.NewUUID(),
		State:    tork.JobStateRunning,
		Position: 1,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateRunning,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		Retry: &tork.TaskRetry{
			Limit: 1,
		},
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = handler(ctx, task.StateChange, t1)
	assert.NoError(t, err)

	<-processed

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateFailed, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	// verify that the job was
	// NOT marked as FAILED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateRunning, j2.State)
}
