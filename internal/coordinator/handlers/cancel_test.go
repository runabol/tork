package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_cancelActiveTasks(t *testing.T) {
	ctx := context.Background()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	b := broker.NewInMemoryBroker()

	j1 := &tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStatePending,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
	}

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	err = ds.CreateTask(ctx, &tork.Task{
		ID:        uuid.NewUUID(),
		JobID:     j1.ID,
		State:     tork.TaskStateRunning,
		Position:  1,
		CreatedAt: &now,
	})
	assert.NoError(t, err)

	actives, err := ds.GetActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Len(t, actives, 1)

	err = cancelActiveTasks(ctx, ds, b, j1.ID)
	assert.NoError(t, err)

	actives, err = ds.GetActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Len(t, actives, 0)
	assert.NoError(t, ds.Close())
}
