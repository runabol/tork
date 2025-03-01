package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_handleLog(t *testing.T) {
	ctx := context.Background()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewLogHandler(ds)
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

	p1 := tork.TaskLogPart{
		TaskID:   tk.ID,
		Number:   1,
		Contents: "line 1",
	}

	handler(&p1)

	n11, err := ds.GetTaskLogParts(ctx, p1.TaskID, "", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 1, n11.TotalItems)
	assert.Equal(t, "line 1", n11.Items[0].Contents)
	assert.NoError(t, ds.Close())
}
