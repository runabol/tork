package handlers

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/middleware/task"
	"github.com/stretchr/testify/assert"
)

func Test_handleRedeliveredTask(t *testing.T) {
	ctx := context.Background()
	b := broker.NewInMemoryBroker()
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewRedeliveredHandler(ds, b)
	assert.NotNil(t, handler)
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateRunning,
	}
	for i := 0; i < 5; i++ {
		err = handler(ctx, task.Redelivered, t1)
		assert.NoError(t, err)
		assert.Equal(t, tork.TaskStateRunning, t1.State)
		assert.Equal(t, i+1, t1.Redelivered)
	}
	err = handler(ctx, task.Redelivered, t1)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateFailed, t1.State)
	assert.Equal(t, 5, t1.Redelivered)
	assert.NotNil(t, t1.FailedAt)
	assert.Equal(t, "task redelivered too many times", t1.Error)
	assert.NoError(t, ds.Close())
}
