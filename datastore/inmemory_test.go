package datastore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tork/datastore"
	"github.com/tork/job"
	"github.com/tork/node"
	"github.com/tork/task"
	"github.com/tork/uuid"
)

func TestInMemoryCreateAndGetTask(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	t1 := task.Task{
		ID: uuid.NewUUID(),
	}
	err := ds.CreateTask(ctx, t1)
	assert.NoError(t, err)
	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, t1.ID, t2.ID)
}

func TestInMemoryUpdateTask(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	t1 := task.Task{
		ID:    uuid.NewUUID(),
		State: task.Pending,
	}
	err := ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = ds.UpdateTask(ctx, t1.ID, func(u *task.Task) error {
		u.State = task.Scheduled
		return nil
	})
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.Scheduled, t2.State)
}

func TestInMemoryCreateAndGetNode(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	n1 := node.Node{
		ID: uuid.NewUUID(),
	}
	err := ds.CreateNode(ctx, n1)
	assert.NoError(t, err)
	n2, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, n1.ID, n2.ID)
}

func TestInMemoryUpdateNode(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	n1 := node.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute),
	}
	err := ds.CreateNode(ctx, n1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	err = ds.UpdateNode(ctx, n1.ID, func(u *node.Node) error {
		u.LastHeartbeatAt = now
		return nil
	})
	assert.NoError(t, err)

	n2, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, now, n2.LastHeartbeatAt)
}

func TestInMemoryGetActiveNodes(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	n1 := node.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute),
	}
	n2 := node.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 4),
	}
	n3 := node.Node{ // inactive
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 10),
	}
	err := ds.CreateNode(ctx, n1)
	assert.NoError(t, err)

	err = ds.CreateNode(ctx, n2)
	assert.NoError(t, err)

	err = ds.CreateNode(ctx, n3)
	assert.NoError(t, err)

	ns, err := ds.GetActiveNodes(ctx, time.Now().UTC().Add(-time.Minute*5))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ns))
}

func TestInMemoryCreateAndGetJob(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	j1 := job.Job{
		ID: uuid.NewUUID(),
	}
	err := ds.CreateJob(ctx, j1)
	assert.NoError(t, err)
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
}

func TestInMemoryUpdateJob(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	j1 := job.Job{
		ID:    uuid.NewUUID(),
		State: job.Pending,
		Context: job.Context{
			Inputs: map[string]string{
				"var1": "val1",
			},
		},
	}
	err := ds.CreateJob(ctx, j1)
	assert.NoError(t, err)
	err = ds.UpdateJob(ctx, j1.ID, func(u *job.Job) error {
		u.State = job.Completed
		u.Context.Inputs["var2"] = "val2"
		return nil
	})
	assert.NoError(t, err)
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, job.Completed, j2.State)
	assert.Equal(t, "val1", j2.Context.Inputs["var1"])
	assert.Equal(t, "val2", j2.Context.Inputs["var2"])
}
