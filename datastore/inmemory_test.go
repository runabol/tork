package datastore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/job"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/task"
	"github.com/runabol/tork/uuid"
	"github.com/stretchr/testify/assert"
)

func TestInMemoryCreateAndGetTask(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	t1 := task.Task{
		ID: uuid.NewUUID(),
	}
	err := ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)
	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, t1.ID, t2.ID)
}

func TestInMemoryGetActiveTasks(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	jid := uuid.NewUUID()

	tasks := []task.Task{{
		ID:    uuid.NewUUID(),
		State: task.Pending,
		JobID: jid,
	}, {
		ID:    uuid.NewUUID(),
		State: task.Scheduled,
		JobID: jid,
	}, {
		ID:    uuid.NewUUID(),
		State: task.Running,
		JobID: jid,
	}, {
		ID:    uuid.NewUUID(),
		State: task.Cancelled,
		JobID: jid,
	}, {
		ID:    uuid.NewUUID(),
		State: task.Completed,
		JobID: jid,
	}, {
		ID:    uuid.NewUUID(),
		State: task.Failed,
		JobID: jid,
	}}

	for _, ta := range tasks {
		err := ds.CreateTask(ctx, &ta)
		assert.NoError(t, err)
	}
	at, err := ds.GetActiveTasks(ctx, jid)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(at))
}

func TestInMemoryUpdateTask(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	t1 := task.Task{
		ID:    uuid.NewUUID(),
		State: task.Pending,
	}
	err := ds.CreateTask(ctx, &t1)
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
	err := ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
}

func TestInMemoryGetJobs(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	for i := 0; i < 101; i++ {
		j1 := job.Job{
			ID:   uuid.NewUUID(),
			Name: fmt.Sprintf("Job %d", (i + 1)),
		}
		err := ds.CreateJob(ctx, &j1)
		assert.NoError(t, err)
	}
	p1, err := ds.GetJobs(ctx, 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, "Job 101", p1.Items[0].Name)

	p2, err := ds.GetJobs(ctx, 2, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p2.Size)

	p10, err := ds.GetJobs(ctx, 10, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p10.Size)

	p11, err := ds.GetJobs(ctx, 11, 10)
	assert.NoError(t, err)
	assert.Equal(t, 1, p11.Size)

	assert.NotEqual(t, p2.Items[0].ID, p1.Items[9].ID)
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
	err := ds.CreateJob(ctx, &j1)
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
