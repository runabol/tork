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

func TestPostgresCreateAndGetTask(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	now := time.Now().UTC()
	j1 := job.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	t1 := task.Task{
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)
	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, t1.ID, t2.ID)
}

func TestPostgresGetActiveTasks(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	j1 := job.Job{
		ID:        uuid.NewUUID(),
		CreatedAt: time.Now().UTC(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tasks := []*task.Task{{
		ID:        uuid.NewUUID(),
		State:     task.Pending,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     task.Scheduled,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     task.Running,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     task.Cancelled,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     task.Completed,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     task.Failed,
		CreatedAt: &now,
		JobID:     j1.ID,
	}}

	for _, ta := range tasks {
		err := ds.CreateTask(ctx, ta)
		assert.NoError(t, err)
	}
	at, err := ds.GetActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(at))
}

func TestPostgresUpdateTask(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	now := time.Now().UTC()
	j1 := job.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	t1 := &task.Task{
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, t1)
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

func TestPostgresCreateAndGetNode(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	n1 := node.Node{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateNode(ctx, n1)
	assert.NoError(t, err)
	n2, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, n1.ID, n2.ID)
}

func TestPostgresUpdateNode(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	n1 := node.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute),
	}
	err = ds.CreateNode(ctx, n1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	err = ds.UpdateNode(ctx, n1.ID, func(u *node.Node) error {
		u.LastHeartbeatAt = now
		return nil
	})
	assert.NoError(t, err)

	n2, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, now.Hour(), n2.LastHeartbeatAt.Hour())
	assert.Equal(t, now.Minute(), n2.LastHeartbeatAt.Minute())
	assert.Equal(t, now.Second(), n2.LastHeartbeatAt.Second())
}

func TestPostgresGetActiveNodes(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)
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
	err = ds.CreateNode(ctx, n1)
	assert.NoError(t, err)

	err = ds.CreateNode(ctx, n2)
	assert.NoError(t, err)

	err = ds.CreateNode(ctx, n3)
	assert.NoError(t, err)

	ns, err := ds.GetActiveNodes(ctx, time.Now().UTC().Add(-time.Minute*5))
	for _, n := range ns {
		assert.True(t, n.LastHeartbeatAt.After(time.Now().UTC().Add(-time.Minute*5)))
	}
	assert.NoError(t, err)
}

func TestPostgresCreateAndGetJob(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	j1 := job.Job{
		ID: uuid.NewUUID(),
		Inputs: map[string]string{
			"var1": "val1",
		},
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, "val1", j2.Inputs["var1"])
}

func TestPostgresUpdateJob(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	j1 := job.Job{
		ID:    uuid.NewUUID(),
		State: job.Pending,
		Context: job.Context{
			Inputs: map[string]string{
				"var1": "val1",
			},
		},
	}
	err = ds.CreateJob(ctx, &j1)
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
