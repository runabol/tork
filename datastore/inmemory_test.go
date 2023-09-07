package datastore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"

	"github.com/runabol/tork/uuid"
	"github.com/stretchr/testify/assert"
)

func TestInMemoryCreateAndGetTask(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	t1 := tork.Task{
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

	tasks := []tork.Task{{
		ID:    uuid.NewUUID(),
		State: tork.TaskStatePending,
		JobID: jid,
	}, {
		ID:    uuid.NewUUID(),
		State: tork.TaskStateScheduled,
		JobID: jid,
	}, {
		ID:    uuid.NewUUID(),
		State: tork.TaskStateRunning,
		JobID: jid,
	}, {
		ID:    uuid.NewUUID(),
		State: tork.TaskStateCancelled,
		JobID: jid,
	}, {
		ID:    uuid.NewUUID(),
		State: tork.TaskStateCompleted,
		JobID: jid,
	}, {
		ID:    uuid.NewUUID(),
		State: tork.TaskStateFailed,
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
	t1 := tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStatePending,
	}
	err := ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)

	err = ds.UpdateTask(ctx, t1.ID, func(u *tork.Task) error {
		u.State = tork.TaskStateScheduled
		return nil
	})
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, t2.State)
}

func TestInMemoryCreateAndGetNode(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	n1 := tork.Node{
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
	n1 := tork.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute),
	}
	err := ds.CreateNode(ctx, n1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	err = ds.UpdateNode(ctx, n1.ID, func(u *tork.Node) error {
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
	n1 := tork.Node{
		ID:              uuid.NewUUID(),
		Status:          tork.NodeStatusUP,
		LastHeartbeatAt: time.Now().UTC().Add(-time.Second * 20),
	}
	n2 := tork.Node{
		ID:              uuid.NewUUID(),
		Status:          tork.NodeStatusUP,
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 4),
	}
	n3 := tork.Node{ // inactive
		ID:              uuid.NewUUID(),
		Status:          tork.NodeStatusUP,
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 10),
	}
	err := ds.CreateNode(ctx, n1)
	assert.NoError(t, err)

	err = ds.CreateNode(ctx, n2)
	assert.NoError(t, err)

	err = ds.CreateNode(ctx, n3)
	assert.NoError(t, err)

	ns, err := ds.GetActiveNodes(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ns))
	assert.Equal(t, tork.NodeStatusUP, ns[0].Status)
	assert.Equal(t, tork.NodeStatusOffline, ns[1].Status)
}

func TestInMemoryCreateAndGetJob(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	j1 := tork.Job{
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
		now := time.Now().UTC()
		j1 := tork.Job{
			ID:        uuid.NewUUID(),
			Name:      fmt.Sprintf("Job %d", (i + 1)),
			CreatedAt: now,
			State:     tork.JobStateRunning,
			Tasks: []*tork.Task{
				{
					Name: "some task",
				},
			},
		}
		err := ds.CreateJob(ctx, &j1)
		assert.NoError(t, err)

		err = ds.CreateTask(ctx, &tork.Task{
			ID:    uuid.NewUUID(),
			JobID: j1.ID,
			State: tork.TaskStateRunning,
		})
		assert.NoError(t, err)
		time.Sleep(time.Millisecond)
	}
	p1, err := ds.GetJobs(ctx, "", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, "Job 101", p1.Items[0].Name)
	assert.Empty(t, p1.Items[0].Tasks)
	assert.Empty(t, p1.Items[0].Execution)

	p2, err := ds.GetJobs(ctx, "", 2, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p2.Size)

	p10, err := ds.GetJobs(ctx, "", 10, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p10.Size)

	p11, err := ds.GetJobs(ctx, "", 11, 10)
	assert.NoError(t, err)
	assert.Equal(t, 1, p11.Size)
	assert.NotEqual(t, p2.Items[0].ID, p1.Items[9].ID)
	assert.Equal(t, 101, p1.TotalItems)

	ps1, err := ds.GetJobs(ctx, "Job", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, ps1.Size)
	assert.Equal(t, 101, ps1.TotalItems)

	ps1, err = ds.GetJobs(ctx, "101", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 1, ps1.Size)
	assert.Equal(t, 1, ps1.TotalItems)

	ps1, err = ds.GetJobs(ctx, "running", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, ps1.Size)
	assert.Equal(t, 101, ps1.TotalItems)
}

func TestInMemoryUpdateJob(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	j1 := tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStatePending,
		Context: tork.JobContext{
			Inputs: map[string]string{
				"var1": "val1",
			},
		},
	}
	err := ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	err = ds.UpdateJob(ctx, j1.ID, func(u *tork.Job) error {
		u.State = tork.JobStateCompleted
		u.Context.Inputs["var2"] = "val2"
		return nil
	})
	assert.NoError(t, err)
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateCompleted, j2.State)
	assert.Equal(t, "val1", j2.Context.Inputs["var1"])
	assert.Equal(t, "val2", j2.Context.Inputs["var2"])
}

func TestInMemoryGetStats(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()

	s, err := ds.GetStats(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, s.Jobs.Running)
	assert.Equal(t, 0, s.Tasks.Running)
	assert.Equal(t, float64(0), s.Nodes.CPUPercent)
	assert.Equal(t, 0, s.Nodes.Running)

	for i := 0; i < 100; i++ {
		var state tork.JobState
		if i%2 == 0 {
			state = tork.JobStateRunning
		} else {
			state = tork.JobStatePending
		}
		err := ds.CreateJob(ctx, &tork.Job{
			ID:    uuid.NewUUID(),
			State: state,
		})
		assert.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		var state tork.TaskState
		if i%2 == 0 {
			state = tork.TaskStateRunning
		} else {
			state = tork.TaskStatePending
		}
		err := ds.CreateTask(ctx, &tork.Task{
			ID:    uuid.NewUUID(),
			State: state,
		})
		assert.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		err := ds.CreateNode(ctx, tork.Node{
			ID:              uuid.NewUUID(),
			LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * time.Duration(i)),
			CPUPercent:      float64(i * 10),
		})
		assert.NoError(t, err)
	}

	s, err = ds.GetStats(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 50, s.Jobs.Running)
	assert.Equal(t, 50, s.Tasks.Running)
	assert.Equal(t, float64(20), s.Nodes.CPUPercent)
	assert.Equal(t, 5, s.Nodes.Running)
}
