package datastore

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/db/postgres"

	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func TestPostgresCreateAndGetTask(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	now := time.Now().UTC()
	j1 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	t1 := tork.Task{
		ID:          uuid.NewUUID(),
		CreatedAt:   &now,
		JobID:       j1.ID,
		Description: "some description",
		Networks:    []string{"some-network"},
		Files:       map[string]string{"myfile": "hello world"},
		Registry:    &tork.Registry{Username: "me", Password: "secret"},
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)
	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, t1.ID, t2.ID)
	assert.Equal(t, t1.Description, t2.Description)
	assert.Equal(t, []string([]string{"some-network"}), t2.Networks)
	assert.Equal(t, map[string]string{"myfile": "hello world"}, t2.Files)
	assert.Equal(t, "me", t2.Registry.Username)
	assert.Equal(t, "secret", t2.Registry.Password)
}

func TestPostgresCreateTaskBadOutput(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	now := time.Now().UTC()
	j1 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	t1 := tork.Task{
		ID:          uuid.NewUUID(),
		CreatedAt:   &now,
		JobID:       j1.ID,
		Description: "some description",
		Result:      string([]byte{0}),
		Error:       string([]byte{0}),
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)
	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, t1.ID, t2.ID)
	assert.Equal(t, t1.Description, t2.Description)
}

func TestPostgresGetActiveTasks(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	j1 := tork.Job{
		ID:        uuid.NewUUID(),
		CreatedAt: time.Now().UTC(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tasks := []*tork.Task{{
		ID:        uuid.NewUUID(),
		State:     tork.TaskStatePending,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateScheduled,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateRunning,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateCancelled,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateCompleted,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateFailed,
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
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	now := time.Now().UTC()
	j1 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	t1 := &tork.Task{
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = ds.UpdateTask(ctx, t1.ID, func(u *tork.Task) error {
		u.State = tork.TaskStateScheduled
		u.Result = "my result"
		return nil
	})
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, t2.State)
	assert.Equal(t, "my result", t2.Result)
}

func TestPostgresUpdateTaskConcurrently(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	now := time.Now().UTC()
	j1 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	t1 := &tork.Task{
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
		Parallel:  &tork.ParallelTask{},
	}
	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			err := ds.UpdateTask(ctx, t1.ID, func(u *tork.Task) error {
				u.State = tork.TaskStateScheduled
				u.Result = "my result"
				u.Parallel.Completions = u.Parallel.Completions + 1
				return nil
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, t2.State)
	assert.Equal(t, "my result", t2.Result)
	assert.Equal(t, 5, t2.Parallel.Completions)
}

func TestPostgresUpdateTaskBadStrings(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	now := time.Now().UTC()
	j1 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	t1 := &tork.Task{
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = ds.UpdateTask(ctx, t1.ID, func(u *tork.Task) error {
		u.State = tork.TaskStateScheduled
		u.Result = string([]byte{0})
		u.Error = string([]byte{0})
		return nil
	})
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, t2.State)
}

func TestPostgresCreateAndGetNode(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	n1 := &tork.Node{
		ID:       uuid.NewUUID(),
		Hostname: "some-name",
		Version:  "1.0.0",
	}
	err = ds.CreateNode(ctx, n1)
	assert.NoError(t, err)
	n2, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, n1.ID, n2.ID)
	assert.Equal(t, "some-name", n2.Hostname)
	assert.Equal(t, "1.0.0", n2.Version)
}

func TestPostgresUpdateNode(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	n1 := &tork.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute),
	}
	err = ds.CreateNode(ctx, n1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	err = ds.UpdateNode(ctx, n1.ID, func(u *tork.Node) error {
		u.LastHeartbeatAt = now
		u.TaskCount = 2
		return nil
	})
	assert.NoError(t, err)

	n2, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, now.Hour(), n2.LastHeartbeatAt.Hour())
	assert.Equal(t, now.Minute(), n2.LastHeartbeatAt.Minute())
	assert.Equal(t, now.Second(), n2.LastHeartbeatAt.Second())
	assert.Equal(t, 2, n2.TaskCount)
}

func TestPostgresUpdateNodeConcurrently(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	n1 := &tork.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute),
	}
	err = ds.CreateNode(ctx, n1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	wg := sync.WaitGroup{}
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			err := ds.UpdateNode(ctx, n1.ID, func(u *tork.Node) error {
				u.LastHeartbeatAt = now
				u.CPUPercent = u.CPUPercent + 1
				return nil
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	n2, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, now.Hour(), n2.LastHeartbeatAt.Hour())
	assert.Equal(t, now.Minute(), n2.LastHeartbeatAt.Minute())
	assert.Equal(t, now.Second(), n2.LastHeartbeatAt.Second())
	assert.Equal(t, float64(5), n2.CPUPercent)
}

func TestPostgresGetActiveNodes(t *testing.T) {
	ctx := context.Background()
	schemaName := fmt.Sprintf("tork%d", rand.Int())
	dsn := `host=localhost user=tork password=tork dbname=tork search_path=%s sslmode=disable`
	ds, err := NewPostgresDataStore(fmt.Sprintf(dsn, schemaName))
	assert.NoError(t, err)
	_, err = ds.db.Exec(fmt.Sprintf("create schema %s", schemaName))
	assert.NoError(t, err)
	defer func() {
		_, err = ds.db.Exec(fmt.Sprintf("drop schema %s cascade", schemaName))
		assert.NoError(t, err)
	}()
	err = ds.ExecScript(postgres.SCHEMA)
	assert.NoError(t, err)
	n1 := &tork.Node{
		ID:              uuid.NewUUID(),
		Status:          tork.NodeStatusUP,
		LastHeartbeatAt: time.Now().UTC().Add(-time.Second * 20),
	}
	n2 := &tork.Node{
		ID:              uuid.NewUUID(),
		Status:          tork.NodeStatusUP,
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 4),
	}
	n3 := &tork.Node{ // inactive
		ID:              uuid.NewUUID(),
		Status:          tork.NodeStatusUP,
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 10),
	}
	err = ds.CreateNode(ctx, n1)
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

func TestPostgresCreateAndGetJob(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	j1 := tork.Job{
		ID: uuid.NewUUID(),
		Inputs: map[string]string{
			"var1": "val1",
		},
		Defaults: &tork.JobDefaults{
			Timeout: "5s",
			Retry: &tork.TaskRetry{
				Limit: 2,
			},
			Limits: &tork.TaskLimits{
				CPUs:   ".5",
				Memory: "10MB",
			},
		},
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, "val1", j2.Inputs["var1"])
	assert.Equal(t, "5s", j2.Defaults.Timeout)
	assert.Equal(t, 2, j2.Defaults.Retry.Limit)
	assert.Equal(t, ".5", j2.Defaults.Limits.CPUs)
	assert.Equal(t, "10MB", j2.Defaults.Limits.Memory)
}

func TestPostgresUpdateJob(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	j1 := tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStatePending,
		Context: tork.JobContext{
			Inputs: map[string]string{
				"var1": "val1",
			},
		},
	}
	err = ds.CreateJob(ctx, &j1)
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

func TestPostgresUpdateJobConcurrently(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	j1 := tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStatePending,
		Context: tork.JobContext{
			Inputs: map[string]string{
				"var1": "val1",
			},
		},
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			err := ds.UpdateJob(ctx, j1.ID, func(u *tork.Job) error {
				u.State = tork.JobStateCompleted
				u.Context.Inputs["var2"] = "val2"
				u.Position = u.Position + 1
				return nil
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	assert.NoError(t, err)
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateCompleted, j2.State)
	assert.Equal(t, "val1", j2.Context.Inputs["var1"])
	assert.Equal(t, "val2", j2.Context.Inputs["var2"])
	assert.Equal(t, 5, j2.Position)
}

func TestPostgresGetJobs(t *testing.T) {
	ctx := context.Background()
	schemaName := fmt.Sprintf("tork%d", rand.Int())
	dsn := `host=localhost user=tork password=tork dbname=tork search_path=%s sslmode=disable`
	ds, err := NewPostgresDataStore(fmt.Sprintf(dsn, schemaName))
	assert.NoError(t, err)
	_, err = ds.db.Exec(fmt.Sprintf("create schema %s", schemaName))
	assert.NoError(t, err)
	defer func() {
		_, err = ds.db.Exec(fmt.Sprintf("drop schema %s cascade", schemaName))
		assert.NoError(t, err)
	}()
	err = ds.ExecScript(postgres.SCHEMA)
	assert.NoError(t, err)
	for i := 0; i < 101; i++ {
		j1 := tork.Job{
			ID:   uuid.NewUUID(),
			Name: fmt.Sprintf("Job %d", (i + 1)),
			Tasks: []*tork.Task{
				{
					Name: "some task",
				},
			},
		}
		err := ds.CreateJob(ctx, &j1)
		assert.NoError(t, err)

		now := time.Now().UTC()
		err = ds.CreateTask(ctx, &tork.Task{
			ID:        uuid.NewUUID(),
			JobID:     j1.ID,
			State:     tork.TaskStateRunning,
			CreatedAt: &now,
		})
		assert.NoError(t, err)
	}
	p1, err := ds.GetJobs(ctx, "", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, 101, p1.TotalItems)

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
	assert.NotEqual(t, p2.Items[0].ID, p1.Items[9].ID)
}

func TestPostgresSearchJobs(t *testing.T) {
	ctx := context.Background()
	schemaName := fmt.Sprintf("tork%d", rand.Int())
	dsn := `host=localhost user=tork password=tork dbname=tork search_path=%s sslmode=disable`
	ds, err := NewPostgresDataStore(fmt.Sprintf(dsn, schemaName))
	assert.NoError(t, err)
	_, err = ds.db.Exec(fmt.Sprintf("create schema %s", schemaName))
	assert.NoError(t, err)
	defer func() {
		_, err = ds.db.Exec(fmt.Sprintf("drop schema %s cascade", schemaName))
		assert.NoError(t, err)
	}()
	err = ds.ExecScript(postgres.SCHEMA)
	assert.NoError(t, err)
	for i := 0; i < 101; i++ {
		j1 := tork.Job{
			ID:    uuid.NewUUID(),
			Name:  fmt.Sprintf("Job %d", (i + 1)),
			State: tork.JobStateRunning,
			Tasks: []*tork.Task{
				{
					Name: "some task",
				},
			},
		}
		err := ds.CreateJob(ctx, &j1)
		assert.NoError(t, err)

		now := time.Now().UTC()
		err = ds.CreateTask(ctx, &tork.Task{
			ID:        uuid.NewUUID(),
			JobID:     j1.ID,
			State:     tork.TaskStateRunning,
			CreatedAt: &now,
		})
		assert.NoError(t, err)
	}
	p1, err := ds.GetJobs(ctx, "101", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 1, p1.Size)
	assert.Equal(t, 1, p1.TotalItems)

	p1, err = ds.GetJobs(ctx, "Job", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, 101, p1.TotalItems)

	p1, err = ds.GetJobs(ctx, "running", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, 101, p1.TotalItems)
}

func TestPostgresGetMetrics(t *testing.T) {
	ctx := context.Background()
	schemaName := fmt.Sprintf("tork%d", rand.Int())
	dsn := `host=localhost user=tork password=tork dbname=tork search_path=%s sslmode=disable`
	ds, err := NewPostgresDataStore(fmt.Sprintf(dsn, schemaName))
	assert.NoError(t, err)
	_, err = ds.db.Exec(fmt.Sprintf("create schema %s", schemaName))
	assert.NoError(t, err)
	defer func() {
		_, err = ds.db.Exec(fmt.Sprintf("drop schema %s cascade", schemaName))
		assert.NoError(t, err)
	}()
	err = ds.ExecScript(postgres.SCHEMA)
	assert.NoError(t, err)
	s, err := ds.GetMetrics(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, s.Jobs.Running)
	assert.Equal(t, 0, s.Tasks.Running)
	assert.Equal(t, float64(0), s.Nodes.CPUPercent)
	assert.Equal(t, 0, s.Nodes.Running)

	now := time.Now().UTC()

	jobIDs := []string{}

	for i := 0; i < 100; i++ {
		var state tork.JobState
		if i%2 == 0 {
			state = tork.JobStateRunning
		} else {
			state = tork.JobStatePending
		}
		jid := uuid.NewUUID()
		err := ds.CreateJob(ctx, &tork.Job{
			ID:        jid,
			State:     state,
			CreatedAt: now,
		})
		assert.NoError(t, err)
		jobIDs = append(jobIDs, jid)
	}

	for i := 0; i < 100; i++ {
		var state tork.TaskState
		if i%2 == 0 {
			state = tork.TaskStateRunning
		} else {
			state = tork.TaskStatePending
		}
		err := ds.CreateTask(ctx, &tork.Task{
			ID:        uuid.NewUUID(),
			JobID:     jobIDs[i],
			State:     state,
			CreatedAt: &now,
		})
		assert.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		err := ds.CreateNode(ctx, &tork.Node{
			ID:              uuid.NewUUID(),
			LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * time.Duration(i)),
			CPUPercent:      float64(i * 10),
		})
		assert.NoError(t, err)
	}

	s, err = ds.GetMetrics(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 50, s.Jobs.Running)
	assert.Equal(t, 50, s.Tasks.Running)
	assert.Equal(t, float64(20), s.Nodes.CPUPercent)
	assert.Equal(t, 5, s.Nodes.Running)
}

func TestPostgresWithTxCreateTask(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	j1 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.WithTx(ctx, func(tx Datastore) error {
		err = tx.CreateJob(ctx, &j1)
		assert.NoError(t, err)
		t1 := tork.Task{}
		err = tx.CreateTask(ctx, &t1)
		return err
	})
	assert.Error(t, err)

	// job was created in a bad tx. should not exist
	_, err = ds.GetJobByID(ctx, j1.ID)
	assert.Error(t, err)
}

func TestPostgresWithTxUpdateTask(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	j1 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	now := time.Now().UTC()
	t1 := tork.Task{
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		State:     tork.TaskStateRunning,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)
	err = ds.WithTx(ctx, func(tx Datastore) error {
		return tx.UpdateTask(ctx, t1.ID, func(u *tork.Task) error {
			u.State = tork.TaskStateFailed
			u.State = tork.TaskState(strings.Repeat("x", 100)) // invalid state
			return nil
		})
	})
	assert.Error(t, err)
	t11, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, t11.State)
}

func TestPostgresHealthCheck(t *testing.T) {
	ctx := context.Background()
	schemaName := fmt.Sprintf("tork%d", rand.Int())
	dsn := `host=localhost user=tork password=tork dbname=tork search_path=%s sslmode=disable`
	ds, err := NewPostgresDataStore(fmt.Sprintf(dsn, schemaName))
	assert.NoError(t, err)
	_, err = ds.db.Exec(fmt.Sprintf("create schema %s", schemaName))
	assert.NoError(t, err)
	defer func() {
		_, err = ds.db.Exec(fmt.Sprintf("drop schema %s cascade", schemaName))
		assert.NoError(t, err)
	}()
	err = ds.ExecScript(postgres.SCHEMA)
	assert.NoError(t, err)

	err = ds.HealthCheck(ctx)
	assert.NoError(t, err)

	_, err = ds.db.Exec("drop table nodes cascade")
	assert.NoError(t, err)

	err = ds.HealthCheck(ctx)
	assert.Error(t, err)
}
