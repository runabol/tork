package postgres

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
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
	assert.Equal(t, tork.USER_GUEST, j1.CreatedBy.Username)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.USER_GUEST, j2.CreatedBy.Username)

	t1 := tork.Task{
		ID:          uuid.NewUUID(),
		CreatedAt:   &now,
		JobID:       j1.ID,
		Description: "some description",
		Networks:    []string{"some-network"},
		Files:       map[string]string{"myfile": "hello world"},
		Registry:    &tork.Registry{Username: "me", Password: "secret"},
		GPUs:        "all",
		If:          "true",
		Tags:        []string{"tag1", "tag2"},
		Workdir:     "/some/dir",
		Priority:    2,
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
	assert.Equal(t, "all", t2.GPUs)
	assert.Equal(t, "true", t2.If)
	assert.Nil(t, t2.Parallel)
	assert.Equal(t, []string([]string{"tag1", "tag2"}), t2.Tags)
	assert.Equal(t, "/some/dir", t2.Workdir)
	assert.Equal(t, 2, t2.Priority)
}

func TestPostgresCreateJob(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	now := time.Now().UTC()
	u := &tork.User{
		ID:        uuid.NewUUID(),
		Username:  uuid.NewShortUUID(),
		Name:      "Tester",
		CreatedAt: &now,
	}
	err = ds.CreateUser(ctx, u)
	assert.NoError(t, err)
	j1 := tork.Job{
		ID:        uuid.NewUUID(),
		CreatedBy: u,
		Tags:      []string{"tag-a", "tag-b"},
		AutoDelete: &tork.AutoDelete{
			After: "5h",
		},
		Secrets: map[string]string{
			"password": "secret",
		},
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	assert.Equal(t, u.Username, j1.CreatedBy.Username)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, u.Username, j2.CreatedBy.Username)
	assert.Equal(t, []string{"tag-a", "tag-b"}, j2.Tags)
	assert.Equal(t, "5h", j2.AutoDelete.After)
	assert.Equal(t, map[string]string{"password": "secret"}, j2.Secrets)
}

func TestPostgresCreateAndGetParallelTask(t *testing.T) {
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
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
		Parallel: &tork.ParallelTask{
			Tasks: []*tork.Task{{
				Name: "parallel task1",
			}, {
				Name: "parallel task2",
			}},
		},
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)
	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.NotNil(t, t2.Parallel)
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
		u.Queue = "somequeue"
		u.Progress = 57.3
		return nil
	})
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, t2.State)
	assert.Equal(t, "my result", t2.Result)
	assert.Equal(t, "somequeue", t2.Queue)
	assert.Equal(t, 57.3, t2.Progress)
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
		Name:     "some node",
		Hostname: "some-name",
		Port:     1234,
		Version:  "1.0.0",
	}
	err = ds.CreateNode(ctx, n1)
	assert.NoError(t, err)
	n2, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, n1.ID, n2.ID)
	assert.Equal(t, "some-name", n2.Hostname)
	assert.Equal(t, 1234, n2.Port)
	assert.Equal(t, "1.0.0", n2.Version)
	assert.Equal(t, "some node", n2.Name)
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
		Webhooks: []*tork.Webhook{
			{
				URL: "http://example.com/1",
				Headers: map[string]string{
					"header1": "value1",
				},
			},
			{
				URL: "http://example.com/2",
				Headers: map[string]string{
					"header1": "value1",
				},
				Event: "job.StatusChange",
			},
		},
		Permissions: []*tork.Permission{{
			User: &tork.User{
				Username: tork.USER_GUEST,
			},
		}},
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
	assert.Len(t, j2.Webhooks, 2)
	assert.Equal(t, j1.Webhooks[0], j2.Webhooks[0])
	assert.Equal(t, j1.Webhooks[1], j2.Webhooks[1])
	assert.Equal(t, "guest", j2.Permissions[0].User.Username)

	j3 := tork.Job{
		ID: uuid.NewUUID(),
		Permissions: []*tork.Permission{{
			Role: &tork.Role{
				Slug: tork.ROLE_PUBLIC,
			},
		}},
	}
	err = ds.CreateJob(ctx, &j3)
	assert.NoError(t, err)
	j4, err := ds.GetJobByID(ctx, j3.ID)
	assert.NoError(t, err)
	assert.Equal(t, "public", j4.Permissions[0].Role.Slug)
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
	deleteAt := time.Now().UTC()
	err = ds.UpdateJob(ctx, j1.ID, func(u *tork.Job) error {
		u.State = tork.JobStateCompleted
		u.Context.Inputs["var2"] = "val2"
		u.DeleteAt = &deleteAt
		u.Progress = 56
		return nil
	})
	assert.NoError(t, err)
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateCompleted, j2.State)
	assert.Equal(t, "val1", j2.Context.Inputs["var1"])
	assert.Equal(t, "val2", j2.Context.Inputs["var2"])
	assert.Equal(t, deleteAt.Unix(), j2.DeleteAt.Unix())
	assert.Equal(t, float64(56), j2.Progress)
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
	p1, err := ds.GetJobs(ctx, "", "", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, 101, p1.TotalItems)

	p2, err := ds.GetJobs(ctx, "", "", 2, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p2.Size)

	p10, err := ds.GetJobs(ctx, "", "", 10, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p10.Size)

	p11, err := ds.GetJobs(ctx, "", "", 11, 10)
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

	u1 := &tork.User{
		ID:       uuid.NewUUID(),
		Username: uuid.NewShortUUID(),
		Name:     "Tester",
	}
	err = ds.CreateUser(ctx, u1)
	assert.NoError(t, err)

	u2 := &tork.User{
		ID:       uuid.NewUUID(),
		Username: uuid.NewShortUUID(),
		Name:     "Tester",
	}
	err = ds.CreateUser(ctx, u2)
	assert.NoError(t, err)

	r := &tork.Role{
		Slug: "test-role",
		Name: "Test Role",
	}
	err = ds.CreateRole(ctx, r)
	assert.NoError(t, err)

	err = ds.AssignRole(ctx, u2.ID, r.ID)
	assert.NoError(t, err)

	u3 := &tork.User{
		ID:       uuid.NewUUID(),
		Username: uuid.NewShortUUID(),
		Name:     "Tester",
	}
	err = ds.CreateUser(ctx, u3)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		j1 := tork.Job{
			ID:    uuid.NewUUID(),
			Name:  fmt.Sprintf("Job %d", (i + 1)),
			State: tork.JobStateRunning,
			Tasks: []*tork.Task{{
				Name: "some task",
			}},
			Tags: []string{fmt.Sprintf("tag-%d", i)},
			Permissions: []*tork.Permission{{
				User: u1,
			}, {
				Role: r,
			}},
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

	for i := 100; i < 101; i++ {
		j1 := tork.Job{
			ID:    uuid.NewUUID(),
			Name:  fmt.Sprintf("Job %d", (i + 1)),
			State: tork.JobStateRunning,
			Tasks: []*tork.Task{{
				Name: "some task",
			}},
			Tags: []string{fmt.Sprintf("tag-%d", i)},
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

	p1, err := ds.GetJobs(ctx, "", "", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, 101, p1.TotalItems)

	p1, err = ds.GetJobs(ctx, "", "101", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 1, p1.Size)
	assert.Equal(t, 1, p1.TotalItems)

	p1, err = ds.GetJobs(ctx, "", "tag:tag-1", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 1, p1.Size)
	assert.Equal(t, 1, p1.TotalItems)

	p1, err = ds.GetJobs(ctx, "", "tag:not-a-tag", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 0, p1.Size)
	assert.Equal(t, 0, p1.TotalItems)

	p1, err = ds.GetJobs(ctx, "", "tags:not-a-tag,tag-1", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 1, p1.Size)
	assert.Equal(t, 1, p1.TotalItems)

	p1, err = ds.GetJobs(ctx, "", "Job", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, 101, p1.TotalItems)

	p1, err = ds.GetJobs(ctx, "", "running", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, 101, p1.TotalItems)

	p1, err = ds.GetJobs(ctx, u1.Username, "running", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, 101, p1.TotalItems)

	p1, err = ds.GetJobs(ctx, u2.Username, "running", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, 101, p1.TotalItems)

	p1, err = ds.GetJobs(ctx, u3.Username, "running", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 1, p1.Size)
	assert.Equal(t, 1, p1.TotalItems)

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
	err = ds.WithTx(ctx, func(tx datastore.Datastore) error {
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
	err = ds.WithTx(ctx, func(tx datastore.Datastore) error {
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
}

func TestPostgresCreateAndGetTaskLogs(t *testing.T) {
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
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)

	err = ds.CreateTaskLogPart(ctx, &tork.TaskLogPart{
		Number:   1,
		TaskID:   t1.ID,
		Contents: "line 1",
	})
	assert.NoError(t, err)

	logs, err := ds.GetTaskLogParts(ctx, t1.ID, "", 1, 10)
	assert.NoError(t, err)
	assert.Len(t, logs.Items, 1)
	assert.Equal(t, "line 1", logs.Items[0].Contents)
	assert.NotEmpty(t, logs.Items[0].ID)
}

func TestPostgresCreateAndGetTaskLogsMultiParts(t *testing.T) {
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
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)

	parts := 10

	wg := sync.WaitGroup{}
	wg.Add(parts)

	for i := 1; i <= parts; i++ {
		go func(n int) {
			defer wg.Done()
			err := ds.CreateTaskLogPart(ctx, &tork.TaskLogPart{
				Number:   n,
				TaskID:   t1.ID,
				Contents: fmt.Sprintf("line %d", n),
			})
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	logs, err := ds.GetTaskLogParts(ctx, t1.ID, "", 1, 10)
	assert.NoError(t, err)
	assert.Len(t, logs.Items, 10)
	assert.Equal(t, "line 10", logs.Items[0].Contents)
	assert.Equal(t, "line 1", logs.Items[9].Contents)
}

func TestPostgresCreateAndGetTaskLogsLarge(t *testing.T) {
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
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)

	for i := 1; i <= 100; i++ {
		err := ds.CreateTaskLogPart(ctx, &tork.TaskLogPart{
			Number:   i,
			TaskID:   t1.ID,
			Contents: fmt.Sprintf("line %d", i),
		})
		assert.NoError(t, err)
	}

	logs, err := ds.GetTaskLogParts(ctx, t1.ID, "", 1, 10)
	assert.NoError(t, err)
	assert.Len(t, logs.Items, 10)
	assert.Equal(t, "line 100", logs.Items[0].Contents)
	assert.Equal(t, "line 91", logs.Items[9].Contents)
	assert.Equal(t, 10, logs.Size)
	assert.Equal(t, 10, logs.TotalPages)
}

func TestPostgresQueryTaskLogs(t *testing.T) {
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
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)

	for i := 1; i <= 100; i++ {
		err := ds.CreateTaskLogPart(ctx, &tork.TaskLogPart{
			Number:   i,
			TaskID:   t1.ID,
			Contents: fmt.Sprintf("line %d", i),
		})
		assert.NoError(t, err)
	}

	logs, err := ds.GetTaskLogParts(ctx, t1.ID, "line 91", 1, 10)
	assert.NoError(t, err)
	assert.Len(t, logs.Items, 1)
	assert.Equal(t, "line 91", logs.Items[0].Contents)
}

func TestPostgresCreateAndExpungeTaskLogs(t *testing.T) {
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
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)

	for i := 1; i <= 100; i++ {
		err := ds.CreateTaskLogPart(ctx, &tork.TaskLogPart{
			Number:   i,
			TaskID:   t1.ID,
			Contents: fmt.Sprintf("line %d", i),
		})
		assert.NoError(t, err)
	}

	n, err := ds.expungeExpiredTaskLogPart()
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	logs, err := ds.GetTaskLogParts(ctx, t1.ID, "", 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, 100, logs.TotalItems)

	retentionPeriod := time.Microsecond
	ds.logsRetentionDuration = &retentionPeriod

	n, err = ds.expungeExpiredTaskLogPart()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, n, 100)

	logs, err = ds.GetTaskLogParts(ctx, t1.ID, "", 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, logs.TotalItems)
}

func Test_cleanup(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn, WithDisableCleanup(true))
	assert.NoError(t, err)
	now := time.Now().UTC()
	j1 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	j2 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j2)
	assert.NoError(t, err)

	past := time.Now().UTC().Add(-time.Minute)
	err = ds.UpdateJob(ctx, j2.ID, func(u *tork.Job) error {
		u.DeleteAt = &past
		return nil
	})
	assert.NoError(t, err)

	j3 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j3)
	assert.NoError(t, err)

	t1 := tork.Task{
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)

	for i := 1; i <= 100; i++ {
		err := ds.CreateTaskLogPart(ctx, &tork.TaskLogPart{
			Number:   i,
			TaskID:   t1.ID,
			Contents: fmt.Sprintf("line %d", i),
		})
		assert.NoError(t, err)
	}

	err = ds.cleanup()
	assert.NoError(t, err)
	assert.Equal(t, time.Minute, *ds.cleanupInterval)

	logs, err := ds.GetTaskLogParts(ctx, t1.ID, "", 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, 100, logs.TotalItems)

	retentionPeriod := time.Microsecond
	ds.logsRetentionDuration = &retentionPeriod

	err = ds.cleanup()
	assert.NoError(t, err)
	assert.Equal(t, time.Minute, *ds.cleanupInterval)

	logs, err = ds.GetTaskLogParts(ctx, t1.ID, "", 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, logs.TotalItems)

	_, err = ds.GetJobByID(ctx, j2.ID)
	assert.Error(t, err)

	_, err = ds.GetJobByID(ctx, j3.ID)
	assert.NoError(t, err)
}

func TestPostgresGetJobLogParts(t *testing.T) {
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
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)

	err = ds.CreateTaskLogPart(ctx, &tork.TaskLogPart{
		Number:   1,
		TaskID:   t1.ID,
		Contents: "line 1",
	})
	assert.NoError(t, err)

	logs, err := ds.GetJobLogParts(ctx, j1.ID, "", 1, 10)
	assert.NoError(t, err)
	assert.Len(t, logs.Items, 1)
	assert.Equal(t, "line 1", logs.Items[0].Contents)
}

func TestPostgresQueryJobLogParts(t *testing.T) {
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
		ID:        uuid.NewUUID(),
		CreatedAt: &now,
		JobID:     j1.ID,
	}
	err = ds.CreateTask(ctx, &t1)
	assert.NoError(t, err)

	for i := 1; i <= 100; i++ {
		err := ds.CreateTaskLogPart(ctx, &tork.TaskLogPart{
			Number:   i,
			TaskID:   t1.ID,
			Contents: fmt.Sprintf("line %d", i),
		})
		assert.NoError(t, err)
	}

	logs, err := ds.GetJobLogParts(ctx, j1.ID, "line 91", 1, 10)
	assert.NoError(t, err)
	assert.Len(t, logs.Items, 1)
	assert.Equal(t, "line 91", logs.Items[0].Contents)
}

func TestPostgresCreateRole(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	now := time.Now().UTC()
	uid := uuid.NewUUID()
	r := &tork.Role{
		ID:        uid,
		Slug:      "test-role-" + uuid.NewUUID(),
		Name:      "Test Role",
		CreatedAt: &now,
	}
	err = ds.CreateRole(ctx, r)
	assert.NoError(t, err)

	role, err := ds.GetRole(ctx, r.Slug)
	assert.NoError(t, err)
	assert.Equal(t, r.Slug, role.Slug)

	roles, err := ds.GetRoles(ctx)
	assert.NoError(t, err)
	assert.Greater(t, len(roles), 0)
	assert.Equal(t, "Public", roles[0].Name)

	u := &tork.User{
		ID:        uuid.NewUUID(),
		Username:  uuid.NewShortUUID(),
		Name:      "Tester",
		CreatedAt: &now,
	}
	err = ds.CreateUser(ctx, u)
	assert.NoError(t, err)

	err = ds.AssignRole(ctx, u.ID, r.ID)
	assert.NoError(t, err)

	uroles, err := ds.GetUserRoles(ctx, u.ID)
	assert.NoError(t, err)
	assert.Len(t, uroles, 1)
	assert.Equal(t, r.ID, uroles[0].ID)

	err = ds.UnassignRole(ctx, u.ID, r.ID)
	assert.NoError(t, err)

	uroles, err = ds.GetUserRoles(ctx, u.ID)
	assert.NoError(t, err)
	assert.Len(t, uroles, 0)
}

func TestPostgresGetNextTask(t *testing.T) {
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

	parentTaskID := uuid.NewUUID()
	childTaskID := uuid.NewUUID()

	tasks := []*tork.Task{{
		ID:        parentTaskID,
		State:     tork.TaskStatePending,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        childTaskID,
		ParentID:  parentTaskID,
		State:     tork.TaskStateCreated,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateCreated,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateCreated,
		CreatedAt: &now,
		JobID:     j1.ID,
	}}

	for _, ta := range tasks {
		err := ds.CreateTask(ctx, ta)
		assert.NoError(t, err)
	}
	nt, err := ds.GetNextTask(ctx, parentTaskID)
	assert.NoError(t, err)
	assert.Equal(t, childTaskID, nt.ID)

	_, err = ds.GetNextTask(ctx, childTaskID)
	assert.Error(t, err)
}

func TestPostgresUpdateScheduledJob(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	now := time.Now().UTC()
	sj := tork.ScheduledJob{
		ID:        uuid.NewUUID(),
		Name:      "Test Scheduled Job",
		CreatedAt: now,
		State:     tork.ScheduledJobStateActive,
	}
	err = ds.CreateScheduledJob(ctx, &sj)
	assert.NoError(t, err)

	err = ds.UpdateScheduledJob(ctx, sj.ID, func(u *tork.ScheduledJob) error {
		u.State = tork.ScheduledJobStatePaused
		return nil
	})
	assert.NoError(t, err)

	updatedSJ, err := ds.GetScheduledJobByID(ctx, sj.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.ScheduledJobStatePaused, updatedSJ.State)
}

func TestPostgresGetScheduledJobs(t *testing.T) {
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
		j1 := tork.ScheduledJob{
			ID:   uuid.NewUUID(),
			Cron: "* * * * *",
			Name: fmt.Sprintf("Scheduled Job %d", (i + 1)),
			Tasks: []*tork.Task{
				{
					Name: "some task",
				},
			},
		}
		err := ds.CreateScheduledJob(ctx, &j1)
		assert.NoError(t, err)
	}
	p1, err := ds.GetScheduledJobs(ctx, "", 1, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p1.Size)
	assert.Equal(t, 101, p1.TotalItems)

	sj, err := ds.GetScheduledJobByID(ctx, p1.Items[0].ID)
	assert.NoError(t, err)
	assert.Equal(t, p1.Items[0].ID, sj.ID)

	p2, err := ds.GetScheduledJobs(ctx, "", 2, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p2.Size)

	p10, err := ds.GetScheduledJobs(ctx, "", 10, 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, p10.Size)

	p11, err := ds.GetScheduledJobs(ctx, "", 11, 10)
	assert.NoError(t, err)
	assert.Equal(t, 1, p11.Size)

	assert.NotEqual(t, p2.Items[0].ID, p1.Items[9].ID)
	assert.NotEqual(t, p2.Items[0].ID, p1.Items[9].ID)
}

func TestPostgresGetActiveScheduledJobs(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	now := time.Now().UTC()
	u := &tork.User{
		ID:        uuid.NewUUID(),
		Username:  uuid.NewShortUUID(),
		Name:      "Tester",
		CreatedAt: &now,
	}
	err = ds.CreateUser(ctx, u)
	assert.NoError(t, err)

	sj1 := &tork.ScheduledJob{
		ID:        uuid.NewUUID(),
		Name:      "Scheduled Job 1",
		Cron:      "* * * * *",
		CreatedAt: now,
		CreatedBy: u,
		State:     tork.ScheduledJobStateActive,
	}
	err = ds.CreateScheduledJob(ctx, sj1)
	assert.NoError(t, err)

	sj2 := &tork.ScheduledJob{
		ID:        uuid.NewUUID(),
		Name:      "Scheduled Job 2",
		Cron:      "* * * * *",
		CreatedAt: now,
		CreatedBy: u,
		State:     tork.ScheduledJobStatePaused,
	}
	err = ds.CreateScheduledJob(ctx, sj2)
	assert.NoError(t, err)

	activeJobs, err := ds.GetActiveScheduledJobs(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, activeJobs)
	for _, aj := range activeJobs {
		assert.Equal(t, tork.ScheduledJobStateActive, aj.State)
	}
}

func TestPostgresExpungeExpiredJobs(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn, WithJobsRetentionDuration(time.Hour*24*30))
	assert.NoError(t, err)

	now := time.Now().UTC()

	// Create jobs with different states and delete_at times
	jobs := []*tork.Job{
		{
			ID:        uuid.NewUUID(),
			State:     tork.JobStateCompleted,
			CreatedAt: now.Add(-time.Hour * 24 * 31), // older than default retention
		},
		{
			ID:        uuid.NewUUID(),
			State:     tork.JobStateFailed,
			CreatedAt: now.Add(-time.Hour * 24 * 31), // older than default retention
		},
		{
			ID:        uuid.NewUUID(),
			State:     tork.JobStateCancelled,
			CreatedAt: now.Add(-time.Hour * 24 * 31), // older than default retention
		},
		{
			ID:        uuid.NewUUID(),
			State:     tork.JobStateRunning,
			CreatedAt: now.Add(-time.Hour * 24 * 31), // should not be deleted
		},
		{
			ID:        uuid.NewUUID(),
			State:     tork.JobStatePending,
			CreatedAt: now.Add(-time.Hour * 24 * 31), // should not be deleted
		},
		{
			ID:        uuid.NewUUID(),
			State:     tork.JobStateCompleted,
			CreatedAt: now,
			DeleteAt:  &now, // should be deleted
		},
	}

	for _, job := range jobs {
		err = ds.CreateJob(ctx, job)
		assert.NoError(t, err)
		if job.DeleteAt != nil {
			err = ds.UpdateJob(ctx, job.ID, func(u *tork.Job) error {
				u.DeleteAt = job.DeleteAt
				return nil
			})
			assert.NoError(t, err)
		}
	}

	// Expunge expired jobs
	n, err := ds.expungeExpiredJobs()
	assert.NoError(t, err)
	assert.Equal(t, 4, n) // 3 jobs older than retention + 1 job with delete_at

	// Verify remaining jobs
	for _, job := range jobs {
		_, err := ds.GetJobByID(ctx, job.ID)
		if job.State == tork.JobStateRunning || job.State == tork.JobStatePending {
			assert.NoError(t, err)
		} else if job.DeleteAt == nil || job.DeleteAt.Before(now) {
			assert.Error(t, err)
		}
	}
}

func TestPostgresDeleteScheduledJob(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	now := time.Now().UTC()
	sj := tork.ScheduledJob{
		ID:        uuid.NewUUID(),
		Name:      "Test Scheduled Job",
		CreatedAt: now,
		State:     tork.ScheduledJobStateActive,
	}
	err = ds.CreateScheduledJob(ctx, &sj)
	assert.NoError(t, err)

	err = ds.DeleteScheduledJob(ctx, sj.ID)
	assert.NoError(t, err)

	_, err = ds.GetScheduledJobByID(ctx, sj.ID)
	assert.Error(t, err)
}
