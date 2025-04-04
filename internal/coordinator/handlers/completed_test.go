package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/middleware/task"
	"github.com/stretchr/testify/assert"
)

func Test_handleCompletedLastTask(t *testing.T) {
	ctx := context.Background()
	b := broker.NewInMemoryBroker()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewCompletedHandler(ds, b)

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		Position:  2,
		TaskCount: 2,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
			{
				Name: "task-2",
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
		CreatedAt:   &now,
		JobID:       j1.ID,
		Position:    2,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	t1.State = tork.TaskStateCompleted

	err = handler(ctx, task.StateChange, t1)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt.Unix(), t2.CompletedAt.Unix())

	j2, err := ds.GetJobByID(ctx, t1.JobID)
	assert.NoError(t, err)
	assert.Equal(t, float64(100), j2.Progress)
	assert.NoError(t, ds.Close())
}

func Test_handleSkippedTask(t *testing.T) {
	ctx := context.Background()
	b := broker.NewInMemoryBroker()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewCompletedHandler(ds, b)

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		Position:  2,
		TaskCount: 2,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
			{
				Name: "task-2",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateSkipped,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		CreatedAt:   &now,
		Position:    2,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	t1.State = tork.TaskStateSkipped

	err = handler(ctx, task.StateChange, t1)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateSkipped, t2.State)
	assert.Equal(t, t1.CompletedAt.Unix(), t2.CompletedAt.Unix())

	j2, err := ds.GetJobByID(ctx, t1.JobID)
	assert.NoError(t, err)
	assert.Equal(t, float64(100), j2.Progress)
	assert.NoError(t, ds.Close())
}

func Test_handleCompletedLastSubJobTask(t *testing.T) {
	ctx := context.Background()
	b := broker.NewInMemoryBroker()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewCompletedHandler(ds, b)
	assert.NotNil(t, handler)

	now := time.Now().UTC()

	parentJob := &tork.Job{
		ID:       uuid.NewUUID(),
		State:    tork.JobStateRunning,
		Position: 1,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
		TaskCount: 1,
	}
	err = ds.CreateJob(ctx, parentJob)
	assert.NoError(t, err)

	parentTask := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateRunning,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       parentJob.ID,
		Position:    2,
		CreatedAt:   &now,
	}
	err = ds.CreateTask(ctx, parentTask)
	assert.NoError(t, err)

	err = b.SubscribeForTasks(broker.QUEUE_COMPLETED, func(t1 *tork.Task) error {
		// expecting completion of parent task
		assert.Equal(t, parentTask.ID, t1.ID)
		assert.Equal(t, tork.TaskStateCompleted, t1.State)
		return nil
	})
	assert.NoError(t, err)

	j1 := &tork.Job{
		ID:       uuid.NewUUID(),
		State:    tork.JobStateRunning,
		Position: 1,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
				Run:  "echo hello",
			},
		},
		ParentID:  parentTask.ID,
		TaskCount: 1,
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
		Position:    2,
		CreatedAt:   &now,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	t1.State = tork.TaskStateCompleted

	err = handler(ctx, task.StateChange, t1)
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 100)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt.Unix(), t2.CompletedAt.Unix())

	// verify that the job was marked
	// as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateCompleted, j2.State)
	assert.NoError(t, ds.Close())
}

func Test_handleCompletedFirstTask(t *testing.T) {
	ctx := context.Background()
	b := broker.NewInMemoryBroker()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewCompletedHandler(ds, b)
	assert.NotNil(t, handler)

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		Position:  1,
		TaskCount: 2,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
			{
				Name: "task-2",
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
		CreatedAt:   &now,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	t1.State = tork.TaskStateCompleted

	err = handler(ctx, task.StateChange, t1)
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 100)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt.Unix(), t2.CompletedAt.Unix())

	// verify that the job was NOT
	// marked as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateRunning, j2.State)
	assert.Equal(t, float64(50), j2.Progress)
	assert.NoError(t, ds.Close())
}

func Test_handleCompletedScheduledTask(t *testing.T) {
	ctx := context.Background()
	b := broker.NewInMemoryBroker()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewCompletedHandler(ds, b)
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
		CreatedAt: time.Now().UTC(),
		TaskCount: 1,
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateScheduled,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		CreatedAt:   &now,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	t1.State = tork.TaskStateCompleted

	err = handler(ctx, task.StateChange, t1)
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 100)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt.Unix(), t2.CompletedAt.Unix())

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateCompleted, j2.State)
	assert.NoError(t, ds.Close())
}

func Test_handleCompletedParallelTask(t *testing.T) {
	ctx := context.Background()
	b := broker.NewInMemoryBroker()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewCompletedHandler(ds, b)
	assert.NotNil(t, handler)

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		Position:  1,
		TaskCount: 2,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
				Parallel: &tork.ParallelTask{
					Tasks: []*tork.Task{
						{
							Name: "parallel-task-1",
						},
						{
							Name: "parallel-task-2",
						},
					},
				},
			},
			{
				Name: "task-2",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	pt := &tork.Task{
		ID:    uuid.NewUUID(),
		JobID: j1.ID,
		Parallel: &tork.ParallelTask{
			Tasks: []*tork.Task{
				{
					Name: "parallel-task-1",
				},
				{
					Name: "parallel-task-2",
				},
			},
		},
		State:     tork.TaskStateRunning,
		CreatedAt: &now,
	}

	err = ds.CreateTask(ctx, pt)
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateRunning,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		ParentID:    pt.ID,
		CreatedAt:   &now,
	}

	t5 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateScheduled,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		ParentID:    pt.ID,
		CreatedAt:   &now,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = ds.CreateTask(ctx, t5)
	assert.NoError(t, err)

	t1.State = tork.TaskStateCompleted

	err = handler(ctx, task.StateChange, t1)
	assert.NoError(t, err)

	t5.State = tork.TaskStateCompleted

	err = handler(ctx, task.StateChange, t5)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt.Unix(), t2.CompletedAt.Unix())

	pt1, err := ds.GetTaskByID(ctx, t1.ParentID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, pt1.State)

	// verify that the job was NOT
	// marked as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateRunning, j2.State)
	assert.NoError(t, ds.Close())
}

func Test_handleCompletedEachTask(t *testing.T) {
	ctx := context.Background()
	b := broker.NewInMemoryBroker()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewCompletedHandler(ds, b)
	assert.NotNil(t, handler)

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		Position:  1,
		TaskCount: 2,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
				Each: &tork.EachTask{
					Size: 2,
					List: "some expression",
					Task: &tork.Task{
						Name: "some task",
					},
				},
			},
			{
				Name: "task-2",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	pt := &tork.Task{
		ID:       uuid.NewUUID(),
		JobID:    j1.ID,
		Position: 1,
		Name:     "parent task",
		Each: &tork.EachTask{
			Size: 2,
			List: "some expression",
			Task: &tork.Task{
				Name: "some task",
			},
		},
		State:     tork.TaskStateRunning,
		CreatedAt: &now,
	}

	err = ds.CreateTask(ctx, pt)
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateRunning,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		ParentID:    pt.ID,
		CreatedAt:   &now,
	}

	t5 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateScheduled,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		ParentID:    pt.ID,
		CreatedAt:   &now,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	t1.State = tork.TaskStateCompleted

	err = ds.CreateTask(ctx, t5)
	assert.NoError(t, err)

	t5.State = tork.TaskStateCompleted

	err = handler(ctx, task.StateChange, t1)
	assert.NoError(t, err)

	err = handler(ctx, task.StateChange, t5)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt.Unix(), t2.CompletedAt.Unix())

	pt1, err := ds.GetTaskByID(ctx, t1.ParentID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, pt1.State)

	// verify that the job was NOT
	// marked as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateRunning, j2.State)
	assert.NoError(t, ds.Close())
}

func Test_completeTopLevelTaskWithTxRollback(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := postgres.NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	handler := NewCompletedHandler(ds, b)
	assert.NotNil(t, handler)

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		Position:  1,
		CreatedAt: time.Now().UTC(),
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
			{
				Name: "task-2",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	t1 := &tork.Task{
		ID:        uuid.NewUUID(),
		JobID:     j1.ID,
		State:     tork.TaskStateRunning,
		Position:  1,
		CreatedAt: &now,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	t1.JobID = "bad_job_id"

	err = handler(ctx, task.StateChange, t1)
	assert.Error(t, err)

	t11, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, t11.State)
	assert.NoError(t, ds.Close())
}

func Test_completeTopLevelTaskWithTx(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := postgres.NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	handler := NewCompletedHandler(ds, b)
	assert.NotNil(t, handler)

	err = b.SubscribeForTasks(broker.QUEUE_PENDING, func(t1 *tork.Task) error {
		return nil
	})
	assert.NoError(t, err)

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		Position:  1,
		TaskCount: 2,
		CreatedAt: time.Now().UTC(),
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
			{
				Name: "task-2",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	t1 := &tork.Task{
		ID:        uuid.NewUUID(),
		JobID:     j1.ID,
		State:     tork.TaskStateRunning,
		Position:  1,
		CreatedAt: &now,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	t1.State = tork.TaskStateCompleted

	err = handler(ctx, task.StateChange, t1)
	assert.NoError(t, err)

	t11, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t11.State)
	assert.NoError(t, ds.Close())
}

func Test_completeParallelTaskWithTx(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := postgres.NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	handler := NewCompletedHandler(ds, broker.NewInMemoryBroker())
	assert.NotNil(t, handler)

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		Position:  1,
		CreatedAt: time.Now().UTC(),
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	t1 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateRunning,
		StartedAt:   &now,
		CompletedAt: &now,
		CreatedAt:   &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		ParentID:    "no_such_parent_id",
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	t1.JobID = "bad_job_id"

	err = handler(ctx, task.StateChange, t1)
	assert.Error(t, err)

	t11, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, t11.State)
	assert.NoError(t, ds.Close())
}

func Test_completeEachTaskWithTx(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := postgres.NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	handler := NewCompletedHandler(ds, broker.NewInMemoryBroker())
	assert.NotNil(t, handler)

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		Position:  1,
		CreatedAt: time.Now().UTC(),
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	t1 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateRunning,
		StartedAt:   &now,
		CompletedAt: &now,
		CreatedAt:   &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		ParentID:    "no_such_parent_id",
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	t1.JobID = "bad_job_id"

	err = handler(ctx, task.StateChange, t1)
	assert.Error(t, err)

	t11, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, t11.State)
	assert.NoError(t, ds.Close())
}

func Test_handleCompletedEachTaskWithNextTask(t *testing.T) {
	ctx := context.Background()
	b := broker.NewInMemoryBroker()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewCompletedHandler(ds, b)
	assert.NotNil(t, handler)

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		Position:  1,
		TaskCount: 2,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
				Each: &tork.EachTask{
					Size:        3,
					Concurrency: 1,
					List:        "some expression",
					Task: &tork.Task{
						Name: "some task",
					},
				},
			},
			{
				Name: "task-2",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	pt := &tork.Task{
		ID:       uuid.NewUUID(),
		JobID:    j1.ID,
		Position: 1,
		Name:     "parent task",
		Each: &tork.EachTask{
			Size:        3,
			List:        "some expression",
			Concurrency: 1,
			Task: &tork.Task{
				Name: "some task",
			},
		},
		State:     tork.TaskStateRunning,
		CreatedAt: &now,
	}

	err = ds.CreateTask(ctx, pt)
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateRunning,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		ParentID:    pt.ID,
		CreatedAt:   &now,
	}

	t6 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateCreated,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		ParentID:    pt.ID,
		CreatedAt:   &now,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	t1.State = tork.TaskStateCompleted

	err = ds.CreateTask(ctx, t6)
	assert.NoError(t, err)

	err = handler(ctx, task.StateChange, t1)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt.Unix(), t2.CompletedAt.Unix())

	t3, err := ds.GetTaskByID(ctx, t6.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStatePending, t3.State)

	pt1, err := ds.GetTaskByID(ctx, t1.ParentID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, pt1.State)
	assert.NoError(t, ds.Close())
}
