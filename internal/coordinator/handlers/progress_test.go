package handlers

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore/inmemory"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/task"
	"github.com/stretchr/testify/assert"
)

func Test_handleProgress(t *testing.T) {
	ctx := context.Background()
	ds := inmemory.NewInMemoryDatastore()
	handler := NewProgressHandler(ds, job.NoOpHandlerFunc)
	assert.NotNil(t, handler)

	t.Run("no progress", func(t *testing.T) {
		j1 := &tork.Job{
			ID:        uuid.NewUUID(),
			Name:      "test job",
			TaskCount: 2,
			Position:  1,
		}
		err := ds.CreateJob(ctx, j1)
		assert.NoError(t, err)

		tk := &tork.Task{
			ID:       uuid.NewUUID(),
			Queue:    "test-queue",
			JobID:    j1.ID,
			Progress: 0,
		}

		err = ds.CreateTask(ctx, tk)
		assert.NoError(t, err)

		err = handler(ctx, task.Progress, tk)
		assert.NoError(t, err)

		tk2, err := ds.GetTaskByID(ctx, tk.ID)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), tk2.Progress)

		j2, err := ds.GetJobByID(ctx, tk.JobID)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), j2.Progress)
	})

	t.Run("little progress", func(t *testing.T) {
		j1 := &tork.Job{
			ID:        uuid.NewUUID(),
			Name:      "test job",
			TaskCount: 2,
			Position:  1,
		}
		err := ds.CreateJob(ctx, j1)
		assert.NoError(t, err)

		tk := &tork.Task{
			ID:       uuid.NewUUID(),
			Queue:    "test-queue",
			JobID:    j1.ID,
			Progress: 5,
		}

		err = ds.CreateTask(ctx, tk)
		assert.NoError(t, err)

		err = handler(ctx, task.Progress, tk)
		assert.NoError(t, err)

		tk2, err := ds.GetTaskByID(ctx, tk.ID)
		assert.NoError(t, err)
		assert.Equal(t, float64(5), tk2.Progress)

		j2, err := ds.GetJobByID(ctx, tk.JobID)
		assert.NoError(t, err)
		assert.Equal(t, float64(2.5), j2.Progress)
	})

	t.Run("half progress", func(t *testing.T) {
		j1 := &tork.Job{
			ID:        uuid.NewUUID(),
			Name:      "test job",
			TaskCount: 2,
			Position:  1,
		}
		err := ds.CreateJob(ctx, j1)
		assert.NoError(t, err)

		tk := &tork.Task{
			ID:       uuid.NewUUID(),
			Queue:    "test-queue",
			JobID:    j1.ID,
			Progress: 50,
		}

		err = ds.CreateTask(ctx, tk)
		assert.NoError(t, err)

		err = handler(ctx, task.Progress, tk)
		assert.NoError(t, err)

		tk2, err := ds.GetTaskByID(ctx, tk.ID)
		assert.NoError(t, err)
		assert.Equal(t, float64(50), tk2.Progress)

		j2, err := ds.GetJobByID(ctx, tk.JobID)
		assert.NoError(t, err)
		assert.Equal(t, float64(25), j2.Progress)
	})

	t.Run("done", func(t *testing.T) {
		j1 := &tork.Job{
			ID:        uuid.NewUUID(),
			Name:      "test job",
			TaskCount: 2,
			Position:  1,
		}
		err := ds.CreateJob(ctx, j1)
		assert.NoError(t, err)

		tk := &tork.Task{
			ID:       uuid.NewUUID(),
			Queue:    "test-queue",
			JobID:    j1.ID,
			Progress: 100,
		}

		err = ds.CreateTask(ctx, tk)
		assert.NoError(t, err)

		err = handler(ctx, task.Progress, tk)
		assert.NoError(t, err)

		tk2, err := ds.GetTaskByID(ctx, tk.ID)
		assert.NoError(t, err)
		assert.Equal(t, float64(100), tk2.Progress)

		j2, err := ds.GetJobByID(ctx, tk.JobID)
		assert.NoError(t, err)
		assert.Equal(t, float64(50), j2.Progress)
	})

	t.Run("backward progress", func(t *testing.T) {
		j1 := &tork.Job{
			ID:        uuid.NewUUID(),
			Name:      "test job",
			TaskCount: 2,
			Position:  1,
		}
		err := ds.CreateJob(ctx, j1)
		assert.NoError(t, err)

		tk := &tork.Task{
			ID:       uuid.NewUUID(),
			Queue:    "test-queue",
			JobID:    j1.ID,
			Progress: -10,
		}

		err = ds.CreateTask(ctx, tk)
		assert.NoError(t, err)

		err = handler(ctx, task.Progress, tk)
		assert.NoError(t, err)

		tk2, err := ds.GetTaskByID(ctx, tk.ID)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), tk2.Progress)

		j2, err := ds.GetJobByID(ctx, tk.JobID)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), j2.Progress)
	})

	t.Run("too much progress", func(t *testing.T) {
		j1 := &tork.Job{
			ID:        uuid.NewUUID(),
			Name:      "test job",
			TaskCount: 2,
			Position:  1,
		}
		err := ds.CreateJob(ctx, j1)
		assert.NoError(t, err)

		tk := &tork.Task{
			ID:       uuid.NewUUID(),
			Queue:    "test-queue",
			JobID:    j1.ID,
			Progress: 101,
		}

		err = ds.CreateTask(ctx, tk)
		assert.NoError(t, err)

		err = handler(ctx, task.Progress, tk)
		assert.NoError(t, err)

		tk2, err := ds.GetTaskByID(ctx, tk.ID)
		assert.NoError(t, err)
		assert.Equal(t, float64(100), tk2.Progress)

		j2, err := ds.GetJobByID(ctx, tk.JobID)
		assert.NoError(t, err)
		assert.Equal(t, float64(50), j2.Progress)
	})

}
