package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/task"
	"github.com/stretchr/testify/assert"
)

func Test_handleProgress(t *testing.T) {
	ctx := context.Background()
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
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

		now := time.Now().UTC()

		tk := &tork.Task{
			ID:        uuid.NewUUID(),
			Queue:     "test-queue",
			JobID:     j1.ID,
			Progress:  0,
			CreatedAt: &now,
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

		now := time.Now().UTC()

		tk := &tork.Task{
			ID:        uuid.NewUUID(),
			Queue:     "test-queue",
			JobID:     j1.ID,
			Progress:  5,
			CreatedAt: &now,
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

		now := time.Now().UTC()

		tk := &tork.Task{
			ID:        uuid.NewUUID(),
			Queue:     "test-queue",
			JobID:     j1.ID,
			Progress:  50,
			CreatedAt: &now,
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

		now := time.Now().UTC()

		tk := &tork.Task{
			ID:        uuid.NewUUID(),
			Queue:     "test-queue",
			JobID:     j1.ID,
			Progress:  100,
			CreatedAt: &now,
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

		now := time.Now().UTC()

		tk := &tork.Task{
			ID:        uuid.NewUUID(),
			Queue:     "test-queue",
			JobID:     j1.ID,
			Progress:  -10,
			CreatedAt: &now,
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

		now := time.Now().UTC()

		tk := &tork.Task{
			ID:        uuid.NewUUID(),
			Queue:     "test-queue",
			JobID:     j1.ID,
			Progress:  101,
			CreatedAt: &now,
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

	assert.NoError(t, ds.Close())

}
