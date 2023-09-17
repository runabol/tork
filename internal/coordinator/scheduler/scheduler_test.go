package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/mq"
	"github.com/stretchr/testify/assert"
)

func Test_scheduleRegularTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := make(chan any)
	err := b.SubscribeForTasks("test-queue", func(t *tork.Task) error {
		close(processed)
		return nil
	})
	assert.NoError(t, err)

	ds := datastore.NewInMemoryDatastore()
	s := NewScheduler(ds, b)
	assert.NoError(t, err)
	assert.NotNil(t, s)

	j1 := &tork.Job{
		ID:   uuid.NewUUID(),
		Name: "test job",
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		Queue: "test-queue",
		JobID: j1.ID,
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = s.scheduleRegularTask(ctx, tk)
	assert.NoError(t, err)

	<-processed

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, tk.State)
}

func Test_scheduleRegularTaskJobDefaults(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	s := NewScheduler(ds, b)

	j1 := &tork.Job{
		ID:   uuid.NewUUID(),
		Name: "test job",
		Defaults: &tork.JobDefaults{
			Queue: "some-queue",
			Retry: &tork.TaskRetry{
				Limit: 5,
			},
			Limits: &tork.TaskLimits{
				CPUs:   ".5",
				Memory: "10m",
			},
			Timeout: "5s",
		},
	}

	err := ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		JobID: j1.ID,
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = s.scheduleRegularTask(ctx, tk)
	assert.NoError(t, err)

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, tk.State)
	assert.Equal(t, "some-queue", tk.Queue)
	assert.Equal(t, 5, tk.Retry.Limit)
	assert.Equal(t, ".5", tk.Limits.CPUs)
	assert.Equal(t, "10m", tk.Limits.Memory)
	assert.Equal(t, "5s", tk.Timeout)
}

func Test_scheduleParallelTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := 0
	err := b.SubscribeForTasks(mq.QUEUE_PENDING, func(tk *tork.Task) error {
		processed = processed + 1
		assert.Equal(t, "test-queue", tk.Queue)
		return nil
	})
	assert.NoError(t, err)

	ds := datastore.NewInMemoryDatastore()
	s := NewScheduler(ds, b)
	assert.NoError(t, err)
	assert.NotNil(t, s)

	j := &tork.Job{
		ID:   uuid.NewUUID(),
		Name: "test job",
	}

	err = ds.CreateJob(ctx, j)
	assert.NoError(t, err)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		JobID: j.ID,
		Parallel: &tork.ParallelTask{
			Tasks: []*tork.Task{
				{
					Name:  "my parallel task",
					Queue: "test-queue",
				},
			},
		},
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = s.scheduleParallelTask(ctx, tk)
	assert.NoError(t, err)

	// wait for the task to get processed
	time.Sleep(time.Millisecond * 100)

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, tk.State)
	// task should only be processed once
	assert.Equal(t, 1, processed)
}

func Test_scheduleEachTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := 0
	err := b.SubscribeForTasks(mq.QUEUE_PENDING, func(tk *tork.Task) error {
		processed = processed + 1
		assert.Equal(t, "test-queue", tk.Queue)
		return nil
	})
	assert.NoError(t, err)

	ds := datastore.NewInMemoryDatastore()
	s := NewScheduler(ds, b)
	assert.NoError(t, err)
	assert.NotNil(t, s)

	j := &tork.Job{
		ID:   uuid.NewUUID(),
		Name: "test job",
	}

	err = ds.CreateJob(ctx, j)
	assert.NoError(t, err)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		JobID: j.ID,
		Each: &tork.EachTask{
			List: "{{ sequence (1,3) }}",
			Task: &tork.Task{
				Queue: "test-queue",
			},
		},
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = s.scheduleEachTask(ctx, tk)
	assert.NoError(t, err)

	// wait for the task to get processed
	time.Sleep(time.Millisecond * 100)

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, tk.State)
	// task should only be processed once
	assert.Equal(t, 2, processed)
}

func Test_scheduleSubJobTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := make(chan any)
	err := b.SubscribeForJobs(func(j *tork.Job) error {
		close(processed)
		return nil
	})
	assert.NoError(t, err)

	ds := datastore.NewInMemoryDatastore()
	s := NewScheduler(ds, b)
	assert.NoError(t, err)
	assert.NotNil(t, s)

	j := &tork.Job{
		ID:   uuid.NewUUID(),
		Name: "test job",
	}

	err = ds.CreateJob(ctx, j)
	assert.NoError(t, err)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		JobID: j.ID,
		SubJob: &tork.SubJobTask{
			Name: "my sub job",
			Tasks: []*tork.Task{
				{
					Name: "some task",
				},
			},
		},
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = s.scheduleSubJob(ctx, tk)
	assert.NoError(t, err)

	// wait for the task to get processed
	<-processed

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, tk.State)
}
