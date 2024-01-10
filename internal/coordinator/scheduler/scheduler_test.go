package scheduler

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore/inmemory"
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

	ds := inmemory.NewInMemoryDatastore()
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

	ds := inmemory.NewInMemoryDatastore()
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

	ds := inmemory.NewInMemoryDatastore()
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

	processed := make(chan any, 2)
	var counter atomic.Int32
	err := b.SubscribeForTasks(mq.QUEUE_PENDING, func(tk *tork.Task) error {
		assert.Equal(t, "test-queue", tk.Queue)
		assert.Equal(t, fmt.Sprintf("%d", counter.Load()), tk.Env["ITEM_INDEX"])
		processed <- 1
		counter.Add(1)
		return nil
	})
	assert.NoError(t, err)

	ds := inmemory.NewInMemoryDatastore()
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
				Env: map[string]string{
					"ITEM_INDEX": "{{item.index}}",
					"ITEM_VAL":   "{{item.value}}",
				},
			},
		},
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = s.scheduleEachTask(ctx, tk)
	assert.NoError(t, err)

	// wait for the tasks to get processed
	<-processed
	<-processed

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, tk.State)
	// task should only be processed once
	assert.Equal(t, int32(2), counter.Load())
}

func Test_scheduleEachTaskBadExpression(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := make(chan any, 1)
	err := b.SubscribeForTasks(mq.QUEUE_ERROR, func(tk *tork.Task) error {
		processed <- 1
		return nil
	})
	assert.NoError(t, err)

	ds := inmemory.NewInMemoryDatastore()
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
		State: tork.TaskStatePending,
		Each: &tork.EachTask{
			List: "{{ bad_expression }}",
			Task: &tork.Task{},
		},
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = s.scheduleEachTask(ctx, tk)
	assert.NoError(t, err)

	assert.Equal(t, tork.TaskStateFailed, tk.State)
}

func Test_scheduleEachTaskCustomVar(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := make(chan any, 2)
	err := b.SubscribeForTasks(mq.QUEUE_PENDING, func(tk *tork.Task) error {
		assert.Equal(t, "test-queue", tk.Queue)
		assert.Equal(t, fmt.Sprintf("%d", len(processed)), tk.Env["ITEM_INDEX"])
		processed <- 1
		return nil
	})
	assert.NoError(t, err)

	ds := inmemory.NewInMemoryDatastore()
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
			Var:  "myItem",
			List: "{{ sequence (1,3) }}",
			Task: &tork.Task{
				Queue: "test-queue",
				Env: map[string]string{
					"ITEM_INDEX": "{{myItem.index}}",
					"ITEM_VAL":   "{{myItem.value}}",
				},
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
	assert.Len(t, processed, 2)
}

func Test_scheduleSubJobTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := make(chan any)
	err := b.SubscribeForJobs(func(j *tork.Job) error {
		assert.NotEmpty(t, j.ParentID)
		close(processed)
		return nil
	})
	assert.NoError(t, err)

	ds := inmemory.NewInMemoryDatastore()
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

func Test_scheduleDetachedSubJobTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := make(chan any)
	err := b.SubscribeForJobs(func(j *tork.Job) error {
		assert.Empty(t, j.ParentID)
		assert.Equal(t, "http://example.com/callback", j.Webhooks[0].URL)
		close(processed)
		return nil
	})
	assert.NoError(t, err)

	completed := make(chan any)
	err = b.SubscribeForTasks(mq.QUEUE_COMPLETED, func(tk *tork.Task) error {
		close(completed)
		return nil
	})
	assert.NoError(t, err)

	ds := inmemory.NewInMemoryDatastore()
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
			Name:     "my sub job",
			Detached: true,
			Tasks: []*tork.Task{
				{
					Name: "some task",
				},
			},
			Webhooks: []*tork.Webhook{{
				URL: "http://example.com/callback",
			}},
		},
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = s.scheduleSubJob(ctx, tk)
	assert.NoError(t, err)

	// wait for the task to get processed
	<-processed

	// wait for the completion task
	<-completed

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, tk.State)
}
