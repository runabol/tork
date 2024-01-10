package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore/inmemory"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/mq"
	"github.com/stretchr/testify/assert"
)

func Test_handleJobs(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := inmemory.NewInMemoryDatastore()
	handler := NewJobHandler(ds, b)
	assert.NotNil(t, handler)

	j1 := &tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStatePending,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
	}

	err := ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	err = handler(ctx, job.StateChange, j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateScheduled, j2.State)
}

func Test_handleCancelJob(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := inmemory.NewInMemoryDatastore()
	handler := NewJobHandler(ds, b)
	assert.NotNil(t, handler)

	now := time.Now().UTC()

	pj := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		CreatedAt: now,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
	}
	err := ds.CreateJob(ctx, pj)
	assert.NoError(t, err)

	err = b.SubscribeForJobs(func(j *tork.Job) error {
		// the cancellation for the parent job
		assert.Equal(t, pj.ID, j.ID)
		return nil
	})
	assert.NoError(t, err)

	pt := &tork.Task{
		ID:        uuid.NewUUID(),
		JobID:     pj.ID,
		State:     tork.TaskStateRunning,
		CreatedAt: &now,
	}

	err = ds.CreateTask(ctx, pt)
	assert.NoError(t, err)

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStatePending,
		CreatedAt: now,
		ParentID:  pt.ID,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
	}

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	// start the job
	err = handler(ctx, job.StateChange, j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateScheduled, j2.State)

	j1.State = tork.JobStateCancelled
	// cancel the job
	err = handler(ctx, job.StateChange, j1)
	assert.NoError(t, err)

	// wait for the cancellation
	// to propagate to the parent job
	time.Sleep(time.Millisecond * 100)
}

func Test_handleRestartJob(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := inmemory.NewInMemoryDatastore()
	handler := NewJobHandler(ds, b)
	assert.NotNil(t, handler)

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStatePending,
		CreatedAt: now,
		Position:  1,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
	}

	err := ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	// start the job
	err = handler(ctx, job.StateChange, j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateScheduled, j2.State)

	// cancel the job
	j1.State = tork.JobStateCancelled
	err = handler(ctx, job.StateChange, j1)
	assert.NoError(t, err)

	j2, err = ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateCancelled, j2.State)

	// restart the job
	j1.State = tork.JobStateRestart
	err = handler(ctx, job.StateChange, j1)
	assert.NoError(t, err)

	j2, err = ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateRunning, j2.State)

	// try to restart again
	j1.State = tork.JobStateRestart
	err = handler(ctx, job.StateChange, j1)
	assert.Error(t, err)
}

func Test_handleJobWithTaskEvalFailure(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := inmemory.NewInMemoryDatastore()
	handler := NewJobHandler(ds, b)
	assert.NotNil(t, handler)

	j1 := &tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStatePending,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
				Env: map[string]string{
					"SOMEVAR": "{{ bad_expression }}",
				},
			},
		},
	}

	err := ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	err = handler(ctx, job.StateChange, j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateFailed, j2.State)
}

func Test_handleCompleteJob(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	events := 0
	err := b.SubscribeForEvents(ctx, mq.TOPIC_JOB_COMPLETED, func(event any) {
		j, ok := event.(*tork.Job)
		assert.True(t, ok)
		assert.Equal(t, tork.JobStateCompleted, j.State)
		events = events + 1
	})
	assert.NoError(t, err)

	ds := inmemory.NewInMemoryDatastore()
	handler := NewJobHandler(ds, b)
	assert.NotNil(t, handler)

	j1 := &tork.Job{
		ID:     uuid.NewUUID(),
		State:  tork.JobStateRunning,
		Output: "some output",
		Tasks: []*tork.Task{
			{
				Name: "task-1",
				Env: map[string]string{
					"SOMEVAR": "{{ bad_expression }}",
				},
			},
		},
	}

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	j1.State = tork.JobStateCompleted

	err = handler(ctx, job.StateChange, j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateCompleted, j2.State)
	assert.Equal(t, "some output", j1.Result)

	assert.NoError(t, err)
}

func Test_handleCompleteJobWithBadOutput(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	events := 0
	err := b.SubscribeForEvents(ctx, mq.TOPIC_JOB_COMPLETED, func(event any) {
		j, ok := event.(*tork.Job)
		assert.True(t, ok)
		assert.Equal(t, tork.JobStateCompleted, j.State)
		events = events + 1
	})
	assert.NoError(t, err)

	ds := inmemory.NewInMemoryDatastore()
	handler := NewJobHandler(ds, b)
	assert.NotNil(t, handler)

	j1 := &tork.Job{
		ID:     uuid.NewUUID(),
		State:  tork.JobStateRunning,
		Output: "{{ bad_function() }}",
		Tasks: []*tork.Task{
			{
				Name: "task-1",
				Env: map[string]string{
					"SOMEVAR": "{{ bad_expression }}",
				},
			},
		},
	}

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	j1.State = tork.JobStateCompleted

	err = handler(ctx, job.StateChange, j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateFailed, j2.State)
	assert.Contains(t, j1.Error, "unknown name bad_function")

	assert.NoError(t, err)
}
