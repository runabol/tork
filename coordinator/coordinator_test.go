package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tork/datastore"
	"github.com/tork/job"
	"github.com/tork/mq"
	"github.com/tork/node"
	"github.com/tork/task"
	"github.com/tork/uuid"
)

func TestNewCoordinatorFail(t *testing.T) {
	c, err := NewCoordinator(Config{})
	assert.Error(t, err)
	assert.Nil(t, c)
}

func TestNewCoordinatorOK(t *testing.T) {
	c, err := NewCoordinator(Config{
		Broker:    mq.NewInMemoryBroker(),
		DataStore: datastore.NewInMemoryDatastore(),
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)
}

func TestStartCoordinator(t *testing.T) {
	c, err := NewCoordinator(Config{
		Broker:    mq.NewInMemoryBroker(),
		DataStore: datastore.NewInMemoryDatastore(),
		Address:   ":4444",
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	err = c.Start()
	assert.NoError(t, err)
}

func Test_handlePendingTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := 0
	err := b.SubscribeForTasks("test-queue", func(ctx context.Context, t task.Task) error {
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	tk := task.Task{
		ID:    uuid.NewUUID(),
		Queue: "test-queue",
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = c.handlePendingTask(ctx, tk)
	assert.NoError(t, err)

	// wait for the task to get processed
	time.Sleep(time.Millisecond * 100)

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.Scheduled, tk.State)
	// task should only be processed once
	assert.Equal(t, 1, processed)
}

func Test_handleStartedTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	now := time.Now()

	t1 := task.Task{
		ID:        uuid.NewUUID(),
		State:     task.Scheduled,
		StartedAt: &now,
		Node:      uuid.NewUUID(),
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleStartedTask(ctx, t1)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.Running, t2.State)
	assert.Equal(t, t1.StartedAt, t2.StartedAt)
	assert.Equal(t, t1.Node, t2.Node)
}

func Test_handleCompletedLastTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	now := time.Now()

	j1 := job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 2,
		Tasks: []task.Task{
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

	t1 := task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
		StartedAt:   &now,
		CompletedAt: &now,
		Node:        uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    2,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleCompletedTask(ctx, t1)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.Completed, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	// verify that the job was marked
	// as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, job.Completed, j2.State)
}

func Test_handleCompletedFirstTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	now := time.Now()

	j1 := job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 1,
		Tasks: []task.Task{
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

	t1 := task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
		StartedAt:   &now,
		CompletedAt: &now,
		Node:        uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleCompletedTask(ctx, t1)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.Completed, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	// verify that the job was NOT
	// marked as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, job.Running, j2.State)
}

func Test_handleFailedTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	now := time.Now()

	j1 := job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 1,
		Tasks: []task.Task{
			{
				Name: "task-1",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	t1 := task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
		StartedAt:   &now,
		CompletedAt: &now,
		Node:        uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleFailedTask(ctx, t1)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.Failed, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	// verify that the job was
	// marked as FAILED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, job.Failed, j2.State)
}

func Test_handleFailedTaskRetry(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := 0
	err := b.SubscribeForTasks(mq.QUEUE_PENDING, func(ctx context.Context, t task.Task) error {
		processed = processed + 1
		return nil
	})
	assert.NoError(t, err)

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	now := time.Now()

	j1 := job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 1,
		Tasks: []task.Task{
			{
				Name: "task-1",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	t1 := task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
		StartedAt:   &now,
		CompletedAt: &now,
		Node:        uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		Retry: &task.Retry{
			Limit:         1,
			InitialDelay:  "1ms",
			ScalingFactor: 3,
		},
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleFailedTask(ctx, t1)
	assert.NoError(t, err)

	// wait for the retry delay
	time.Sleep(time.Millisecond * 100)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.Failed, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	// verify that the job was
	// NOT marked as FAILED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, job.Running, j2.State)
	assert.Equal(t, 1, processed)
}

func Test_handleHeartbeat(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	n1 := node.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().Add(-time.Minute),
		CPUPercent:      75,
	}

	err = c.handleHeartbeats(ctx, n1)
	assert.NoError(t, err)

	n2, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.True(t, n2.LastHeartbeatAt.After(n1.LastHeartbeatAt))
	assert.Equal(t, n1.CPUPercent, n2.CPUPercent)
}

func Test_handleJobs(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	j1 := job.Job{
		ID:    uuid.NewUUID(),
		State: job.Pending,
		Tasks: []task.Task{
			{
				Name: "task-1",
			},
		},
	}

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	err = c.handleJobs(ctx, j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, job.Running, j2.State)
}
