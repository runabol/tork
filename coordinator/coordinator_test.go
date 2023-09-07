package coordinator

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/mq"

	"github.com/runabol/tork/runtime"

	"github.com/runabol/tork/uuid"
	"github.com/runabol/tork/worker"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
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
	err := b.SubscribeForTasks("test-queue", func(t *tork.Task) error {
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

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		Queue: "test-queue",
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = c.handlePendingTask(tk)
	assert.NoError(t, err)

	// wait for the task to get processed
	time.Sleep(time.Millisecond * 100)

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, tk.State)
	// task should only be processed once
	assert.Equal(t, 1, processed)
}

func Test_handleConditionalTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	completed := 0
	err := b.SubscribeForTasks(mq.QUEUE_COMPLETED, func(t *tork.Task) error {
		completed = completed + 1
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

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		Queue: "test-queue",
		If:    "false",
	}

	err = ds.CreateTask(ctx, tk)
	assert.NoError(t, err)

	err = c.handlePendingTask(tk)
	assert.NoError(t, err)

	// wait for the task to get processed
	time.Sleep(time.Millisecond * 100)

	tk, err = ds.GetTaskByID(ctx, tk.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, tk.State)
	// task should only be processed once
	assert.Equal(t, 1, completed)
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

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStateRunning,
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateScheduled,
		StartedAt: &now,
		NodeID:    uuid.NewUUID(),
		JobID:     j1.ID,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleStartedTask(t1)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, t2.State)
	assert.Equal(t, t1.StartedAt, t2.StartedAt)
	assert.Equal(t, t1.NodeID, t2.NodeID)
}

func Test_handleStartedTaskOfFailedJob(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	qname := uuid.NewUUID()

	cancellations := 0
	err := b.SubscribeForTasks(qname, func(t *tork.Task) error {
		cancellations = cancellations + 1
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

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStateFailed,
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	n1 := tork.Node{
		ID:    uuid.NewUUID(),
		Queue: qname,
	}
	err = ds.CreateNode(ctx, n1)
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateScheduled,
		StartedAt: &now,
		JobID:     j1.ID,
		NodeID:    n1.ID,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleStartedTask(t1)
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 100)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateScheduled, t2.State)
	assert.Equal(t, 1, cancellations)
	assert.Equal(t, t1.StartedAt, t2.StartedAt)
	assert.Equal(t, t1.NodeID, t2.NodeID)
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

	err = c.Start()
	assert.NoError(t, err)

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:       uuid.NewUUID(),
		State:    tork.JobStateRunning,
		Position: 2,
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
		Position:    2,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleCompletedTask(t1)
	assert.NoError(t, err)

	// wait for the job itself to complete
	time.Sleep(time.Millisecond * 100)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	// verify that the job was marked
	// as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateCompleted, j2.State)
}

func Test_handleCompletedLastSubJobTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	err = c.Start()
	assert.NoError(t, err)
	defer func() {
		err = c.Stop()
		assert.NoError(t, err)
	}()

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
	}
	err = ds.CreateTask(ctx, parentTask)
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
		ParentID: parentTask.ID,
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
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleCompletedTask(t1)
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 100)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	// verify that the job was marked
	// as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateCompleted, j2.State)

	// verify that parent task is COMPLETED
	pt, err := ds.GetTaskByID(ctx, parentTask.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, pt.State)

	// verify that parent job is COMPLETED
	pj, err := ds.GetJobByID(ctx, parentJob.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateCompleted, pj.State)
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

	err = c.Start()
	assert.NoError(t, err)

	defer func() {
		err := c.Stop()
		assert.NoError(t, err)
	}()

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:       uuid.NewUUID(),
		State:    tork.JobStateRunning,
		Position: 1,
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
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleCompletedTask(t1)
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 100)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	// verify that the job was NOT
	// marked as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateRunning, j2.State)
}

func Test_handleCompletedScheduledTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	err = c.Start()
	assert.NoError(t, err)

	defer func() {
		err := c.Stop()
		assert.NoError(t, err)
	}()

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
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleCompletedTask(t1)
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 100)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateCompleted, j2.State)
}

func Test_handleCompletedParallelTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:       uuid.NewUUID(),
		State:    tork.JobStateRunning,
		Position: 1,
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
		State: tork.TaskStateRunning,
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
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = ds.CreateTask(ctx, t5)
	assert.NoError(t, err)

	err = c.handleCompletedTask(t1)
	assert.NoError(t, err)

	err = c.handleCompletedTask(t5)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	pt1, err := ds.GetTaskByID(ctx, t1.ParentID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, pt1.State)

	// verify that the job was NOT
	// marked as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateRunning, j2.State)
}

func Test_handleCompletedEachTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	now := time.Now().UTC()

	j1 := &tork.Job{
		ID:       uuid.NewUUID(),
		State:    tork.JobStateRunning,
		Position: 1,
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
		State: tork.TaskStateRunning,
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
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = ds.CreateTask(ctx, t5)
	assert.NoError(t, err)

	err = c.handleCompletedTask(t1)
	assert.NoError(t, err)

	err = c.handleCompletedTask(t5)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	pt1, err := ds.GetTaskByID(ctx, t1.ParentID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, pt1.State)

	// verify that the job was NOT
	// marked as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateRunning, j2.State)
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

	now := time.Now().UTC()

	node := tork.Node{ID: uuid.NewUUID(), Queue: uuid.NewUUID()}
	err = ds.CreateNode(ctx, node)
	assert.NoError(t, err)

	j1 := &tork.Job{
		ID:       uuid.NewUUID(),
		State:    tork.JobStateRunning,
		Position: 1,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
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
		NodeID:      node.ID,
		JobID:       j1.ID,
		Position:    1,
	}

	t2 := &tork.Task{
		ID:          uuid.NewUUID(),
		State:       tork.TaskStateRunning,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      node.ID,
		JobID:       j1.ID,
		Position:    1,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = ds.CreateTask(ctx, t2)
	assert.NoError(t, err)

	actives, err := ds.GetActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Len(t, actives, 2)

	err = c.handleFailedTask(t1)
	assert.NoError(t, err)

	t11, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateFailed, t11.State)
	assert.Equal(t, t1.CompletedAt, t11.CompletedAt)

	// verify that the job was
	// marked as FAILED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateFailed, j2.State)

	actives, err = ds.GetActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Len(t, actives, 0)
}

func Test_handleFailedTaskRetry(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := 0
	err := b.SubscribeForTasks(mq.QUEUE_PENDING, func(t *tork.Task) error {
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
		Retry: &tork.TaskRetry{
			Limit: 1,
		},
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleFailedTask(t1)
	assert.NoError(t, err)

	// wait for the retry delay
	time.Sleep(time.Millisecond * 100)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateFailed, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	// verify that the job was
	// NOT marked as FAILED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, tork.JobStateRunning, j2.State)
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

	n1 := tork.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 5),
		CPUPercent:      75,
		Hostname:        "host-1",
		Status:          tork.NodeStatusUP,
	}

	err = c.handleHeartbeats(n1)
	assert.NoError(t, err)

	n11, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, n1.LastHeartbeatAt, n11.LastHeartbeatAt)
	assert.Equal(t, n1.CPUPercent, n11.CPUPercent)
	assert.Equal(t, n1.Status, n11.Status)
	assert.Equal(t, n1.TaskCount, n11.TaskCount)

	n2 := tork.Node{
		ID:              n1.ID,
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 2),
		CPUPercent:      75,
		Status:          tork.NodeStatusDown,
		TaskCount:       3,
	}

	err = c.handleHeartbeats(n2)
	assert.NoError(t, err)

	n22, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, n2.LastHeartbeatAt, n22.LastHeartbeatAt)
	assert.Equal(t, n2.CPUPercent, n22.CPUPercent)
	assert.Equal(t, n2.Status, n22.Status)
	assert.Equal(t, n2.TaskCount, n22.TaskCount)

	n3 := tork.Node{
		ID:              n1.ID,
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 7),
		CPUPercent:      75,
	}

	err = c.handleHeartbeats(n3)
	assert.NoError(t, err)

	n33, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, n2.LastHeartbeatAt, n33.LastHeartbeatAt) // should keep the latest
	assert.Equal(t, n3.CPUPercent, n33.CPUPercent)

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

	j1 := &tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStatePending,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
	}

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	err = c.handleJob(j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateRunning, j2.State)
}

func Test_cancelActiveTasks(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	j1 := &tork.Job{
		ID:    uuid.NewUUID(),
		State: tork.JobStatePending,
		Tasks: []*tork.Task{
			{
				Name: "task-1",
			},
		},
	}

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	err = ds.CreateTask(ctx, &tork.Task{
		ID:        uuid.NewUUID(),
		JobID:     j1.ID,
		State:     tork.TaskStateRunning,
		Position:  1,
		CreatedAt: &now,
	})
	assert.NoError(t, err)

	actives, err := ds.GetActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Len(t, actives, 1)

	err = c.cancelActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)

	actives, err = ds.GetActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Len(t, actives, 0)
}

func Test_handleCancelJob(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	err = c.Start()
	assert.NoError(t, err)

	defer func() {
		err = c.Stop()
		assert.NoError(t, err)
	}()

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
	err = ds.CreateJob(ctx, pj)
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
	err = c.handleJob(j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateRunning, j2.State)

	j1.State = tork.JobStateCancelled
	// cancel the job
	err = c.handleJob(j1)
	assert.NoError(t, err)

	// wait for the cancellation
	// to propagate to the parent job
	time.Sleep(time.Millisecond * 100)

	j3, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateCancelled, j3.State)

	pj1, err := ds.GetJobByID(ctx, pj.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateCancelled, pj1.State)
}

func Test_handleRestartJob(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

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

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	// start the job
	err = c.handleJob(j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateRunning, j2.State)

	// cancel the job
	j1.State = tork.JobStateCancelled
	err = c.handleJob(j1)
	assert.NoError(t, err)

	j2, err = ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateCancelled, j2.State)

	// restart the job
	j1.State = tork.JobStateRestart
	err = c.handleJob(j1)
	assert.NoError(t, err)

	j2, err = ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateRunning, j2.State)

	// try to restart again
	j1.State = tork.JobStateRestart
	err = c.handleJob(j1)
	assert.Error(t, err)
}

func Test_handleJobWithTaskEvalFailure(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

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

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	err = c.handleJob(j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStateFailed, j2.State)
}

func Test_completeTopLevelTaskWithTxRollback(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	b := mq.NewInMemoryBroker()

	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)
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

	err = c.completeTopLevelTask(ctx, t1)
	assert.Error(t, err)

	t11, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, t11.State)
}

func Test_completeTopLevelTaskWithTx(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)

	b := mq.NewInMemoryBroker()

	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)

	err = b.SubscribeForTasks(mq.QUEUE_PENDING, func(t1 *tork.Task) error {
		err := c.handlePendingTask(t1)
		assert.NoError(t, err)
		return err
	})
	assert.NoError(t, err)

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

	err = c.completeTopLevelTask(ctx, t1)
	assert.NoError(t, err)

	t11, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, t11.State)
}

func Test_completeParallelTaskWithTx(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	c, err := NewCoordinator(Config{
		Broker:    mq.NewInMemoryBroker(),
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

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

	err = c.completeParallelTask(ctx, t1)
	assert.Error(t, err)

	t11, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, t11.State)
}

func Test_completeEachTaskWithTx(t *testing.T) {
	ctx := context.Background()
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	ds, err := datastore.NewPostgresDataStore(dsn)
	assert.NoError(t, err)
	c, err := NewCoordinator(Config{
		Broker:    mq.NewInMemoryBroker(),
		DataStore: ds,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

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

	err = c.completeEachTask(ctx, t1)
	assert.Error(t, err)

	t11, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateRunning, t11.State)
}

func TestRunHelloWorldJob(t *testing.T) {
	j1 := doRunJob(t, "../examples/hello.yaml")
	assert.Equal(t, tork.JobStateCompleted, j1.State)
	assert.Equal(t, 1, len(j1.Execution))
}

func TestRunParallelJob(t *testing.T) {
	j1 := doRunJob(t, "../examples/parallel.yaml")
	assert.Equal(t, tork.JobStateCompleted, j1.State)
	assert.Equal(t, 9, len(j1.Execution))
}

func TestRunEachJob(t *testing.T) {
	j1 := doRunJob(t, "../examples/each.yaml")
	assert.Equal(t, tork.JobStateCompleted, j1.State)
	assert.Equal(t, 7, len(j1.Execution))
}

func TestRunSubjobJob(t *testing.T) {
	j1 := doRunJob(t, "../examples/subjob.yaml")
	assert.Equal(t, tork.JobStateCompleted, j1.State)
	assert.Equal(t, 6, len(j1.Execution))
}

func doRunJob(t *testing.T, filename string) *tork.Job {
	ctx := context.Background()

	b := mq.NewInMemoryBroker()
	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
	})
	assert.NoError(t, err)

	err = c.Start()
	assert.NoError(t, err)

	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)

	w, err := worker.NewWorker(worker.Config{
		Broker:  b,
		Runtime: rt,
		Queues: map[string]int{
			"default": 2,
		},
	})
	assert.NoError(t, err)

	err = w.Start()
	assert.NoError(t, err)

	contents, err := os.ReadFile(filename)
	assert.NoError(t, err)

	j1 := &tork.Job{}
	err = yaml.Unmarshal(contents, j1)
	assert.NoError(t, err)

	j1.ID = uuid.NewUUID()
	j1.State = tork.JobStatePending

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	err = c.handleJob(j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)

	iter := 0
	for j2.State == tork.JobStateRunning && iter < 10 {
		time.Sleep(time.Second)
		j2, err = ds.GetJobByID(ctx, j2.ID)
		assert.NoError(t, err)
		iter++
	}

	return j2
}
