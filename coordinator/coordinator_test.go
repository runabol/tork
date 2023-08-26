package coordinator

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/job"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/runtime"
	"github.com/runabol/tork/task"
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
	err := b.SubscribeForTasks("test-queue", func(t *task.Task) error {
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

	tk := &task.Task{
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
	assert.Equal(t, task.Scheduled, tk.State)
	// task should only be processed once
	assert.Equal(t, 1, processed)
}

func Test_handleConditionalTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	completed := 0
	err := b.SubscribeForTasks(mq.QUEUE_COMPLETED, func(t *task.Task) error {
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

	tk := &task.Task{
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
	assert.Equal(t, task.Scheduled, tk.State)
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

	j1 := &job.Job{
		ID:    uuid.NewUUID(),
		State: job.Running,
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	t1 := &task.Task{
		ID:        uuid.NewUUID(),
		State:     task.Scheduled,
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
	assert.Equal(t, task.Running, t2.State)
	assert.Equal(t, t1.StartedAt, t2.StartedAt)
	assert.Equal(t, t1.NodeID, t2.NodeID)
}

func Test_handleStartedTaskOfFailedJob(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	qname := uuid.NewUUID()

	cancellations := 0
	err := b.SubscribeForTasks(qname, func(t *task.Task) error {
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

	j1 := &job.Job{
		ID:    uuid.NewUUID(),
		State: job.Failed,
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	n1 := node.Node{
		ID:    uuid.NewUUID(),
		Queue: qname,
	}
	err = ds.CreateNode(ctx, n1)
	assert.NoError(t, err)

	t1 := &task.Task{
		ID:        uuid.NewUUID(),
		State:     task.Scheduled,
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
	assert.Equal(t, task.Scheduled, t2.State)
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
		Address:   fmt.Sprintf(":%d", rand.Int31n(50000)+10000),
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	err = c.Start()
	assert.NoError(t, err)

	now := time.Now().UTC()

	j1 := &job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 2,
		Tasks: []*task.Task{
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

	t1 := &task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
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
	assert.Equal(t, task.Completed, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	// verify that the job was marked
	// as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, job.Completed, j2.State)
}

func Test_handleCompletedLastSubJobTask(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
		Address:   fmt.Sprintf(":%d", rand.Int31n(50000)+10000),
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

	parentJob := &job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 1,
		Tasks: []*task.Task{
			{
				Name: "task-1",
			},
		},
	}
	err = ds.CreateJob(ctx, parentJob)
	assert.NoError(t, err)

	parentTask := &task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       parentJob.ID,
		Position:    2,
	}
	err = ds.CreateTask(ctx, parentTask)
	assert.NoError(t, err)

	j1 := &job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 1,
		Tasks: []*task.Task{
			{
				Name: "task-1",
				Run:  "echo hello",
			},
		},
		ParentID: parentTask.ID,
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	t1 := &task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
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
	assert.Equal(t, task.Completed, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	// verify that the job was marked
	// as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, job.Completed, j2.State)

	// verify that parent task is COMPLETED
	pt, err := ds.GetTaskByID(ctx, parentTask.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.Completed, pt.State)

	// verify that parent job is COMPLETED
	pj, err := ds.GetJobByID(ctx, parentJob.ID)
	assert.NoError(t, err)
	assert.Equal(t, job.Completed, pj.State)
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

	now := time.Now().UTC()

	j1 := &job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 1,
		Tasks: []*task.Task{
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

	t1 := &task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
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

	j1 := &job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 1,
		Tasks: []*task.Task{
			{
				Name: "task-1",
				Parallel: &task.Parallel{
					Tasks: []*task.Task{
						{
							Name: "parallel-task-1",
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

	pt := &task.Task{
		ID:    uuid.NewUUID(),
		JobID: j1.ID,
		Parallel: &task.Parallel{
			Tasks: []*task.Task{
				{
					Name: "parallel-task-1",
				},
			},
		},
		State: task.Running,
	}

	err = ds.CreateTask(ctx, pt)
	assert.NoError(t, err)

	t1 := &task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		ParentID:    pt.ID,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleCompletedTask(t1)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.Completed, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	pt1, err := ds.GetTaskByID(ctx, t1.ParentID)
	assert.NoError(t, err)
	assert.Equal(t, task.Completed, pt1.State)

	// verify that the job was NOT
	// marked as COMPLETED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, job.Running, j2.State)
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

	j1 := &job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 1,
		Tasks: []*task.Task{
			{
				Name: "task-1",
				Each: &task.Each{
					List: "some expression",
					Task: &task.Task{
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

	pt := &task.Task{
		ID:       uuid.NewUUID(),
		JobID:    j1.ID,
		Position: 1,
		Each: &task.Each{
			List: "some expression",
			Task: &task.Task{
				Name: "some task",
			},
		},
		State: task.Running,
	}

	err = ds.CreateTask(ctx, pt)
	assert.NoError(t, err)

	t1 := &task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		ParentID:    pt.ID,
	}

	err = ds.CreateTask(ctx, t1)
	assert.NoError(t, err)

	err = c.handleCompletedTask(t1)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(ctx, t1.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.Completed, t2.State)
	assert.Equal(t, t1.CompletedAt, t2.CompletedAt)

	pt1, err := ds.GetTaskByID(ctx, t1.ParentID)
	assert.NoError(t, err)
	assert.Equal(t, task.Completed, pt1.State)

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

	now := time.Now().UTC()

	node := node.Node{ID: uuid.NewUUID(), Queue: uuid.NewUUID()}
	err = ds.CreateNode(ctx, node)
	assert.NoError(t, err)

	j1 := &job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 1,
		Tasks: []*task.Task{
			{
				Name: "task-1",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	t1 := &task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      node.ID,
		JobID:       j1.ID,
		Position:    1,
	}

	t2 := &task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
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
	assert.Equal(t, task.Failed, t11.State)
	assert.Equal(t, t1.CompletedAt, t11.CompletedAt)

	// verify that the job was
	// marked as FAILED
	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, j1.ID, j2.ID)
	assert.Equal(t, job.Failed, j2.State)

	actives, err = ds.GetActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Len(t, actives, 0)
}

func Test_handleFailedTaskRetry(t *testing.T) {
	ctx := context.Background()
	b := mq.NewInMemoryBroker()

	processed := 0
	err := b.SubscribeForTasks(mq.QUEUE_PENDING, func(t *task.Task) error {
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

	j1 := &job.Job{
		ID:       uuid.NewUUID(),
		State:    job.Running,
		Position: 1,
		Tasks: []*task.Task{
			{
				Name: "task-1",
			},
		},
	}
	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	t1 := &task.Task{
		ID:          uuid.NewUUID(),
		State:       task.Running,
		StartedAt:   &now,
		CompletedAt: &now,
		NodeID:      uuid.NewUUID(),
		JobID:       j1.ID,
		Position:    1,
		Retry: &task.Retry{
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
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute),
		CPUPercent:      75,
	}

	err = c.handleHeartbeats(n1)
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

	j1 := &job.Job{
		ID:    uuid.NewUUID(),
		State: job.Pending,
		Tasks: []*task.Task{
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
	assert.Equal(t, job.Running, j2.State)
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

	j1 := &job.Job{
		ID:    uuid.NewUUID(),
		State: job.Pending,
		Tasks: []*task.Task{
			{
				Name: "task-1",
			},
		},
	}

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	err = ds.CreateTask(ctx, &task.Task{
		ID:        uuid.NewUUID(),
		JobID:     j1.ID,
		State:     task.Running,
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
		Address:   fmt.Sprintf(":%d", rand.Int31n(50000)+10000),
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

	pj := &job.Job{
		ID:        uuid.NewUUID(),
		State:     job.Running,
		CreatedAt: now,
		Tasks: []*task.Task{
			{
				Name: "task-1",
			},
		},
	}
	err = ds.CreateJob(ctx, pj)
	assert.NoError(t, err)

	pt := &task.Task{
		ID:        uuid.NewUUID(),
		JobID:     pj.ID,
		State:     task.Running,
		CreatedAt: &now,
	}

	err = ds.CreateTask(ctx, pt)
	assert.NoError(t, err)

	j1 := &job.Job{
		ID:        uuid.NewUUID(),
		State:     job.Pending,
		CreatedAt: now,
		ParentID:  pt.ID,
		Tasks: []*task.Task{
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
	assert.Equal(t, job.Running, j2.State)

	j1.State = job.Cancelled
	// cancel the job
	err = c.handleJob(j1)
	assert.NoError(t, err)

	// wait for the cancellation
	// to propogate to the parent job
	time.Sleep(time.Millisecond * 100)

	j3, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, job.Cancelled, j3.State)

	pj1, err := ds.GetJobByID(ctx, pj.ID)
	assert.NoError(t, err)
	assert.Equal(t, job.Cancelled, pj1.State)
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

	j1 := &job.Job{
		ID:        uuid.NewUUID(),
		State:     job.Pending,
		CreatedAt: now,
		Position:  1,
		Tasks: []*task.Task{
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
	assert.Equal(t, job.Running, j2.State)

	// cancel the job
	j1.State = job.Cancelled
	err = c.handleJob(j1)
	assert.NoError(t, err)

	j2, err = ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, job.Cancelled, j2.State)

	// restart the job
	j1.State = job.Restart
	err = c.handleJob(j1)
	assert.NoError(t, err)

	j2, err = ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Equal(t, job.Running, j2.State)

	// try to restart again
	j1.State = job.Restart
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

	j1 := &job.Job{
		ID:    uuid.NewUUID(),
		State: job.Pending,
		Tasks: []*task.Task{
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
	assert.Equal(t, job.Failed, j2.State)
}

func TestRunHelloWorldJob(t *testing.T) {
	j1 := doRunJob(t, "../examples/hello.yaml")
	assert.Equal(t, job.Completed, j1.State)
	assert.Equal(t, 1, len(j1.Execution))
}

func TestRunParallelJob(t *testing.T) {
	j1 := doRunJob(t, "../examples/parallel.yaml")
	assert.Equal(t, job.Completed, j1.State)
	assert.Equal(t, 9, len(j1.Execution))
}

func TestRunEachJob(t *testing.T) {
	j1 := doRunJob(t, "../examples/each.yaml")
	assert.Equal(t, job.Completed, j1.State)
	assert.Equal(t, 7, len(j1.Execution))
}

func TestRunSubjobJob(t *testing.T) {
	j1 := doRunJob(t, "../examples/subjob.yaml")
	assert.Equal(t, job.Completed, j1.State)
	assert.Equal(t, 6, len(j1.Execution))
}

func doRunJob(t *testing.T, filename string) *job.Job {
	ctx := context.Background()

	b := mq.NewInMemoryBroker()
	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    b,
		DataStore: ds,
		Address:   fmt.Sprintf(":%d", rand.Int31n(50000)+10000),
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

	j1 := &job.Job{}
	err = yaml.Unmarshal(contents, j1)
	assert.NoError(t, err)

	j1.ID = uuid.NewUUID()
	j1.State = job.Pending

	err = ds.CreateJob(ctx, j1)
	assert.NoError(t, err)

	err = c.handleJob(j1)
	assert.NoError(t, err)

	j2, err := ds.GetJobByID(ctx, j1.ID)
	assert.NoError(t, err)

	iter := 0
	for j2.State == job.Running && iter < 10 {
		time.Sleep(time.Second)
		j2, err = ds.GetJobByID(ctx, j2.ID)
		assert.NoError(t, err)
		iter++
	}

	return j2
}
