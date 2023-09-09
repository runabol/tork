package coordinator

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/mq"

	"github.com/runabol/tork/runtime"

	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/internal/worker"
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

func TestTaskMiddlewareWithResult(t *testing.T) {
	c, err := NewCoordinator(Config{
		Broker:    mq.NewInMemoryBroker(),
		DataStore: datastore.NewInMemoryDatastore(),
		TaskMiddlewares: []task.MiddlewareFunc{
			func(next task.HandlerFunc) task.HandlerFunc {
				return func(ctx context.Context, t *tork.Task) error {
					t.Result = "some result"
					return nil
				}
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	tk := &tork.Task{}
	assert.NoError(t, c.onPending(context.Background(), tk))
	assert.Equal(t, "some result", tk.Result)
}

func TestTaskMiddlewareWithError(t *testing.T) {
	Err := errors.New("some error")
	c, err := NewCoordinator(Config{
		Broker:    mq.NewInMemoryBroker(),
		DataStore: datastore.NewInMemoryDatastore(),
		TaskMiddlewares: []task.MiddlewareFunc{
			func(next task.HandlerFunc) task.HandlerFunc {
				return func(ctx context.Context, t *tork.Task) error {
					return Err
				}
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	tk := &tork.Task{}
	assert.ErrorIs(t, c.onPending(context.Background(), tk), Err)
}

func TestTaskMiddlewareNoOp(t *testing.T) {
	ds := datastore.NewInMemoryDatastore()
	c, err := NewCoordinator(Config{
		Broker:    mq.NewInMemoryBroker(),
		DataStore: ds,
		TaskMiddlewares: []task.MiddlewareFunc{
			func(next task.HandlerFunc) task.HandlerFunc {
				return func(ctx context.Context, t *tork.Task) error {
					return next(ctx, t)
				}
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		Name:  "my task",
		State: tork.TaskStatePending,
	}

	err = ds.CreateTask(context.Background(), tk)
	assert.NoError(t, err)

	err = c.onPending(context.Background(), tk)
	assert.NoError(t, err)

	t2, err := ds.GetTaskByID(context.Background(), tk.ID)
	assert.NoError(t, err)

	assert.Equal(t, tork.TaskStateScheduled, t2.State)
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

func TestRunHelloWorldJob(t *testing.T) {
	j1 := doRunJob(t, "../../examples/hello.yaml")
	assert.Equal(t, tork.JobStateCompleted, j1.State)
	assert.Equal(t, 1, len(j1.Execution))
}

func TestRunParallelJob(t *testing.T) {
	j1 := doRunJob(t, "../../examples/parallel.yaml")
	assert.Equal(t, tork.JobStateCompleted, j1.State)
	assert.Equal(t, 9, len(j1.Execution))
}

func TestRunEachJob(t *testing.T) {
	j1 := doRunJob(t, "../../examples/each.yaml")
	assert.Equal(t, tork.JobStateCompleted, j1.State)
	assert.Equal(t, 7, len(j1.Execution))
}

func TestRunSubjobJob(t *testing.T) {
	j1 := doRunJob(t, "../../examples/subjob.yaml")
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

	err = c.onJob(ctx, j1)
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
