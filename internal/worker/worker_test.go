package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/runtime/docker"

	"github.com/stretchr/testify/assert"
)

func TestNewWorker(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)
	w, err := NewWorker(Config{})
	assert.Error(t, err)
	assert.Nil(t, w)

	w, err = NewWorker(Config{
		Broker:  broker.NewInMemoryBroker(),
		Runtime: rt,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
}

func TestStart(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  broker.NewInMemoryBroker(),
		Runtime: rt,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)
}

func Test_handleTaskRun(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)

	completions := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_COMPLETED, func(tk *tork.Task) error {
		close(completions)
		return nil
	})
	assert.NoError(t, err)

	starts := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_STARTED, func(tk *tork.Task) error {
		assert.Equal(t, int32(1), atomic.LoadInt32(&w.taskCount))
		close(starts)
		return nil
	})
	assert.NoError(t, err)

	err = w.Start()
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateRunning,
		Image: "ubuntu:mantic",
		CMD:   []string{"ls"},
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/somevolume",
			},
		},
	}

	err = w.handleTask(t1)

	<-starts
	<-completions

	assert.NoError(t, err)
	assert.Equal(t, "/somevolume", t1.Mounts[0].Target)
}

func Test_handleTaskRunOutput(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateRunning,
		Image: "alpine:3.18.3",
		Run:   "echo -n hello world > $TORK_OUTPUT",
	}

	err = w.handleTask(t1)

	// give the task some time to "process"
	time.Sleep(time.Millisecond * 100)

	assert.NoError(t, err)
	assert.Equal(t, "hello world", t1.Result)
}

func Test_handleTaskRunWithPrePost(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	completions := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_COMPLETED, func(tk *tork.Task) error {
		close(completions)
		return nil
	})
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateRunning,
		Image: "ubuntu:mantic",
		Run:   "cat /somevolume/pre > $TORK_OUTPUT",
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/somevolume",
			},
		},
		Pre: []*tork.Task{
			{
				Image: "ubuntu:mantic",
				Run:   "echo -n prestuff > /somevolume/pre",
			},
		},
		Post: []*tork.Task{
			{
				Image: "ubuntu:mantic",
				Run:   "echo -n poststuff > /somevolume/post",
			},
		},
	}

	err = w.handleTask(t1)

	<-completions

	assert.NoError(t, err)
	assert.Equal(t, "/somevolume", t1.Mounts[0].Target)
	assert.Equal(t, "prestuff", t1.Result)
}

func Test_handleTaskCancel(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
	})
	assert.NoError(t, err)

	tid := uuid.NewUUID()

	errs := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_ERROR, func(tk *tork.Task) error {
		assert.NotEmpty(t, tk.Error)
		close(errs)
		return nil
	})
	assert.NoError(t, err)

	// cancel the task immediately upon start
	err = b.SubscribeForTasks(broker.QUEUE_STARTED, func(tk *tork.Task) error {
		err := w.cancelTask(&tork.Task{
			ID:    tid,
			State: tork.TaskStateCancelled,
		})
		assert.NoError(t, err)
		return nil
	})
	assert.NoError(t, err)

	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	err = w.handleTask(&tork.Task{
		ID:    tid,
		State: tork.TaskStateRunning,
		Image: "ubuntu:mantic",
		CMD:   []string{"sleep", "10"},
	})
	assert.NoError(t, err)

	<-errs
}

func Test_handleTaskError(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	errs := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_ERROR, func(tk *tork.Task) error {
		assert.NotEmpty(t, tk.Error)
		close(errs)
		return nil
	})
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	err = w.handleTask(&tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateRunning,
		Image: "ubuntu:mantic",
		CMD:   []string{"no_such_thing"},
	})

	<-errs

	assert.NoError(t, err)
}

func Test_handleTaskOutput(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	completions := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_COMPLETED, func(tk *tork.Task) error {
		assert.NotEmpty(t, tk.Result)
		close(completions)
		return nil
	})
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
		Middleware: []task.MiddlewareFunc{
			func(next task.HandlerFunc) task.HandlerFunc {
				return func(ctx context.Context, et task.EventType, tk *tork.Task) error {
					assert.Equal(t, tork.TaskStateRunning, tk.State)
					return next(ctx, et, tk)
				}
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateScheduled,
		Image: "ubuntu:mantic",
		Run:   "echo -n 'hello world' >> $TORK_OUTPUT",
	}

	err = w.handleTask(tk)

	<-completions

	assert.NoError(t, err)
	assert.Equal(t, tork.TaskStateCompleted, tk.State)
}

func Test_middleware(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	completions := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_COMPLETED, func(tk *tork.Task) error {
		assert.Equal(t, "someval", tk.Result)
		assert.Equal(t, "", tk.Env["SOMEVAR"])
		close(completions)
		return nil
	})
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
		Queues:  map[string]int{"someq": 1},
		Middleware: []task.MiddlewareFunc{
			func(next task.HandlerFunc) task.HandlerFunc {
				return func(ctx context.Context, et task.EventType, t *tork.Task) error {
					if t.Env == nil {
						t.Env = make(map[string]string)
					}
					t.Env["SOMEVAR"] = "someval"
					return next(ctx, et, t)
				}
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateScheduled,
		Image: "alpine:3.18.3",
		Run:   "echo -n $SOMEVAR > $TORK_OUTPUT",
	}

	err = b.PublishTask(context.Background(), "someq", tk)
	assert.NoError(t, err)

	<-completions

	assert.NoError(t, err)
}

func Test_middlewareFailure(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	ch := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_ERROR, func(tk *tork.Task) error {
		close(ch)
		return nil
	})
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
		Queues:  map[string]int{"someq": 1},
		Middleware: []task.MiddlewareFunc{
			func(next task.HandlerFunc) task.HandlerFunc {
				return func(ctx context.Context, et task.EventType, t *tork.Task) error {
					return errors.Errorf("some error")
				}
			},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	err = b.PublishTask(context.Background(), "someq", &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateScheduled,
		Image: "alpine:3.18.3",
		Run:   "echo hello world > $TORK_OUTPUT",
	})
	assert.NoError(t, err)

	<-ch

	assert.NoError(t, err)
}

func Test_sendHeartbeat(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)
	b := broker.NewInMemoryBroker()
	heartbeats := make(chan any)
	err = b.SubscribeForHeartbeats(func(n *tork.Node) error {
		assert.Contains(t, n.Version, tork.Version)
		heartbeats <- 1
		return nil
	})
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	<-heartbeats
	assert.NoError(t, w.Stop())
}

func Test_handleTaskRunDefaultLimitExceeded(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
		Limits: Limits{
			DefaultTimeout: "1s",
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)

	errors := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_ERROR, func(tk *tork.Task) error {
		assert.Contains(t, tk.Error, "context deadline exceeded")
		close(errors)
		return nil
	})
	assert.NoError(t, err)

	starts := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_STARTED, func(tk *tork.Task) error {
		assert.Equal(t, int32(1), atomic.LoadInt32(&w.taskCount))
		close(starts)
		return nil
	})
	assert.NoError(t, err)

	err = w.Start()
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateRunning,
		Image: "ubuntu:mantic",
		CMD:   []string{"sleep", "5"},
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/somevolume",
			},
		},
	}

	err = w.handleTask(t1)

	<-starts
	<-errors

	assert.NoError(t, err)
	assert.Equal(t, "/somevolume", t1.Mounts[0].Target)
}

func Test_handleTaskRunDefaultLimitOK(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	b := broker.NewInMemoryBroker()

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
		Limits: Limits{
			DefaultTimeout: "5s",
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)

	completions := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_COMPLETED, func(tk *tork.Task) error {
		close(completions)
		return nil
	})
	assert.NoError(t, err)

	starts := make(chan any)
	err = b.SubscribeForTasks(broker.QUEUE_STARTED, func(tk *tork.Task) error {
		assert.Equal(t, int32(1), atomic.LoadInt32(&w.taskCount))
		close(starts)
		return nil
	})
	assert.NoError(t, err)

	err = w.Start()
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateRunning,
		Image: "ubuntu:mantic",
		CMD:   []string{"sleep", "1"},
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/somevolume",
			},
		},
	}

	err = w.handleTask(t1)

	<-starts
	<-completions

	assert.NoError(t, err)
	assert.Equal(t, "/somevolume", t1.Mounts[0].Target)
}
