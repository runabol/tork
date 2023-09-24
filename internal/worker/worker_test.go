package worker

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/runtime"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/mount"
	"github.com/runabol/tork/mq"

	"github.com/stretchr/testify/assert"
)

func TestNewWorker(t *testing.T) {
	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)
	w, err := NewWorker(Config{})
	assert.Error(t, err)
	assert.Nil(t, w)

	w, err = NewWorker(Config{
		Broker:  mq.NewInMemoryBroker(),
		Runtime: rt,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
}

func TestStart(t *testing.T) {
	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  mq.NewInMemoryBroker(),
		Runtime: rt,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)
}

func Test_handleTaskRun(t *testing.T) {
	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)

	b := mq.NewInMemoryBroker()

	mounter, err := mount.NewVolumeMounter()
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
		Mounter: mounter,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)

	completions := make(chan any)
	err = b.SubscribeForTasks(mq.QUEUE_COMPLETED, func(tk *tork.Task) error {
		close(completions)
		return nil
	})
	assert.NoError(t, err)

	starts := make(chan any)
	err = b.SubscribeForTasks(mq.QUEUE_STARTED, func(tk *tork.Task) error {
		assert.Equal(t, int32(1), atomic.LoadInt32(&w.taskCount))
		close(starts)
		return nil
	})
	assert.NoError(t, err)

	err = w.Start()
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateScheduled,
		Image: "ubuntu:mantic",
		CMD:   []string{"ls"},
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeVolume,
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
	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)

	b := mq.NewInMemoryBroker()

	mounter, err := mount.NewVolumeMounter()
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
		Mounter: mounter,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateScheduled,
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
	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)

	b := mq.NewInMemoryBroker()

	completions := make(chan any)
	err = b.SubscribeForTasks(mq.QUEUE_COMPLETED, func(tk *tork.Task) error {
		close(completions)
		return nil
	})
	assert.NoError(t, err)

	mounter, err := mount.NewVolumeMounter()
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
		Mounter: mounter,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateScheduled,
		Image: "ubuntu:mantic",
		CMD:   []string{"ls"},
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeVolume,
				Target: "/somevolume",
			},
		},
		Pre: []*tork.Task{
			{
				Image: "ubuntu:mantic",
				CMD:   []string{"echo", "do work"},
			},
		},
		Post: []*tork.Task{
			{
				Image: "ubuntu:mantic",
				CMD:   []string{"echo", "do work"},
			},
		},
	}

	err = w.handleTask(t1)

	<-completions

	assert.NoError(t, err)
	assert.Equal(t, "/somevolume", t1.Mounts[0].Target)
}

func Test_handleTaskCancel(t *testing.T) {
	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)

	b := mq.NewInMemoryBroker()

	mounter, err := mount.NewVolumeMounter()
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
		Mounter: mounter,
	})
	assert.NoError(t, err)

	tid := uuid.NewUUID()

	errs := make(chan any)
	err = b.SubscribeForTasks(mq.QUEUE_ERROR, func(tk *tork.Task) error {
		assert.NotEmpty(t, tk.Error)
		close(errs)
		return nil
	})
	assert.NoError(t, err)

	// cancel the task immediately upon start
	err = b.SubscribeForTasks(mq.QUEUE_STARTED, func(tk *tork.Task) error {
		err := w.handleTask(&tork.Task{
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
		State: tork.TaskStateScheduled,
		Image: "ubuntu:mantic",
		CMD:   []string{"sleep", "10"},
	})
	assert.NoError(t, err)

	<-errs
}

func Test_handleTaskError(t *testing.T) {
	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)

	b := mq.NewInMemoryBroker()

	mounter, err := mount.NewVolumeMounter()
	assert.NoError(t, err)

	errs := make(chan any)
	err = b.SubscribeForTasks(mq.QUEUE_ERROR, func(tk *tork.Task) error {
		assert.NotEmpty(t, tk.Error)
		close(errs)
		return nil
	})
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
		Mounter: mounter,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	err = w.handleTask(&tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateScheduled,
		Image: "ubuntu:mantic",
		CMD:   []string{"no_such_thing"},
	})

	<-errs

	assert.NoError(t, err)
}

func Test_handleTaskOutput(t *testing.T) {
	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)

	b := mq.NewInMemoryBroker()

	completions := make(chan any)
	err = b.SubscribeForTasks(mq.QUEUE_COMPLETED, func(tk *tork.Task) error {
		assert.NotEmpty(t, tk.Result)
		close(completions)
		return nil
	})
	assert.NoError(t, err)

	mounter, err := mount.NewVolumeMounter()
	assert.NoError(t, err)

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
		Mounter: mounter,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	err = w.handleTask(&tork.Task{
		ID:    uuid.NewUUID(),
		State: tork.TaskStateScheduled,
		Image: "ubuntu:mantic",
		Run:   "echo -n 'hello world' >> $TORK_OUTPUT",
	})

	<-completions

	assert.NoError(t, err)
}

func Test_sendHeartbeat(t *testing.T) {
	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)

	b := mq.NewInMemoryBroker()

	heartbeats := 0
	err = b.SubscribeForHeartbeats(func(n *tork.Node) error {
		assert.Contains(t, n.Version, tork.Version)
		heartbeats = heartbeats + 1
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

	time.Sleep(time.Second)
	assert.NoError(t, w.Stop())

	assert.Equal(t, 1, heartbeats)
}
