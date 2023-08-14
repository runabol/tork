package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tork/mq"
	"github.com/tork/runtime"
	"github.com/tork/task"
	"github.com/tork/uuid"
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

	completions := 0
	b.SubscribeForTasks(mq.QUEUE_COMPLETED, func(tk task.Task) error {
		completions = completions + 1
		assert.NotEmpty(t, tk.Result)
		return nil
	})

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	err = w.handleTask(task.Task{
		ID:    uuid.NewUUID(),
		State: task.Scheduled,
		Image: "ubuntu:mantic",
		CMD:   []string{"ls"},
	})

	// give the task some time to "process"
	time.Sleep(time.Millisecond * 100)

	assert.NoError(t, err)
	assert.Equal(t, 1, completions)
}

func Test_handleTaskCancel(t *testing.T) {
	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)

	b := mq.NewInMemoryBroker()

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
	})

	tid := uuid.NewUUID()

	errs := 0
	b.SubscribeForTasks(mq.QUEUE_ERROR, func(tk task.Task) error {
		errs = errs + 1
		assert.NotEmpty(t, tk.Error)
		return nil
	})

	// cancel the task immediately upon start
	b.SubscribeForTasks(mq.QUEUE_STARTED, func(tk task.Task) error {
		err = w.handleTask(task.Task{
			ID:    tid,
			State: task.Cancelled,
		})
		assert.NoError(t, err)
		return nil
	})

	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	err = w.handleTask(task.Task{
		ID:    tid,
		State: task.Scheduled,
		Image: "ubuntu:mantic",
		CMD:   []string{"sleep", "10"},
	})
	assert.NoError(t, err)

	// some time to process the task
	time.Sleep(time.Millisecond * 100)

	assert.Equal(t, 1, errs)
}

func Test_handleTaskError(t *testing.T) {
	rt, err := runtime.NewDockerRuntime()
	assert.NoError(t, err)

	b := mq.NewInMemoryBroker()

	errs := 0
	b.SubscribeForTasks(mq.QUEUE_ERROR, func(tk task.Task) error {
		errs = errs + 1
		assert.NotEmpty(t, tk.Error)
		return nil
	})

	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: rt,
	})
	assert.NoError(t, err)
	assert.NotNil(t, w)
	err = w.Start()
	assert.NoError(t, err)

	err = w.handleTask(task.Task{
		ID:    uuid.NewUUID(),
		State: task.Scheduled,
		Image: "ubuntu:mantic",
		CMD:   []string{"no_such_thing"},
	})

	// give the task some time to "process"
	time.Sleep(time.Millisecond * 100)

	assert.NoError(t, err)
	assert.Equal(t, 1, errs)
}
