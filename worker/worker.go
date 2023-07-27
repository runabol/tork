package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/tork/broker"
	"github.com/tork/runtime"
	"github.com/tork/task"
)

type Worker struct {
	name    string
	runtime runtime.Runtime
}

func NewWorker(b broker.Broker) (*Worker, error) {
	name := fmt.Sprintf("worker-%s", uuid.NewString())
	r, err := runtime.NewDockerRuntime()
	if err != nil {
		return nil, err
	}
	w := &Worker{
		name:    name,
		runtime: r,
	}
	err = b.Receive(name, w.HandleTask)
	if err != nil {
		return nil, errors.Wrapf(err, "error subscribing for queue: %s", name)
	}
	return w, nil
}

func (w *Worker) Name() string {
	return w.name
}

func (w *Worker) CollectStats() {
	fmt.Println("I will collect stats")
}

func (w *Worker) HandleTask(ctx context.Context, t task.Task) error {
	return w.StartTask(ctx, t)
}

func (w *Worker) StartTask(ctx context.Context, t task.Task) error {
	err := w.runtime.Start(ctx, t)
	if err != nil {
		log.Printf("Err running task %v: %v\n", t.ID, err)
		return err
	}
	return nil
}

func (w *Worker) StopTask(ctx context.Context, t task.Task) error {
	err := w.runtime.Stop(ctx, t)
	if err != nil {
		log.Printf("Error stopping task %s: %v", t.ID, err)
	}
	t.EndTime = time.Now().UTC()
	log.Printf("Stopped and removed task %s", t.ID)
	return err
}
