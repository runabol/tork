package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pkg/errors"
	"github.com/tork/broker"
	"github.com/tork/runtime"
	"github.com/tork/task"
	"github.com/tork/uuid"
)

type Worker struct {
	Name    string
	runtime runtime.Runtime
}

func NewWorker(b broker.Broker) (*Worker, error) {
	name := fmt.Sprintf("worker-%s", uuid.NewUUID())
	r, err := runtime.NewDockerRuntime()
	if err != nil {
		return nil, err
	}
	w := &Worker{
		Name:    name,
		runtime: r,
	}
	err = b.Receive(name, w.HandleTask)
	if err != nil {
		return nil, errors.Wrapf(err, "error subscribing for queue: %s", name)
	}
	return w, nil
}

func (w *Worker) CollectStats() {
	fmt.Println("I will collect stats")
}

func (w *Worker) HandleTask(ctx context.Context, t task.Task) error {
	switch t.State {
	case task.Pending,
		task.Scheduled:
		return w.startTask(ctx, t)
	case task.Cancelled:
		return w.stopTask(ctx, t)
	default:
		return errors.Errorf("invalid task state: %v", t.State)
	}
}

func (w *Worker) startTask(ctx context.Context, t task.Task) error {
	err := w.runtime.Start(ctx, t)
	if err != nil {
		log.Printf("Err running task %v: %v\n", t.ID, err)
		return err
	}
	return nil
}

func (w *Worker) stopTask(ctx context.Context, t task.Task) error {
	err := w.runtime.Stop(ctx, t)
	if err != nil {
		log.Printf("Error stopping task %s: %v", t.ID, err)
	}
	t.EndTime = time.Now().UTC()
	log.Printf("Stopped and removed task %s", t.ID)
	return err
}
