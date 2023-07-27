package worker

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
	cfg     Config
	done    chan os.Signal
}

type Config struct {
	Broker broker.Broker
}

func NewWorker(cfg Config) *Worker {
	name := fmt.Sprintf("worker-%s", uuid.NewUUID())
	w := &Worker{
		Name: name,
		cfg:  cfg,
		done: make(chan os.Signal),
	}
	signal.Notify(w.done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	return w
}

func (w *Worker) CollectStats() {
	fmt.Println("I will collect stats")
}

func (w *Worker) handleMessage(ctx context.Context, msg any) error {
	switch v := msg.(type) {
	case task.Task:
		return w.startTask(ctx, v)
	case task.CancelRequest:
		return w.stopTask(ctx, v.Task)
	default:
		return errors.Errorf("unknown message type: %T", msg)
	}
}

func (w *Worker) startTask(ctx context.Context, t task.Task) error {
	if t.State != task.Scheduled {
		return errors.Errorf("can't start a task in %s state", t.State)
	}
	err := w.runtime.Start(ctx, t)
	if err != nil {
		log.Printf("error running task %v: %v\n", t.ID, err)
		return err
	}
	return nil
}

func (w *Worker) stopTask(ctx context.Context, t task.Task) error {
	err := w.runtime.Stop(ctx, t)
	if err != nil {
		log.Printf("error stopping task %s: %v", t.ID, err)
	}
	log.Printf("stopped and removed task %s", t.ID)
	return err
}

func (w *Worker) Start() error {
	log.Info().Msgf("starting %s", w.Name)
	r, err := runtime.NewDockerRuntime()
	if err != nil {
		return err
	}
	w.runtime = r
	err = w.cfg.Broker.Receive(w.Name, w.handleMessage)
	if err != nil {
		return errors.Wrapf(err, "error subscribing for queue: %s", w.Name)
	}
	<-w.done
	return nil
}
