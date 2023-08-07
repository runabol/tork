package worker

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pkg/errors"
	"github.com/tork/mq"
	"github.com/tork/runtime"
	"github.com/tork/task"
	"github.com/tork/uuid"
)

type Worker struct {
	Name    string
	runtime runtime.Runtime
	broker  mq.Broker
}

type Config struct {
	Broker  mq.Broker
	Runtime runtime.Runtime
}

func NewWorker(cfg Config) *Worker {
	name := fmt.Sprintf("worker-%s", uuid.NewUUID())
	w := &Worker{
		Name:    name,
		broker:  cfg.Broker,
		runtime: cfg.Runtime,
	}
	return w
}

func (w *Worker) handleTask(ctx context.Context, t task.Task) error {
	if t.State != task.Pending {
		return errors.Errorf("can't start a task in %s state", t.State)
	}
	result, err := w.runtime.Run(ctx, t)
	if err != nil {
		log.Printf("error running task %v: %v\n", t.ID, err)
		return err
	}
	now := time.Now()
	t.Result = result
	t.CompletedAt = &now
	t.State = task.Completed
	w.broker.Enqueue(ctx, mq.QUEUE_COMPLETED, t)
	return nil
}

func (w *Worker) collectStats() {
	for {
		s, err := getStats()
		if err != nil {
			log.Error().Msgf("error collecting stats for %s", w.Name)
		} else {
			log.Debug().Float64("cpu-percent", s.CPUPercent).Msgf("collecting stats for %s", w.Name)
		}
		time.Sleep(1 * time.Minute)
	}
}

func (w *Worker) Start() error {
	log.Info().Msgf("starting %s", w.Name)
	err := w.broker.Subscribe(mq.QUEUE_DEFAULT, w.handleTask)
	if err != nil {
		return errors.Wrapf(err, "error subscribing for queue: %s", w.Name)
	}
	go w.collectStats()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Debug().Msgf("shutting down %s", w.Name)
	return nil
}
