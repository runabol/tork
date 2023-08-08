package worker

import (
	"context"
	"fmt"
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
	stop    bool
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

func (w *Worker) handleTask(ctx context.Context, t *task.Task) error {
	if t.State != task.Scheduled {
		return errors.Errorf("can't start a task in %s state", t.State)
	}
	started := time.Now()
	t.StartedAt = &started
	t.State = task.Running
	if err := w.broker.Publish(ctx, mq.QUEUE_STARTED, t); err != nil {
		return err
	}
	// prepare volumes
	vols := []string{}
	for _, v := range t.Volumes {
		volName := uuid.NewUUID()
		if err := w.runtime.CreateVolume(ctx, volName); err != nil {
			return err
		}
		defer func() {
			if err := w.runtime.DeleteVolume(ctx, volName); err != nil {
				log.Error().Err(err).Msgf("error deleting volume: %s", volName)
			}
		}()
		vols = append(vols, fmt.Sprintf("%s:%s", volName, v))
	}
	t.Volumes = vols
	// excute pre-tasks
	for _, pre := range t.Pre {
		pre.Volumes = t.Volumes
		result, err := w.runtime.Run(ctx, &pre)
		finished := time.Now()
		if err != nil {
			// we also want to mark the
			// actual task as FAILED
			t.State = task.Failed
			t.Error = err.Error()
			t.FailedAt = &finished
			if err := w.broker.Publish(ctx, mq.QUEUE_ERROR, t); err != nil {
				return err
			}
			return nil
		}
		pre.Result = result
	}
	result, err := w.runtime.Run(ctx, t)
	finished := time.Now()
	if err != nil {
		t.State = task.Failed
		t.Error = err.Error()
		t.FailedAt = &finished
		if err := w.broker.Publish(ctx, mq.QUEUE_ERROR, t); err != nil {
			return err
		}
		return nil
	}
	// execute post tasks
	for _, post := range t.Post {
		post.Volumes = t.Volumes
		result, err := w.runtime.Run(ctx, &post)
		finished := time.Now()
		if err != nil {
			// we also want to mark the
			// actual task as FAILED
			t.State = task.Failed
			t.Error = err.Error()
			t.FailedAt = &finished
			if err := w.broker.Publish(ctx, mq.QUEUE_ERROR, t); err != nil {
				return err
			}
			return nil
		}
		post.Result = result
	}
	// send completion to the coordinator
	t.Result = result
	t.CompletedAt = &finished
	t.State = task.Completed
	return w.broker.Publish(ctx, mq.QUEUE_COMPLETED, t)
}

func (w *Worker) collectStats() {
	for !w.stop {
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
	return nil
}

func (w *Worker) Stop() error {
	log.Debug().Msgf("shutting down %s", w.Name)
	w.stop = true
	return nil
}
