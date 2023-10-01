package worker

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/mq"

	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/runtime"

	"github.com/runabol/tork/internal/uuid"
)

type Worker struct {
	id         string
	startTime  time.Time
	runtime    runtime.Runtime
	broker     mq.Broker
	stop       chan any
	queues     map[string]int
	tasks      *syncx.Map[string, runningTask]
	limits     Limits
	api        *api
	taskCount  int32
	middleware []task.MiddlewareFunc
}

type Config struct {
	Address    string
	Broker     mq.Broker
	Runtime    runtime.Runtime
	Queues     map[string]int
	Limits     Limits
	Middleware []task.MiddlewareFunc
}

type Limits struct {
	DefaultCPUsLimit   string
	DefaultMemoryLimit string
}

type runningTask struct {
	cancel context.CancelFunc
}

func NewWorker(cfg Config) (*Worker, error) {
	if len(cfg.Queues) == 0 {
		cfg.Queues = map[string]int{mq.QUEUE_DEFAULT: 1}
	}
	if cfg.Broker == nil {
		return nil, errors.New("must provide broker")
	}
	if cfg.Runtime == nil {
		return nil, errors.New("must provide runtime")
	}
	w := &Worker{
		id:         uuid.NewUUID(),
		startTime:  time.Now().UTC(),
		broker:     cfg.Broker,
		runtime:    cfg.Runtime,
		queues:     cfg.Queues,
		tasks:      new(syncx.Map[string, runningTask]),
		limits:     cfg.Limits,
		api:        newAPI(cfg),
		stop:       make(chan any),
		middleware: cfg.Middleware,
	}
	return w, nil
}

func (w *Worker) handleTask(t *tork.Task) error {
	log.Debug().
		Str("task-id", t.ID).
		Msg("received task")
	switch t.State {
	case tork.TaskStateScheduled:
		return w.runTask(t)
	case tork.TaskStateCancelled:
		return w.cancelTask(t)
	default:
		return errors.Errorf("invalid task state: %s", t.State)
	}
}

func (w *Worker) cancelTask(t *tork.Task) error {
	rt, ok := w.tasks.Get(t.ID)
	if !ok {
		log.Debug().Msgf("unknown task %s. nothing to cancel", t.ID)
		return nil
	}
	log.Debug().Msgf("cancelling task %s", t.ID)
	rt.cancel()
	w.tasks.Delete(t.ID)
	return nil
}

func (w *Worker) runTask(t *tork.Task) error {
	atomic.AddInt32(&w.taskCount, 1)
	defer func() {
		atomic.AddInt32(&w.taskCount, -1)
	}()
	// create a cancellation context in case
	// the coordinator wants to cancel the
	// task later on
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.tasks.Set(t.ID, runningTask{
		cancel: cancel,
	})
	defer w.tasks.Delete(t.ID)
	// let the coordinator know that the task started executing
	started := time.Now().UTC()
	t.StartedAt = &started
	t.State = tork.TaskStateRunning
	t.NodeID = w.id
	if err := w.broker.PublishTask(ctx, mq.QUEUE_STARTED, t); err != nil {
		return err
	}
	// clone the task so that the downstream
	// process can mutate the task without
	// affecting the original
	rt := t.Clone()
	if err := w.doRunTask(ctx, rt); err != nil {
		return err
	}
	switch rt.State {
	case tork.TaskStateCompleted:
		t.Result = rt.Result
		t.CompletedAt = rt.CompletedAt
		t.State = rt.State
		if err := w.broker.PublishTask(ctx, mq.QUEUE_COMPLETED, t); err != nil {
			return err
		}
	case tork.TaskStateFailed:
		t.Error = rt.Error
		t.FailedAt = rt.FailedAt
		t.State = rt.State
		if err := w.broker.PublishTask(ctx, mq.QUEUE_ERROR, t); err != nil {
			return err
		}
	default:
		return errors.Errorf("unexpected state %s for task %s", rt.State, t.ID)
	}
	return nil
}

func (w *Worker) doRunTask(ctx context.Context, t *tork.Task) error {
	// prepare limits
	if t.Limits == nil && (w.limits.DefaultCPUsLimit != "" || w.limits.DefaultMemoryLimit != "") {
		t.Limits = &tork.TaskLimits{}
	}
	if t.Limits != nil && t.Limits.CPUs == "" {
		t.Limits.CPUs = w.limits.DefaultCPUsLimit
	}
	if t.Limits != nil && t.Limits.Memory == "" {
		t.Limits.Memory = w.limits.DefaultMemoryLimit
	}
	// create timeout context -- if timeout is defined
	rctx := ctx
	if t.Timeout != "" {
		dur, err := time.ParseDuration(t.Timeout)
		if err != nil {
			return errors.Wrapf(err, "invalid timeout duration: %s", t.Timeout)
		}
		tctx, cancel := context.WithTimeout(ctx, dur)
		defer cancel()
		rctx = tctx
	}
	// run the task
	rtTask := t.Clone()
	if err := w.runtime.Run(rctx, rtTask); err != nil {
		finished := time.Now().UTC()
		t.FailedAt = &finished
		t.State = tork.TaskStateFailed
		t.Error = err.Error()
		return nil
	}
	finished := time.Now().UTC()
	t.CompletedAt = &finished
	t.State = tork.TaskStateCompleted
	t.Result = rtTask.Result
	return nil
}

func (w *Worker) sendHeartbeats() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		status := tork.NodeStatusUP
		if err := w.runtime.HealthCheck(ctx); err != nil {
			log.Error().Err(err).Msgf("node %s failed health check", w.id)
			status = tork.NodeStatusDown
		}
		hostname, err := os.Hostname()
		if err != nil {
			log.Error().Err(err).Msgf("failed to get hostname for worker %s", w.id)
		}
		cpuPercent := getCPUPercent()
		err = w.broker.PublishHeartbeat(
			context.Background(),
			&tork.Node{
				ID:              w.id,
				StartedAt:       w.startTime,
				CPUPercent:      cpuPercent,
				Queue:           fmt.Sprintf("%s%s", mq.QUEUE_EXCLUSIVE_PREFIX, w.id),
				Status:          status,
				LastHeartbeatAt: time.Now().UTC(),
				Hostname:        hostname,
				TaskCount:       int(atomic.LoadInt32(&w.taskCount)),
				Version:         tork.FormattedVersion(),
			},
		)
		if err != nil {
			log.Error().
				Err(err).
				Msgf("error publishing heartbeat for %s", w.id)
		}
		select {
		case <-w.stop:
			return
		case <-time.After(tork.HEARTBEAT_RATE):
		}
	}
}

func (w *Worker) Start() error {
	log.Info().Msgf("starting worker %s", w.id)
	if err := w.api.start(); err != nil {
		return err
	}
	// subscribe for a private queue for the node
	if err := w.broker.SubscribeForTasks(fmt.Sprintf("%s%s", mq.QUEUE_EXCLUSIVE_PREFIX, w.id), w.handleTask); err != nil {
		return errors.Wrapf(err, "error subscribing for queue: %s", w.id)
	}
	onTask := task.ApplyMiddleware(func(ctx context.Context, et task.EventType, t *tork.Task) error {
		return w.handleTask(t)
	}, w.middleware)
	// subscribe to shared work queues
	for qname, concurrency := range w.queues {
		if !mq.IsWorkerQueue(qname) {
			continue
		}
		for i := 0; i < concurrency; i++ {
			err := w.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
				return onTask(context.Background(), task.StateChange, t)
			})
			if err != nil {
				return errors.Wrapf(err, "error subscribing for queue: %s", qname)
			}
		}
	}
	go w.sendHeartbeats()
	return nil
}

func (w *Worker) Stop() error {
	log.Debug().Msgf("shutting down worker %s", w.id)
	w.stop <- 1
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := w.broker.Shutdown(ctx); err != nil {
		return errors.Wrapf(err, "error shutting down broker")
	}
	if err := w.api.shutdown(ctx); err != nil {
		return errors.Wrapf(err, "error shutting down worker %s", w.id)
	}
	return nil
}
