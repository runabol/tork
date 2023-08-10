package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pkg/errors"
	"github.com/tork/mq"
	"github.com/tork/node"
	"github.com/tork/runtime"
	"github.com/tork/task"
	"github.com/tork/uuid"
)

type Worker struct {
	id        string
	startTime time.Time
	runtime   runtime.Runtime
	broker    mq.Broker
	stop      bool
	queues    map[string]int
	tasks     map[string]*runningTask
	mu        sync.RWMutex
}

type Config struct {
	Broker  mq.Broker
	Runtime runtime.Runtime
	Queues  map[string]int
}

type runningTask struct {
	task   *task.Task
	cancel context.CancelFunc
}

func NewWorker(cfg Config) *Worker {
	if len(cfg.Queues) == 0 {
		cfg.Queues = map[string]int{mq.QUEUE_DEFAULT: 1}
	}
	w := &Worker{
		id:        uuid.NewUUID(),
		startTime: time.Now(),
		broker:    cfg.Broker,
		runtime:   cfg.Runtime,
		queues:    cfg.Queues,
		tasks:     make(map[string]*runningTask),
	}
	return w
}

func (w *Worker) handleTask(threadname string) func(ctx context.Context, t *task.Task) error {
	return func(ctx context.Context, t *task.Task) error {
		log.Debug().
			Str("thread", threadname).
			Str("task-id", t.ID).
			Msg("received task")
		switch t.State {
		case task.Scheduled:
			return w.runTask(ctx, t)
		case task.Cancelled:
			return w.cancelTask(ctx, t)
		default:
			return errors.Errorf("can't start a task in %s state", t.State)
		}
	}
}

func (w *Worker) cancelTask(ctx context.Context, t *task.Task) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	rt, ok := w.tasks[t.ID]
	if !ok {
		log.Debug().Msgf("unknown task %s. nothing to cancel", t.ID)
		return nil
	}
	rt.cancel()
	delete(w.tasks, t.ID)
	return nil
}

func (w *Worker) runTask(c context.Context, t *task.Task) error {
	ctx, cancel := context.WithCancel(c)
	w.mu.Lock()
	w.tasks[t.ID] = &runningTask{
		task:   t,
		cancel: cancel,
	}
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		delete(w.tasks, t.ID)
	}()
	started := time.Now()
	t.StartedAt = &started
	t.State = task.Running
	t.Node = w.id
	if err := w.broker.PublishTask(ctx, mq.QUEUE_STARTED, t); err != nil {
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
				log.Error().
					Err(err).
					Msgf("error deleting volume: %s", volName)
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
			if err := w.broker.PublishTask(ctx, mq.QUEUE_ERROR, t); err != nil {
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
		if err := w.broker.PublishTask(ctx, mq.QUEUE_ERROR, t); err != nil {
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
			if err := w.broker.PublishTask(ctx, mq.QUEUE_ERROR, t); err != nil {
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
	return w.broker.PublishTask(ctx, mq.QUEUE_COMPLETED, t)
}

func (w *Worker) sendHeartbeats() {
	for !w.stop {
		s, err := getStats()
		if err != nil {
			log.Error().Msgf("error collecting stats for %s", w.id)
		} else {
			log.Debug().Float64("cpu-percent", s.CPUPercent).Msgf("collecting stats for %s", w.id)
		}
		err = w.broker.PublishHeartbeat(
			context.Background(),
			&node.Node{
				ID:         w.id,
				StartedAt:  w.startTime,
				CPUPercent: s.CPUPercent,
				Queue:      fmt.Sprintf("%s%s", mq.QUEUE_EXCLUSIVE_PREFIX, w.id),
			},
		)
		if err != nil {
			log.Error().Msgf("error publishing heartbeat for %s", w.id)
		}
		time.Sleep(1 * time.Minute)
	}
}

func (w *Worker) Start() error {
	log.Info().Msgf("starting worker %s", w.id)
	// subscribe for a private queue for the node
	if err := w.broker.SubscribeForTasks(fmt.Sprintf("%s%s", mq.QUEUE_EXCLUSIVE_PREFIX, w.id), w.handleTask("main")); err != nil {
		return errors.Wrapf(err, "error subscribing for queue: %s", w.id)
	}
	// subscribe to shared work queues
	for qname, concurrency := range w.queues {
		if !mq.IsWorkerQueue(qname) {
			continue
		}
		for i := 0; i < concurrency; i++ {
			err := w.broker.SubscribeForTasks(qname, w.handleTask(fmt.Sprintf("%s-%d", qname, i)))
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
	w.stop = true
	return nil
}
