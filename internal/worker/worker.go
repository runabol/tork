package worker

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/middleware/task"

	"github.com/runabol/tork/internal/host"
	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/runtime"

	"github.com/runabol/tork/internal/uuid"
)

type Worker struct {
	id         string
	name       string
	startTime  time.Time
	runtime    runtime.Runtime
	broker     broker.Broker
	stop       chan any
	queues     map[string]int
	tasks      *syncx.Map[string, runningTask]
	limits     Limits
	api        *api
	taskCount  int32
	middleware []task.MiddlewareFunc
	mu         sync.Mutex
	cordoned   bool
}

type Config struct {
	Name       string
	Address    string
	Broker     broker.Broker
	Runtime    runtime.Runtime
	Queues     map[string]int
	Limits     Limits
	Middleware []task.MiddlewareFunc
	// CordonToken gates the /cordon endpoints; if empty they are disabled.
	CordonToken string
}

type Limits struct {
	DefaultCPUsLimit   string
	DefaultMemoryLimit string
	Timeout            string
	MaxResultSize      int
}

type runningTask struct {
	cancel context.CancelFunc
	task   *tork.Task
}

func NewWorker(cfg Config) (*Worker, error) {
	if len(cfg.Queues) == 0 {
		cfg.Queues = map[string]int{broker.QUEUE_DEFAULT: 1}
	}
	if cfg.Broker == nil {
		return nil, errors.New("must provide broker")
	}
	if cfg.Runtime == nil {
		return nil, errors.New("must provide runtime")
	}
	if cfg.Limits.MaxResultSize <= 0 {
		cfg.Limits.MaxResultSize = DefaultMaxTaskResultSize
	}
	tasks := new(syncx.Map[string, runningTask])
	w := &Worker{
		id:         uuid.NewShortUUID(),
		name:       cfg.Name,
		startTime:  time.Now().UTC(),
		broker:     cfg.Broker,
		runtime:    cfg.Runtime,
		queues:     cfg.Queues,
		tasks:      tasks,
		limits:     cfg.Limits,
		stop:       make(chan any),
		middleware: cfg.Middleware,
	}
	w.api = newAPI(cfg, tasks, w)
	return w, nil
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

func (w *Worker) handleTask(t *tork.Task) error {
	return w.doHandleTask(context.Background(), t)
}

func (w *Worker) doHandleTask(ctx context.Context, t *tork.Task) error {
	started := time.Now().UTC()
	t.StartedAt = &started
	t.NodeID = w.id
	t.State = tork.TaskStateRunning
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
	if w.limits.Timeout != "" {
		limitDur, err := time.ParseDuration(w.limits.Timeout)
		if err != nil {
			return errors.Wrapf(err, "invalid worker timeout duration: %s", w.limits.Timeout)
		}
		if t.Timeout == "" {
			t.Timeout = w.limits.Timeout
		} else {
			taskDur, err := time.ParseDuration(t.Timeout)
			if err != nil {
				return errors.Wrapf(err, "invalid timeout duration: %s", t.Timeout)
			}
			if taskDur > limitDur {
				t.Timeout = w.limits.Timeout
			}
		}
	}
	adapter := func(ctx context.Context, et task.EventType, t *tork.Task) error {
		return w.runTask(t)
	}
	// clone the task so that the downstream
	// process can mutate the task without
	// affecting the original
	rt := t.Clone()
	mw := task.ApplyMiddleware(adapter, w.middleware)
	if err := mw(ctx, task.StateChange, rt); err != nil {
		now := time.Now().UTC()
		t.Error = err.Error()
		t.FailedAt = &now
		t.State = tork.TaskStateFailed
		return w.broker.PublishTask(ctx, broker.QUEUE_ERROR, t)
	}
	switch rt.State {
	case tork.TaskStateCompleted:
		t.Result = rt.Result
		t.CompletedAt = rt.CompletedAt
		t.State = rt.State
		if err := w.broker.PublishTask(ctx, broker.QUEUE_COMPLETED, t); err != nil {
			return err
		}
	case tork.TaskStateFailed:
		t.Error = rt.Error
		t.FailedAt = rt.FailedAt
		t.State = rt.State
		if err := w.broker.PublishTask(ctx, broker.QUEUE_ERROR, t); err != nil {
			return err
		}
	default:
		return errors.Errorf("unexpected state %s for task %s", rt.State, t.ID)
	}
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
		task:   t,
	})
	defer w.tasks.Delete(t.ID)
	// let the coordinator know that the task started executing
	if err := w.broker.PublishTask(ctx, broker.QUEUE_STARTED, t); err != nil {
		return err
	}
	if err := w.doRunTask(ctx, t); err != nil {
		return err
	}
	return nil
}

func (w *Worker) doRunTask(ctx context.Context, t *tork.Task) error {
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
	if err := w.runtime.Run(rctx, t); err != nil {
		finished := time.Now().UTC()
		t.FailedAt = &finished
		t.State = tork.TaskStateFailed
		t.Error = err.Error()
		return nil
	}
	if len(t.Result) > w.limits.MaxResultSize {
		finished := time.Now().UTC()
		t.FailedAt = &finished
		t.State = tork.TaskStateFailed
		t.Error = fmt.Errorf("task result exceeds maximum size of %d bytes (got %d)", w.limits.MaxResultSize, len(t.Result)).Error()
		return nil
	}
	finished := time.Now().UTC()
	t.CompletedAt = &finished
	t.State = tork.TaskStateCompleted
	return nil
}

// nodeStatus reports the status published in heartbeats. DOWN (failed health
// check) takes precedence over CORDONED so a real fault isn't masked.
func (w *Worker) nodeStatus(ctx context.Context) tork.NodeStatus {
	if err := w.runtime.HealthCheck(ctx); err != nil {
		log.Error().Err(err).Msgf("node %s failed health check", w.id)
		return tork.NodeStatusDown
	}
	if w.isCordoned() {
		return tork.NodeStatusCordoned
	}
	return tork.NodeStatusUP
}

func (w *Worker) sendHeartbeats() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		status := w.nodeStatus(ctx)
		hostname, err := os.Hostname()
		if err != nil {
			log.Error().Err(err).Msgf("failed to get hostname for worker %s", w.id)
		}
		cpuPercent := host.GetCPUPercent()
		err = w.broker.PublishHeartbeat(
			context.Background(),
			&tork.Node{
				ID:              w.id,
				Name:            w.name,
				StartedAt:       w.startTime,
				CPUPercent:      cpuPercent,
				Queue:           fmt.Sprintf("%s%s", broker.QUEUE_EXCLUSIVE_PREFIX, w.id),
				Status:          status,
				LastHeartbeatAt: time.Now().UTC(),
				Hostname:        hostname,
				Port:            w.api.port,
				TaskCount:       w.TaskCount(),
				Version:         tork.Version,
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
	if err := w.broker.SubscribeForTasks(fmt.Sprintf("%s%s", broker.QUEUE_EXCLUSIVE_PREFIX, w.id), w.cancelTask); err != nil {
		return errors.Wrapf(err, "error subscribing for queue: %s", w.id)
	}
	// subscribe to shared work queues
	if err := w.subscribeForWork(); err != nil {
		return err
	}
	go w.sendHeartbeats()
	return nil
}

// subscribeForWork subscribes to the shared work queues (startup and uncordon).
func (w *Worker) subscribeForWork() error {
	for qname, concurrency := range w.queues {
		if !broker.IsWorkerQueue(qname) {
			continue
		}
		for i := 0; i < concurrency; i++ {
			if err := w.broker.SubscribeForTasks(qname, w.handleTask); err != nil {
				return errors.Wrapf(err, "error subscribing for queue: %s", qname)
			}
		}
	}
	return nil
}

// Cordon takes the worker out of rotation: it stops claiming new tasks while
// running tasks finish normally. Idempotent.
func (w *Worker) Cordon() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cordoned {
		return nil
	}
	log.Info().Msgf("cordoning worker %s", w.id)
	for qname := range w.queues {
		if !broker.IsWorkerQueue(qname) {
			continue
		}
		if err := w.broker.Unsubscribe(qname); err != nil {
			return errors.Wrapf(err, "error unsubscribing from queue: %s", qname)
		}
	}
	w.cordoned = true
	return nil
}

// Uncordon puts the worker back into rotation. Idempotent.
func (w *Worker) Uncordon() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.cordoned {
		return nil
	}
	log.Info().Msgf("uncordoning worker %s", w.id)
	if err := w.subscribeForWork(); err != nil {
		return err
	}
	w.cordoned = false
	return nil
}

func (w *Worker) isCordoned() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cordoned
}

func (w *Worker) TaskCount() int {
	return int(atomic.LoadInt32(&w.taskCount))
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
