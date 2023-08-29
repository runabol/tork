package worker

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pkg/errors"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/runtime"
	"github.com/runabol/tork/task"
	"github.com/runabol/tork/uuid"
)

const defaultOutputPath = "/tork/output"

type Worker struct {
	id        string
	startTime time.Time
	runtime   runtime.Runtime
	broker    mq.Broker
	stop      bool
	queues    map[string]int
	tasks     map[string]runningTask
	mu        sync.RWMutex
	limits    Limits
	tempdir   string
	api       *api
}

type Limits struct {
	DefaultCPUsLimit   string
	DefaultMemoryLimit string
}

type Config struct {
	Address string
	Broker  mq.Broker
	Runtime runtime.Runtime
	Queues  map[string]int
	Limits  Limits
	TempDir string
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
		id:        uuid.NewUUID(),
		startTime: time.Now().UTC(),
		broker:    cfg.Broker,
		runtime:   cfg.Runtime,
		queues:    cfg.Queues,
		tasks:     make(map[string]runningTask),
		limits:    cfg.Limits,
		tempdir:   cfg.TempDir,
		api:       newAPI(cfg),
	}
	return w, nil
}

func (w *Worker) handleTask(t *task.Task) error {
	log.Debug().
		Str("task-id", t.ID).
		Msg("received task")
	switch t.State {
	case task.Scheduled:
		return w.runTask(t)
	case task.Cancelled:
		return w.cancelTask(t)
	default:
		return errors.Errorf("invalid task state: %s", t.State)
	}
}

func (w *Worker) cancelTask(t *task.Task) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	rt, ok := w.tasks[t.ID]
	if !ok {
		log.Debug().Msgf("unknown task %s. nothing to cancel", t.ID)
		return nil
	}
	log.Debug().Msgf("cancelling task %s", t.ID)
	rt.cancel()
	delete(w.tasks, t.ID)
	return nil
}

func (w *Worker) runTask(t *task.Task) error {
	// create a cancellation context in case
	// the coordinator wants to cancel the
	// task later on
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.mu.Lock()
	w.tasks[t.ID] = runningTask{
		cancel: cancel,
	}
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		delete(w.tasks, t.ID)
	}()
	// let the coordinator know that the task started executing
	started := time.Now().UTC()
	t.StartedAt = &started
	t.State = task.Running
	t.NodeID = w.id
	if err := w.broker.PublishTask(ctx, mq.QUEUE_STARTED, t); err != nil {
		return err
	}
	// clone the task so that the downstream
	// process can mutate the task without
	// affecting the original
	rt := t.Clone()
	if err := w.executeTask(ctx, rt); err != nil {
		return err
	}
	switch rt.State {
	case task.Completed:
		t.Result = rt.Result
		t.CompletedAt = rt.CompletedAt
		t.State = rt.State
		if err := w.broker.PublishTask(ctx, mq.QUEUE_COMPLETED, t); err != nil {
			return err
		}
	case task.Failed:
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

func (w *Worker) executeTask(ctx context.Context, t *task.Task) error {
	// prepare limits
	if t.Limits == nil && (w.limits.DefaultCPUsLimit != "" || w.limits.DefaultMemoryLimit != "") {
		t.Limits = &task.Limits{}
	}
	if t.Limits != nil && t.Limits.CPUs == "" {
		t.Limits.CPUs = w.limits.DefaultCPUsLimit
	}
	if t.Limits != nil && t.Limits.Memory == "" {
		t.Limits.Memory = w.limits.DefaultMemoryLimit
	}
	// prepare shared volumes

	vols := []string{}
	for _, v := range t.Volumes {
		volName := uuid.NewUUID()
		if err := w.runtime.CreateVolume(ctx, volName); err != nil {
			return err
		}
		defer func(vname string) {
			if err := w.runtime.DeleteVolume(ctx, vname); err != nil {
				log.Error().
					Err(err).
					Msgf("error deleting volume: %s", vname)
			}
		}(volName)
		vols = append(vols, fmt.Sprintf("volume:%s:%s", volName, v))
	}
	t.Volumes = vols
	// excute pre-tasks
	for _, pre := range t.Pre {
		pre.ID = uuid.NewUUID()
		pre.Volumes = t.Volumes
		pre.Networks = t.Networks
		pre.Limits = t.Limits
		if err := w.doExecuteTask(ctx, pre); err != nil {
			log.Error().
				Str("task-id", t.ID).
				Err(err).
				Msg("error processing pre-task")
			// we also want to mark the
			// actual task as FAILED
			finished := time.Now().UTC()
			t.State = task.Failed
			t.Error = err.Error()
			t.FailedAt = &finished
			return nil
		}
	}
	// run the actual task
	if err := w.doExecuteTask(ctx, t); err != nil {
		log.Error().
			Str("task-id", t.ID).
			Err(err).
			Msg("error processing task")
		finished := time.Now().UTC()
		t.State = task.Failed
		t.Error = err.Error()
		t.FailedAt = &finished
		return nil
	}
	// execute post tasks
	for _, post := range t.Post {
		post.ID = uuid.NewUUID()
		post.Volumes = t.Volumes
		post.Networks = t.Networks
		post.Limits = t.Limits
		if err := w.doExecuteTask(ctx, post); err != nil {
			log.Error().
				Str("task-id", t.ID).
				Err(err).
				Msg("error processing post-task")
			// we also want to mark the
			// actual task as FAILED
			finished := time.Now().UTC()
			t.State = task.Failed
			t.Error = err.Error()
			t.FailedAt = &finished
			return nil
		}
	}
	finished := time.Now().UTC()
	t.CompletedAt = &finished
	t.State = task.Completed
	return nil
}

func (w *Worker) doExecuteTask(ctx context.Context, o *task.Task) error {
	t := o.Clone()
	// create a temporary mount point
	// we can use to write the run script to
	rundir, err := os.MkdirTemp(w.tempdir, "tork-")
	if err != nil {
		return errors.Wrapf(err, "error creating temp dir")
	}
	defer deleteTempDir(rundir)
	if err := os.WriteFile(path.Join(rundir, "run"), []byte(t.Run), os.ModePerm); err != nil {
		return err
	}
	t.Volumes = append(t.Volumes, fmt.Sprintf("bind:%s:%s", rundir, "/tork"))
	// set the path for task outputs
	if t.Env == nil {
		t.Env = make(map[string]string)
	}
	t.Env["TORK_OUTPUT"] = defaultOutputPath
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
	if err := w.runtime.Run(rctx, t); err != nil {
		return err
	}
	if _, err := os.Stat(path.Join(rundir, "output")); err == nil {
		contents, err := os.ReadFile(path.Join(rundir, "output"))
		if err != nil {
			return errors.Wrapf(err, "error reading output file")
		}
		o.Result = string(contents)
	}
	return nil
}

func deleteTempDir(dirname string) {
	if err := os.RemoveAll(dirname); err != nil {
		log.Error().
			Err(err).
			Msgf("error deleting volume: %s", dirname)
	}
}

func (w *Worker) sendHeartbeats() {
	for !w.stop {
		s, err := getStats()
		if err != nil {
			log.Error().Msgf("error collecting stats for %s", w.id)
		} else {
			log.Debug().Float64("cpu-percent", s.CPUPercent).Msgf("collecting stats for %s", w.id)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		status := node.UP
		if err := w.runtime.HealthCheck(ctx); err != nil {
			log.Error().Err(err).Msgf("node %s failed health check", w.id)
			status = node.Down
		}
		hostname, err := os.Hostname()
		if err != nil {
			log.Error().Err(err).Msgf("failed to get hostname for worker %s", w.id)
		}
		err = w.broker.PublishHeartbeat(
			context.Background(),
			node.Node{
				ID:              w.id,
				StartedAt:       w.startTime,
				CPUPercent:      s.CPUPercent,
				Queue:           fmt.Sprintf("%s%s", mq.QUEUE_EXCLUSIVE_PREFIX, w.id),
				Status:          status,
				LastHeartbeatAt: time.Now().UTC(),
				Hostname:        hostname,
			},
		)
		if err != nil {
			log.Error().
				Err(err).
				Msgf("error publishing heartbeat for %s", w.id)
		}
		time.Sleep(node.HEARTBEAT_RATE)
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
	// subscribe to shared work queues
	for qname, concurrency := range w.queues {
		if !mq.IsWorkerQueue(qname) {
			continue
		}
		for i := 0; i < concurrency; i++ {
			err := w.broker.SubscribeForTasks(qname, w.handleTask)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.broker.Shutdown(ctx); err != nil {
		return errors.Wrapf(err, "error shutting down broker")
	}
	if err := w.api.shutdown(ctx); err != nil {
		return errors.Wrapf(err, "error shutting down worker %s", w.id)
	}
	return nil
}
