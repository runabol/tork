package coordinator

import (
	"context"
	"os"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/coordinator/api"
	"github.com/runabol/tork/internal/coordinator/handlers"
	"github.com/runabol/tork/internal/host"
	"github.com/runabol/tork/locker"

	"github.com/runabol/tork/input"
	"github.com/runabol/tork/middleware/job"
	logmw "github.com/runabol/tork/middleware/log"
	"github.com/runabol/tork/middleware/node"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/middleware/web"

	"github.com/runabol/tork/broker"

	"github.com/runabol/tork/internal/uuid"
)

// Coordinator is responsible for accepting tasks from
// clients, scheduling tasks for workers to execute and for
// exposing the cluster's state to the outside world.
type Coordinator struct {
	id             string
	startTime      time.Time
	Name           string
	broker         broker.Broker
	api            *api.API
	ds             datastore.Datastore
	queues         map[string]int
	onPending      task.HandlerFunc
	onStarted      task.HandlerFunc
	onError        task.HandlerFunc
	onJob          job.HandlerFunc
	onHeartbeat    node.HandlerFunc
	onCompleted    task.HandlerFunc
	onLogPart      func(*tork.TaskLogPart)
	onProgress     task.HandlerFunc
	onScheduledJob func(ctx context.Context, s *tork.ScheduledJob) error
	stop           chan any
}

type Config struct {
	Name       string
	Broker     broker.Broker
	DataStore  datastore.Datastore
	Locker     locker.Locker
	Address    string
	Queues     map[string]int
	Endpoints  map[string]web.HandlerFunc
	Enabled    map[string]bool
	Middleware Middleware
}

type Middleware struct {
	Web  []web.MiddlewareFunc
	Task []task.MiddlewareFunc
	Job  []job.MiddlewareFunc
	Node []node.MiddlewareFunc
	Log  []logmw.MiddlewareFunc
	Echo []echo.MiddlewareFunc
}

func NewCoordinator(cfg Config) (*Coordinator, error) {
	if cfg.Broker == nil {
		return nil, errors.New("most provide a broker")
	}
	if cfg.DataStore == nil {
		return nil, errors.New("most provide a datastore")
	}
	if cfg.Locker == nil {
		return nil, errors.New("most provide a locker")
	}
	if cfg.Queues == nil {
		cfg.Queues = make(map[string]int)
	}
	if cfg.Endpoints == nil {
		cfg.Endpoints = make(map[string]web.HandlerFunc)
	}
	if cfg.Enabled == nil {
		cfg.Enabled = make(map[string]bool)
	}
	if cfg.Queues[broker.QUEUE_COMPLETED] < 1 {
		cfg.Queues[broker.QUEUE_COMPLETED] = 1
	}
	if cfg.Queues[broker.QUEUE_ERROR] < 1 {
		cfg.Queues[broker.QUEUE_ERROR] = 1
	}
	if cfg.Queues[broker.QUEUE_PENDING] < 1 {
		cfg.Queues[broker.QUEUE_PENDING] = 1
	}
	if cfg.Queues[broker.QUEUE_STARTED] < 1 {
		cfg.Queues[broker.QUEUE_STARTED] = 1
	}
	if cfg.Queues[broker.QUEUE_HEARTBEAT] < 1 {
		cfg.Queues[broker.QUEUE_HEARTBEAT] = 1
	}
	if cfg.Queues[broker.QUEUE_JOBS] < 1 {
		cfg.Queues[broker.QUEUE_JOBS] = 1
	}
	if cfg.Queues[broker.QUEUE_LOGS] < 1 {
		cfg.Queues[broker.QUEUE_LOGS] = 1
	}
	if cfg.Queues[broker.QUEUE_PROGRESS] < 1 {
		cfg.Queues[broker.QUEUE_PROGRESS] = 1
	}
	api, err := api.NewAPI(api.Config{
		Broker:    cfg.Broker,
		DataStore: cfg.DataStore,
		Address:   cfg.Address,
		Middleware: api.Middleware{
			Web:  cfg.Middleware.Web,
			Echo: cfg.Middleware.Echo,
			Job:  cfg.Middleware.Job,
			Task: cfg.Middleware.Task,
			Log:  cfg.Middleware.Log,
		},
		Endpoints: cfg.Endpoints,
		Enabled:   cfg.Enabled,
	})
	if err != nil {
		return nil, err
	}

	onPending := task.ApplyMiddleware(
		handlers.NewPendingHandler(cfg.DataStore, cfg.Broker),
		cfg.Middleware.Task,
	)

	onStarted := task.ApplyMiddleware(
		handlers.NewStartedHandler(
			cfg.DataStore,
			cfg.Broker,
			cfg.Middleware.Job...,
		),
		cfg.Middleware.Task,
	)

	onError := task.ApplyMiddleware(
		handlers.NewErrorHandler(
			cfg.DataStore,
			cfg.Broker,
			cfg.Middleware.Job...,
		),
		cfg.Middleware.Task,
	)

	onCompleted := task.ApplyMiddleware(
		handlers.NewCompletedHandler(
			cfg.DataStore,
			cfg.Broker,
			cfg.Middleware.Job...,
		),
		cfg.Middleware.Task,
	)

	onJob := job.ApplyMiddleware(
		handlers.NewJobHandler(
			cfg.DataStore,
			cfg.Broker,
			cfg.Middleware.Task...,
		),
		cfg.Middleware.Job,
	)

	onHeartbeat := node.ApplyMiddleware(
		handlers.NewHeartbeatHandler(cfg.DataStore),
		cfg.Middleware.Node,
	)

	onLogPart := handlers.NewLogHandler(cfg.DataStore)

	onProgress := task.ApplyMiddleware(
		handlers.NewProgressHandler(
			cfg.DataStore,
			onJob,
		),
		cfg.Middleware.Task,
	)

	onScheduledJob, err := handlers.NewJobSchedulerHandler(cfg.DataStore, cfg.Broker, cfg.Locker)
	if err != nil {
		return nil, errors.Wrapf(err, "error initializing the job scheduler")
	}

	return &Coordinator{
		id:             uuid.NewShortUUID(),
		startTime:      time.Now(),
		Name:           cfg.Name,
		api:            api,
		broker:         cfg.Broker,
		ds:             cfg.DataStore,
		queues:         cfg.Queues,
		onPending:      onPending,
		onStarted:      onStarted,
		onError:        onError,
		onJob:          onJob,
		onHeartbeat:    onHeartbeat,
		onCompleted:    onCompleted,
		onLogPart:      onLogPart,
		onProgress:     onProgress,
		onScheduledJob: onScheduledJob,
		stop:           make(chan any),
	}, nil
}

func (c *Coordinator) SubmitJob(ctx context.Context, ij *input.Job) (*tork.Job, error) {
	return c.api.SubmitJob(ctx, ij)
}

func (c *Coordinator) Start() error {
	log.Info().Msgf("starting Coordinator")
	// start the coordinator API
	if err := c.api.Start(); err != nil {
		return err
	}
	// subscribe to task queues
	for qname, conc := range c.queues {
		if !broker.IsCoordinatorQueue(qname) {
			continue
		}
		for i := 0; i < conc; i++ {
			var err error
			switch qname {
			case broker.QUEUE_PENDING:
				pendingHandler := c.taskHandler(c.onPending)
				err = c.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
					return pendingHandler(context.Background(), task.StateChange, t)
				})
			case broker.QUEUE_COMPLETED:
				completedHandler := c.taskHandler(c.onCompleted)
				err = c.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
					return completedHandler(context.Background(), task.StateChange, t)
				})
			case broker.QUEUE_STARTED:
				startedHandler := c.taskHandler(c.onStarted)
				err = c.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
					return startedHandler(context.Background(), task.StateChange, t)
				})
			case broker.QUEUE_ERROR:
				errorHandler := c.taskHandler(c.onError)
				err = c.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
					return errorHandler(context.Background(), task.StateChange, t)
				})
			case broker.QUEUE_HEARTBEAT:
				err = c.broker.SubscribeForHeartbeats(func(n *tork.Node) error {
					return c.onHeartbeat(context.Background(), n)
				})
			case broker.QUEUE_JOBS:
				jobHandler := c.jobHandler(c.onJob)
				err = c.broker.SubscribeForJobs(func(j *tork.Job) error {
					return jobHandler(context.Background(), job.StateChange, j)
				})
			case broker.QUEUE_LOGS:
				err = c.broker.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {
					c.onLogPart(p)
				})
			case broker.QUEUE_PROGRESS:
				progressHandler := c.taskHandler(c.onProgress)
				err = c.broker.SubscribeForTaskProgress(func(t *tork.Task) error {
					return progressHandler(context.Background(), task.Progress, t)
				})
			}
			if err != nil {
				return err
			}
		}
	}
	if err := c.broker.SubscribeForEvents(context.Background(), broker.TOPIC_SCHEDULED_JOB, func(ev any) {
		sj, ok := ev.(*tork.ScheduledJob)
		if !ok {
			log.Error().Msgf("error casting scheduled job: %v", ev)
			return
		}
		if err := c.onScheduledJob(context.Background(), sj); err != nil {
			log.Error().Err(err).Msgf("error handling scheduled job: %s", sj.ID)
		}
	}); err != nil {
		return err
	}
	go c.sendHeartbeats()
	return nil
}

func (c *Coordinator) taskHandler(handler task.HandlerFunc) task.HandlerFunc {
	onError := handlers.NewErrorHandler(c.ds, c.broker)
	return func(ctx context.Context, et task.EventType, t *tork.Task) error {
		err := handler(ctx, et, t)
		if err != nil {
			now := time.Now().UTC()
			t.FailedAt = &now
			t.State = tork.TaskStateFailed
			t.Error = err.Error()
			return onError(ctx, et, t)
		}
		return nil
	}
}

func (c *Coordinator) jobHandler(handler job.HandlerFunc) job.HandlerFunc {
	onError := handlers.NewJobHandler(c.ds, c.broker)
	return func(ctx context.Context, et job.EventType, j *tork.Job) error {
		err := handler(ctx, et, j)
		if err != nil {
			now := time.Now().UTC()
			j.FailedAt = &now
			j.State = tork.JobStateFailed
			return onError(ctx, et, j)
		}
		return nil
	}
}

func (c *Coordinator) Stop() error {
	log.Debug().Msgf("shutting down %s", c.Name)
	close(c.stop)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.broker.Shutdown(ctx); err != nil {
		return errors.Wrapf(err, "error shutting down broker")
	}
	if err := c.api.Shutdown(ctx); err != nil {
		return errors.Wrapf(err, "error shutting down API")
	}
	return nil
}

func (c *Coordinator) sendHeartbeats() {
	for {
		status := tork.NodeStatusUP
		hostname, err := os.Hostname()
		if err != nil {
			log.Error().Err(err).Msgf("failed to get hostname for coordinator %s", c.id)
		}
		cpuPercent := host.GetCPUPercent()
		err = c.broker.PublishHeartbeat(
			context.Background(),
			&tork.Node{
				ID:              c.id,
				Name:            c.Name,
				StartedAt:       c.startTime,
				Status:          status,
				CPUPercent:      cpuPercent,
				LastHeartbeatAt: time.Now().UTC(),
				Hostname:        hostname,
				Version:         tork.Version,
			},
		)
		if err != nil {
			log.Error().
				Err(err).
				Msgf("error publishing heartbeat for %s", c.id)
		}
		select {
		case <-c.stop:
			return
		case <-time.After(tork.HEARTBEAT_RATE):
		}
	}
}
