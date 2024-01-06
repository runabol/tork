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

	"github.com/runabol/tork/input"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/node"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/middleware/web"

	"github.com/runabol/tork/mq"

	"github.com/runabol/tork/internal/uuid"
)

// Coordinator is responsible for accepting tasks from
// clients, scheduling tasks for workers to execute and for
// exposing the cluster's state to the outside world.
type Coordinator struct {
	id          string
	startTime   time.Time
	Name        string
	broker      mq.Broker
	api         *api.API
	ds          datastore.Datastore
	queues      map[string]int
	onPending   task.HandlerFunc
	onStarted   task.HandlerFunc
	onError     task.HandlerFunc
	onJob       job.HandlerFunc
	onHeartbeat node.HandlerFunc
	onCompleted task.HandlerFunc
	onLogPart   func(*tork.TaskLogPart)
	stop        chan any
}

type Config struct {
	Name       string
	Broker     mq.Broker
	DataStore  datastore.Datastore
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
	Echo []echo.MiddlewareFunc
}

func NewCoordinator(cfg Config) (*Coordinator, error) {
	if cfg.Broker == nil {
		return nil, errors.New("most provide a broker")
	}
	if cfg.DataStore == nil {
		return nil, errors.New("most provide a datastore")
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
	if cfg.Queues[mq.QUEUE_COMPLETED] < 1 {
		cfg.Queues[mq.QUEUE_COMPLETED] = 1
	}
	if cfg.Queues[mq.QUEUE_ERROR] < 1 {
		cfg.Queues[mq.QUEUE_ERROR] = 1
	}
	if cfg.Queues[mq.QUEUE_PENDING] < 1 {
		cfg.Queues[mq.QUEUE_PENDING] = 1
	}
	if cfg.Queues[mq.QUEUE_STARTED] < 1 {
		cfg.Queues[mq.QUEUE_STARTED] = 1
	}
	if cfg.Queues[mq.QUEUE_HEARTBEAT] < 1 {
		cfg.Queues[mq.QUEUE_HEARTBEAT] = 1
	}
	if cfg.Queues[mq.QUEUE_JOBS] < 1 {
		cfg.Queues[mq.QUEUE_JOBS] = 1
	}
	if cfg.Queues[mq.QUEUE_LOGS] < 1 {
		cfg.Queues[mq.QUEUE_LOGS] = 1
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

	return &Coordinator{
		id:          uuid.NewShortUUID(),
		startTime:   time.Now(),
		Name:        cfg.Name,
		api:         api,
		broker:      cfg.Broker,
		ds:          cfg.DataStore,
		queues:      cfg.Queues,
		onPending:   onPending,
		onStarted:   onStarted,
		onError:     onError,
		onJob:       onJob,
		onHeartbeat: onHeartbeat,
		onCompleted: onCompleted,
		onLogPart:   onLogPart,
		stop:        make(chan any),
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
		if !mq.IsCoordinatorQueue(qname) {
			continue
		}
		for i := 0; i < conc; i++ {
			var err error
			switch qname {
			case mq.QUEUE_PENDING:
				err = c.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
					return c.onPending(context.Background(), task.StateChange, t)
				})
			case mq.QUEUE_COMPLETED:
				err = c.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
					return c.onCompleted(context.Background(), task.StateChange, t)
				})
			case mq.QUEUE_STARTED:
				err = c.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
					return c.onStarted(context.Background(), task.Started, t)
				})
			case mq.QUEUE_ERROR:
				err = c.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
					return c.onError(context.Background(), task.StateChange, t)
				})
			case mq.QUEUE_HEARTBEAT:
				err = c.broker.SubscribeForHeartbeats(func(n *tork.Node) error {
					return c.onHeartbeat(context.Background(), n)
				})
			case mq.QUEUE_JOBS:
				err = c.broker.SubscribeForJobs(func(j *tork.Job) error {
					return c.onJob(context.Background(), job.StateChange, j)
				})
			case mq.QUEUE_LOGS:
				err = c.broker.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {
					c.onLogPart(p)
				})
			}
			if err != nil {
				return err
			}
		}
	}
	go c.sendHeartbeats()
	return nil
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
