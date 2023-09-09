package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/coordinator/api"
	"github.com/runabol/tork/internal/coordinator/handlers"
	"github.com/runabol/tork/middleware"

	"github.com/runabol/tork/mq"

	"github.com/runabol/tork/internal/uuid"
)

// Coordinator is responsible for accepting tasks from
// clients, scheduling tasks for workers to execute and for
// exposing the cluster's state to the outside world.
type Coordinator struct {
	Name        string
	broker      mq.Broker
	api         *api.API
	ds          datastore.Datastore
	queues      map[string]int
	onPending   tork.TaskHandler
	onStarted   tork.TaskHandler
	onError     tork.TaskHandler
	onJob       tork.JobHandler
	onHeartbeat tork.NodeHandler
	onCompleted tork.TaskHandler
}

type Config struct {
	Broker      mq.Broker
	DataStore   datastore.Datastore
	Address     string
	Queues      map[string]int
	Middlewares []middleware.MiddlewareFunc
	Endpoints   map[string]middleware.HandlerFunc
	Enabled     map[string]bool
}

func NewCoordinator(cfg Config) (*Coordinator, error) {
	if cfg.Broker == nil {
		return nil, errors.New("most provide a broker")
	}
	if cfg.DataStore == nil {
		return nil, errors.New("most provide a datastore")
	}
	name := fmt.Sprintf("coordinator-%s", uuid.NewUUID())
	if cfg.Queues == nil {
		cfg.Queues = make(map[string]int)
	}
	if cfg.Endpoints == nil {
		cfg.Endpoints = make(map[string]middleware.HandlerFunc)
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
	if cfg.Queues[mq.QUEUE_HEARBEAT] < 1 {
		cfg.Queues[mq.QUEUE_HEARBEAT] = 1
	}
	if cfg.Queues[mq.QUEUE_JOBS] < 1 {
		cfg.Queues[mq.QUEUE_JOBS] = 1
	}
	api, err := api.NewAPI(api.Config{
		Broker:      cfg.Broker,
		DataStore:   cfg.DataStore,
		Address:     cfg.Address,
		Middlewares: cfg.Middlewares,
		Endpoints:   cfg.Endpoints,
		Enabled:     cfg.Enabled,
	})
	if err != nil {
		return nil, err
	}

	return &Coordinator{
		Name:   name,
		api:    api,
		broker: cfg.Broker,
		ds:     cfg.DataStore,
		queues: cfg.Queues,
		onPending: handlers.NewPendingHandler(
			cfg.DataStore,
			cfg.Broker,
		),
		onStarted: handlers.NewStartedHandler(
			cfg.DataStore,
			cfg.Broker,
		),
		onError: handlers.NewErrorHandler(
			cfg.DataStore,
			cfg.Broker,
		),
		onJob: handlers.NewJobHandler(
			cfg.DataStore,
			cfg.Broker,
		),
		onHeartbeat: handlers.NewHeartbeatHandler(
			cfg.DataStore,
		),
		onCompleted: handlers.NewCompletedHandler(
			cfg.DataStore,
			cfg.Broker,
		),
	}, nil
}

func (c *Coordinator) Start() error {
	log.Info().Msgf("starting %s", c.Name)
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
					ctx := context.Background()
					return c.onPending(ctx, t)
				})
			case mq.QUEUE_COMPLETED:
				err = c.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
					ctx := context.Background()
					return c.onCompleted(ctx, t)
				})
			case mq.QUEUE_STARTED:
				err = c.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
					ctx := context.Background()
					return c.onStarted(ctx, t)
				})
			case mq.QUEUE_ERROR:
				err = c.broker.SubscribeForTasks(qname, func(t *tork.Task) error {
					ctx := context.Background()
					return c.onError(ctx, t)
				})
			case mq.QUEUE_HEARBEAT:
				err = c.broker.SubscribeForHeartbeats(func(n tork.Node) error {
					ctx := context.Background()
					return c.onHeartbeat(ctx, n)
				})
			case mq.QUEUE_JOBS:
				err = c.broker.SubscribeForJobs(func(j *tork.Job) error {
					ctx := context.Background()
					return c.onJob(ctx, j)
				})
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Coordinator) Stop() error {
	log.Debug().Msgf("shutting down %s", c.Name)
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
