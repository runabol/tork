package engine

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/coordinator"
	"github.com/runabol/tork/internal/worker"

	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/request"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/runtime"
)

const (
	ModeCoordinator Mode = "coordinator"
	ModeWorker      Mode = "worker"
	ModeStandalone  Mode = "standalone"
)

type Mode string

type Engine struct {
	quit      chan os.Signal
	terminate chan any
	onStarted func() error
	requestmw []request.MiddlewareFunc
	taskmw    []task.MiddlewareFunc
	jobmw     []job.MiddlewareFunc
	endpoints map[string]request.HandlerFunc
	mode      Mode
}

func New(mode Mode) *Engine {
	return &Engine{
		quit:      make(chan os.Signal, 1),
		terminate: make(chan any, 1),
		onStarted: func() error { return nil },
		requestmw: make([]request.MiddlewareFunc, 0),
		taskmw:    make([]task.MiddlewareFunc, 0),
		endpoints: make(map[string]request.HandlerFunc, 0),
		mode:      mode,
	}
}

func (e *Engine) Start() error {
	switch e.mode {
	case ModeCoordinator:
		return e.runCoordinator()
	case ModeWorker:
		return e.runWorker()
	case ModeStandalone:
		return e.runStandalone()

	default:
		return errors.Errorf("Unknown mode: %s", e.mode)
	}
}

func (e *Engine) Terminate() {
	e.terminate <- 1
}

func (e *Engine) OnStarted(h OnStartedHandler) {
	e.onStarted = h
}

func (e *Engine) runCoordinator() error {
	broker, err := createBroker()
	if err != nil {
		return err
	}

	ds, err := createDatastore()
	if err != nil {
		return err
	}

	c, err := e.createCoordinator(broker, ds)
	if err != nil {
		return err
	}

	// trigger the on-started hook
	if err := e.onStarted(); err != nil {
		return errors.Wrapf(err, "error on-started hook")
	}

	e.awaitTerm()

	log.Debug().Msg("shutting down")
	if c != nil {
		if err := c.Stop(); err != nil {
			log.Error().Err(err).Msg("error stopping coordinator")
		}
	}
	return nil
}

func (e *Engine) runWorker() error {
	broker, err := createBroker()
	if err != nil {
		return err
	}

	w, err := createWorker(broker)
	if err != nil {
		return err
	}

	// trigger the on-started hook
	if err := e.onStarted(); err != nil {
		return errors.Wrapf(err, "error on-started hook")
	}

	e.awaitTerm()

	log.Debug().Msg("shutting down")
	if w != nil {
		if err := w.Stop(); err != nil {
			log.Error().Err(err).Msg("error stopping worker")
		}
	}

	return nil
}

func (e *Engine) runStandalone() error {
	broker, err := createBroker()
	if err != nil {
		return err
	}

	ds, err := createDatastore()
	if err != nil {
		return err
	}

	w, err := createWorker(broker)
	if err != nil {
		return err
	}
	c, err := e.createCoordinator(broker, ds)
	if err != nil {
		return err
	}

	// trigger the on-started hook
	if err := e.onStarted(); err != nil {
		return errors.Wrapf(err, "error on-started hook")
	}

	e.awaitTerm()

	log.Debug().Msg("shutting down")
	if w != nil {
		if err := w.Stop(); err != nil {
			log.Error().Err(err).Msg("error stopping worker")
		}
	}
	if c != nil {
		if err := c.Stop(); err != nil {
			log.Error().Err(err).Msg("error stopping coordinator")
		}
	}

	return nil
}

func createDatastore() (datastore.Datastore, error) {
	dstype := conf.StringDefault("datastore.type", datastore.DATASTORE_INMEMORY)
	var ds datastore.Datastore
	ds, err := datastore.NewFromProvider(dstype)
	if err != nil && !errors.Is(err, datastore.ErrProviderNotFound) {
		return nil, err
	}
	if ds != nil {
		return ds, nil
	}
	switch dstype {
	case datastore.DATASTORE_INMEMORY:
		ds = datastore.NewInMemoryDatastore()
	case datastore.DATASTORE_POSTGRES:
		dsn := conf.StringDefault(
			"datastore.postgres.dsn",
			"host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable",
		)
		pg, err := datastore.NewPostgresDataStore(dsn)
		if err != nil {
			return nil, err
		}
		ds = pg
	default:
		return nil, errors.Errorf("unknown datastore type: %s", dstype)
	}
	return ds, nil
}

func createBroker() (mq.Broker, error) {
	var b mq.Broker
	bt := conf.StringDefault("broker.type", mq.BROKER_INMEMORY)

	b, err := mq.NewFromProvider(bt)
	if err != nil && !errors.Is(err, mq.ErrProviderNotFound) {
		return nil, err
	}
	if b != nil {
		return b, nil
	}
	switch bt {
	case "inmemory":
		b = mq.NewInMemoryBroker()
	case "rabbitmq":
		rb, err := mq.NewRabbitMQBroker(conf.StringDefault("broker.rabbitmq.url", "amqp://guest:guest@localhost:5672/"))
		if err != nil {
			return nil, errors.Wrapf(err, "unable to connect to RabbitMQ")
		}
		b = rb
	default:
		return nil, errors.Errorf("invalid broker type: %s", bt)
	}
	return b, nil
}

func (e *Engine) createCoordinator(broker mq.Broker, ds datastore.Datastore) (*coordinator.Coordinator, error) {
	queues := conf.IntMap("coordinator.queues")
	c, err := coordinator.NewCoordinator(coordinator.Config{
		Broker:             broker,
		DataStore:          ds,
		Queues:             queues,
		Address:            conf.String("coordinator.address"),
		RequestMiddlewares: e.requestmw,
		Endpoints:          e.endpoints,
		Enabled:            conf.BoolMap("coordinator.api.endpoints"),
		TaskMiddlewares:    e.taskmw,
		JobMiddlewares:     e.jobmw,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error creating the coordinator")
	}
	if err := c.Start(); err != nil {
		return nil, err
	}
	return c, nil
}

func createWorker(b mq.Broker) (*worker.Worker, error) {
	queues := conf.IntMap("worker.queues")
	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		return nil, err
	}
	w, err := worker.NewWorker(worker.Config{
		Broker:  b,
		Runtime: rt,
		Queues:  queues,
		Limits: worker.Limits{
			DefaultCPUsLimit:   conf.String("worker.limits.cpus"),
			DefaultMemoryLimit: conf.String("worker.limits.memory"),
		},
		TempDir: conf.String("worker.tempdir"),
		Address: conf.String("worker.address"),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error creating worker")
	}
	if err := w.Start(); err != nil {
		return nil, err
	}
	return w, nil
}

func (e *Engine) awaitTerm() {
	signal.Notify(e.quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-e.quit:
	case <-e.terminate:
	}
}

func (e *Engine) RegisterRequestMiddleware(mw request.MiddlewareFunc) {
	e.requestmw = append(e.requestmw, mw)
}

func (e *Engine) RegisterTaskMiddleware(mw task.MiddlewareFunc) {
	e.taskmw = append(e.taskmw, mw)
}

func (e *Engine) RegisterJobMiddleware(mw job.MiddlewareFunc) {
	e.jobmw = append(e.jobmw, mw)
}

func (e *Engine) RegisterEndpoint(method, path string, handler request.HandlerFunc) {
	e.endpoints[fmt.Sprintf("%s %s", method, path)] = handler
}
