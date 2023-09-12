package engine

import (
	"crypto/subtle"
	"os"
	"os/signal"
	"syscall"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/coordinator"
	"github.com/runabol/tork/internal/uuid"
	"golang.org/x/time/rate"

	"github.com/runabol/tork/internal/worker"
	"github.com/runabol/tork/pkg/conf"

	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/pkg/middleware/job"
	"github.com/runabol/tork/pkg/middleware/node"
	"github.com/runabol/tork/pkg/middleware/task"
	"github.com/runabol/tork/pkg/middleware/web"
	"github.com/runabol/tork/runtime"
)

const (
	ModeCoordinator Mode = "coordinator"
	ModeWorker      Mode = "worker"
	ModeStandalone  Mode = "standalone"
)

// OnStartedHandler a bootstrap hook that is
// called after Tork has finished starting up.
// If a non-nil error is returned it will
// terminate the bootstrap process.
type OnStartedHandler func() error

type Mode string

type Engine struct {
	quit      chan os.Signal
	terminate chan any
	onStarted func() error
	cfg       Config
}

type Config struct {
	Mode       Mode
	Middleware Middleware
	Endpoints  map[string]web.HandlerFunc
}

type Middleware struct {
	Web  []web.MiddlewareFunc
	Task []task.MiddlewareFunc
	Job  []job.MiddlewareFunc
	Node []node.MiddlewareFunc
}

func New(cfg Config) *Engine {
	if cfg.Endpoints == nil {
		cfg.Endpoints = make(map[string]web.HandlerFunc)
	}
	return &Engine{
		quit:      make(chan os.Signal, 1),
		terminate: make(chan any, 1),
		onStarted: func() error { return nil },
		cfg:       cfg,
	}
}

func (e *Engine) Start() error {
	switch e.cfg.Mode {
	case ModeCoordinator:
		return e.runCoordinator()
	case ModeWorker:
		return e.runWorker()
	case ModeStandalone:
		return e.runStandalone()

	default:
		return errors.Errorf("Unknown mode: %s", e.cfg.Mode)
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

	cfg := coordinator.Config{
		Broker:    broker,
		DataStore: ds,
		Queues:    queues,
		Address:   conf.String("coordinator.address"),
		Middleware: coordinator.Middleware{
			Web:  e.cfg.Middleware.Web,
			Task: e.cfg.Middleware.Task,
			Job:  e.cfg.Middleware.Job,
			Node: e.cfg.Middleware.Node,
			Echo: echoMiddleware(),
		},
		Endpoints: e.cfg.Endpoints,
		Enabled:   conf.BoolMap("coordinator.api.endpoints"),
	}

	c, err := coordinator.NewCoordinator(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "error creating the coordinator")
	}

	if err := c.Start(); err != nil {
		return nil, err
	}
	return c, nil
}

func echoMiddleware() []echo.MiddlewareFunc {
	mw := make([]echo.MiddlewareFunc, 0)
	// cors
	corsEnabled := conf.Bool("coordinator.api.middleware.cors.enabled")
	if corsEnabled {
		mw = append(mw, cors())
	}
	// basic auth
	basicAuthEnabled := conf.Bool("coordinator.api.middleware.basicauth.enabled")
	if basicAuthEnabled {
		mw = append(mw, basicAuth())
	}

	// rate limit
	rateLimitEnabled := conf.Bool("coordinator.api.middleware.ratelimit.enabled")
	if rateLimitEnabled {
		mw = append(mw, rateLimit())
	}

	return mw
}

func rateLimit() echo.MiddlewareFunc {
	rps := conf.IntDefault("coordinator.api.middleware.ratelimit.rps", 20)
	return middleware.RateLimiter(middleware.NewRateLimiterMemoryStore(rate.Limit(rps)))
}

func basicAuth() echo.MiddlewareFunc {
	username := conf.StringDefault("coordinator.api.middleware.basicauth.username", "tork")
	password := conf.String("coordinator.api.middleware.basicauth.password")
	if password == "" {
		password = uuid.NewUUID()
		log.Debug().Msgf("Basic Auth Password: %s", password)
	}
	return middleware.BasicAuth(func(user, pass string, ctx echo.Context) (bool, error) {
		if subtle.ConstantTimeCompare([]byte(user), []byte(username)) == 1 &&
			subtle.ConstantTimeCompare([]byte(pass), []byte(password)) == 1 {
			return true, nil
		}
		return false, nil
	})
}

func cors() echo.MiddlewareFunc {
	log.Debug().Msg("CORS middleware enabled")
	return middleware.CORSWithConfig(
		middleware.CORSConfig{
			AllowOrigins: conf.StringsDefault(
				"coordinator.api.middleware.cors.allow_origins",
				[]string{"*"},
			),
			AllowMethods: conf.StringsDefault(
				"coordinator.api.middleware.cors.allow_methods",
				[]string{"*"},
			),
			AllowHeaders: conf.StringsDefault(
				"coordinator.api.middleware.cors.allow_headers",
				[]string{"*"},
			),
			AllowCredentials: conf.Bool(
				"coordinator.api.middleware.cors.allow_credentials",
			),
			ExposeHeaders: conf.StringsDefault(
				"coordinator.api.middleware.cors.expose_headers",
				[]string{"*"},
			),
		},
	)
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
