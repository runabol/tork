package bootstrap

import (
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/coordinator"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/db/postgres"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/runtime"
	"github.com/runabol/tork/signals"
	"github.com/runabol/tork/worker"
)

type Mode string

const (
	ModeCoordinator Mode = "coordinator"
	ModeWorker      Mode = "worker"
	ModeStandalone  Mode = "standalone"
	ModeMigration   Mode = "migration"
)

func Start(mode Mode) error {
	if err := setupLogging(); err != nil {
		return err
	}

	switch mode {
	case ModeCoordinator:
		return runCoordinator()
	case ModeWorker:
		return runWorker()
	case ModeStandalone:
		return runStandalone()
	case ModeMigration:
		return runMigration()
	default:
		return errors.Errorf("Unknown mode: %s", mode)
	}
}

func runCoordinator() error {
	broker, err := createBroker()
	if err != nil {
		return err
	}

	ds, err := createDatastore()
	if err != nil {
		return err
	}

	c, err := createCoordinator(broker, ds)
	if err != nil {
		return err
	}

	signals.AwaitTerm()

	log.Debug().Msg("shutting down")
	if c != nil {
		if err := c.Stop(); err != nil {
			log.Error().Err(err).Msg("error stopping coordinator")
		}
	}
	return nil
}

func runWorker() error {
	broker, err := createBroker()
	if err != nil {
		return err
	}

	w, err := createWorker(broker)
	if err != nil {
		return err
	}

	signals.AwaitTerm()

	log.Debug().Msg("shutting down")
	if w != nil {
		if err := w.Stop(); err != nil {
			log.Error().Err(err).Msg("error stopping worker")
		}
	}

	return nil
}

func runStandalone() error {
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
	c, err := createCoordinator(broker, ds)
	if err != nil {
		return err
	}

	signals.AwaitTerm()

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

func createCoordinator(broker mq.Broker, ds datastore.Datastore) (*coordinator.Coordinator, error) {
	queues := conf.IntMap("coordinator.queues")
	c, err := coordinator.NewCoordinator(coordinator.Config{
		Broker:    broker,
		DataStore: ds,
		Queues:    queues,
		Address:   conf.String("coordinator.address"),
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
		TempDir: conf.String("worker.temp_dir"),
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

func setupLogging() error {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	logLevel := strings.ToLower(conf.StringDefault("logging.level", "debug"))
	// setup log level
	switch logLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn", "warning":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		return errors.Errorf("invalid logging level: %s", logLevel)
	}
	// setup log format (pretty / json)
	logFormat := strings.ToLower(conf.StringDefault("logging.format", "pretty"))
	switch logFormat {
	case "pretty":
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	case "json":
		log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	default:
		return errors.Errorf("invalid logging format: %s", logFormat)
	}
	return nil
}

func runMigration() error {
	ds, err := createDatastore()
	if err != nil {
		return err
	}
	dstype := conf.StringDefault("datastore.type", datastore.DATASTORE_INMEMORY)
	switch dstype {
	case datastore.DATASTORE_POSTGRES:
		if err := ds.(*datastore.PostgresDatastore).ExecScript(postgres.SCHEMA); err != nil {
			return errors.Wrapf(err, "error when trying to create db schema")
		}
	default:
		return errors.Errorf("can't perform db migration on: %s", dstype)
	}
	log.Info().Msg("migration completed!")
	return nil
}
