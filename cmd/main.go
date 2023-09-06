package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/fatih/color"
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
	"github.com/runabol/tork/version"
	"github.com/runabol/tork/worker"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "tork",
		Usage: "a distributed workflow engine",
		Flags: []cli.Flag{
			config(),
		},
		Before: func(ctx *cli.Context) error {
			if err := loadConfig(ctx); err != nil {
				return err
			}

			if err := setupLogging(ctx); err != nil {
				return err
			}

			displayBanner(ctx)

			return nil
		},
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "Run Tork",
				Subcommands: []*cli.Command{
					{
						Name:   "coordinator",
						Usage:  "Run the coordinator",
						Flags:  []cli.Flag{},
						Action: runCoordinator,
					},
					{
						Name:   "worker",
						Usage:  "Run a worker",
						Flags:  []cli.Flag{},
						Action: runWorker,
					},
					{
						Name:   "standalone",
						Usage:  "Run the coordinator and a worker",
						Flags:  []cli.Flag{},
						Action: runStandalone,
					},
				},
			},
			{
				Name:   "migration",
				Usage:  "Run the db migration script",
				Flags:  []cli.Flag{},
				Action: runMigration,
			},
			{
				Name:   "health",
				Usage:  "Perform a health check",
				Flags:  []cli.Flag{},
				Action: health,
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runCoordinator(_ *cli.Context) error {
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

func runWorker(_ *cli.Context) error {
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

func runStandalone(_ *cli.Context) error {
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

func health(_ *cli.Context) error {
	chk, err := http.Get(fmt.Sprintf("%s/health", conf.StringDefault("endpoint", "http://localhost:8000")))
	if err != nil {
		return err
	}
	if chk.StatusCode != http.StatusOK {
		return errors.Errorf("Health check failed. Status Code: %d", chk.StatusCode)
	}
	body, err := io.ReadAll(chk.Body)
	if err != nil {
		return errors.Wrapf(err, "error reading body")
	}

	type resp struct {
		Status string `json:"status"`
	}
	r := resp{}

	if err := json.Unmarshal(body, &r); err != nil {
		return errors.Wrapf(err, "error unmarshalling body")
	}

	fmt.Printf("Status: %s\n", r.Status)

	return nil
}

func runMigration(_ *cli.Context) error {
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

func loadConfig(ctx *cli.Context) error {
	if ctx.String("config") == "" {
		return conf.LoadConfig()
	}
	return conf.LoadConfig(ctx.String("config"))
}

func setupLogging(ctx *cli.Context) error {
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

func displayBanner(c *cli.Context) {
	mode := conf.StringDefault("cli.banner_mode", "console")
	if mode == "off" {
		return
	}
	banner := color.WhiteString(fmt.Sprintf(`
 _______  _______  ______    ___   _ 
|       ||       ||    _ |  |   | | |
|_     _||   _   ||   | ||  |   |_| |
  |   |  |  | |  ||   |_||_ |      _|
  |   |  |  |_|  ||    __  ||     |_ 
  |   |  |       ||   |  | ||    _  |
  |___|  |_______||___|  |_||___| |_|

 %s (%s)
`, version.Version, version.GitCommit))

	if mode == "console" {
		fmt.Println(banner)
	} else {
		log.Info().Msg(banner)
	}
}

func config() cli.Flag {
	return &cli.StringFlag{
		Name:  "config",
		Usage: "Set the location of the config file",
	}
}
