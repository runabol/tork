package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/coordinator"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/runtime"
	"github.com/runabol/tork/worker"
	"github.com/urfave/cli/v2"
)

func main() {
	fmt.Println(getBanner())

	app := &cli.App{
		Name:  "tork",
		Usage: "a distributed workflow engine",
		Commands: []*cli.Command{
			{
				Name:  "coordinator",
				Usage: "run the coordinator",
				Flags: []cli.Flag{
					queueFlag(),
					brokerFlag(),
					rabbitmqURLFlag(),
					datastoreFlag(),
					postgresDSNFlag(),
					defaultCPUsLimit(),
					defaultMemoryLimit(),
					tempDirFlag(),
					coordinatorAddressFlag(),
					logLevel(),
					logFormat(),
				},
				Action: runCoordinator,
			},
			{
				Name:  "worker",
				Usage: "run a worker",
				Flags: []cli.Flag{
					queueFlag(),
					brokerFlag(),
					rabbitmqURLFlag(),
					defaultCPUsLimit(),
					defaultMemoryLimit(),
					tempDirFlag(),
					workerAddressFlag(),
					logLevel(),
					logFormat(),
				},
				Action: runWorker,
			},
			{
				Name:  "standalone",
				Usage: "run the coordinator and a worker",
				Flags: []cli.Flag{
					queueFlag(),
					brokerFlag(),
					rabbitmqURLFlag(),
					datastoreFlag(),
					postgresDSNFlag(),
					defaultCPUsLimit(),
					defaultMemoryLimit(),
					tempDirFlag(),
					logLevel(),
					logFormat(),
				},
				Action: runStandalone,
			},
			{
				Name:  "migration",
				Usage: "run the db migration script",
				Flags: []cli.Flag{
					datastoreFlag(),
					postgresDSNFlag(),
					logLevel(),
				},
				Action: runMigration,
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runCoordinator(ctx *cli.Context) error {
	if err := setupLogging(ctx); err != nil {
		return err
	}

	broker, err := createBroker(ctx)
	if err != nil {
		return err
	}

	ds, err := createDatastore(ctx)
	if err != nil {
		return err
	}

	c, err := createCoordinator(broker, ds, ctx)
	if err != nil {
		return err
	}

	// wait for the termination signal
	// so we can do a clean shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Debug().Msg("shutting down")
	if c != nil {
		if err := c.Stop(); err != nil {
			log.Error().Err(err).Msg("error stopping coordinator")
		}
	}
	return nil
}

func runWorker(ctx *cli.Context) error {
	if err := setupLogging(ctx); err != nil {
		return err
	}

	broker, err := createBroker(ctx)
	if err != nil {
		return err
	}

	w, err := createWorker(broker, ctx)
	if err != nil {
		return err
	}

	// wait for the termination signal
	// so we can do a clean shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Debug().Msg("shutting down")
	if w != nil {
		if err := w.Stop(); err != nil {
			log.Error().Err(err).Msg("error stopping worker")
		}
	}

	return nil
}

func runStandalone(ctx *cli.Context) error {
	if err := setupLogging(ctx); err != nil {
		return err
	}

	broker, err := createBroker(ctx)
	if err != nil {
		return err
	}

	ds, err := createDatastore(ctx)
	if err != nil {
		return err
	}

	w, err := createWorker(broker, ctx)
	if err != nil {
		return err
	}
	c, err := createCoordinator(broker, ds, ctx)
	if err != nil {
		return err
	}

	// wait for the termination signal
	// so we can do a clean shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
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

func runMigration(ctx *cli.Context) error {
	if err := setupLogging(ctx); err != nil {
		return err
	}

	ds, err := createDatastore(ctx)
	if err != nil {
		return err
	}
	dstype := ctx.String("datastore")
	switch dstype {
	case datastore.DATASTORE_POSTGRES:
		if err := ds.(*datastore.PostgresDatastore).ExecScript("db/postgres/schema.sql"); err != nil {
			return errors.Wrapf(err, "error when trying to create db schema")
		}
	default:
		return errors.Errorf("can't perform db migration on: %s", dstype)
	}
	log.Info().Msg("migration completed!")
	return nil
}

func createDatastore(ctx *cli.Context) (datastore.Datastore, error) {
	dsname := ctx.String("datastore")
	var ds datastore.Datastore
	switch dsname {
	case datastore.DATASTORE_INMEMORY:
		ds = datastore.NewInMemoryDatastore()
	case datastore.DATASTORE_POSTGRES:
		pg, err := datastore.NewPostgresDataStore(ctx.String("postgres-dsn"))
		if err != nil {
			return nil, err
		}
		ds = pg
	default:
		return nil, errors.Errorf("unknown datastore type: %s", dsname)
	}
	return ds, nil
}

func createBroker(ctx *cli.Context) (mq.Broker, error) {
	var b mq.Broker
	bt := ctx.String("broker")
	switch bt {
	case "inmemory":
		b = mq.NewInMemoryBroker()
	case "rabbitmq":
		rb, err := mq.NewRabbitMQBroker(ctx.String("rabbitmq-url"))
		if err != nil {
			return nil, errors.Wrapf(err, "unable to connect to RabbitMQ")
		}
		b = rb
	default:
		return nil, errors.Errorf("invalid broker type: %s", bt)
	}
	return b, nil
}

func createCoordinator(broker mq.Broker, ds datastore.Datastore, ctx *cli.Context) (*coordinator.Coordinator, error) {
	queues, err := parseQueueConfig(ctx)
	if err != nil {
		return nil, err
	}
	c, err := coordinator.NewCoordinator(coordinator.Config{
		Broker:    broker,
		DataStore: ds,
		Queues:    queues,
		Address:   ctx.String("address"),
	})
	if err != nil {
		return nil, errors.Wrap(err, "error creating the coordinator")
	}
	if err := c.Start(); err != nil {
		return nil, err
	}
	return c, nil
}

func createWorker(b mq.Broker, ctx *cli.Context) (*worker.Worker, error) {
	queues, err := parseQueueConfig(ctx)
	if err != nil {
		return nil, err
	}
	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		return nil, err
	}
	w, err := worker.NewWorker(worker.Config{
		Broker:  b,
		Runtime: rt,
		Queues:  queues,
		Limits: worker.Limits{
			DefaultCPUsLimit:   ctx.String("default-cpus-limit"),
			DefaultMemoryLimit: ctx.String("default-memory-limit"),
		},
		TempDir: ctx.String("temp-dir"),
		Address: ctx.String("address"),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error creating worker")
	}
	if err := w.Start(); err != nil {
		return nil, err
	}
	return w, nil
}

func parseQueueConfig(ctx *cli.Context) (map[string]int, error) {
	qs := ctx.StringSlice("queue")
	queues := make(map[string]int)
	for _, q := range qs {
		def := strings.Split(q, ":")
		qname := def[0]
		conc, err := strconv.Atoi(def[1])
		if err != nil {
			return nil, errors.Errorf("invalid queue definition: %s", q)
		}
		queues[qname] = conc
	}
	return queues, nil
}

func setupLogging(ctx *cli.Context) error {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	logLevel := strings.ToLower(ctx.String("log-level"))
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
	logFormat := strings.ToLower(ctx.String("log-format"))
	switch logFormat {
	case "pretty":
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	case "json":
		// default
	default:
		return errors.Errorf("invalid logging format: %s", logFormat)
	}
	return nil
}

func queueFlag() cli.Flag {
	return &cli.StringSliceFlag{
		Name:  "queue",
		Usage: "Specify a task queue configuration: <queuename>:<concurrency>",
	}
}

func brokerFlag() cli.Flag {
	allBrokerTypes := []string{
		mq.BROKER_INMEMORY,
		mq.BROKER_RABBITMQ,
	}
	return &cli.StringFlag{
		Name:  "broker",
		Usage: strings.Join(allBrokerTypes, "|"),
		Value: mq.BROKER_INMEMORY,
	}
}

func rabbitmqURLFlag() cli.Flag {
	return &cli.StringFlag{
		Name:  "rabbitmq-url",
		Usage: "amqp://<username>:<password>@<hostname>:<port>/",
		Value: "amqp://guest:guest@localhost:5672/",
	}
}

func defaultCPUsLimit() cli.Flag {
	return &cli.StringFlag{
		Name:  "default-cpus-limit",
		Usage: "The default CPUs limit for an executing task (e.g. 1). Default is no limit.",
		Value: "",
	}
}

func defaultMemoryLimit() cli.Flag {
	return &cli.StringFlag{
		Name:  "default-memory-limit",
		Usage: "The default RAM limit for an executing task (e.g. 6MB). Default is no limit.",
		Value: "",
	}
}

func datastoreFlag() cli.Flag {
	allDSTypes := []string{
		datastore.DATASTORE_INMEMORY,
		datastore.DATASTORE_POSTGRES,
	}
	return &cli.StringFlag{
		Name:  "datastore",
		Usage: strings.Join(allDSTypes, "|"),
		Value: datastore.DATASTORE_INMEMORY,
	}
}

func postgresDSNFlag() cli.Flag {
	return &cli.StringFlag{
		Name:  "postgres-dsn",
		Usage: "host=<hostname> user=<username> password=<username> dbname=<username> port=<port> sslmode=<disable|enable>",
		Value: "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable",
	}
}

func tempDirFlag() cli.Flag {
	return &cli.StringFlag{
		Name:  "temp-dir",
		Usage: "The temporary dir to use by the worker: (e.g. /tmp)",
	}
}

func workerAddressFlag() cli.Flag {
	return &cli.StringFlag{
		Name:  "address",
		Usage: "API Address",
		Value: ":8001",
	}
}

func coordinatorAddressFlag() cli.Flag {
	return &cli.StringFlag{
		Name:  "address",
		Usage: "REST API Address",
		Value: ":8000",
	}
}

func logLevel() cli.Flag {
	return &cli.StringFlag{
		Name:  "log-level",
		Usage: "Configure the logging level (debug|info|warn|error)",
		Value: "info",
	}
}

func logFormat() cli.Flag {
	return &cli.StringFlag{
		Name:  "log-format",
		Usage: "Configure the logging format (pretty|json)",
		Value: "pretty",
	}
}

func getBanner() string {
	return color.WhiteString(`
_______  _______  ______    ___   _ 
|       ||       ||    _ |  |   | | |
|_     _||   _   ||   | ||  |   |_| |
  |   |  |  | |  ||   |_||_ |      _|
  |   |  |  |_|  ||    __  ||     |_ 
  |   |  |       ||   |  | ||    _  |
  |___|  |_______||___|  |_||___| |_|
`)
}
