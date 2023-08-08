package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tork/coordinator"
	"github.com/tork/datastore"
	"github.com/tork/mq"
	"github.com/tork/runtime"
	"github.com/tork/worker"
	"github.com/urfave/cli/v2"
)

type mode string

const (
	MODE_STANDALONE  mode = "standalone"
	MODE_COORDINATOR mode = "coordinator"
	MODE_WORKER      mode = "worker"
)

func main() {
	app := &cli.App{
		Name:        "tork",
		Description: "a distributed workflow engine",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "mode",
				Usage:    "standalone|worker|coordinator",
				Required: true,
			},
			&cli.StringSliceFlag{
				Name:  "queue",
				Usage: "<queuename>:<concurrency>",
			},
			&cli.StringFlag{
				Name:  "broker",
				Usage: "inmemory|rabbitmq",
				Value: "inmemory",
			},
			&cli.StringFlag{
				Name:  "rabbitmq-url",
				Usage: "amqp://<username>:<password>@<hostname>:<port>/",
				Value: "amqp://guest:guest@localhost:5672/",
			},
		},
		Action: func(ctx *cli.Context) error {
			// loggging
			zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

			md := mode(ctx.String("mode"))
			if md != MODE_STANDALONE && md != MODE_WORKER && md != MODE_COORDINATOR {
				return errors.Errorf("invalid mode: %s", md)
			}

			bk := ctx.String("broker")
			var b mq.Broker
			switch bk {
			case "inmemory":
				b = mq.NewInMemoryBroker()
			case "rabbitmq":
				rb, err := mq.NewRabbitMQBroker(ctx.String("rabbitmq-url"))
				if err != nil {
					return errors.Wrapf(err, "unable to connect to RabbitMQ")
				}
				b = rb
			default:
				return errors.Errorf("invalid broker type: %s", bk)
			}

			// parse queue definitions
			qs := ctx.StringSlice("queue")
			queues := make(map[string]int)
			for _, q := range qs {
				def := strings.Split(q, ":")
				qname := def[0]
				conc, err := strconv.Atoi(def[1])
				if err != nil {
					return errors.Errorf("invalid queue definition: %s", q)
				}
				queues[qname] = conc
			}

			// start the worker
			var w *worker.Worker
			if md == MODE_WORKER || md == MODE_STANDALONE {
				rt, err := runtime.NewDockerRuntime()
				if err != nil {
					return err
				}
				w = worker.NewWorker(worker.Config{
					Broker:  b,
					Runtime: rt,
					Queues:  queues,
				})
				if err := w.Start(); err != nil {
					return err
				}
			}

			// start the coordinator
			var c *coordinator.Coordinator
			if md == MODE_COORDINATOR || md == MODE_STANDALONE {
				c = coordinator.NewCoordinator(coordinator.Config{
					Broker:        b,
					TaskDataStore: datastore.NewInMemoryDatastore(),
					Queues:        queues,
				})
				if err := c.Start(); err != nil {
					return err
				}
			}

			// wait for the termination signal
			// so we can do a clean shutdown
			quit := make(chan os.Signal, 1)
			signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
			<-quit
			log.Debug().Msg("shutting down")
			if w != nil {
				w.Stop()
			}
			if c != nil {
				c.Stop()
			}

			return nil
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
