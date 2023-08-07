package main

import (
	"log"
	"os"

	"github.com/rs/zerolog"
	"github.com/tork/coordinator"
	"github.com/tork/datastore"
	"github.com/tork/mq"
	"github.com/tork/runtime"
	"github.com/tork/worker"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:        "tork",
		Description: "a distributed workflow engine",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "mode",
				Value:    "standalone",
				Usage:    "standalone|worker|coordinator",
				Required: true,
			},
		},
		Action: func(ctx *cli.Context) error {
			// loggging
			zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

			// create a broker
			b := mq.NewInMemoryBroker()

			// create a Docker-based runtime
			rt, err := runtime.NewDockerRuntime()
			if err != nil {
				return err
			}

			// create a worker
			w := worker.NewWorker(worker.Config{
				Broker:  b,
				Runtime: rt,
			})

			// start the worker
			go func() {
				if err := w.Start(); err != nil {
					panic(err)
				}
			}()

			// create a coordinator
			c := coordinator.NewCoordinator(coordinator.Config{
				Broker:        b,
				TaskDataStore: datastore.NewInMemoryDatastore(),
			})

			// start the coordinator
			if err := c.Start(); err != nil {
				return err
			}

			return nil
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
