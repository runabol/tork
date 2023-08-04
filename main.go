package main

import (
	"github.com/rs/zerolog"
	"github.com/tork/broker"
	"github.com/tork/runtime"
	"github.com/tork/worker"
)

func main() {
	// loggging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// create a broker
	b := broker.NewInMemoryBroker()

	// create a Docker-based runtime
	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		panic(err)
	}

	// create a worker
	w := worker.NewWorker(worker.Config{
		Broker:  b,
		Runtime: rt,
	})

	// start the worker
	if err := w.Start(); err != nil {
		panic(err)
	}
}
