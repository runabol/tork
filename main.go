package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/tork/broker"
	"github.com/tork/task"
	"github.com/tork/uuid"
	"github.com/tork/worker"
)

func main() {
	ctx := context.Background()

	// loggging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// create a broker
	b := broker.NewInMemoryBroker()

	// create a worker
	w := worker.NewWorker(worker.Config{Broker: b})

	// send a dummy task
	t := task.Task{
		ID:    uuid.NewUUID(),
		State: task.Pending,
		Name:  "test-container-1",
		Image: "postgres:13",
		Env: []string{
			"POSTGRES_USER=cube",
			"POSTGRES_PASSWORD=secret",
		},
	}
	err := b.Send(ctx, w.Name, t)
	if err != nil {
		panic(err)
	}

	// cancel the dummy task
	t.State = task.Cancelled
	err = b.Send(ctx, w.Name, t)
	if err != nil {
		panic(err)
	}

	// start the worker
	err = w.Start()
	if err != nil {
		panic(err)
	}
}
