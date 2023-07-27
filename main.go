package main

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tork/broker"
	"github.com/tork/task"
	"github.com/tork/uuid"
	"github.com/tork/worker"
)

func main() {
	ctx := context.Background()

	// loggging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Info().Msg("Starting up")

	b := broker.NewInMemoryBroker()
	w, err := worker.NewWorker(b)
	if err != nil {
		panic(err)
	}
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

	b.Send(ctx, w.Name, t)

	time.Sleep(2 * time.Second)

	t.State = task.Cancelled

	err = b.Send(ctx, w.Name, t)

	if err != nil {
		panic(err)
	}

	time.Sleep(2 * time.Second)
}
