package coordinator

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/tork/broker"
	"github.com/tork/task"
	"github.com/tork/uuid"
)

// Coordinator is the responsible for accepting tasks from
// clients, scheduling tasks for workers to execute and for
// exposing the cluster's state to the outside world.
type Coordinator struct {
	Name      string
	broker    broker.Broker
	scheduler Scheduler
	api       *api
}

type Config struct {
	Scheduler Scheduler
	Broker    broker.Broker
	Address   string
}

func NewCoordinator(cfg Config) *Coordinator {
	name := fmt.Sprintf("coordinator-%s", uuid.NewUUID())
	return &Coordinator{
		Name:      name,
		api:       newAPI(cfg),
		broker:    cfg.Broker,
		scheduler: cfg.Scheduler,
	}
}

func (c *Coordinator) handlePendingTask(ctx context.Context, t task.Task) error {
	t.ID = uuid.NewUUID()
	if err := c.scheduler.Schedule(ctx, t); err != nil {
		return err
	}
	n := time.Now()
	t.ScheduledAt = &n
	t.State = task.Scheduled
	return nil
}

func (c *Coordinator) Start() error {
	log.Info().Msgf("starting %s", c.Name)
	if err := c.api.start(); err != nil {
		return err
	}
	c.broker.Subscribe(broker.QUEUE_PENDING, c.handlePendingTask)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Debug().Msgf("shutting down %s", c.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.api.shutdown(ctx); err != nil {
		return err
	}
	return nil
}
