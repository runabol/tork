package coordinator

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/tork/datastore"
	"github.com/tork/mq"
	"github.com/tork/task"
	"github.com/tork/uuid"
)

// Coordinator is the responsible for accepting tasks from
// clients, scheduling tasks for workers to execute and for
// exposing the cluster's state to the outside world.
type Coordinator struct {
	Name      string
	broker    mq.Broker
	scheduler Scheduler
	api       *api
	ds        datastore.TaskDatastore
}

type Config struct {
	Scheduler     Scheduler
	Broker        mq.Broker
	TaskDataStore datastore.TaskDatastore
	Address       string
}

func NewCoordinator(cfg Config) *Coordinator {
	name := fmt.Sprintf("coordinator-%s", uuid.NewUUID())
	return &Coordinator{
		Name:      name,
		api:       newAPI(cfg),
		broker:    cfg.Broker,
		scheduler: cfg.Scheduler,
		ds:        cfg.TaskDataStore,
	}
}

func (c *Coordinator) handlePendingTask(ctx context.Context, t *task.Task) error {
	if err := c.scheduler.Schedule(ctx, t); err != nil {
		return err
	}
	return c.ds.Update(ctx, t.ID, func(u *task.Task) {
		// we don't want to mark the task as SCHEDULED
		// if an out-of-order task completion/failure
		// arrived earlier
		if u.State == task.Pending {
			u.State = t.State
			u.ScheduledAt = t.ScheduledAt
		}
	})
}

func (c *Coordinator) handleStartedTask(ctx context.Context, t *task.Task) error {
	log.Debug().
		Str("task-id", t.ID).
		Msg("received task start")
	return c.ds.Update(ctx, t.ID, func(u *task.Task) {
		// we don't want to mark the task as RUNNING
		// if an out-of-order task completion/failure
		// arrived earlier
		if u.State == task.Scheduled {
			u.State = t.State
			u.StartedAt = t.StartedAt
		}
	})
}

func (c *Coordinator) handleCompletedTask(ctx context.Context, t *task.Task) error {
	log.Debug().
		Str("task-id", t.ID).
		Msg("received task completion")
	return c.ds.Save(ctx, t)
}

func (c *Coordinator) handleFailedTask(ctx context.Context, t *task.Task) error {
	log.Error().
		Str("task-id", t.ID).
		Str("task-error", t.Error).
		Msg("received task failure")
	return c.ds.Save(ctx, t)
}

func (c *Coordinator) Start() error {
	log.Info().Msgf("starting %s", c.Name)
	// start the coordinator API
	if err := c.api.start(); err != nil {
		return err
	}
	// subscribe for the pending tasks queue
	if err := c.broker.Subscribe(mq.QUEUE_PENDING, c.handlePendingTask); err != nil {
		return err
	}
	// subscribe for task completions queue
	if err := c.broker.Subscribe(mq.QUEUE_COMPLETED, c.handleCompletedTask); err != nil {
		return err
	}
	// subscribe for failed tasks notifications
	if err := c.broker.Subscribe(mq.QUEUE_ERROR, c.handleFailedTask); err != nil {
		return err
	}
	// subscribe for starting tasks notifications
	if err := c.broker.Subscribe(mq.QUEUE_STARTED, c.handleStartedTask); err != nil {
		return err
	}
	// listen for termination signal
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
