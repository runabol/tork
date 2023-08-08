package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/tork/datastore"
	"github.com/tork/mq"
	"github.com/tork/task"
	"github.com/tork/uuid"
)

// Coordinator is responsible for accepting tasks from
// clients, scheduling tasks for workers to execute and for
// exposing the cluster's state to the outside world.
type Coordinator struct {
	Name   string
	broker mq.Broker
	api    *api
	ds     datastore.TaskDatastore
	queues map[string]int
}

type Config struct {
	Broker        mq.Broker
	TaskDataStore datastore.TaskDatastore
	Address       string
	Queues        map[string]int
}

func NewCoordinator(cfg Config) *Coordinator {
	name := fmt.Sprintf("coordinator-%s", uuid.NewUUID())
	if len(cfg.Queues) == 0 {
		cfg.Queues = make(map[string]int)
	}
	if cfg.Queues[mq.QUEUE_COMPLETED] < 1 {
		cfg.Queues[mq.QUEUE_COMPLETED] = 1
	}
	if cfg.Queues[mq.QUEUE_ERROR] < 1 {
		cfg.Queues[mq.QUEUE_ERROR] = 1
	}
	if cfg.Queues[mq.QUEUE_PENDING] < 1 {
		cfg.Queues[mq.QUEUE_PENDING] = 1
	}
	if cfg.Queues[mq.QUEUE_STARTED] < 1 {
		cfg.Queues[mq.QUEUE_STARTED] = 1
	}
	return &Coordinator{
		Name:   name,
		api:    newAPI(cfg),
		broker: cfg.Broker,
		ds:     cfg.TaskDataStore,
		queues: cfg.Queues,
	}
}

func (c *Coordinator) taskPendingHandler(thread string) func(ctx context.Context, t *task.Task) error {
	return func(ctx context.Context, t *task.Task) error {
		log.Info().
			Str("task-id", t.ID).
			Str("thread", thread).
			Msg("routing task")
		qname := t.Queue
		if qname == "" {
			qname = mq.QUEUE_DEFAULT
		}
		t.State = task.Scheduled
		n := time.Now()
		t.ScheduledAt = &n
		t.State = task.Scheduled
		if err := c.broker.Publish(ctx, qname, t); err != nil {
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
}

func (c *Coordinator) taskStartedHandler(thread string) func(ctx context.Context, t *task.Task) error {
	return func(ctx context.Context, t *task.Task) error {
		log.Debug().
			Str("task-id", t.ID).
			Str("thread", thread).
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
}

func (c *Coordinator) taskCompletedHandler(thread string) func(ctx context.Context, t *task.Task) error {
	return func(ctx context.Context, t *task.Task) error {
		log.Debug().
			Str("task-id", t.ID).
			Str("thread", thread).
			Msg("received task completion")
		return c.ds.Save(ctx, t)
	}
}

func (c *Coordinator) taskFailedHandler(thread string) func(ctx context.Context, t *task.Task) error {
	return func(ctx context.Context, t *task.Task) error {
		log.Error().
			Str("task-id", t.ID).
			Str("task-error", t.Error).
			Str("thread", thread).
			Msg("received task failure")
		return c.ds.Save(ctx, t)
	}
}

func (c *Coordinator) Start() error {
	log.Info().Msgf("starting %s", c.Name)
	// start the coordinator API
	if err := c.api.start(); err != nil {
		return err
	}
	// subscribe to task queues
	for qname, conc := range c.queues {
		if !mq.IsCoordinatorQueue(qname) {
			continue
		}
		for i := 0; i < conc; i++ {
			var handler func(ctx context.Context, t *task.Task) error
			threadName := fmt.Sprintf("%s-%d", qname, i)
			switch qname {
			case mq.QUEUE_PENDING:
				handler = c.taskPendingHandler(threadName)
			case mq.QUEUE_COMPLETED:
				handler = c.taskCompletedHandler(threadName)
			case mq.QUEUE_STARTED:
				handler = c.taskStartedHandler(threadName)
			case mq.QUEUE_ERROR:
				handler = c.taskFailedHandler(threadName)
			}
			if err := c.broker.Subscribe(qname, handler); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Coordinator) Stop() error {
	log.Debug().Msgf("shutting down %s", c.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.api.shutdown(ctx); err != nil {
		return err
	}
	return nil
}
