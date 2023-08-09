package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/tork/datastore"
	"github.com/tork/mq"
	"github.com/tork/node"
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
	ds     datastore.Datastore
	queues map[string]int
}

type Config struct {
	Broker    mq.Broker
	DataStore datastore.Datastore
	Address   string
	Queues    map[string]int
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
	if cfg.Queues[mq.QUEUE_HEARBEAT] < 1 {
		cfg.Queues[mq.QUEUE_HEARBEAT] = 1
	}
	return &Coordinator{
		Name:   name,
		api:    newAPI(cfg),
		broker: cfg.Broker,
		ds:     cfg.DataStore,
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
		if err := c.broker.PublishTask(ctx, qname, t); err != nil {
			return err
		}
		return c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) {
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
		return c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) {
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
		return c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) {
			u.State = task.Completed
			u.CompletedAt = t.CompletedAt
			u.Result = t.Result
		})
	}
}

func (c *Coordinator) taskFailedHandler(thread string) func(ctx context.Context, t *task.Task) error {
	return func(ctx context.Context, t *task.Task) error {
		log.Error().
			Str("task-id", t.ID).
			Str("task-error", t.Error).
			Str("thread", thread).
			Msg("received task failure")
		return c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) {
			u.State = task.Failed
			u.FailedAt = t.FailedAt
			u.Error = t.Error
		})
	}
}

func (c *Coordinator) handleHeartbeats(ctx context.Context, n *node.Node) error {
	n.LastHeartbeatAt = time.Now()
	_, err := c.ds.GetNodeByID(ctx, n.ID)
	if err == datastore.ErrNodeNotFound {
		log.Info().
			Str("node-id", n.ID).
			Msg("received first heartbeat")
		return c.ds.CreateNode(ctx, n)
	}
	return c.ds.UpdateNode(ctx, n.ID, func(u *node.Node) {
		log.Info().
			Str("node-id", n.ID).
			Float64("cpu-percent", n.CPUPercent).
			Msg("received heartbeat")
		u.LastHeartbeatAt = n.LastHeartbeatAt
		u.CPUPercent = n.CPUPercent
	})

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
			threadName := fmt.Sprintf("%s-%d", qname, i)
			var err error
			switch qname {
			case mq.QUEUE_PENDING:
				err = c.broker.SubscribeForTasks(qname, c.taskPendingHandler(threadName))
			case mq.QUEUE_COMPLETED:
				err = c.broker.SubscribeForTasks(qname, c.taskCompletedHandler(threadName))
			case mq.QUEUE_STARTED:
				err = c.broker.SubscribeForTasks(qname, c.taskStartedHandler(threadName))
			case mq.QUEUE_ERROR:
				err = c.broker.SubscribeForTasks(qname, c.taskFailedHandler(threadName))
			case mq.QUEUE_HEARBEAT:
				err = c.broker.SubscribeForHeartbeats(c.handleHeartbeats)
			}
			if err != nil {
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
