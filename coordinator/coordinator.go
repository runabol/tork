package coordinator

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/tork/datastore"
	"github.com/tork/job"
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
	if cfg.Queues[mq.QUEUE_JOBS] < 1 {
		cfg.Queues[mq.QUEUE_JOBS] = 1
	}
	return &Coordinator{
		Name:   name,
		api:    newAPI(cfg),
		broker: cfg.Broker,
		ds:     cfg.DataStore,
		queues: cfg.Queues,
	}
}

func (c *Coordinator) handlePendingTask(ctx context.Context, t task.Task) error {
	log.Info().
		Str("task-id", t.ID).
		Msg("routing task")
	qname := t.Queue
	if qname == "" {
		qname = mq.QUEUE_DEFAULT
	}
	t.State = task.Scheduled
	n := time.Now()
	t.ScheduledAt = &n
	t.State = task.Scheduled
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		u.State = t.State
		u.ScheduledAt = t.ScheduledAt
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	return c.broker.PublishTask(ctx, qname, t)
}

func (c *Coordinator) handleStartedTask(ctx context.Context, t task.Task) error {
	log.Debug().
		Str("task-id", t.ID).
		Msg("received task start")
	return c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		// we don't want to mark the task as RUNNING
		// if an out-of-order task completion/failure
		// arrived earlier
		if u.State == task.Scheduled {
			u.State = t.State
			u.StartedAt = t.StartedAt
			u.Node = t.Node
		}
		return nil
	})
}

func (c *Coordinator) handleCompletedTask(ctx context.Context, t task.Task) error {
	log.Debug().Str("task-id", t.ID).Msg("received task completion")
	// update task in DB
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		u.State = task.Completed
		u.CompletedAt = t.CompletedAt
		u.Result = t.Result
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	// update job in DB
	if err := c.ds.UpdateJob(ctx, t.JobID, func(u *job.Job) error {
		u.Position = t.Position + 1
		if u.Position > len(u.Tasks) {
			now := time.Now()
			u.State = job.Completed
			u.CompletedAt = &now
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating job in datastore")
	}
	// fire the next task (if the job isn't completed)
	j, err := c.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return errors.Wrapf(err, "error getting job from datatstore")
	}
	if j.State == job.Running {
		now := time.Now()
		next := j.Tasks[j.Position-1]
		next.ID = uuid.NewUUID()
		next.JobID = j.ID
		next.State = task.Pending
		next.Position = j.Position
		next.CreatedAt = &now
		if err := c.ds.CreateTask(ctx, next); err != nil {
			return err
		}
		return c.handlePendingTask(ctx, next)
	}
	return nil
}

func (c *Coordinator) handleFailedTask(ctx context.Context, t task.Task) error {
	log.Error().
		Str("task-id", t.ID).
		Str("task-error", t.Error).
		Msg("received task failure")
	if t.Retry != nil && t.Retry.Attempts < t.Retry.Limit {
		// create a new retry task
		rt := t
		rt.ID = uuid.NewUUID()
		rt.Retry.Attempts = rt.Retry.Attempts + 1
		rt.State = task.Pending
		rt.Error = ""
		if err := c.ds.CreateTask(ctx, rt); err != nil {
			return errors.Wrapf(err, "error creating a retry task")
		}
		dur, err := time.ParseDuration(rt.Retry.InitialDelay)
		if err != nil {
			return errors.Wrapf(err, "invalid retry.initialDelay: %s", rt.Retry.InitialDelay)
		}
		go func() {
			delay := dur * time.Duration(math.Pow(float64(rt.Retry.ScalingFactor), float64(rt.Retry.Attempts-1)))
			log.Debug().Msgf("delaying retry %s", delay)
			time.Sleep(delay)
			if err := c.broker.PublishTask(ctx, mq.QUEUE_PENDING, rt); err != nil {
				log.Error().Err(err).Msg("error publishing retry task")
			}
		}()
	} else {
		// mark the job as FAILED
		if err := c.ds.UpdateJob(ctx, t.JobID, func(u *job.Job) error {
			u.State = job.Failed
			u.FailedAt = t.FailedAt
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error marking the job as failed in the datastore")
		}
	}
	// mark the task as FAILED
	return c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		u.State = task.Failed
		u.FailedAt = t.FailedAt
		u.Error = t.Error
		return nil
	})
}

func (c *Coordinator) handleHeartbeats(ctx context.Context, n node.Node) error {
	n.LastHeartbeatAt = time.Now()
	_, err := c.ds.GetNodeByID(ctx, n.ID)
	if err == datastore.ErrNodeNotFound {
		log.Info().
			Str("node-id", n.ID).
			Msg("received first heartbeat")
		return c.ds.CreateNode(ctx, n)
	}
	return c.ds.UpdateNode(ctx, n.ID, func(u *node.Node) error {
		log.Info().
			Str("node-id", n.ID).
			Float64("cpu-percent", n.CPUPercent).
			Msg("received heartbeat")
		u.LastHeartbeatAt = n.LastHeartbeatAt
		u.CPUPercent = n.CPUPercent
		return nil
	})

}

func (c *Coordinator) handleJobs(ctx context.Context, j job.Job) error {
	log.Debug().Msgf("starting job %s", j.ID)
	now := time.Now()
	t := j.Tasks[0]
	t.ID = uuid.NewUUID()
	t.JobID = j.ID
	t.State = task.Pending
	t.Position = 1
	t.CreatedAt = &now
	if err := c.ds.CreateTask(ctx, t); err != nil {
		return err
	}
	if err := c.ds.UpdateJob(ctx, j.ID, func(u *job.Job) error {
		n := time.Now()
		u.State = job.Running
		u.StartedAt = &n
		return nil
	}); err != nil {
		return err
	}
	return c.handlePendingTask(ctx, t)
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
			var err error
			switch qname {
			case mq.QUEUE_PENDING:
				err = c.broker.SubscribeForTasks(qname, c.handlePendingTask)
			case mq.QUEUE_COMPLETED:
				err = c.broker.SubscribeForTasks(qname, c.handleCompletedTask)
			case mq.QUEUE_STARTED:
				err = c.broker.SubscribeForTasks(qname, c.handleStartedTask)
			case mq.QUEUE_ERROR:
				err = c.broker.SubscribeForTasks(qname, c.handleFailedTask)
			case mq.QUEUE_HEARBEAT:
				err = c.broker.SubscribeForHeartbeats(c.handleHeartbeats)
			case mq.QUEUE_JOBS:
				err = c.broker.SubscribeForJobs(c.handleJobs)
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
