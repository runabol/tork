package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/tork/datastore"
	"github.com/tork/eval"
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

func NewCoordinator(cfg Config) (*Coordinator, error) {
	if cfg.Broker == nil {
		return nil, errors.New("most provide a broker")
	}
	if cfg.DataStore == nil {
		return nil, errors.New("most provide a datastore")
	}
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
	}, nil
}

func (c *Coordinator) handlePendingTask(t task.Task) error {
	ctx := context.Background()
	log.Info().
		Str("task-id", t.ID).
		Msg("routing task")
	qname := t.Queue
	if qname == "" {
		qname = mq.QUEUE_DEFAULT
	}
	t.State = task.Scheduled
	n := time.Now().UTC()
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

func (c *Coordinator) handleStartedTask(t task.Task) error {
	ctx := context.Background()
	log.Debug().
		Str("task-id", t.ID).
		Msg("received task start")
	return c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		// we don't want to mark the task as RUNNING
		// if an out-of-order task completion/failure
		// arrived earlier
		if u.State == task.Scheduled {
			u.State = task.Running
			u.StartedAt = t.StartedAt
			u.Node = t.Node
		}
		return nil
	})
}

func (c *Coordinator) handleCompletedTask(t task.Task) error {
	ctx := context.Background()
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
			now := time.Now().UTC()
			u.State = job.Completed
			u.CompletedAt = &now
		}
		if t.Result != "" && t.Var != "" {
			if u.Context.Tasks == nil {
				u.Context.Tasks = make(map[string]string)
			}
			u.Context.Tasks[t.Var] = t.Result
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating job in datastore")
	}
	// fire the next task
	j, err := c.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return errors.Wrapf(err, "error getting job from datatstore")
	}
	if j.State == job.Running {
		now := time.Now().UTC()
		next := j.Tasks[j.Position-1]
		next.ID = uuid.NewUUID()
		next.JobID = j.ID
		next.State = task.Pending
		next.Position = j.Position
		next.CreatedAt = &now
		if err := eval.Evaluate(&next, j.Context); err != nil {
			return errors.Wrapf(err, "error evaluating task for job: %s", j.ID)
		}
		if err := c.ds.CreateTask(ctx, next); err != nil {
			return err
		}
		return c.handlePendingTask(next)
	}
	return nil
}

func (c *Coordinator) handleFailedTask(t task.Task) error {
	ctx := context.Background()
	j, err := c.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return errors.Wrapf(err, "unknown task: %s", t.ID)
	}
	log.Error().
		Str("task-id", t.ID).
		Str("task-error", t.Error).
		Str("task-state", string(t.State)).
		Msg("received task failure")
	if j.State == job.Running &&
		t.Retry != nil &&
		t.Retry.Attempts < t.Retry.Limit {
		// create a new retry task
		rt := t
		rt.ID = uuid.NewUUID()
		rt.Retry.Attempts = rt.Retry.Attempts + 1
		rt.State = task.Pending
		rt.Error = ""
		if err := c.ds.CreateTask(ctx, rt); err != nil {
			return errors.Wrapf(err, "error creating a retry task")
		}
		if err := c.broker.PublishTask(ctx, mq.QUEUE_PENDING, rt); err != nil {
			log.Error().Err(err).Msg("error publishing retry task")
		}
	} else {
		// mark the job as FAILED
		if err := c.ds.UpdateJob(ctx, t.JobID, func(u *job.Job) error {
			// we only want to make the job as FAILED
			// if it's actually running as opposed to
			// possibly being CANCELLED
			if u.State == job.Running {
				u.State = job.Failed
				u.FailedAt = t.FailedAt
			}
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error marking the job as failed in the datastore")
		}
	}
	// mark the task as FAILED
	return c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		if u.State.IsActive() {
			u.State = task.Failed
			u.FailedAt = t.FailedAt
			u.Error = t.Error
		}
		return nil
	})
}

func (c *Coordinator) handleHeartbeats(n node.Node) error {
	ctx := context.Background()
	n.LastHeartbeatAt = time.Now().UTC()
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
		u.LastHeartbeatAt = time.Now().UTC()
		u.CPUPercent = n.CPUPercent
		return nil
	})

}

func (c *Coordinator) handleJob(j job.Job) error {
	ctx := context.Background()
	log.Debug().Msgf("starting job %s", j.ID)
	now := time.Now().UTC()
	t := j.Tasks[0]
	t.ID = uuid.NewUUID()
	t.JobID = j.ID
	t.State = task.Pending
	t.Position = 1
	t.CreatedAt = &now
	if err := eval.Evaluate(&t, j.Context); err != nil {
		return errors.Wrapf(err, "error evaluating task")
	}
	if err := c.ds.CreateTask(ctx, t); err != nil {
		return err
	}
	if err := c.ds.UpdateJob(ctx, j.ID, func(u *job.Job) error {
		n := time.Now().UTC()
		u.State = job.Running
		u.StartedAt = &n
		return nil
	}); err != nil {
		return err
	}
	return c.handlePendingTask(t)
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
				err = c.broker.SubscribeForJobs(c.handleJob)
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
