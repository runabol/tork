package coordinator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/coordinator/api"
	"github.com/runabol/tork/internal/coordinator/scheduler"
	"github.com/runabol/tork/internal/eval"
	"github.com/runabol/tork/middleware"

	"github.com/runabol/tork/mq"

	"github.com/runabol/tork/internal/uuid"
)

// Coordinator is responsible for accepting tasks from
// clients, scheduling tasks for workers to execute and for
// exposing the cluster's state to the outside world.
type Coordinator struct {
	Name   string
	broker mq.Broker
	api    *api.API
	ds     datastore.Datastore
	queues map[string]int
	sched  *scheduler.Scheduler
}

type Config struct {
	Broker      mq.Broker
	DataStore   datastore.Datastore
	Address     string
	Queues      map[string]int
	Middlewares []middleware.MiddlewareFunc
	Endpoints   map[string]middleware.HandlerFunc
	Enabled     map[string]bool
}

func NewCoordinator(cfg Config) (*Coordinator, error) {
	if cfg.Broker == nil {
		return nil, errors.New("most provide a broker")
	}
	if cfg.DataStore == nil {
		return nil, errors.New("most provide a datastore")
	}
	name := fmt.Sprintf("coordinator-%s", uuid.NewUUID())
	if cfg.Queues == nil {
		cfg.Queues = make(map[string]int)
	}
	if cfg.Endpoints == nil {
		cfg.Endpoints = make(map[string]middleware.HandlerFunc)
	}
	if cfg.Enabled == nil {
		cfg.Enabled = make(map[string]bool)
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
	api, err := api.NewAPI(api.Config{
		Broker:      cfg.Broker,
		DataStore:   cfg.DataStore,
		Address:     cfg.Address,
		Middlewares: cfg.Middlewares,
		Endpoints:   cfg.Endpoints,
		Enabled:     cfg.Enabled,
	})
	if err != nil {
		return nil, err
	}
	return &Coordinator{
		Name:   name,
		api:    api,
		broker: cfg.Broker,
		ds:     cfg.DataStore,
		queues: cfg.Queues,
		sched: scheduler.NewScheduler(
			cfg.DataStore,
			cfg.Broker,
		),
	}, nil
}

func (c *Coordinator) handleTask(t *tork.Task) error {
	switch t.State {
	case tork.TaskStatePending:
		return c.handlePendingTask(t)
	case tork.TaskStateFailed:
		return c.handleFailedTask(t)
	default:
		return errors.Errorf("could not handle task %s. unknown state: %s", t.ID, t.State)
	}
}

func (c *Coordinator) handlePendingTask(t *tork.Task) error {
	ctx := context.Background()
	log.Debug().
		Str("task-id", t.ID).
		Msg("handling pending task")
	if strings.TrimSpace(t.If) == "false" {
		return c.skipTask(ctx, t)
	} else {
		return c.sched.ScheduleTask(ctx, t)
	}
}

func (c *Coordinator) skipTask(ctx context.Context, t *tork.Task) error {
	now := time.Now().UTC()
	t.State = tork.TaskStateScheduled
	t.ScheduledAt = &now
	t.StartedAt = &now
	t.CompletedAt = &now
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
		u.State = t.State
		u.ScheduledAt = t.ScheduledAt
		u.StartedAt = t.StartedAt
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	return c.broker.PublishTask(ctx, mq.QUEUE_COMPLETED, t)
}

func (c *Coordinator) handleStartedTask(t *tork.Task) error {
	ctx := context.Background()
	log.Debug().
		Str("task-id", t.ID).
		Msg("received task start")
	// verify that the job is still running
	j, err := c.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return err
	}
	// if the job isn't running anymore we need
	// to cancel the task
	if j.State != tork.JobStateRunning {
		t.State = tork.TaskStateCancelled
		node, err := c.ds.GetNodeByID(ctx, t.NodeID)
		if err != nil {
			return err
		}
		return c.broker.PublishTask(ctx, node.Queue, t)
	}
	return c.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
		// we don't want to mark the task as RUNNING
		// if an out-of-order task completion/failure
		// arrived earlier
		if u.State == tork.TaskStateScheduled {
			u.State = tork.TaskStateRunning
			u.StartedAt = t.StartedAt
			u.NodeID = t.NodeID
		}
		return nil
	})
}

func (c *Coordinator) handleCompletedTask(t *tork.Task) error {
	ctx := context.Background()
	return c.completeTask(ctx, t)
}

func (c *Coordinator) completeTask(ctx context.Context, t *tork.Task) error {
	if t.ParentID != "" {
		return c.completeSubTask(ctx, t)
	}
	return c.completeTopLevelTask(ctx, t)
}

func (c *Coordinator) completeSubTask(ctx context.Context, t *tork.Task) error {
	parent, err := c.ds.GetTaskByID(ctx, t.ParentID)
	if err != nil {
		return errors.Wrapf(err, "error getting parent composite task: %s", t.ParentID)
	}
	if parent.Parallel != nil {
		return c.completeParallelTask(ctx, t)
	}
	return c.completeEachTask(ctx, t)
}

func (c *Coordinator) completeEachTask(ctx context.Context, t *tork.Task) error {
	var isLast bool
	err := c.ds.WithTx(ctx, func(tx datastore.Datastore) error {
		// update actual task
		if err := tx.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
			if u.State != tork.TaskStateRunning && u.State != tork.TaskStateScheduled {
				return errors.Errorf("can't complete task %s because it's %s", t.ID, u.State)
			}
			u.State = tork.TaskStateCompleted
			u.CompletedAt = t.CompletedAt
			u.Result = t.Result
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error updating task in datastore")
		}
		// update parent task
		if err := tx.UpdateTask(ctx, t.ParentID, func(u *tork.Task) error {
			u.Each.Completions = u.Each.Completions + 1
			isLast = u.Each.Completions >= u.Each.Size
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error updating task in datastore")
		}
		// update job context
		if t.Result != "" && t.Var != "" {
			if err := tx.UpdateJob(ctx, t.JobID, func(u *tork.Job) error {
				if u.Context.Tasks == nil {
					u.Context.Tasks = make(map[string]string)
				}
				u.Context.Tasks[t.Var] = t.Result
				return nil
			}); err != nil {
				return errors.Wrapf(err, "error updating job in datastore")
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "error complating each task: %s", t.ID)
	}
	// complete the parent task
	if isLast {
		parent, err := c.ds.GetTaskByID(ctx, t.ParentID)
		if err != nil {
			return errors.New("error fetching the parent task")
		}
		now := time.Now().UTC()
		parent.State = tork.TaskStateCompleted
		parent.CompletedAt = &now
		return c.completeTask(ctx, parent)
	}
	return nil
}

func (c *Coordinator) completeParallelTask(ctx context.Context, t *tork.Task) error {
	// complete actual task
	var isLast bool
	err := c.ds.WithTx(ctx, func(tx datastore.Datastore) error {
		if err := tx.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
			if u.State != tork.TaskStateRunning && u.State != tork.TaskStateScheduled {
				return errors.Errorf("can't complete task %s because it's %s", t.ID, u.State)
			}
			u.State = tork.TaskStateCompleted
			u.CompletedAt = t.CompletedAt
			u.Result = t.Result
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error updating task in datastore")
		}
		// update parent task
		if err := tx.UpdateTask(ctx, t.ParentID, func(u *tork.Task) error {
			u.Parallel.Completions = u.Parallel.Completions + 1
			isLast = u.Parallel.Completions >= len(u.Parallel.Tasks)
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error updating task in datastore")
		}
		// update job context
		if t.Result != "" && t.Var != "" {
			if err := tx.UpdateJob(ctx, t.JobID, func(u *tork.Job) error {
				if u.Context.Tasks == nil {
					u.Context.Tasks = make(map[string]string)
				}
				u.Context.Tasks[t.Var] = t.Result
				return nil
			}); err != nil {
				return errors.Wrapf(err, "error updating job in datastore")
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "error completing task %s", t.ID)
	}
	// complete the parent task
	if isLast {
		parent, err := c.ds.GetTaskByID(ctx, t.ParentID)
		if err != nil {
			return errors.New("error fetching the parent task")
		}
		now := time.Now().UTC()
		parent.State = tork.TaskStateCompleted
		parent.CompletedAt = &now
		return c.completeTask(ctx, parent)
	}
	return nil
}

func (c *Coordinator) completeTopLevelTask(ctx context.Context, t *tork.Task) error {
	log.Debug().Str("task-id", t.ID).Msg("received task completion")
	err := c.ds.WithTx(ctx, func(tx datastore.Datastore) error {
		// update task in DB
		if err := tx.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
			if u.State != tork.TaskStateRunning && u.State != tork.TaskStateScheduled {
				return errors.Errorf("can't complete task %s because it's %s", t.ID, u.State)
			}
			u.State = tork.TaskStateCompleted
			u.CompletedAt = t.CompletedAt
			u.Result = t.Result
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error updating task in datastore")
		}
		// update job in DB
		if err := tx.UpdateJob(ctx, t.JobID, func(u *tork.Job) error {
			u.Position = u.Position + 1
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
		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "error completing task: %s", t.ID)
	}
	// fire the next task
	j, err := c.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return errors.Wrapf(err, "error getting job from datatstore")
	}
	if j.Position <= len(j.Tasks) {
		now := time.Now().UTC()
		next := j.Tasks[j.Position-1]
		next.ID = uuid.NewUUID()
		next.JobID = j.ID
		next.State = tork.TaskStatePending
		next.Position = j.Position
		next.CreatedAt = &now
		if err := eval.EvaluateTask(next, j.Context.AsMap()); err != nil {
			next.Error = err.Error()
			next.State = tork.TaskStateFailed
			next.FailedAt = &now
		}
		if err := c.ds.CreateTask(ctx, next); err != nil {
			return err
		}
		return c.broker.PublishTask(ctx, mq.QUEUE_PENDING, next)
	} else {
		j.State = tork.JobStateCompleted
		return c.broker.PublishJob(ctx, j)
	}

}

func (c *Coordinator) handleFailedTask(t *tork.Task) error {
	ctx := context.Background()
	j, err := c.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return errors.Wrapf(err, "unknown job: %s", t.JobID)
	}
	log.Error().
		Str("task-id", t.ID).
		Str("task-error", t.Error).
		Str("task-state", string(t.State)).
		Msg("received task failure")

	// mark the task as FAILED
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
		if u.State.IsActive() {
			u.State = tork.TaskStateFailed
			u.FailedAt = t.FailedAt
			u.Error = t.Error
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error marking task %s as FAILED", t.ID)
	}
	// eligible for retry?
	if j.State == tork.JobStateRunning &&
		t.Retry != nil &&
		t.Retry.Attempts < t.Retry.Limit {
		// create a new retry task
		now := time.Now().UTC()
		rt := t.Clone()
		rt.ID = uuid.NewUUID()
		rt.CreatedAt = &now
		rt.Retry.Attempts = rt.Retry.Attempts + 1
		rt.State = tork.TaskStatePending
		rt.Error = ""
		if err := eval.EvaluateTask(rt, j.Context.AsMap()); err != nil {
			return errors.Wrapf(err, "error evaluating task")
		}
		if err := c.ds.CreateTask(ctx, rt); err != nil {
			return errors.Wrapf(err, "error creating a retry task")
		}
		if err := c.broker.PublishTask(ctx, mq.QUEUE_PENDING, rt); err != nil {
			log.Error().Err(err).Msg("error publishing retry task")
		}
	} else {
		// mark the job as FAILED
		if err := c.ds.UpdateJob(ctx, t.JobID, func(u *tork.Job) error {
			// we only want to make the job as FAILED
			// if it's actually running as opposed to
			// possibly being CANCELLED
			if u.State == tork.JobStateRunning {
				u.State = tork.JobStateFailed
				u.FailedAt = t.FailedAt
			}
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error marking the job as failed in the datastore")
		}
		// if this is a sub-job -- FAIL the parent task
		if j.ParentID != "" {
			parent, err := c.ds.GetTaskByID(ctx, j.ParentID)
			if err != nil {
				return errors.Wrapf(err, "could not find parent task for subtask: %s", j.ParentID)
			}
			now := time.Now().UTC()
			parent.State = tork.TaskStateFailed
			parent.FailedAt = &now
			parent.Error = t.Error
			return c.broker.PublishTask(ctx, mq.QUEUE_ERROR, parent)
		}
		// cancel all currently running tasks
		if err := c.cancelActiveTasks(ctx, j.ID); err != nil {
			return err
		}
		j, err := c.ds.GetJobByID(ctx, t.JobID)
		if err != nil {
			return errors.Wrapf(err, "unknown job: %s", t.JobID)
		}
		if j.State == tork.JobStateFailed {
			return c.broker.PublishEvent(ctx, mq.TOPIC_JOB_FAILED, j)
		}
	}
	return nil
}

func (c *Coordinator) handleHeartbeats(n tork.Node) error {
	ctx := context.Background()
	_, err := c.ds.GetNodeByID(ctx, n.ID)
	if err == datastore.ErrNodeNotFound {
		log.Info().
			Str("node-id", n.ID).
			Str("hostname", n.Hostname).
			Msg("received first heartbeat")
		return c.ds.CreateNode(ctx, n)
	}
	return c.ds.UpdateNode(ctx, n.ID, func(u *tork.Node) error {
		// ignore "old" heartbeats
		if u.LastHeartbeatAt.After(n.LastHeartbeatAt) {
			return nil
		}
		log.Debug().
			Str("node-id", n.ID).
			Float64("cpu-percent", n.CPUPercent).
			Str("hostname", n.Hostname).
			Str("status", string(n.Status)).
			Time("heartbeat-time", n.LastHeartbeatAt).
			Msg("received heartbeat")
		u.LastHeartbeatAt = n.LastHeartbeatAt
		u.CPUPercent = n.CPUPercent
		u.Status = n.Status
		u.TaskCount = n.TaskCount
		return nil
	})

}

func (c *Coordinator) handleJob(j *tork.Job) error {
	ctx := context.Background()
	switch j.State {
	case tork.JobStatePending:
		return c.startJob(ctx, j)
	case tork.JobStateCancelled:
		return c.cancelJob(ctx, j)
	case tork.JobStateRestart:
		return c.restartJob(ctx, j)
	case tork.JobStateCompleted:
		return c.completeJob(ctx, j)
	default:
		return errors.Errorf("invalud job state: %s", j.State)
	}
}

func (c *Coordinator) completeJob(ctx context.Context, j *tork.Job) error {
	var result string
	var jobErr error
	// mark the job as completed
	if err := c.ds.UpdateJob(ctx, j.ID, func(u *tork.Job) error {
		if u.State != tork.JobStateRunning {
			return errors.Errorf("job %s is %s and can not be completed", u.ID, u.State)
		}
		now := time.Now().UTC()
		// evaluate the job's output
		result, jobErr = eval.EvaluateTemplate(j.Output, j.Context.AsMap())
		if jobErr != nil {
			log.Error().Err(jobErr).Msgf("error evaluating job %s output", j.ID)
			j.State = tork.JobStateFailed
			u.FailedAt = &now
			u.Error = jobErr.Error()
		} else {
			u.State = tork.JobStateCompleted
			u.CompletedAt = &now
			u.Result = result
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating job in datastore")
	}
	// if this is a sub-job -- complete/fail the parent task
	if j.ParentID != "" {
		parent, err := c.ds.GetTaskByID(ctx, j.ParentID)
		if err != nil {
			return errors.Wrapf(err, "could not find parent task for subtask: %s", j.ParentID)
		}
		now := time.Now().UTC()
		if jobErr != nil {
			parent.State = tork.TaskStateFailed
			parent.FailedAt = &now
			parent.Error = jobErr.Error()
		} else {
			parent.State = tork.TaskStateCompleted
			parent.CompletedAt = &now
			parent.Result = result
		}
		return c.broker.PublishTask(ctx, mq.QUEUE_COMPLETED, parent)
	}
	// publish job completd/failed event
	if jobErr != nil {
		return c.broker.PublishEvent(ctx, mq.TOPIC_JOB_FAILED, j)
	} else {
		return c.broker.PublishEvent(ctx, mq.TOPIC_JOB_COMPLETED, j)
	}
}

func (c *Coordinator) restartJob(ctx context.Context, j *tork.Job) error {
	// mark the job as running
	if err := c.ds.UpdateJob(ctx, j.ID, func(u *tork.Job) error {
		if u.State != tork.JobStateFailed && u.State != tork.JobStateCancelled {
			return errors.Errorf("job %s is in %s state and can't be restarted", j.ID, j.State)
		}
		u.State = tork.JobStateRunning
		u.FailedAt = nil
		return nil
	}); err != nil {
		return err
	}
	// retry the current top level task
	now := time.Now().UTC()
	t := j.Tasks[j.Position-1]
	t.ID = uuid.NewUUID()
	t.JobID = j.ID
	t.State = tork.TaskStatePending
	t.Position = j.Position
	t.CreatedAt = &now
	if err := eval.EvaluateTask(t, j.Context.AsMap()); err != nil {
		t.Error = err.Error()
		t.State = tork.TaskStateFailed
		t.FailedAt = &now
	}
	if err := c.ds.CreateTask(ctx, t); err != nil {
		return err
	}
	return c.broker.PublishTask(ctx, mq.QUEUE_PENDING, t)
}

func (c *Coordinator) cancelJob(ctx context.Context, j *tork.Job) error {
	// mark the job as cancelled
	if err := c.ds.UpdateJob(ctx, j.ID, func(u *tork.Job) error {
		if u.State != tork.JobStateRunning {
			// job is not running -- nothing to cancel
			return nil
		}
		u.State = tork.JobStateCancelled
		return nil
	}); err != nil {
		return err
	}
	// if there's a parent task notify the parent job to cancel as well
	if j.ParentID != "" {
		pt, err := c.ds.GetTaskByID(ctx, j.ParentID)
		if err != nil {
			return errors.Wrapf(err, "error fetching parent task: %s", pt.ID)
		}
		pj, err := c.ds.GetJobByID(ctx, pt.JobID)
		if err != nil {
			return errors.Wrapf(err, "error fetching parent job: %s", pj.ID)
		}
		pj.State = tork.JobStateCancelled
		if err := c.broker.PublishJob(ctx, pj); err != nil {
			log.Error().Err(err).Msgf("error cancelling sub-job: %s", pj.ID)
		}
	}
	// cancel all running tasks
	if err := c.cancelActiveTasks(ctx, j.ID); err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) cancelActiveTasks(ctx context.Context, jobID string) error {
	// get a list of active tasks for the job
	tasks, err := c.ds.GetActiveTasks(ctx, jobID)
	if err != nil {
		return errors.Wrapf(err, "error getting active tasks for job: %s", jobID)
	}
	for _, t := range tasks {
		t.State = tork.TaskStateCancelled
		// mark tasks as cancelled
		if err := c.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
			u.State = tork.TaskStateCancelled
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error cancelling task: %s", t.ID)
		}
		// if this task is a sub-job, notify the sub-job to cancel
		if t.SubJob != nil {
			// cancel the sub-job
			sj, err := c.ds.GetJobByID(ctx, t.SubJob.ID)
			if err != nil {
				return err
			}
			sj.State = tork.JobStateCancelled
			if err := c.broker.PublishJob(ctx, sj); err != nil {
				return errors.Wrapf(err, "error publishing cancelllation for sub-job %s", sj.ID)
			}
		} else if t.NodeID != "" {
			// notify the node currently running the task
			// to cancel it
			node, err := c.ds.GetNodeByID(ctx, t.NodeID)
			if err != nil {
				return err
			}
			if err := c.broker.PublishTask(ctx, node.Queue, t); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Coordinator) startJob(ctx context.Context, j *tork.Job) error {

	log.Debug().Msgf("starting job %s", j.ID)
	now := time.Now().UTC()
	t := j.Tasks[0]
	t.ID = uuid.NewUUID()
	t.JobID = j.ID
	t.State = tork.TaskStatePending
	t.Position = 1
	t.CreatedAt = &now
	if err := eval.EvaluateTask(t, j.Context.AsMap()); err != nil {
		t.Error = err.Error()
		t.State = tork.TaskStateFailed
		t.FailedAt = &now
	}
	if err := c.ds.CreateTask(ctx, t); err != nil {
		return err
	}
	if err := c.ds.UpdateJob(ctx, j.ID, func(u *tork.Job) error {
		n := time.Now().UTC()
		u.State = tork.JobStateRunning
		u.StartedAt = &n
		u.Position = 1
		return nil
	}); err != nil {
		return err
	}
	return c.handleTask(t)
}

func (c *Coordinator) Start() error {
	log.Info().Msgf("starting %s", c.Name)
	// start the coordinator API
	if err := c.api.Start(); err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.broker.Shutdown(ctx); err != nil {
		return errors.Wrapf(err, "error shutting down broker")
	}
	if err := c.api.Shutdown(ctx); err != nil {
		return errors.Wrapf(err, "error shutting down API")
	}
	return nil
}
