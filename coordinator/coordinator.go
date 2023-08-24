package coordinator

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/eval"
	"github.com/runabol/tork/job"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/task"
	"github.com/runabol/tork/uuid"
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
	Debug     bool
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

func (c *Coordinator) handleTask(t *task.Task) error {
	switch t.State {
	case task.Pending:
		return c.handlePendingTask(t)
	case task.Failed:
		return c.handleFailedTask(t)
	default:
		return errors.Errorf("could not handle task %s. unknown state: %s", t.ID, t.State)
	}
}

func (c *Coordinator) handlePendingTask(t *task.Task) error {
	ctx := context.Background()
	log.Info().
		Str("task-id", t.ID).
		Msg("handling pending task")
	if strings.TrimSpace(t.If) == "false" {
		return c.skipTask(ctx, t)
	} else {
		return c.scheduleTask(ctx, t)
	}
}

func (c *Coordinator) scheduleTask(ctx context.Context, t *task.Task) error {
	if t.Parallel != nil {
		return c.scheduleParallelTask(ctx, t)
	} else if t.Each != nil {
		return c.scheduleEachTask(ctx, t)
	} else if t.SubJob != nil {
		return c.scheduleSubJob(ctx, t)
	}
	return c.scheduleRegularTask(ctx, t)
}

func (c *Coordinator) scheduleRegularTask(ctx context.Context, t *task.Task) error {
	now := time.Now().UTC()
	if t.Queue == "" {
		t.Queue = mq.QUEUE_DEFAULT
	}
	t.State = task.Scheduled
	t.ScheduledAt = &now
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		u.State = t.State
		u.ScheduledAt = t.ScheduledAt
		u.Queue = t.Queue
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	return c.broker.PublishTask(ctx, t.Queue, t)
}

func (c *Coordinator) scheduleSubJob(ctx context.Context, t *task.Task) error {
	now := time.Now().UTC()
	subjob := &job.Job{
		ID:          uuid.NewUUID(),
		CreatedAt:   now,
		ParentID:    t.ID,
		Name:        t.SubJob.Name,
		Description: t.SubJob.Description,
		State:       job.Pending,
		Tasks:       t.SubJob.Tasks,
		Inputs:      t.SubJob.Inputs,
		Context:     job.Context{Inputs: t.SubJob.Inputs},
		TaskCount:   len(t.SubJob.Tasks),
		Output:      t.SubJob.Output,
	}
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		u.State = task.Running
		u.ScheduledAt = &now
		u.StartedAt = &now
		u.SubJob.ID = subjob.ID
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	if err := c.ds.CreateJob(ctx, subjob); err != nil {
		return errors.Wrapf(err, "error creating subjob")
	}
	return c.handleJob(subjob)
}

func (c *Coordinator) scheduleEachTask(ctx context.Context, t *task.Task) error {
	now := time.Now().UTC()
	// get the job's context
	j, err := c.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return errors.Wrapf(err, "error getting job: %s", t.JobID)
	}
	// evaluate the list expression
	lraw, err := eval.EvaluateExpr(t.Each.List, j.Context.AsMap())
	if err != nil {
		return errors.Wrapf(err, "error evaluating each.list expression: %s", t.Each.List)
	}
	var list []any
	rlist := reflect.ValueOf(lraw)
	if rlist.Kind() == reflect.Slice {
		for i := 0; i < rlist.Len(); i++ {
			list = append(list, rlist.Index(i).Interface())
		}
	} else {
		return errors.Wrapf(err, "each.list expression does not evaluate to a list: %s", t.Each.List)
	}
	// mark the task as running
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		u.State = task.Running
		u.ScheduledAt = &now
		u.StartedAt = &now
		u.Each.Size = len(list)
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	// schedule a task for each elements in the list
	for ix, item := range list {
		cx := j.Context.Clone().AsMap()
		cx["item"] = map[string]any{
			"index": fmt.Sprintf("%d", ix),
			"value": item,
		}
		et := t.Each.Task.Clone()
		et.ID = uuid.NewUUID()
		et.JobID = j.ID
		et.State = task.Pending
		et.Position = t.Position
		et.CreatedAt = &now
		et.ParentID = t.ID
		if err := eval.EvaluateTask(et, cx); err != nil {
			t.Error = err.Error()
			t.State = task.Failed
			return c.handleFailedTask(t)
		}
		if err := c.ds.CreateTask(ctx, et); err != nil {
			return err
		}
		if err := c.handlePendingTask(et); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) scheduleParallelTask(ctx context.Context, t *task.Task) error {
	now := time.Now().UTC()
	// mark the task as running
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		u.State = task.Running
		u.ScheduledAt = &now
		u.StartedAt = &now
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	// get the job's context
	j, err := c.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return errors.Wrapf(err, "error getting job: %s", t.JobID)
	}
	// fire all parallel tasks
	for _, pt := range t.Parallel.Tasks {
		pt.ID = uuid.NewUUID()
		pt.JobID = j.ID
		pt.State = task.Pending
		pt.Position = t.Position
		pt.CreatedAt = &now
		pt.ParentID = t.ID
		if err := eval.EvaluateTask(pt, j.Context.AsMap()); err != nil {
			t.Error = err.Error()
			t.State = task.Failed
			return c.handleFailedTask(t)
		}
		if err := c.ds.CreateTask(ctx, pt); err != nil {
			return err
		}
		if err := c.handlePendingTask(pt); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) skipTask(ctx context.Context, t *task.Task) error {
	now := time.Now().UTC()
	t.State = task.Scheduled
	t.ScheduledAt = &now
	t.StartedAt = &now
	t.CompletedAt = &now
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		u.State = t.State
		u.ScheduledAt = t.ScheduledAt
		u.StartedAt = t.StartedAt
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	return c.broker.PublishTask(ctx, mq.QUEUE_COMPLETED, t)
}

func (c *Coordinator) handleStartedTask(t *task.Task) error {
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
	if j.State != job.Running {
		t.State = task.Cancelled
		node, err := c.ds.GetNodeByID(ctx, t.NodeID)
		if err != nil {
			return err
		}
		return c.broker.PublishTask(ctx, node.Queue, t)
	}
	return c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		// we don't want to mark the task as RUNNING
		// if an out-of-order task completion/failure
		// arrived earlier
		if u.State == task.Scheduled {
			u.State = task.Running
			u.StartedAt = t.StartedAt
			u.NodeID = t.NodeID
		}
		return nil
	})
}

func (c *Coordinator) handleCompletedTask(t *task.Task) error {
	ctx := context.Background()
	return c.completeTask(ctx, t)
}

func (c *Coordinator) completeTask(ctx context.Context, t *task.Task) error {
	if t.ParentID != "" {
		return c.completeSubTask(ctx, t)
	}
	return c.completeTopLevelTask(ctx, t)
}

func (c *Coordinator) completeSubTask(ctx context.Context, t *task.Task) error {
	parent, err := c.ds.GetTaskByID(ctx, t.ParentID)
	if err != nil {
		return errors.Wrapf(err, "error getting parent composite task: %s", t.ParentID)
	}
	if parent.Parallel != nil {
		return c.completeParallelTask(ctx, t)
	}
	return c.completeEachTask(ctx, t)
}

func (c *Coordinator) completeEachTask(ctx context.Context, t *task.Task) error {
	// update actual task
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		u.State = task.Completed
		u.CompletedAt = t.CompletedAt
		u.Result = t.Result
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	// update parent task
	if err := c.ds.UpdateTask(ctx, t.ParentID, func(u *task.Task) error {
		u.Each.Completions = u.Each.Completions + 1
		if u.Each.Completions >= u.Each.Size {
			now := time.Now().UTC()
			u.State = task.Completed
			u.CompletedAt = &now
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	// update job context
	if t.Result != "" && t.Var != "" {
		if err := c.ds.UpdateJob(ctx, t.JobID, func(u *job.Job) error {
			if u.Context.Tasks == nil {
				u.Context.Tasks = make(map[string]string)
			}
			u.Context.Tasks[t.Var] = t.Result
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error updating job in datastore")
		}
	}
	// complete the parent task
	parent, err := c.ds.GetTaskByID(ctx, t.ParentID)
	if err != nil {
		return errors.New("error fetching the parent task")
	}
	if parent.State == task.Completed {
		return c.completeTask(ctx, parent)
	}
	return nil
}

func (c *Coordinator) completeParallelTask(ctx context.Context, t *task.Task) error {
	// update actual task
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		u.State = task.Completed
		u.CompletedAt = t.CompletedAt
		u.Result = t.Result
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	// update parent task
	if err := c.ds.UpdateTask(ctx, t.ParentID, func(u *task.Task) error {
		u.Parallel.Completions = u.Parallel.Completions + 1
		if u.Parallel.Completions >= len(u.Parallel.Tasks) {
			now := time.Now().UTC()
			u.State = task.Completed
			u.CompletedAt = &now
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	// update job context
	if t.Result != "" && t.Var != "" {
		if err := c.ds.UpdateJob(ctx, t.JobID, func(u *job.Job) error {
			if u.Context.Tasks == nil {
				u.Context.Tasks = make(map[string]string)
			}
			u.Context.Tasks[t.Var] = t.Result
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error updating job in datastore")
		}
	}
	parent, err := c.ds.GetTaskByID(ctx, t.ParentID)
	if err != nil {
		return errors.New("error fetching the parent task")
	}
	// complete the parent task
	if parent.State == task.Completed {
		return c.completeTask(ctx, parent)
	}
	return nil
}

func (c *Coordinator) completeTopLevelTask(ctx context.Context, t *task.Task) error {
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
		next.State = task.Pending
		next.Position = j.Position
		next.CreatedAt = &now
		if err := eval.EvaluateTask(next, j.Context.AsMap()); err != nil {
			next.Error = err.Error()
			next.State = task.Failed
			next.FailedAt = &now
		}
		if err := c.ds.CreateTask(ctx, next); err != nil {
			return err
		}
		return c.handleTask(next)
	} else {
		j.State = job.Completed
		return c.broker.PublishJob(ctx, j)
	}
}

func (c *Coordinator) handleFailedTask(t *task.Task) error {
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
	if err := c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
		if u.State.IsActive() {
			u.State = task.Failed
			u.FailedAt = t.FailedAt
			u.Error = t.Error
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error marking task %s as FAILED", t.ID)
	}
	// eligible for retry?
	if j.State == job.Running &&
		t.Retry != nil &&
		t.Retry.Attempts < t.Retry.Limit {
		// create a new retry task
		now := time.Now().UTC()
		rt := t.Clone()
		rt.ID = uuid.NewUUID()
		rt.CreatedAt = &now
		rt.Retry.Attempts = rt.Retry.Attempts + 1
		rt.State = task.Pending
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
		// if this is a sub-job -- FAIL the parent task
		if j.ParentID != "" {
			parent, err := c.ds.GetTaskByID(ctx, j.ParentID)
			if err != nil {
				return errors.Wrapf(err, "could not find parent task for subtask: %s", j.ParentID)
			}
			now := time.Now().UTC()
			parent.State = task.Failed
			parent.FailedAt = &now
			parent.Error = t.Error
			return c.broker.PublishTask(ctx, mq.QUEUE_ERROR, parent)
		}
	}
	return nil
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

func (c *Coordinator) handleJob(j *job.Job) error {
	ctx := context.Background()
	switch j.State {
	case job.Pending:
		return c.startJob(ctx, j)
	case job.Cancelled:
		return c.cancelJob(ctx, j)
	case job.Restart:
		return c.restartJob(ctx, j)
	case job.Completed:
		return c.completeJob(ctx, j)
	default:
		return errors.Errorf("invalud job state: %s", j.State)
	}
}

func (c *Coordinator) completeJob(ctx context.Context, j *job.Job) error {
	var result string
	var jobErr error
	// mark the job as completed
	if err := c.ds.UpdateJob(ctx, j.ID, func(u *job.Job) error {
		if u.State != job.Running {
			return errors.Errorf("job %s is %s and can not be completed", u.ID, u.State)
		}
		now := time.Now().UTC()
		// evaluate the job's output
		result, jobErr = eval.EvaluateTemplate(j.Output, j.Context.AsMap())
		if jobErr != nil {
			log.Error().Err(jobErr).Msgf("error evaluating job %s output", j.ID)
			j.State = job.Failed
			u.FailedAt = &now
			u.Error = jobErr.Error()
		} else {
			u.State = job.Completed
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
			parent.State = task.Failed
			parent.FailedAt = &now
			parent.Error = jobErr.Error()
		} else {
			parent.State = task.Completed
			parent.CompletedAt = &now
			parent.Result = result
		}
		return c.broker.PublishTask(ctx, mq.QUEUE_COMPLETED, parent)
	}
	return nil
}

func (c *Coordinator) restartJob(ctx context.Context, j *job.Job) error {
	// mark the job as running
	if err := c.ds.UpdateJob(ctx, j.ID, func(u *job.Job) error {
		if u.State != job.Failed && u.State != job.Cancelled {
			return errors.Errorf("job %s is in %s state and can't be restarted", j.ID, j.State)
		}
		u.State = job.Running
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
	t.State = task.Pending
	t.Position = j.Position
	t.CreatedAt = &now
	if err := eval.EvaluateTask(t, j.Context.AsMap()); err != nil {
		t.Error = err.Error()
		t.State = task.Failed
		t.FailedAt = &now
	}
	if err := c.ds.CreateTask(ctx, t); err != nil {
		return err
	}
	return c.broker.PublishTask(ctx, mq.QUEUE_PENDING, t)
}

func (c *Coordinator) cancelJob(ctx context.Context, j *job.Job) error {
	// mark the job as cancelled
	if err := c.ds.UpdateJob(ctx, j.ID, func(u *job.Job) error {
		if u.State != job.Running {
			// job is not running -- nothing to cancel
			return nil
		}
		u.State = job.Cancelled
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
		pj.State = job.Cancelled
		if err := c.broker.PublishJob(ctx, pj); err != nil {
			log.Error().Err(err).Msgf("error cancelling sub-job: %s", pj.ID)
		}
	}
	// get list of currently running tasks
	tasks, err := c.ds.GetActiveTasks(ctx, j.ID)
	if err != nil {
		return errors.Wrapf(err, "error getting active tasks for job: %s", j.ID)
	}
	for _, t := range tasks {
		t.State = task.Cancelled
		// mark tasks as cancelled
		if err := c.ds.UpdateTask(ctx, t.ID, func(u *task.Task) error {
			u.State = task.Cancelled
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
			sj.State = job.Cancelled
			if err := c.broker.PublishJob(ctx, sj); err != nil {
				log.Error().Err(err).Msgf("error cancelling sub-job: %s", sj.ID)
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

func (c *Coordinator) startJob(ctx context.Context, j *job.Job) error {

	log.Debug().Msgf("starting job %s", j.ID)
	now := time.Now().UTC()
	t := j.Tasks[0]
	t.ID = uuid.NewUUID()
	t.JobID = j.ID
	t.State = task.Pending
	t.Position = 1
	t.CreatedAt = &now
	if err := eval.EvaluateTask(t, j.Context.AsMap()); err != nil {
		t.Error = err.Error()
		t.State = task.Failed
		t.FailedAt = &now
	}
	if err := c.ds.CreateTask(ctx, t); err != nil {
		return err
	}
	if err := c.ds.UpdateJob(ctx, j.ID, func(u *job.Job) error {
		n := time.Now().UTC()
		u.State = job.Running
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
