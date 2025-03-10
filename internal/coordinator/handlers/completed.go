package handlers

import (
	"context"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"

	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/eval"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/task"
)

type completedHandler struct {
	ds     datastore.Datastore
	broker broker.Broker
	onJob  job.HandlerFunc
}

func NewCompletedHandler(ds datastore.Datastore, b broker.Broker, mw ...job.MiddlewareFunc) task.HandlerFunc {
	h := &completedHandler{
		ds:     ds,
		broker: b,
		onJob:  job.ApplyMiddleware(NewJobHandler(ds, b), mw),
	}
	return h.handle
}

func (h *completedHandler) handle(ctx context.Context, et task.EventType, t *tork.Task) error {
	now := time.Now().UTC()
	if t.State != tork.TaskStateCompleted && t.State != tork.TaskStateSkipped {
		return errors.Errorf("invalid completion state: %s", t.State)
	}
	t.CompletedAt = &now
	return h.completeTask(ctx, t)
}

func (h *completedHandler) completeTask(ctx context.Context, t *tork.Task) error {
	if t.ParentID != "" {
		return h.completeSubTask(ctx, t)
	}
	return h.completeTopLevelTask(ctx, t)
}

func (h *completedHandler) completeSubTask(ctx context.Context, t *tork.Task) error {
	parent, err := h.ds.GetTaskByID(ctx, t.ParentID)
	if err != nil {
		return errors.Wrapf(err, "error getting parent composite task: %s", t.ParentID)
	}
	if parent.Parallel != nil {
		return h.completeParallelTask(ctx, t)
	}
	return h.completeEachTask(ctx, t)
}

func (h *completedHandler) completeEachTask(ctx context.Context, t *tork.Task) error {
	var isLast bool
	err := h.ds.WithTx(ctx, func(tx datastore.Datastore) error {
		// update actual task
		if err := tx.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
			if u.State != tork.TaskStateRunning &&
				u.State != tork.TaskStateScheduled &&
				u.State != tork.TaskStateSkipped {
				return errors.Errorf("can't complete task %s because it's %s", t.ID, u.State)
			}
			u.State = t.State
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
			if !isLast && u.Each.Concurrency > 0 && u.Each.Index < u.Each.Size {
				next, err := h.ds.GetNextTask(ctx, u.ID)
				if err != nil {
					return err
				}
				next.State = tork.TaskStatePending
				if err := h.ds.UpdateTask(ctx, next.ID, func(nu *tork.Task) error {
					nu.State = tork.TaskStatePending
					return nil
				}); err != nil {
					return err
				}
				if err := h.broker.PublishTask(ctx, broker.QUEUE_PENDING, next); err != nil {
					return err
				}
			}
			u.Each.Index = u.Each.Index + 1
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
		parent, err := h.ds.GetTaskByID(ctx, t.ParentID)
		if err != nil {
			return errors.New("error fetching the parent task")
		}
		now := time.Now().UTC()
		parent.State = tork.TaskStateCompleted
		parent.CompletedAt = &now
		return h.completeTask(ctx, parent)
	}
	return nil
}

func (h *completedHandler) completeParallelTask(ctx context.Context, t *tork.Task) error {
	// complete actual task
	var isLast bool
	err := h.ds.WithTx(ctx, func(tx datastore.Datastore) error {
		if err := tx.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
			if u.State != tork.TaskStateRunning &&
				u.State != tork.TaskStateScheduled &&
				u.State != tork.TaskStateSkipped {
				return errors.Errorf("can't complete task %s because it's %s", t.ID, u.State)
			}
			u.State = t.State
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
		parent, err := h.ds.GetTaskByID(ctx, t.ParentID)
		if err != nil {
			return errors.New("error fetching the parent task")
		}
		now := time.Now().UTC()
		parent.State = tork.TaskStateCompleted
		parent.CompletedAt = &now
		return h.completeTask(ctx, parent)
	}
	return nil
}

func (c *completedHandler) completeTopLevelTask(ctx context.Context, t *tork.Task) error {
	log.Debug().Str("task-id", t.ID).Msg("received task completion")
	err := c.ds.WithTx(ctx, func(tx datastore.Datastore) error {
		// update task in DB
		if err := tx.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
			if u.State != tork.TaskStateRunning &&
				u.State != tork.TaskStateScheduled &&
				u.State != tork.TaskStateSkipped {
				return errors.Errorf("can't complete task %s because it's %s", t.ID, u.State)
			}
			u.State = t.State
			u.CompletedAt = t.CompletedAt
			u.Result = t.Result
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error updating task in datastore")
		}
		// update job in DB
		if err := tx.UpdateJob(ctx, t.JobID, func(u *tork.Job) error {
			progress := float64(u.Position) / float64(u.TaskCount) * 100
			// Round progress to two decimal points
			progress = math.Round(progress*100) / 100
			u.Progress = progress
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
	if err := c.onJob(ctx, job.Progress, j); err != nil {
		return err
	}
	now := time.Now().UTC()
	if j.Position <= len(j.Tasks) {
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
		return c.broker.PublishTask(ctx, broker.QUEUE_PENDING, next)
	} else {
		j.State = tork.JobStateCompleted
		j.CompletedAt = &now
		return c.onJob(ctx, job.StateChange, j)
	}

}
