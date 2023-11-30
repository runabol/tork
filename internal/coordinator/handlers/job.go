package handlers

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/eval"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/mq"
)

type jobHandler struct {
	ds        datastore.Datastore
	broker    mq.Broker
	onPending task.HandlerFunc
	onCancel  job.HandlerFunc
}

func NewJobHandler(ds datastore.Datastore, b mq.Broker, mw ...task.MiddlewareFunc) job.HandlerFunc {
	h := &jobHandler{
		ds:        ds,
		broker:    b,
		onPending: task.ApplyMiddleware(NewPendingHandler(ds, b), mw),
		onCancel:  NewCancelHandler(ds, b),
	}
	return h.handle
}

func (h *jobHandler) handle(ctx context.Context, et job.EventType, j *tork.Job) error {
	switch j.State {
	case tork.JobStatePending:
		return h.startJob(ctx, j)
	case tork.JobStateCancelled:
		return h.onCancel(ctx, et, j)
	case tork.JobStateRestart:
		return h.restartJob(ctx, j)
	case tork.JobStateCompleted:
		return h.completeJob(ctx, j)
	case tork.JobStateFailed:
		return h.failJob(ctx, j)
	default:
		return errors.Errorf("invalud job state: %s", j.State)
	}
}

func (h *jobHandler) startJob(ctx context.Context, j *tork.Job) error {
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
	if err := h.ds.CreateTask(ctx, t); err != nil {
		return err
	}
	if err := h.ds.UpdateJob(ctx, j.ID, func(u *tork.Job) error {
		n := time.Now().UTC()
		u.State = tork.JobStateRunning
		u.StartedAt = &n
		u.Position = 1
		return nil
	}); err != nil {
		return err
	}
	if t.State == tork.TaskStateFailed {
		n := time.Now().UTC()
		j.FailedAt = &n
		j.State = tork.JobStateFailed
		return h.handle(ctx, job.StateChange, j)
	}
	return h.onPending(ctx, task.StateChange, t)
}

func (h *jobHandler) completeJob(ctx context.Context, j *tork.Job) error {
	// mark the job as completed
	if err := h.ds.UpdateJob(ctx, j.ID, func(u *tork.Job) error {
		if u.State != tork.JobStateRunning {
			return errors.Errorf("job %s is %s and can not be completed", u.ID, u.State)
		}
		now := time.Now().UTC()
		// evaluate the job's output
		result, jobErr := eval.EvaluateTemplate(j.Output, j.Context.AsMap())
		if jobErr != nil {
			log.Error().Err(jobErr).Msgf("error evaluating job %s output", j.ID)
			j.State = tork.JobStateFailed
			u.State = tork.JobStateFailed
			u.FailedAt = &now
			u.Error = jobErr.Error()
			j.Error = jobErr.Error()
		} else {
			j.State = tork.JobStateCompleted
			u.State = tork.JobStateCompleted
			u.CompletedAt = &now
			u.Result = result
			j.Result = result
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating job in datastore")
	}
	// if this is a sub-job -- complete/fail the parent task
	if j.ParentID != "" {
		parent, err := h.ds.GetTaskByID(ctx, j.ParentID)
		if err != nil {
			return errors.Wrapf(err, "could not find parent task for subtask: %s", j.ParentID)
		}
		now := time.Now().UTC()
		if j.State == tork.JobStateFailed {
			parent.State = tork.TaskStateFailed
			parent.FailedAt = &now
			parent.Error = j.Error
		} else {
			parent.State = tork.TaskStateCompleted
			parent.CompletedAt = &now
			parent.Result = j.Result
		}
		return h.broker.PublishTask(ctx, mq.QUEUE_COMPLETED, parent)
	}
	// publish job completd/failed event
	if j.State == tork.JobStateFailed {
		return h.broker.PublishEvent(ctx, mq.TOPIC_JOB_FAILED, j)
	} else {
		return h.broker.PublishEvent(ctx, mq.TOPIC_JOB_COMPLETED, j)
	}
}

func (h *jobHandler) restartJob(ctx context.Context, j *tork.Job) error {
	// mark the job as running
	if err := h.ds.UpdateJob(ctx, j.ID, func(u *tork.Job) error {
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
	if err := h.ds.CreateTask(ctx, t); err != nil {
		return err
	}
	return h.broker.PublishTask(ctx, mq.QUEUE_PENDING, t)
}

func (h *jobHandler) failJob(ctx context.Context, j *tork.Job) error {
	// mark the job as FAILED
	if err := h.ds.UpdateJob(ctx, j.ID, func(u *tork.Job) error {
		// we only want to make the job as FAILED
		// if it's actually running as opposed to
		// possibly being CANCELLED
		if u.State == tork.JobStateRunning {
			u.State = tork.JobStateFailed
			u.FailedAt = j.FailedAt
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error marking the job as failed in the datastore")
	}
	// if this is a sub-job -- FAIL the parent task
	if j.ParentID != "" {
		parent, err := h.ds.GetTaskByID(ctx, j.ParentID)
		if err != nil {
			return errors.Wrapf(err, "could not find parent task for subtask: %s", j.ParentID)
		}
		parent.State = tork.TaskStateFailed
		parent.FailedAt = j.FailedAt
		parent.Error = j.Error
		return h.broker.PublishTask(ctx, mq.QUEUE_ERROR, parent)
	}
	// cancel all currently running tasks
	if err := cancelActiveTasks(ctx, h.ds, h.broker, j.ID); err != nil {
		return err
	}
	j, err := h.ds.GetJobByID(ctx, j.ID)
	if err != nil {
		return errors.Wrapf(err, "unknown job: %s", j.ID)
	}
	if j.State == tork.JobStateFailed {
		return h.broker.PublishEvent(ctx, mq.TOPIC_JOB_FAILED, j)
	}
	return nil
}
