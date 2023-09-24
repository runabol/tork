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
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/mq"
)

type errorHandler struct {
	ds     datastore.Datastore
	broker mq.Broker
}

func NewErrorHandler(ds datastore.Datastore, b mq.Broker) task.HandlerFunc {
	h := &errorHandler{
		ds:     ds,
		broker: b,
	}
	return h.handle
}

func (h *errorHandler) handle(ctx context.Context, et task.EventType, t *tork.Task) error {
	j, err := h.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return errors.Wrapf(err, "unknown job: %s", t.JobID)
	}
	log.Error().
		Str("task-id", t.ID).
		Str("task-error", t.Error).
		Str("task-state", string(t.State)).
		Msg("received task failure")

	// mark the task as FAILED
	if err := h.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
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
		if err := h.ds.CreateTask(ctx, rt); err != nil {
			return errors.Wrapf(err, "error creating a retry task")
		}
		if err := h.broker.PublishTask(ctx, mq.QUEUE_PENDING, rt); err != nil {
			log.Error().Err(err).Msg("error publishing retry task")
		}
	} else {
		// mark the job as FAILED
		if err := h.ds.UpdateJob(ctx, t.JobID, func(u *tork.Job) error {
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
			parent, err := h.ds.GetTaskByID(ctx, j.ParentID)
			if err != nil {
				return errors.Wrapf(err, "could not find parent task for subtask: %s", j.ParentID)
			}
			now := time.Now().UTC()
			parent.State = tork.TaskStateFailed
			parent.FailedAt = &now
			parent.Error = t.Error
			return h.broker.PublishTask(ctx, mq.QUEUE_ERROR, parent)
		}
		// cancel all currently running tasks
		if err := cancelActiveTasks(ctx, h.ds, h.broker, j.ID); err != nil {
			return err
		}
		j, err := h.ds.GetJobByID(ctx, t.JobID)
		if err != nil {
			return errors.Wrapf(err, "unknown job: %s", t.JobID)
		}
		if j.State == tork.JobStateFailed {
			return h.broker.PublishEvent(ctx, mq.TOPIC_JOB_FAILED, j)
		}
	}
	return nil
}
