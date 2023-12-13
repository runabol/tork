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

type errorHandler struct {
	ds     datastore.Datastore
	broker mq.Broker
	onJob  job.HandlerFunc
}

func NewErrorHandler(ds datastore.Datastore, b mq.Broker, mw ...job.MiddlewareFunc) task.HandlerFunc {
	h := &errorHandler{
		ds:     ds,
		broker: b,
		onJob:  job.ApplyMiddleware(NewJobHandler(ds, b), mw),
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

	now := time.Now().UTC()
	t.FailedAt = &now

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
	if (j.State == tork.JobStateRunning || j.State == tork.JobStateScheduled) &&
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
		j.State = tork.JobStateFailed
		j.FailedAt = t.FailedAt
		return h.onJob(ctx, job.StateChange, j)
	}
	return nil
}
