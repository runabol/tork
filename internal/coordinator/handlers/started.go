package handlers

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/mq"
)

type startedHandler struct {
	ds     datastore.Datastore
	broker mq.Broker
	onJob  job.HandlerFunc
}

func NewStartedHandler(ds datastore.Datastore, b mq.Broker, mw ...job.MiddlewareFunc) task.HandlerFunc {
	h := &startedHandler{
		ds:     ds,
		broker: b,
		onJob:  job.ApplyMiddleware(NewJobHandler(ds, b), mw),
	}
	return h.handle
}

func (h *startedHandler) handle(ctx context.Context, et task.EventType, t *tork.Task) error {
	log.Debug().
		Str("task-id", t.ID).
		Msg("received task start")
	// verify that the job is still running
	j, err := h.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return err
	}
	// if the job isn't running anymore we need
	// to cancel the task
	if j.State != tork.JobStateRunning && j.State != tork.JobStateScheduled {
		t.State = tork.TaskStateCancelled
		node, err := h.ds.GetNodeByID(ctx, t.NodeID)
		if err != nil {
			return err
		}
		return h.broker.PublishTask(ctx, node.Queue, t)
	}
	// if this is the first task that started
	// we want to switch the state of the job
	// from SCHEDULED to RUNNING
	if j.State == tork.JobStateScheduled {
		j.State = tork.JobStateRunning
		if err := h.onJob(ctx, job.StateChange, j); err != nil {
			return nil
		}
	}
	return h.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
		// we don't want to mark the task as RUNNING
		// if an out-of-order task completion/failure
		// arrived earlier
		if u.State == tork.TaskStateScheduled {
			now := time.Now().UTC()
			t.StartedAt = &now
			u.State = tork.TaskStateRunning
			u.StartedAt = &now
			u.NodeID = t.NodeID
		}
		return nil
	})
}
