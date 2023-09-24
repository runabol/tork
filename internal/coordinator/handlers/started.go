package handlers

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/mq"
)

type startedHandler struct {
	ds     datastore.Datastore
	broker mq.Broker
}

func NewStartedHandler(ds datastore.Datastore, b mq.Broker) task.HandlerFunc {
	h := &startedHandler{
		ds:     ds,
		broker: b,
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
	if j.State != tork.JobStateRunning {
		t.State = tork.TaskStateCancelled
		node, err := h.ds.GetNodeByID(ctx, t.NodeID)
		if err != nil {
			return err
		}
		return h.broker.PublishTask(ctx, node.Queue, t)
	}
	return h.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
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
