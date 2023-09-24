package handlers

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/coordinator/scheduler"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/mq"
)

type pendingHandler struct {
	sched  scheduler.Scheduler
	ds     datastore.Datastore
	broker mq.Broker
}

func NewPendingHandler(ds datastore.Datastore, b mq.Broker) task.HandlerFunc {
	h := &pendingHandler{
		ds:     ds,
		broker: b,
		sched:  *scheduler.NewScheduler(ds, b),
	}
	return h.handle
}

func (h *pendingHandler) handle(ctx context.Context, et task.EventType, t *tork.Task) error {
	log.Debug().
		Str("task-id", t.ID).
		Msg("handling pending task")
	if strings.TrimSpace(t.If) == "false" {
		return h.skipTask(ctx, t)
	} else {
		return h.sched.ScheduleTask(ctx, t)
	}
}

func (h *pendingHandler) skipTask(ctx context.Context, t *tork.Task) error {
	now := time.Now().UTC()
	t.State = tork.TaskStateScheduled
	t.ScheduledAt = &now
	t.StartedAt = &now
	t.CompletedAt = &now
	if err := h.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
		u.State = t.State
		u.ScheduledAt = t.ScheduledAt
		u.StartedAt = t.StartedAt
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	return h.broker.PublishTask(ctx, mq.QUEUE_COMPLETED, t)
}
