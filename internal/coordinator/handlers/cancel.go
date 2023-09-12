package handlers

import (
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/mq"
)

type cancelHandler struct {
	ds     datastore.Datastore
	broker mq.Broker
}

func NewCancelHandler(ds datastore.Datastore, b mq.Broker) job.HandlerFunc {
	h := &cancelHandler{
		ds:     ds,
		broker: b,
	}
	return h.handle
}

func (h *cancelHandler) handle(ctx context.Context, _ job.EventType, j *tork.Job) error {
	// mark the job as cancelled
	if err := h.ds.UpdateJob(ctx, j.ID, func(u *tork.Job) error {
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
		pt, err := h.ds.GetTaskByID(ctx, j.ParentID)
		if err != nil {
			return errors.Wrapf(err, "error fetching parent task: %s", pt.ID)
		}
		pj, err := h.ds.GetJobByID(ctx, pt.JobID)
		if err != nil {
			return errors.Wrapf(err, "error fetching parent job: %s", pj.ID)
		}
		pj.State = tork.JobStateCancelled
		if err := h.broker.PublishJob(ctx, pj); err != nil {
			log.Error().Err(err).Msgf("error cancelling sub-job: %s", pj.ID)
		}
	}
	// cancel all running tasks
	if err := cancelActiveTasks(ctx, h.ds, h.broker, j.ID); err != nil {
		return err
	}

	return nil
}

func cancelActiveTasks(ctx context.Context, ds datastore.Datastore, b mq.Broker, jobID string) error {
	// get a list of active tasks for the job
	tasks, err := ds.GetActiveTasks(ctx, jobID)
	if err != nil {
		return errors.Wrapf(err, "error getting active tasks for job: %s", jobID)
	}
	for _, t := range tasks {
		t.State = tork.TaskStateCancelled
		// mark tasks as cancelled
		if err := ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
			u.State = tork.TaskStateCancelled
			return nil
		}); err != nil {
			return errors.Wrapf(err, "error cancelling task: %s", t.ID)
		}
		// if this task is a sub-job, notify the sub-job to cancel
		if t.SubJob != nil {
			// cancel the sub-job
			sj, err := ds.GetJobByID(ctx, t.SubJob.ID)
			if err != nil {
				return err
			}
			sj.State = tork.JobStateCancelled
			if err := b.PublishJob(ctx, sj); err != nil {
				return errors.Wrapf(err, "error publishing cancelllation for sub-job %s", sj.ID)
			}
		} else if t.NodeID != "" {
			// notify the node currently running the task
			// to cancel it
			node, err := ds.GetNodeByID(ctx, t.NodeID)
			if err != nil {
				return err
			}
			if err := b.PublishTask(ctx, node.Queue, t); err != nil {
				return err
			}
		}
	}
	return nil
}
