package handlers

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/task"
)

type progressHandler struct {
	ds    datastore.Datastore
	onJob job.HandlerFunc
}

func NewProgressHandler(ds datastore.Datastore, onJob job.HandlerFunc) task.HandlerFunc {
	h := &progressHandler{
		ds:    ds,
		onJob: onJob,
	}
	return h.handle
}

func (h *progressHandler) handle(ctx context.Context, et task.EventType, t *tork.Task) error {
	log.Debug().Msgf("[Task][%s] %.2f", t.ID, t.Progress)
	if t.Progress < 0 {
		t.Progress = 0
	} else if t.Progress > 100 {
		t.Progress = 100
	}
	if err := h.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
		u.Progress = t.Progress
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task progress: %s", err.Error())
	}
	// calculate the overall job progress
	j, err := h.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return err
	}
	if t.Progress == 0 {
		j.Progress = (float64(j.Position - 1)) / float64(j.TaskCount) * 100
	} else {
		j.Progress = (float64(j.Position-1) + (t.Progress / 100)) / float64(j.TaskCount) * 100
	}
	// Round progress to two decimal points
	j.Progress = math.Round(j.Progress*100) / 100
	if err := h.ds.UpdateJob(ctx, t.JobID, func(u *tork.Job) error {
		u.Progress = j.Progress
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating job progress: %s", err.Error())
	}
	return h.onJob(ctx, job.Progress, j)
}
