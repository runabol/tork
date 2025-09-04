package log

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/redact"
)

func Redact(ds datastore.Datastore) MiddlewareFunc {
	redacter := redact.NewRedacter(ds)
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, et EventType, l []*tork.TaskLogPart) error {
			if et != Read {
				return next(ctx, et, l)
			}
			if len(l) == 0 {
				return next(ctx, et, l)
			}
			task, err := ds.GetTaskByID(ctx, l[0].TaskID)
			if err != nil {
				log.Error().Err(err).Msgf("error getting task for log")
				return err
			}
			job, err := ds.GetJobByID(ctx, task.JobID)
			if err != nil {
				log.Error().Err(err).Msgf("error getting job for log")
				return err
			}
			for _, p := range l {
				redacter.RedactTaskLogPart(p, job.Secrets)
			}
			return next(ctx, et, l)
		}
	}
}
