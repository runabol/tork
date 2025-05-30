package task

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/cache"
	"github.com/runabol/tork/internal/eval"
	"github.com/runabol/tork/internal/webhook"
)

func Webhook(ds datastore.Datastore) MiddlewareFunc {
	cache := cache.New[*tork.Job](time.Hour, time.Minute)
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, et EventType, t *tork.Task) error {
			if err := next(ctx, et, t); err != nil {
				return err
			}
			if et != StateChange && et != Progress {
				return nil
			}
			job, err := getJob(ctx, t, ds, cache)
			if err != nil {
				return err
			}
			if len(job.Webhooks) == 0 {
				return nil
			}
			summary := tork.NewTaskSummary(t)
			for _, wh := range job.Webhooks {
				if wh.Event != webhook.EventTaskStateChange && wh.Event != webhook.EventTaskProgress {
					continue
				}
				if (wh.Event == webhook.EventTaskStateChange && et != StateChange) ||
					(wh.Event == webhook.EventTaskProgress && et != Progress) {
					continue
				}
				if wh.If != "" {
					val, err := eval.EvaluateExpr(wh.If, map[string]any{
						"task": tork.NewTaskSummary(t),
						"job":  tork.NewJobSummary(job),
					})
					if err != nil {
						log.Error().Err(err).Msgf("[Webhook] error evaluating if expression %s", wh.If)
						continue
					}
					ifResult, ok := val.(bool)
					if !ok {
						log.Error().Msgf("[Webhook] if expression %s did not evaluate to a boolean", wh.If)
						continue
					}
					if !ifResult {
						continue
					}
				}
				go func(w *tork.Webhook) {
					callWebhook(w.Clone(), job, summary)
				}(wh)
			}
			return nil
		}
	}
}

func getJob(ctx context.Context, t *tork.Task, ds datastore.Datastore, c *cache.Cache[*tork.Job]) (*tork.Job, error) {
	job, ok := c.Get(t.JobID)
	if ok {
		return job, nil
	}
	job, err := ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return nil, err
	}
	c.Set(job.ID, job)
	return job, nil
}

func callWebhook(wh *tork.Webhook, job *tork.Job, summary *tork.TaskSummary) {
	log.Debug().Msgf("[Webhook] Calling %s for task %s %s", wh.URL, summary.ID, summary.State)
	// evaluate headers
	for name, v := range wh.Headers {
		newv, err := eval.EvaluateTemplate(v, job.Context.AsMap())
		if err != nil {
			log.Error().Err(err).Msgf("[Webhook] error evaluating header %s: %s", name, v)
		}
		wh.Headers[name] = newv
	}
	if err := webhook.Call(wh, summary); err != nil {
		log.Info().Err(err).Msgf("[Webhook] error calling task webhook %s", wh.URL)
	}
}
