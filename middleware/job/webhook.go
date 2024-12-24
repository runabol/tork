package job

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/eval"
	"github.com/runabol/tork/internal/webhook"
)

func Webhook(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, et EventType, j *tork.Job) error {
		if err := next(ctx, et, j); err != nil {
			return err
		}
		if et != StateChange && et != Progress {
			return nil
		}
		if len(j.Webhooks) == 0 {
			return nil
		}
		for _, wh := range j.Webhooks {
			if wh.Event != webhook.EventJobStateChange && wh.Event != webhook.EventDefault && wh.Event != webhook.EventJobProgress {
				continue
			}
			if et == StateChange && wh.Event != webhook.EventJobStateChange && wh.Event != webhook.EventDefault {
				continue
			}
			if et == Progress && wh.Event != webhook.EventJobProgress {
				continue
			}
			if wh.If != "" {
				val, err := eval.EvaluateExpr(wh.If, map[string]any{
					"job": j,
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
				callWebhook(w.Clone(), j)
			}(wh)
		}
		return nil
	}
}

func callWebhook(wh *tork.Webhook, job *tork.Job) {
	log.Debug().Msgf("[Webhook] Calling %s for job %s %s", wh.URL, job.ID, job.State)
	// evaluate headers
	for name, v := range wh.Headers {
		newv, err := eval.EvaluateTemplate(v, job.Context.AsMap())
		if err != nil {
			log.Error().Err(err).Msgf("[Webhook] error evaluating header %s: %s", name, v)
		}
		wh.Headers[name] = newv
	}
	summary := tork.NewJobSummary(job)
	if err := webhook.Call(wh, summary); err != nil {
		log.Error().Err(err).Msgf("[Webhook] error calling job webhook %s", wh.URL)
	}
}
