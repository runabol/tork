package job

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
)

const (
	webhookDefaultMaxAttempts = 5
	webhookDefaultTimeout     = time.Second * 5
)

func Webhook(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, et EventType, j *tork.Job) error {
		if err := next(ctx, et, j); err != nil {
			return err
		}
		if et != StateChange {
			return nil
		}
		if len(j.Webhooks) == 0 {
			return nil
		}
		summary := tork.NewJobSummary(j)
		for _, webhook := range j.Webhooks {
			go func(w *tork.Webhook) {
				callWebhook(w, summary)
			}(webhook)
		}
		return nil
	}
}

func callWebhook(webhook *tork.Webhook, summary *tork.JobSummary) {
	attempts := 1
	client := http.Client{
		Timeout: webhookDefaultTimeout,
	}
	body, err := json.Marshal(summary)
	if err != nil {
		log.Err(err).Msgf("[Webhook] error serializing job summary")
	}
	for attempts <= webhookDefaultMaxAttempts {
		log.Debug().Msgf("[Webhook] Calling %s for job %s %s (attempt: %d)", webhook.URL, summary.ID, summary.State, attempts)
		req, err := http.NewRequest("POST", webhook.URL, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json; charset=UTF-8")
		if err != nil {
			log.Error().Err(err).Msgf("[Webhook] error creating webhook request: %s", webhook.URL)
			return
		}
		if webhook.Headers != nil {
			for name, val := range webhook.Headers {
				req.Header.Set(name, val)
			}
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Error().Err(err).Msgf("[Webhook] error submitting webhook %s", webhook.URL)
			return
		}
		if resp.StatusCode == http.StatusOK {
			return
		}
		log.Warn().Msgf("[Webhook] request to %s failed with %d", webhook.URL, resp.StatusCode)
		// sleep a little before retrying
		time.Sleep(time.Second * time.Duration(attempts*2))
		attempts = attempts + 1
	}
	log.Error().Msgf("[Webhook] failed to call webhook %s. max attempts: %d)", webhook.URL, webhookDefaultMaxAttempts)
}
