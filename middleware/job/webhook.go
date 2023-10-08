package job

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"net/http"

	"github.com/pkg/errors"
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
		if len(j.Webhooks) == 0 {
			return nil
		}
		summary, err := json.Marshal(tork.NewJobSummary(j))
		if err != nil {
			log.Err(err).Msgf(" error serializing")
			return errors.Wrapf(err, "error serializing job summary")
		}
		for _, webhook := range j.Webhooks {
			go func(w *tork.Webhook) {
				callWebhook(w, summary)
			}(webhook)
		}
		return nil
	}
}

func callWebhook(webhook *tork.Webhook, body []byte) {
	attempts := 1
	client := http.Client{
		Timeout: webhookDefaultTimeout,
	}
	for attempts <= webhookDefaultMaxAttempts {
		log.Debug().Msgf("Calling webhook %s (attempt: %d)", webhook.URL, attempts)
		req, err := http.NewRequest("POST", webhook.URL, bytes.NewReader(body))
		if err != nil {
			log.Error().Err(err).Msg("error creating webhook request")
			return
		}
		if webhook.Headers != nil {
			for name, val := range webhook.Headers {
				req.Header.Set(name, val)
			}
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Error().Err(err).Msg("error submitting webhook")
			return
		}
		if resp.StatusCode == http.StatusOK {
			return
		}
		log.Warn().Msgf("webhook to %s failed with %d", webhook.URL, resp.StatusCode)
		// sleep a little before retrying
		time.Sleep(time.Second * time.Duration(attempts*2))
		attempts = attempts + 1
	}
	log.Error().Msgf("failed to call webhook %s. max attempts: %d)", webhook.URL, webhookDefaultMaxAttempts)
}
