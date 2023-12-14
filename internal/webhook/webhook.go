package webhook

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
)

const (
	webhookDefaultMaxAttempts = 5
	webhookDefaultTimeout     = time.Second * 5
)

const (
	EventJobStateChange  = "job.StateChange"
	EventTaskStateChange = "task.StateChange"
	EventDefault         = ""
)

func Call(wh *tork.Webhook, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		log.Err(err).Msgf("[Webhook] error serializing body")
	}
	attempts := 1
	client := http.Client{
		Timeout: webhookDefaultTimeout,
	}
	for attempts <= webhookDefaultMaxAttempts {
		req, err := http.NewRequest("POST", wh.URL, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json; charset=UTF-8")
		if err != nil {
			return err
		}
		if wh.Headers != nil {
			for name, val := range wh.Headers {
				req.Header.Set(name, val)
			}
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		log.Warn().Msgf("[Webhook] request to %s failed with %d", wh.URL, resp.StatusCode)
		// sleep a little before retrying
		time.Sleep(time.Second * time.Duration(attempts*2))
		attempts = attempts + 1
	}
	log.Error().Msgf("[Webhook] failed to call webhook %s. max attempts: %d)", wh.URL, webhookDefaultMaxAttempts)
	return nil
}
