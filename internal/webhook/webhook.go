package webhook

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/fns"
)

const (
	webhookDefaultMaxAttempts = 5
	webhookDefaultTimeout     = time.Second * 5
)

const (
	EventJobStateChange  = "job.StateChange"
	EventJobProgress     = "job.Progress"
	EventTaskStateChange = "task.StateChange"
	EventTaskProgress    = "task.Progress"
	EventDefault         = ""
)

var retryableStatusCodes = map[int]bool{
	http.StatusTooManyRequests:     true, // 429
	http.StatusInternalServerError: true, // 500
	http.StatusBadGateway:          true, // 502
	http.StatusServiceUnavailable:  true, // 503
	http.StatusGatewayTimeout:      true, // 504
}

func isRetryable(statusCode int) bool {
	return retryableStatusCodes[statusCode]
}

func Call(wh *tork.Webhook, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return errors.Wrapf(err, "[Webhook] error serializing body")
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
			log.Error().Msgf("[Webhook] request to %s failed with error: %v", wh.URL, err)
			time.Sleep(time.Second * time.Duration(attempts*2))
			attempts++
			continue
		}
		defer fns.CloseIgnore(resp.Body)
		// Success (2xx)
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		// Check if the status code is retryable
		if !isRetryable(resp.StatusCode) {
			log.Error().Msgf("[Webhook] request to %s failed with non-retryable status %d", wh.URL, resp.StatusCode)
			return errors.Errorf("[Webhook] request to %s failed with non-retryable status %d", wh.URL, resp.StatusCode)
		}
		log.Warn().Msgf("[Webhook] request to %s failed with %d", wh.URL, resp.StatusCode)
		// sleep a little before retrying
		time.Sleep(time.Second * time.Duration(attempts*2))
		attempts = attempts + 1
	}
	log.Error().Msgf("[Webhook] failed to call webhook %s. max attempts: %d)", wh.URL, webhookDefaultMaxAttempts)
	return nil
}
