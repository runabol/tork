package job

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/webhook"
	"github.com/stretchr/testify/assert"
)

func TestWebhookNoEvent(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook})

	received := make(chan any)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		js := tork.JobSummary{}
		err = json.Unmarshal(body, &js)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, "1234", js.ID)
		assert.Equal(t, tork.JobStateCompleted, js.State)
		w.WriteHeader(http.StatusOK)
		close(received)
	}))

	j := &tork.Job{
		ID:    "1234",
		State: tork.JobStateCompleted,
		Webhooks: []*tork.Webhook{{
			URL: svr.URL,
		}},
	}

	assert.NoError(t, hm(context.Background(), StateChange, j))
	<-received
}

func TestWebhookJobEvent(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook})

	received := make(chan any, 2)

	callbackState := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		js := tork.JobSummary{}
		err = json.Unmarshal(body, &js)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, "1234", js.ID)
		assert.Equal(t, tork.JobStateCompleted, js.State)
		w.WriteHeader(http.StatusOK)
		received <- 1
	}))

	callbackProgress := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		js := tork.JobSummary{}
		err = json.Unmarshal(body, &js)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, "5678", js.ID)
		assert.Equal(t, tork.JobStateRunning, js.State)
		w.WriteHeader(http.StatusOK)
		received <- 1
	}))

	j1 := &tork.Job{
		ID:    "1234",
		State: tork.JobStateCompleted,
		Webhooks: []*tork.Webhook{{
			URL:   callbackState.URL,
			Event: webhook.EventJobStateChange,
		}},
	}

	j2 := &tork.Job{
		ID:    "5678",
		State: tork.JobStateRunning,
		Webhooks: []*tork.Webhook{{
			URL:   callbackProgress.URL,
			Event: webhook.EventJobProgress,
		}},
	}

	assert.NoError(t, hm(context.Background(), StateChange, j1))
	<-received
	assert.NoError(t, hm(context.Background(), Progress, j2))
	<-received
}

func TestWebhookRetry(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook})

	received := make(chan any)
	attempt := 1

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempt == 1 {
			attempt = attempt + 1
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		js := tork.JobSummary{}
		err = json.Unmarshal(body, &js)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, "1234", js.ID)
		assert.Equal(t, tork.JobStateCompleted, js.State)
		w.WriteHeader(http.StatusOK)
		close(received)
	}))

	j := &tork.Job{
		ID:    "1234",
		State: tork.JobStateCompleted,
		Webhooks: []*tork.Webhook{{
			URL: svr.URL,
		}},
	}

	assert.NoError(t, hm(context.Background(), StateChange, j))
	<-received
}

func TestWebhookOKWithHeaders(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook})

	received := make(chan any)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		js := tork.JobSummary{}
		err = json.Unmarshal(body, &js)
		if err != nil {
			panic(err)
		}
		ctype := r.Header.Get("Content-Type")
		assert.Equal(t, "application/json; charset=UTF-8", ctype)
		assert.Equal(t, "my-value", r.Header.Get("my-header"))
		assert.Equal(t, "1234-5678", r.Header.Get("secret"))
		assert.Equal(t, "1234", js.ID)
		assert.Equal(t, tork.JobStateCompleted, js.State)
		w.WriteHeader(http.StatusOK)
		close(received)
	}))

	j := &tork.Job{
		ID:    "1234",
		State: tork.JobStateCompleted,
		Context: tork.JobContext{
			Secrets: map[string]string{
				"some_key": "1234-5678",
			},
		},
		Webhooks: []*tork.Webhook{{
			URL: svr.URL,
			Headers: map[string]string{
				"my-header": "my-value",
				"secret":    "{{secrets.some_key}}",
			},
		}},
	}

	assert.NoError(t, hm(context.Background(), StateChange, j))
	<-received
}

func TestWebhookIgnored(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook})
	assert.NoError(t, hm(context.Background(), Read, nil))
}

func TestWebhookWrongEvent(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook})
	received := make(chan any)
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(received)
	}))
	j := &tork.Job{
		ID:    "1234",
		State: tork.JobStateCompleted,
		Webhooks: []*tork.Webhook{{
			URL: svr.URL,
			Headers: map[string]string{
				"my-header": "my-value",
			},
			Event: webhook.EventJobStateChange,
		}},
	}
	assert.NoError(t, hm(context.Background(), "NO_STATE_CHANGE", j))
	select {
	case <-received:
		t.Error("Received a webhook call when it should not have been called")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestWebhookIfTrue(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook})
	received := make(chan any)
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(received)
	}))
	j := &tork.Job{
		ID:    "1234",
		State: tork.JobStateCompleted,
		Webhooks: []*tork.Webhook{{
			URL: svr.URL,
			If:  "true",
		}},
	}

	assert.NoError(t, hm(context.Background(), StateChange, j))
	<-received
}

func TestWebhookIfFalse(t *testing.T) {
	received := make(chan any)
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(received)
	}))
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook})
	j := &tork.Job{
		ID:    "1234",
		State: tork.JobStateCompleted,
		Webhooks: []*tork.Webhook{{
			URL: svr.URL,
			If:  "false",
		}},
	}
	assert.NoError(t, hm(context.Background(), StateChange, j))
	select {
	case <-received:
		t.Error("Received a webhook call when it should not have been called")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestWebhookIfJobStatus(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook})
	received := make(chan any)
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(received)
	}))
	j := &tork.Job{
		ID:    "1234",
		State: tork.JobStateCompleted,
		Webhooks: []*tork.Webhook{{
			URL: svr.URL,
			If:  "{{ job.State == 'COMPLETED' }}",
		}},
	}
	assert.NoError(t, hm(context.Background(), StateChange, j))
	<-received
}
