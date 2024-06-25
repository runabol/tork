package task

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore/inmemory"
	"github.com/runabol/tork/internal/webhook"
	"github.com/stretchr/testify/assert"
)

func TestWebhookOK(t *testing.T) {
	ds := inmemory.NewInMemoryDatastore()

	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook(ds)})

	received := make(chan any, 2)

	callbackState := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		js := tork.TaskSummary{}
		err = json.Unmarshal(body, &js)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, "2", js.ID)
		assert.Equal(t, tork.TaskStateCompleted, js.State)
		assert.Equal(t, "my-value", r.Header.Get("my-header"))
		assert.Equal(t, "1234-5678", r.Header.Get("secret"))
		w.WriteHeader(http.StatusOK)
		received <- 1
	}))

	callbackProgress := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		js := tork.TaskSummary{}
		err = json.Unmarshal(body, &js)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, "3", js.ID)
		assert.Equal(t, tork.TaskStateRunning, js.State)
		assert.Equal(t, float64(75), js.Progress)
		assert.Equal(t, "my-value", r.Header.Get("my-header"))
		assert.Equal(t, "1234-5678", r.Header.Get("secret"))
		w.WriteHeader(http.StatusOK)
		received <- 1
	}))

	j := &tork.Job{
		ID:    "1",
		State: tork.JobStateCompleted,
		Context: tork.JobContext{
			Secrets: map[string]string{
				"some_key": "1234-5678",
			},
		},
		Webhooks: []*tork.Webhook{{
			URL:   callbackState.URL,
			Event: webhook.EventTaskStateChange,
			Headers: map[string]string{
				"my-header": "my-value",
				"secret":    "{{secrets.some_key}}",
			},
		}, {
			URL:   callbackProgress.URL,
			Event: webhook.EventTaskProgress,
			Headers: map[string]string{
				"my-header": "my-value",
				"secret":    "{{secrets.some_key}}",
			},
		}},
	}

	err := ds.CreateJob(context.Background(), j)
	assert.NoError(t, err)

	tk := &tork.Task{
		ID:    "2",
		JobID: j.ID,
		State: tork.TaskStateCompleted,
	}

	tk2 := &tork.Task{
		ID:       "3",
		JobID:    j.ID,
		State:    tork.TaskStateRunning,
		Progress: 75,
	}

	assert.NoError(t, hm(context.Background(), StateChange, tk))
	assert.NoError(t, hm(context.Background(), StateChange, tk))
	assert.NoError(t, hm(context.Background(), Progress, tk2))
	<-received
	<-received
	<-received
}

func TestWebhookNoEvent(t *testing.T) {
	ds := inmemory.NewInMemoryDatastore()

	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook(ds)})

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(1)
	}))

	j := &tork.Job{
		ID:    "1",
		State: tork.JobStateCompleted,
		Webhooks: []*tork.Webhook{{
			URL: svr.URL,
		}},
	}

	err := ds.CreateJob(context.Background(), j)
	assert.NoError(t, err)

	tk := &tork.Task{
		ID:    "2",
		JobID: j.ID,
	}

	assert.NoError(t, hm(context.Background(), StateChange, tk))
}

func TestWebhookIgnored(t *testing.T) {
	ds := inmemory.NewInMemoryDatastore()
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Webhook(ds)})
	assert.NoError(t, hm(context.Background(), Read, nil))
}
