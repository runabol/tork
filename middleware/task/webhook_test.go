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

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		w.WriteHeader(http.StatusOK)
		received <- 1
	}))

	j := &tork.Job{
		ID:    "1",
		State: tork.JobStateCompleted,
		Webhooks: []*tork.Webhook{{
			URL:   svr.URL,
			Event: webhook.EventTaskStateChange,
		}},
	}

	err := ds.CreateJob(context.Background(), j)
	assert.NoError(t, err)

	tk := &tork.Task{
		ID:    "2",
		JobID: j.ID,
		State: tork.TaskStateCompleted,
	}

	assert.NoError(t, hm(context.Background(), StateChange, tk))
	assert.NoError(t, hm(context.Background(), StateChange, tk))
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
