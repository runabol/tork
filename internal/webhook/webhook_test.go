package webhook

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestWebhookOK(t *testing.T) {
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
	}

	wh := &tork.Webhook{
		URL: svr.URL,
	}

	assert.NoError(t, Call(wh, tork.NewJobSummary(j)))
	<-received
}

func TestWebhookRetry(t *testing.T) {
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
	}

	wh := &tork.Webhook{
		URL: svr.URL,
	}

	assert.NoError(t, Call(wh, tork.NewJobSummary(j)))
	<-received
}

func TestWebhookOKWithHeaders(t *testing.T) {
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
		val := r.Header.Get("my-header")
		assert.Equal(t, "my-value", val)
		assert.Equal(t, "1234", js.ID)
		assert.Equal(t, tork.JobStateCompleted, js.State)
		w.WriteHeader(http.StatusOK)
		close(received)
	}))

	j := &tork.Job{
		ID:    "1234",
		State: tork.JobStateCompleted,
	}

	wh := &tork.Webhook{URL: svr.URL,
		Headers: map[string]string{
			"my-header": "my-value",
		}}

	assert.NoError(t, Call(wh, tork.NewJobSummary(j)))
	<-received
}
