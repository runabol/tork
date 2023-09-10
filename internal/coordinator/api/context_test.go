package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/pkg/input"

	"github.com/runabol/tork/mq"
	"github.com/stretchr/testify/assert"
)

func TestSubmitJob(t *testing.T) {
	api, err := NewAPI(Config{
		DataStore: datastore.NewInMemoryDatastore(),
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NoError(t, err)

	req, err := http.NewRequest("GET", "/", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()

	ctx := Context{api: api, ctx: echo.New().NewContext(req, w)}

	called := false
	listener := func(j *tork.Job) {
		called = true
		assert.Equal(t, tork.JobStateCompleted, j.State)
	}

	j, err := ctx.SubmitJob(&input.Job{
		Name: "test job",
		Tasks: []input.Task{
			{
				Name:  "first task",
				Image: "some:image",
			},
		},
	}, listener)
	assert.NoError(t, err)

	j.State = tork.JobStateCompleted

	err = api.broker.PublishEvent(context.Background(), mq.TOPIC_JOB_COMPLETED, j)
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 100)

	assert.True(t, called)
}
