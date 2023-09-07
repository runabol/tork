package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/middleware"

	"github.com/runabol/tork/mq"

	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_getQueues(t *testing.T) {
	b := mq.NewInMemoryBroker()
	err := b.SubscribeForTasks("some-queue", func(t *tork.Task) error {
		return nil
	})
	assert.NoError(t, err)
	api := newAPI(Config{
		DataStore: datastore.NewInMemoryDatastore(),
		Broker:    b,
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/queues", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)

	qs := []mq.QueueInfo{}
	err = json.Unmarshal(body, &qs)
	assert.NoError(t, err)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(qs))
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_listJobs(t *testing.T) {
	b := mq.NewInMemoryBroker()
	ds := datastore.NewInMemoryDatastore()
	api := newAPI(Config{
		DataStore: ds,
		Broker:    b,
	})
	assert.NotNil(t, api)

	for i := 0; i < 101; i++ {
		err := ds.CreateJob(context.Background(), &tork.Job{
			ID: uuid.NewUUID(),
		})
		assert.NoError(t, err)
	}

	req, err := http.NewRequest("GET", "/jobs", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)

	js := datastore.Page[*tork.Job]{}
	err = json.Unmarshal(body, &js)
	assert.NoError(t, err)

	assert.NoError(t, err)
	assert.Equal(t, 10, js.Size)
	assert.Equal(t, 11, js.TotalPages)
	assert.Equal(t, 1, js.Number)
	assert.Equal(t, http.StatusOK, w.Code)

	req, err = http.NewRequest("GET", "/jobs?page=11", nil)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err = io.ReadAll(w.Body)
	assert.NoError(t, err)

	js = datastore.Page[*tork.Job]{}
	err = json.Unmarshal(body, &js)
	assert.NoError(t, err)

	assert.Equal(t, 1, js.Size)
	assert.Equal(t, 11, js.TotalPages)
	assert.Equal(t, 11, js.Number)
	assert.Equal(t, http.StatusOK, w.Code)

	req, err = http.NewRequest("GET", "/jobs?page=1&size=50", nil)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err = io.ReadAll(w.Body)
	assert.NoError(t, err)

	js = datastore.Page[*tork.Job]{}
	err = json.Unmarshal(body, &js)
	assert.NoError(t, err)

	assert.Equal(t, 20, js.Size)
	assert.Equal(t, 6, js.TotalPages)
	assert.Equal(t, 1, js.Number)
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_getActiveNodes(t *testing.T) {
	ds := datastore.NewInMemoryDatastore()
	active := tork.Node{
		ID:              "1234",
		LastHeartbeatAt: time.Now().UTC(),
	}
	inactive := tork.Node{
		ID:              "2345",
		LastHeartbeatAt: time.Now().UTC().Add(-time.Hour),
	}
	err := ds.CreateNode(context.Background(), active)
	assert.NoError(t, err)
	err = ds.CreateNode(context.Background(), inactive)
	assert.NoError(t, err)
	api := newAPI(Config{
		DataStore: ds,
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/nodes", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)

	nrs := []tork.Node{}
	err = json.Unmarshal(body, &nrs)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(nrs))
	assert.Equal(t, "1234", nrs[0].ID)
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_getStatus(t *testing.T) {
	api := newAPI(Config{
		DataStore: datastore.NewInMemoryDatastore(),
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/health", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)

	assert.NoError(t, err)
	assert.Contains(t, string(body), "\"status\":\"UP\"")
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_getUnknownTask(t *testing.T) {
	api := newAPI(Config{
		DataStore: datastore.NewInMemoryDatastore(),
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/task/1", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	_, err = io.ReadAll(w.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func Test_getTask(t *testing.T) {
	ds := datastore.NewInMemoryDatastore()
	ta := tork.Task{
		ID:   "1234",
		Name: "test task",
	}
	err := ds.CreateTask(context.Background(), &ta)
	assert.NoError(t, err)
	api := newAPI(Config{
		DataStore: ds,
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/tasks/1234", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)
	tr := tork.Task{}
	err = json.Unmarshal(body, &tr)
	assert.NoError(t, err)
	assert.Equal(t, "1234", tr.ID)
	assert.Equal(t, "test task", tr.Name)
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_createJob(t *testing.T) {
	api := newAPI(Config{
		DataStore: datastore.NewInMemoryDatastore(),
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("POST", "/jobs", strings.NewReader(`{
		"name":"test job",
		"tasks":[{
			"name":"test task",
			"image":"some:image"
		}]
	}`))
	req.Header.Add("Content-Type", "application/json")
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)
	j := tork.Job{}
	err = json.Unmarshal(body, &j)
	assert.NoError(t, err)
	assert.Equal(t, tork.JobStatePending, j.State)
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_createJobInvalidProperty(t *testing.T) {
	api := newAPI(Config{
		DataStore: datastore.NewInMemoryDatastore(),
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("POST", "/jobs", strings.NewReader(`{
		"tasks":[{
			"nosuch":"thing",
			"image":"some:image"
		}]
	}`))
	req.Header.Add("Content-Type", "application/json")
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func Test_getJob(t *testing.T) {
	ds := datastore.NewInMemoryDatastore()
	err := ds.CreateJob(context.Background(), &tork.Job{
		ID:    "1234",
		State: tork.JobStatePending,
	})
	assert.NoError(t, err)
	api := newAPI(Config{
		DataStore: ds,
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/jobs/1234", nil)
	req.Header.Add("Content-Type", "application/json")
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)
	j := tork.Job{}
	err = json.Unmarshal(body, &j)
	assert.NoError(t, err)
	assert.Equal(t, "1234", j.ID)
	assert.Equal(t, tork.JobStatePending, j.State)
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_cancelRunningJob(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	j1 := tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		CreatedAt: time.Now().UTC(),
	}
	err := ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tasks := []tork.Task{{
		ID:        uuid.NewUUID(),
		State:     tork.TaskStatePending,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateScheduled,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateRunning,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateCancelled,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateCompleted,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     tork.TaskStateFailed,
		CreatedAt: &now,
		JobID:     j1.ID,
	}}

	for _, ta := range tasks {
		err := ds.CreateTask(ctx, &ta)
		assert.NoError(t, err)
	}

	api := newAPI(Config{
		DataStore: ds,
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("PUT", fmt.Sprintf("/jobs/%s/cancel", j1.ID), nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)

	assert.NoError(t, err)
	assert.Equal(t, "{\"status\":\"OK\"}\n", string(body))
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_restartJob(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	j1 := tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateCancelled,
		CreatedAt: time.Now().UTC(),
		Position:  1,
		Tasks: []*tork.Task{
			{
				Name: "some fake task",
			},
		},
	}
	err := ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tasks := []tork.Task{{
		ID:        uuid.NewUUID(),
		State:     tork.TaskStatePending,
		CreatedAt: &now,
		JobID:     j1.ID,
	}}

	for _, ta := range tasks {
		err := ds.CreateTask(ctx, &ta)
		assert.NoError(t, err)
	}

	api := newAPI(Config{
		DataStore: ds,
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("PUT", fmt.Sprintf("/jobs/%s/restart", j1.ID), nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)

	assert.NoError(t, err)
	assert.Equal(t, "{\"status\":\"OK\"}\n", string(body))
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_restartRunningJob(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	j1 := tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateRunning,
		CreatedAt: time.Now().UTC(),
		Position:  1,
		Tasks: []*tork.Task{
			{
				Name: "some fake task",
			},
		},
	}
	err := ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tasks := []tork.Task{{
		ID:        uuid.NewUUID(),
		State:     tork.TaskStatePending,
		CreatedAt: &now,
		JobID:     j1.ID,
	}}

	for _, ta := range tasks {
		err := ds.CreateTask(ctx, &ta)
		assert.NoError(t, err)
	}

	api := newAPI(Config{
		DataStore: ds,
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("PUT", fmt.Sprintf("/jobs/%s/restart", j1.ID), nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func Test_restartRunningNoMoreTasksJob(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	j1 := tork.Job{
		ID:        uuid.NewUUID(),
		State:     tork.JobStateFailed,
		CreatedAt: time.Now().UTC(),
		Position:  2,
		Tasks: []*tork.Task{
			{
				Name: "some fake task",
			},
		},
	}
	err := ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tasks := []tork.Task{{
		ID:        uuid.NewUUID(),
		State:     tork.TaskStatePending,
		CreatedAt: &now,
		JobID:     j1.ID,
	}}

	for _, ta := range tasks {
		err := ds.CreateTask(ctx, &ta)
		assert.NoError(t, err)
	}

	api := newAPI(Config{
		DataStore: ds,
		Broker:    mq.NewInMemoryBroker(),
	})

	assert.NotNil(t, api)
	req, err := http.NewRequest("PUT", fmt.Sprintf("/jobs/%s/restart", j1.ID), nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func Test_middleware(t *testing.T) {
	mw := func(next middleware.HandlerFunc) middleware.HandlerFunc {
		return func(c middleware.Context) error {
			if strings.HasPrefix(c.Request().URL.Path, "/middleware") {
				return c.String(http.StatusOK, "OK")
			}
			return next(c)
		}
	}
	b := mq.NewInMemoryBroker()
	api := newAPI(Config{
		DataStore:   datastore.NewInMemoryDatastore(),
		Broker:      b,
		Middlewares: []middleware.MiddlewareFunc{mw},
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/middleware", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "OK", string(body))
}

func Test_middlewareMultiple(t *testing.T) {
	mw1 := func(next middleware.HandlerFunc) middleware.HandlerFunc {
		return func(c middleware.Context) error {
			if strings.HasPrefix(c.Request().URL.Path, "/middleware1") {
				return c.String(http.StatusOK, "OK1")
			}
			return next(c)
		}
	}
	mw2 := func(next middleware.HandlerFunc) middleware.HandlerFunc {
		return func(c middleware.Context) error {
			if strings.HasPrefix(c.Request().URL.Path, "/middleware2") {
				return c.String(http.StatusOK, "OK2")
			}
			return next(c)
		}
	}
	b := mq.NewInMemoryBroker()
	api := newAPI(Config{
		DataStore:   datastore.NewInMemoryDatastore(),
		Broker:      b,
		Middlewares: []middleware.MiddlewareFunc{mw1, mw2},
	})
	assert.NotNil(t, api)

	req, err := http.NewRequest("GET", "/middleware1", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "OK1", string(body))

	req, err = http.NewRequest("GET", "/middleware2", nil)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err = io.ReadAll(w.Body)
	assert.NoError(t, err)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "OK2", string(body))
}
