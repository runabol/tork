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

	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/job"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/task"
	"github.com/runabol/tork/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_getQueues(t *testing.T) {
	b := mq.NewInMemoryBroker()
	err := b.SubscribeForTasks("some-queue", func(t *task.Task) error {
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
	assert.Equal(t, 3, len(qs))
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
		err := ds.CreateJob(context.Background(), &job.Job{
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

	js := datastore.Page[*job.Job]{}
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

	js = datastore.Page[*job.Job]{}
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

	js = datastore.Page[*job.Job]{}
	err = json.Unmarshal(body, &js)
	assert.NoError(t, err)

	assert.Equal(t, 20, js.Size)
	assert.Equal(t, 6, js.TotalPages)
	assert.Equal(t, 1, js.Number)
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_getActiveNodes(t *testing.T) {
	ds := datastore.NewInMemoryDatastore()
	active := node.Node{
		ID:              "1234",
		LastHeartbeatAt: time.Now().UTC(),
	}
	inactive := node.Node{
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

	nrs := []node.Node{}
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
	req, err := http.NewRequest("GET", "/status", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)

	assert.NoError(t, err)
	assert.Equal(t, `{"status":"OK"}`, string(body))
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
	ta := task.Task{
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
	tr := task.Task{}
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
	j := job.Job{}
	err = json.Unmarshal(body, &j)
	assert.NoError(t, err)
	assert.Equal(t, job.Pending, j.State)
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
	err := ds.CreateJob(context.Background(), &job.Job{
		ID:    "1234",
		State: job.Pending,
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
	j := job.Job{}
	err = json.Unmarshal(body, &j)
	assert.NoError(t, err)
	assert.Equal(t, "1234", j.ID)
	assert.Equal(t, job.Pending, j.State)
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_cancelRunningJob(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	j1 := job.Job{
		ID:        uuid.NewUUID(),
		State:     job.Running,
		CreatedAt: time.Now().UTC(),
	}
	err := ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tasks := []task.Task{{
		ID:        uuid.NewUUID(),
		State:     task.Pending,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     task.Scheduled,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     task.Running,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     task.Cancelled,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     task.Completed,
		CreatedAt: &now,
		JobID:     j1.ID,
	}, {
		ID:        uuid.NewUUID(),
		State:     task.Failed,
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
	assert.Equal(t, `{"status":"OK"}`, string(body))
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_restartJob(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	j1 := job.Job{
		ID:        uuid.NewUUID(),
		State:     job.Cancelled,
		CreatedAt: time.Now().UTC(),
		Position:  1,
		Tasks: []*task.Task{
			{
				Name: "some fake task",
			},
		},
	}
	err := ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tasks := []task.Task{{
		ID:        uuid.NewUUID(),
		State:     task.Pending,
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
	assert.Equal(t, `{"status":"OK"}`, string(body))
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_restartRunningJob(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewInMemoryDatastore()
	j1 := job.Job{
		ID:        uuid.NewUUID(),
		State:     job.Running,
		CreatedAt: time.Now().UTC(),
		Position:  1,
		Tasks: []*task.Task{
			{
				Name: "some fake task",
			},
		},
	}
	err := ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tasks := []task.Task{{
		ID:        uuid.NewUUID(),
		State:     task.Pending,
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
	j1 := job.Job{
		ID:        uuid.NewUUID(),
		State:     job.Failed,
		CreatedAt: time.Now().UTC(),
		Position:  2,
		Tasks: []*task.Task{
			{
				Name: "some fake task",
			},
		},
	}
	err := ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tasks := []task.Task{{
		ID:        uuid.NewUUID(),
		State:     task.Pending,
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
