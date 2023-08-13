package coordinator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tork/datastore"
	"github.com/tork/job"
	"github.com/tork/mq"
	"github.com/tork/node"
	"github.com/tork/task"
)

func Test_getQueues(t *testing.T) {
	b := mq.NewInMemoryBroker()
	err := b.SubscribeForTasks("some-queue", func(ctx context.Context, t task.Task) error {
		return nil
	})
	assert.NoError(t, err)
	api := newAPI(Config{
		DataStore: datastore.NewInMemoryDatastore(),
		Broker:    b,
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/queue", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)

	assert.NoError(t, err)
	assert.Equal(t, `[{"Name":"some-queue","Size":0}]`, string(body))
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_getActiveNodes(t *testing.T) {
	ds := datastore.NewInMemoryDatastore()
	active := node.Node{
		ID:              "1234",
		LastHeartbeatAt: time.Now(),
	}
	inactive := node.Node{
		ID:              "2345",
		LastHeartbeatAt: time.Now().Add(-time.Hour),
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
	req, err := http.NewRequest("GET", "/node", nil)
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
	err := ds.CreateTask(context.Background(), ta)
	assert.NoError(t, err)
	api := newAPI(Config{
		DataStore: ds,
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/task/1234", nil)
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

func Test_creteJob(t *testing.T) {
	api := newAPI(Config{
		DataStore: datastore.NewInMemoryDatastore(),
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("POST", "/job", strings.NewReader(`{
		"tasks":[{
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

func Test_getJob(t *testing.T) {
	ds := datastore.NewInMemoryDatastore()
	ds.CreateJob(context.Background(), job.Job{
		ID:    "1234",
		State: job.Pending,
	})
	api := newAPI(Config{
		DataStore: ds,
		Broker:    mq.NewInMemoryBroker(),
	})
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/job/1234", nil)
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

func Test_validateTaskRetry(t *testing.T) {
	err := validateTask(task.Task{
		Image: "some:image",
		Retry: &task.Retry{
			Limit:         5,
			InitialDelay:  "1m",
			ScalingFactor: 2,
		},
	})
	assert.NoError(t, err)
	err = validateTask(task.Task{
		Image: "some:image",
		Retry: &task.Retry{
			Limit:        3,
			InitialDelay: "1h",
		},
	})
	assert.Error(t, err)
	err = validateTask(task.Task{
		Image: "some:image",
		Retry: &task.Retry{
			Limit: 100,
		},
	})
	assert.Error(t, err)
}

func Test_validateTaskBasic(t *testing.T) {
	err := validateTask(task.Task{})
	assert.Error(t, err)
	err = validateTask(task.Task{Image: "some:image"})
	assert.NoError(t, err)
}
