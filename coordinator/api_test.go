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

	"github.com/stretchr/testify/assert"
	"github.com/tork/datastore"
	"github.com/tork/job"
	"github.com/tork/mq"
	"github.com/tork/node"
	"github.com/tork/task"
	"github.com/tork/uuid"
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
	req, err := http.NewRequest("GET", "/queue", nil)
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
	err := ds.CreateTask(context.Background(), &ta)
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

func Test_createJob(t *testing.T) {
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

func Test_sanitizeTaskRetry(t *testing.T) {
	err := sanitizeTask(&task.Task{
		Image: "some:image",
		Retry: &task.Retry{
			Limit: 5,
		},
	})
	assert.NoError(t, err)
	err = sanitizeTask(&task.Task{
		Image: "some:image",
		Retry: &task.Retry{
			Limit: 100,
		},
	})
	assert.Error(t, err)
	err = sanitizeTask(&task.Task{
		Image:   "some:image",
		Timeout: "-10s",
	})
	assert.Error(t, err)
	err = sanitizeTask(&task.Task{
		Image:   "some:image",
		Timeout: "10s",
	})
	assert.NoError(t, err)
	rt1 := &task.Task{
		Image:   "some:image",
		Timeout: "10s",
		Retry:   &task.Retry{},
	}
	err = sanitizeTask(rt1)
	assert.NoError(t, err)
	err = sanitizeTask(&task.Task{
		Parallel: []*task.Task{
			{
				Image: "some:image",
			},
		},
	})
	assert.NoError(t, err)
	err = sanitizeTask(&task.Task{
		Image: "some:image",
		Parallel: []*task.Task{
			{
				Image: "some:image",
			},
		},
	})
	assert.Error(t, err)
	err = sanitizeTask(&task.Task{
		Parallel: []*task.Task{
			{
				Image: "some:image",
			},
		},
		Each: &task.Each{
			List: "${ some expression }",
			Task: &task.Task{
				Image: "some:image",
			},
		},
	})
	assert.Error(t, err)
	err = sanitizeTask(&task.Task{
		Each: &task.Each{
			List: "${ some expression }",
			Task: &task.Task{
				Image: "some:image",
			},
		},
	})
	assert.NoError(t, err)
	err = sanitizeTask(&task.Task{
		Each: &task.Each{
			Task: &task.Task{
				Image: "some:image",
			},
		},
	})
	assert.Error(t, err)
}

func Test_sanitizeTaskBasic(t *testing.T) {
	err := sanitizeTask(&task.Task{})
	assert.Error(t, err)
	err = sanitizeTask(&task.Task{Image: "some:image"})
	assert.NoError(t, err)
	err = sanitizeTask(&task.Task{
		Parallel: []*task.Task{{Image: "some:image"}},
	})
	assert.NoError(t, err)
	err = sanitizeTask(&task.Task{
		Parallel: []*task.Task{{Name: "bad task"}},
	})
	assert.Error(t, err)
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
	req, err := http.NewRequest("PUT", fmt.Sprintf("/job/%s/cancel", j1.ID), nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	body, err := io.ReadAll(w.Body)
	assert.NoError(t, err)

	assert.NoError(t, err)
	assert.Equal(t, `{"status":"OK"}`, string(body))
	assert.Equal(t, http.StatusOK, w.Code)

	actives, err := ds.GetActiveTasks(ctx, j1.ID)
	assert.NoError(t, err)
	assert.Empty(t, actives)
}
