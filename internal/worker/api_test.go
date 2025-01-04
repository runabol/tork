package worker

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/runtime/docker"
	"github.com/stretchr/testify/assert"
)

func Test_health(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)
	api := newAPI(Config{
		Broker:  broker.NewInMemoryBroker(),
		Runtime: rt,
	}, &syncx.Map[string, runningTask]{})
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

func Test_proxyTaskRoot(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))

	svrURL, err := url.Parse(svr.URL)
	assert.NoError(t, err)

	port := svrURL.Port()
	assert.NoError(t, err)

	tasks := &syncx.Map[string, runningTask]{}
	tasks.Set("1234", runningTask{
		task: &tork.Task{
			ID: "1234",
			Ports: []*tork.Port{{
				Port:     "8080",
				HostPort: port,
			}},
		},
	})

	api := newAPI(Config{
		Broker:  broker.NewInMemoryBroker(),
		Runtime: rt,
	}, tasks)
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/tasks/1234/8080", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func Test_proxyTaskSomePath(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/some/path", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))

	svrURL, err := url.Parse(svr.URL)
	assert.NoError(t, err)

	port := svrURL.Port()
	assert.NoError(t, err)

	tasks := &syncx.Map[string, runningTask]{}
	tasks.Set("1234", runningTask{
		task: &tork.Task{
			ID: "1234",
			Ports: []*tork.Port{{
				Port:     "8080",
				HostPort: port,
			}},
		},
	})

	api := newAPI(Config{
		Broker:  broker.NewInMemoryBroker(),
		Runtime: rt,
	}, tasks)
	assert.NotNil(t, api)
	req, err := http.NewRequest("GET", "/tasks/1234/8080/some/path", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	api.server.Handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}
