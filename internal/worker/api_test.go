package worker

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/runtime/docker"
	"github.com/stretchr/testify/assert"
)

func Test_health(t *testing.T) {
	rt, err := docker.NewDockerRuntime()
	assert.NoError(t, err)
	api := newAPI(Config{
		Broker:  mq.NewInMemoryBroker(),
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
