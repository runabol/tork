package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/stretchr/testify/assert"
)

func TestCordonUncordon(t *testing.T) {
	b := broker.NewInMemoryBroker()
	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: &stubRuntime{},
		Queues:  map[string]int{broker.QUEUE_DEFAULT: 3},
	})
	assert.NoError(t, err)
	assert.NoError(t, w.Start())

	ctx := context.Background()

	// after Start the worker has 3 consumers on the default queue
	qi, err := b.QueueInfo(ctx, broker.QUEUE_DEFAULT)
	assert.NoError(t, err)
	assert.Equal(t, 3, qi.Subscribers)
	assert.False(t, w.isCordoned())

	// cordon -- consumers on the work queue are cancelled
	assert.NoError(t, w.Cordon())
	assert.True(t, w.isCordoned())
	qi, err = b.QueueInfo(ctx, broker.QUEUE_DEFAULT)
	assert.NoError(t, err)
	assert.Equal(t, 0, qi.Subscribers)

	// cordon is idempotent
	assert.NoError(t, w.Cordon())
	assert.True(t, w.isCordoned())

	// uncordon -- consumers are restored
	assert.NoError(t, w.Uncordon())
	assert.False(t, w.isCordoned())
	qi, err = b.QueueInfo(ctx, broker.QUEUE_DEFAULT)
	assert.NoError(t, err)
	assert.Equal(t, 3, qi.Subscribers)

	// uncordon is idempotent
	assert.NoError(t, w.Uncordon())
	assert.False(t, w.isCordoned())
}

func TestCordonKeepsExclusiveQueue(t *testing.T) {
	b := broker.NewInMemoryBroker()
	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: &stubRuntime{},
		Queues:  map[string]int{broker.QUEUE_DEFAULT: 1},
	})
	assert.NoError(t, err)
	assert.NoError(t, w.Start())

	exq := broker.QUEUE_EXCLUSIVE_PREFIX + w.id
	assert.NoError(t, w.Cordon())

	// the node's exclusive (cancellation) queue must remain subscribed so the
	// coordinator can still cancel tasks on a cordoned worker.
	qi, err := b.QueueInfo(context.Background(), exq)
	assert.NoError(t, err)
	assert.Equal(t, 1, qi.Subscribers)
}

func TestCordonEndpointsDisabledWithoutToken(t *testing.T) {
	b := broker.NewInMemoryBroker()
	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: &stubRuntime{},
		Queues:  map[string]int{broker.QUEUE_DEFAULT: 1},
	})
	assert.NoError(t, err)
	assert.NoError(t, w.Start())

	// no token configured -> cordon endpoint is not registered
	req := httptest.NewRequest(http.MethodPost, "/cordon", nil)
	rec := httptest.NewRecorder()
	w.api.server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.False(t, w.isCordoned())
}

func TestCordonEndpointRequiresToken(t *testing.T) {
	b := broker.NewInMemoryBroker()
	w, err := NewWorker(Config{
		Broker:      b,
		Runtime:     &stubRuntime{},
		Queues:      map[string]int{broker.QUEUE_DEFAULT: 1},
		CordonToken: "sekret",
	})
	assert.NoError(t, err)
	assert.NoError(t, w.Start())

	// no token -> 401
	req := httptest.NewRequest(http.MethodPost, "/cordon", nil)
	rec := httptest.NewRecorder()
	w.api.server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.False(t, w.isCordoned())

	// wrong token -> 401
	req = httptest.NewRequest(http.MethodPost, "/cordon", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	w.api.server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.False(t, w.isCordoned())

	// correct token -> 200 and worker is cordoned
	req = httptest.NewRequest(http.MethodPost, "/cordon", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	rec = httptest.NewRecorder()
	w.api.server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, w.isCordoned())

	// uncordon via endpoint
	req = httptest.NewRequest(http.MethodPost, "/uncordon", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	rec = httptest.NewRecorder()
	w.api.server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.False(t, w.isCordoned())
}

func TestStatusEndpoint(t *testing.T) {
	b := broker.NewInMemoryBroker()
	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: &stubRuntime{},
		Queues:  map[string]int{broker.QUEUE_DEFAULT: 1},
	})
	assert.NoError(t, err)
	assert.NoError(t, w.Start())

	get := func() string {
		req := httptest.NewRequest(http.MethodGet, "/status", nil)
		rec := httptest.NewRecorder()
		w.api.server.Handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		return rec.Body.String()
	}

	// status is unauthenticated and reflects cordon state
	assert.Contains(t, get(), "\"cordoned\":false")
	assert.Contains(t, get(), "\"taskCount\":0")
	assert.NoError(t, w.Cordon())
	assert.Contains(t, get(), "\"cordoned\":true")
}

func TestCordonedHeartbeatStatus(t *testing.T) {
	b := broker.NewInMemoryBroker()
	w, err := NewWorker(Config{
		Broker:  b,
		Runtime: &stubRuntime{},
		Queues:  map[string]int{broker.QUEUE_DEFAULT: 1},
	})
	assert.NoError(t, err)

	// nodeStatus is exercised directly (not via the heartbeat loop) so the
	// test stays deterministic and doesn't race the background goroutine.
	ctx := context.Background()
	assert.Equal(t, tork.NodeStatusUP, w.nodeStatus(ctx))
	assert.NoError(t, w.Cordon())
	assert.Equal(t, tork.NodeStatusCordoned, w.nodeStatus(ctx))
	assert.NoError(t, w.Uncordon())
	assert.Equal(t, tork.NodeStatusUP, w.nodeStatus(ctx))

	// a failing health check takes precedence over cordon
	assert.NoError(t, w.Cordon())
	w.runtime.(*stubRuntime).healthErr = errors.New("unhealthy")
	assert.Equal(t, tork.NodeStatusDown, w.nodeStatus(ctx))
}
