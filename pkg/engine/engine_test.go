package engine

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/runabol/tork/pkg/conf"
	"github.com/stretchr/testify/assert"
)

func TestRunStandalone(t *testing.T) {
	err := conf.LoadConfig()
	assert.NoError(t, err)

	eng := New(Config{
		Mode: ModeStandalone,
	})

	started := false
	eng.OnStarted(func() error {
		started = true
		eng.Terminate()
		return nil
	})

	err = eng.Start()
	assert.NoError(t, err)

	assert.True(t, started)
}

func TestRunCoordinator(t *testing.T) {
	err := conf.LoadConfig()
	assert.NoError(t, err)

	eng := New(Config{Mode: ModeCoordinator})

	started := false
	eng.OnStarted(func() error {
		started = true
		eng.Terminate()
		return nil
	})

	err = eng.Start()
	assert.NoError(t, err)

	assert.True(t, started)
}

func TestRunWorker(t *testing.T) {
	err := conf.LoadConfig()
	assert.NoError(t, err)

	eng := New(Config{Mode: ModeWorker})

	started := false
	eng.OnStarted(func() error {
		started = true
		eng.Terminate()
		return nil
	})

	err = eng.Start()
	assert.NoError(t, err)

	assert.True(t, started)
}

func Test_basicAuthWrongPassword(t *testing.T) {
	mw := basicAuth()
	req, err := http.NewRequest("GET", "/health", nil)
	req.SetBasicAuth("tork", "password")
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, w)
	h := func(c echo.Context) error {
		return nil
	}
	x := mw(h)
	err = x(ctx)
	assert.Error(t, err)
}

func Test_basicCorrectPassword(t *testing.T) {
	key := "TORK_COORDINATOR_API_MIDDLEWARE_BASICAUTH_PASSWORD"
	assert.NoError(t, os.Setenv(key, "password"))
	defer func() {
		assert.NoError(t, os.Unsetenv(key))
	}()
	err := conf.LoadConfig()
	assert.NoError(t, err)

	mw := basicAuth()
	req, err := http.NewRequest("GET", "/health", nil)
	req.SetBasicAuth("tork", "password")
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, w)
	h := func(c echo.Context) error {
		return nil
	}
	x := mw(h)
	err = x(ctx)
	assert.NoError(t, err)
}

func Test_rateLimit(t *testing.T) {
	mw := rateLimit()
	req, err := http.NewRequest("GET", "/health", nil)
	req.SetBasicAuth("tork", "password")
	req.Header.Set("X-Real-Ip", "1.1.1.1")
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, w)
	h := func(c echo.Context) error {
		return nil
	}
	for i := 0; i < 30; i++ {
		x := mw(h)
		err = x(ctx)
		assert.NoError(t, err)
		if i < 20 {
			assert.Equal(t, http.StatusOK, w.Code)
		} else {
			assert.Equal(t, http.StatusTooManyRequests, w.Code)
		}
	}
}

func Test_rateLimitCustomRPS(t *testing.T) {
	key := "TORK_COORDINATOR_API_MIDDLEWARE_RATELIMIT_RPS"
	assert.NoError(t, os.Setenv(key, "10"))
	defer func() {
		assert.NoError(t, os.Unsetenv(key))
	}()
	err := conf.LoadConfig()
	assert.NoError(t, err)
	mw := rateLimit()
	req, err := http.NewRequest("GET", "/health", nil)
	req.SetBasicAuth("tork", "password")
	req.Header.Set("X-Real-Ip", "1.1.1.1")
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, w)
	h := func(c echo.Context) error {
		return nil
	}
	for i := 0; i < 30; i++ {
		x := mw(h)
		err = x(ctx)
		assert.NoError(t, err)
		if i < 10 {
			assert.Equal(t, http.StatusOK, w.Code)
		} else {
			assert.Equal(t, http.StatusTooManyRequests, w.Code)
		}
	}
}
