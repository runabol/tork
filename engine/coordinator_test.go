package engine

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestCORS(t *testing.T) {
	mw := cors()
	req, err := http.NewRequest("GET", "/health", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, w)
	h := func(c echo.Context) error {
		return nil
	}
	x := mw(h)
	err = x(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "Origin", w.Header().Get("Vary"))
}

func TestLogger(t *testing.T) {
	oldLogger := log.Logger
	var buf bytes.Buffer
	log.Logger = log.Logger.Output(&buf)
	defer func() {
		log.Logger = oldLogger
	}()
	mw := logger()
	req, err := http.NewRequest("GET", "/jobs", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, w)
	h := func(c echo.Context) error {
		return nil
	}
	x := mw(h)
	err = x(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, buf.String())

	buf = bytes.Buffer{}
	log.Logger = log.Logger.Output(&buf)

	req, err = http.NewRequest("GET", "/health", nil)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	ctx = echo.New().NewContext(req, w)

	x = mw(h)
	err = x(ctx)
	assert.NoError(t, err)
	assert.Empty(t, buf.String())
}
