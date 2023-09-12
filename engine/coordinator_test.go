package engine

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
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
