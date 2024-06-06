package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/stretchr/testify/assert"
)

func TestSetGetContext(t *testing.T) {
	req, err := http.NewRequest("GET", "/health", nil)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	ectx := echo.New().NewContext(req, w)
	ctx := Context{
		ctx: ectx,
	}
	assert.Nil(t, ctx.Get("some-key"))
	ctx.Set("some-key", "some value")
	assert.Equal(t, "some value", ctx.Get("some-key"))
}
