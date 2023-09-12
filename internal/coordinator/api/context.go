package api

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

type Context struct {
	ctx  echo.Context
	api  *API
	err  error
	code int
}

func (c *Context) Request() *http.Request {
	return c.ctx.Request()
}

func (c *Context) Response() http.ResponseWriter {
	return c.ctx.Response().Unwrap()
}

func (c *Context) Get(key string) any {
	return c.ctx.Get(key)
}

func (c *Context) NoContent(code int) error {
	return c.ctx.NoContent(code)
}

func (c *Context) Bind(i any) error {
	return c.ctx.Bind(i)
}

func (c *Context) String(code int, s string) error {
	return c.ctx.String(code, s)
}

func (c *Context) JSON(code int, data any) error {
	return c.ctx.JSON(code, data)
}

func (c *Context) Error(code int, err error) {
	c.err = err
	c.code = code
}

func (c *Context) Done() <-chan any {
	ch := make(chan any)
	go func() {
		select {
		case <-c.api.terminate:
		case <-c.Request().Context().Done():
		}
		ch <- 1
	}()
	return ch
}
