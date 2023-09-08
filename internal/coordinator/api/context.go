package api

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/runabol/tork"
	"github.com/runabol/tork/input"
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

func (c *Context) Bind(i any) error {
	return c.ctx.Bind(i)
}

func (c *Context) String(code int, s string) error {
	return c.ctx.String(code, s)
}

func (c *Context) JSON(code int, data any) error {
	return c.ctx.JSON(code, data)
}

func (c *Context) SubmitJob(j *input.Job) (*tork.Job, error) {
	job, err := c.api.submitJob(c.ctx.Request().Context(), j)
	if err != nil {
		return nil, err
	}
	return job.Clone(), nil
}

func (c *Context) Error(code int, err error) {
	c.err = err
	c.code = code
}
