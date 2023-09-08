package api

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/input"
	"github.com/runabol/tork/middleware"
	"github.com/runabol/tork/mq"
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

func (c *Context) SubmitJob(ij *input.Job, listeners ...middleware.JobListener) (*tork.Job, error) {
	if err := c.api.broker.SubscribeForEvents(c.ctx.Request().Context(), mq.TOPIC_JOB, func(ev any) {
		j, ok := ev.(*tork.Job)
		if !ok {
			log.Error().Msg("unable to cast event to *tork.Job")
		}
		if ij.ID() == j.ID {
			for _, listener := range listeners {
				listener(j)
			}
		}
	}); err != nil {
		return nil, errors.New("error subscribing for job events")
	}
	job, err := c.api.submitJob(c.ctx.Request().Context(), ij)
	if err != nil {
		return nil, err
	}
	return job.Clone(), nil
}

func (c *Context) Error(code int, err error) {
	c.err = err
	c.code = code
}
