package web

import (
	"net/http"

	"github.com/runabol/tork"
	"github.com/runabol/tork/pkg/input"
)

type MiddlewareFunc func(next HandlerFunc) HandlerFunc

type HandlerFunc func(c Context) error

type JobListener func(j *tork.Job)

type Context interface {
	// Request returns `*http.Request`.
	Request() *http.Request

	// String sends a string response with status code.
	String(code int, s string) error

	// JSON sends a JSON response with status code.
	JSON(code int, data any) error

	// Bind binds path params, query params and the request body into provided type `i`. The default binder
	// binds body based on Content-Type header.
	Bind(i any) error

	// Error sends an error back to the client.
	Error(code int, err error)

	// SubmitJob submits a job input for processing
	SubmitJob(j *input.Job, listeners ...JobListener) (*tork.Job, error)

	// Done returns a channel that's closed when work done on behalf of this
	// context should be canceled.
	Done() <-chan any
}
