package web

import (
	"net/http"

	"github.com/runabol/tork"
)

type MiddlewareFunc func(next HandlerFunc) HandlerFunc

type HandlerFunc func(c Context) error

type JobListener func(j *tork.Job)

type Context interface {
	// Request returns `*http.Request`.
	Request() *http.Request

	// Get retrieves data from the context.
	Get(key string) any

	// Response returns `http.ResponseWriter`.
	Response() http.ResponseWriter

	// NoContent sends a response with no body and a status code.
	NoContent(code int) error

	// String sends a string response with status code.
	String(code int, s string) error

	// JSON sends a JSON response with status code.
	JSON(code int, data any) error

	// Bind binds path params, query params and the request body into provided type `i`. The default binder
	// binds body based on Content-Type header.
	Bind(i any) error

	// Error sends an error back to the client.
	Error(code int, err error)

	// Done returns a channel that's closed when work done on behalf of this
	// context should be canceled.
	Done() <-chan any
}
