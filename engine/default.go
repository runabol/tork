package engine

import (
	"context"

	"github.com/runabol/tork"
	"github.com/runabol/tork/input"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/node"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/middleware/web"
)

var defaultEngine *Engine = New(Config{})

func RegisterWebMiddleware(mw web.MiddlewareFunc) {
	defaultEngine.RegisterWebMiddleware(mw)
}

func RegisterTaskMiddleware(mw task.MiddlewareFunc) {
	defaultEngine.RegisterTaskMiddleware(mw)
}

func RegisterJobMiddleware(mw job.MiddlewareFunc) {
	defaultEngine.RegisterJobMiddleware(mw)
}

func RegisterNodeMiddleware(mw node.MiddlewareFunc) {
	defaultEngine.RegisterNodeMiddleware(mw)
}

func RegisterEndpoint(method, path string, handler web.HandlerFunc) {
	defaultEngine.RegisterEndpoint(method, path, handler)
}

func SubmitJob(ctx context.Context, ij *input.Job, listeners ...web.JobListener) (*tork.Job, error) {
	return defaultEngine.SubmitJob(ctx, ij, listeners...)
}

func Start() error {
	return defaultEngine.Start()
}

func Terminate() error {
	return defaultEngine.Terminate()
}

func SetMode(mode Mode) {
	defaultEngine.SetMode(mode)
}

func Run() error {
	return defaultEngine.Run()
}
