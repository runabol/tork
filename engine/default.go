package engine

import (
	"context"

	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/input"
	"github.com/runabol/tork/middleware/job"
	logmw "github.com/runabol/tork/middleware/log"
	"github.com/runabol/tork/middleware/node"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/middleware/web"
	"github.com/runabol/tork/runtime"
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

func RegisterLogMiddleware(mw logmw.MiddlewareFunc) {
	defaultEngine.RegisterLogMiddleware(mw)
}

func RegisterMounter(runtime, name string, mounter runtime.Mounter) {
	defaultEngine.RegisterMounter(runtime, name, mounter)
}

func RegisterRuntime(rt runtime.Runtime) {
	defaultEngine.RegisterRuntime(rt)
}

func RegisterDatastoreProvider(name string, provider datastore.Provider) {
	defaultEngine.RegisterDatastoreProvider(name, provider)
}

func RegisterBrokerProvider(name string, provider broker.Provider) {
	defaultEngine.RegisterBrokerProvider(name, provider)
}

func RegisterEndpoint(method, path string, handler web.HandlerFunc) {
	defaultEngine.RegisterEndpoint(method, path, handler)
}

func SubmitJob(ctx context.Context, ij *input.Job, listeners ...JobListener) (*tork.Job, error) {
	return defaultEngine.SubmitJob(ctx, ij, listeners...)
}

func Broker() broker.Broker {
	return defaultEngine.Broker()
}

func Datastore() datastore.Datastore {
	return defaultEngine.Datastore()
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
