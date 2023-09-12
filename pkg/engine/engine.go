package engine

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/runabol/tork/pkg/middleware/job"
	"github.com/runabol/tork/pkg/middleware/node"
	"github.com/runabol/tork/pkg/middleware/task"
	"github.com/runabol/tork/pkg/middleware/web"
)

const (
	ModeCoordinator Mode = "coordinator"
	ModeWorker      Mode = "worker"
	ModeStandalone  Mode = "standalone"
)

// OnStartedHandler a bootstrap hook that is
// called after Tork has finished starting up.
// If a non-nil error is returned it will
// terminate the bootstrap process.
type OnStartedHandler func() error

type Mode string

const (
	StateIdle        = "IDLE"
	StateRunning     = "RUNNING"
	StateTerminating = "TERMINATING"
	StateTerminated  = "TERMINATED"
)

type Engine struct {
	quit       chan os.Signal
	terminate  chan any
	terminated chan any
	cfg        Config
	state      string
	mu         sync.Mutex
}

type Config struct {
	Mode       Mode
	Middleware Middleware
	Endpoints  map[string]web.HandlerFunc
}

type Middleware struct {
	Web  []web.MiddlewareFunc
	Task []task.MiddlewareFunc
	Job  []job.MiddlewareFunc
	Node []node.MiddlewareFunc
}

func New(cfg Config) *Engine {
	if cfg.Endpoints == nil {
		cfg.Endpoints = make(map[string]web.HandlerFunc)
	}
	return &Engine{
		quit:       make(chan os.Signal, 1),
		terminate:  make(chan any),
		terminated: make(chan any),
		cfg:        cfg,
		state:      StateIdle,
	}
}

func (e *Engine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mustState(StateIdle)
	var err error
	switch e.cfg.Mode {
	case ModeCoordinator:
		err = e.runCoordinator()
	case ModeWorker:
		err = e.runWorker()
	case ModeStandalone:
		err = e.runStandalone()
	default:
		err = errors.Errorf("Unknown mode: %s", e.cfg.Mode)
	}
	if err == nil {
		e.state = StateRunning
	}
	return err
}

func (e *Engine) Run() error {
	if err := e.Start(); err != nil {
		return err
	}
	<-e.terminated
	return nil
}

func (e *Engine) Terminate() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mustState(StateRunning)
	e.state = StateTerminating
	log.Debug().Msg("Terminating engine")
	e.terminate <- 1
	<-e.terminated
	e.state = StateTerminated
	return nil
}

func (e *Engine) SetMode(mode Mode) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mustState(StateIdle)
	e.cfg.Mode = mode
}

func (e *Engine) runCoordinator() error {
	broker, err := createBroker()
	if err != nil {
		return err
	}

	ds, err := createDatastore()
	if err != nil {
		return err
	}

	c, err := e.createCoordinator(broker, ds)
	if err != nil {
		return err
	}

	go func() {
		e.awaitTerm()

		log.Debug().Msg("shutting down")
		if c != nil {
			if err := c.Stop(); err != nil {
				log.Error().Err(err).Msg("error stopping coordinator")
			}
		}
		close(e.terminated)
	}()

	return nil
}

func (e *Engine) runWorker() error {
	broker, err := createBroker()
	if err != nil {
		return err
	}

	w, err := createWorker(broker)
	if err != nil {
		return err
	}

	go func() {
		e.awaitTerm()

		log.Debug().Msg("shutting down")
		if w != nil {
			if err := w.Stop(); err != nil {
				log.Error().Err(err).Msg("error stopping worker")
			}
		}
		close(e.terminated)
	}()

	return nil
}

func (e *Engine) runStandalone() error {
	broker, err := createBroker()
	if err != nil {
		return err
	}

	ds, err := createDatastore()
	if err != nil {
		return err
	}

	w, err := createWorker(broker)
	if err != nil {
		return err
	}
	c, err := e.createCoordinator(broker, ds)
	if err != nil {
		return err
	}

	go func() {
		e.awaitTerm()

		log.Debug().Msg("shutting down")
		if w != nil {
			if err := w.Stop(); err != nil {
				log.Error().Err(err).Msg("error stopping worker")
			}
		}
		if c != nil {
			if err := c.Stop(); err != nil {
				log.Error().Err(err).Msg("error stopping coordinator")
			}
		}
		close(e.terminated)
	}()

	return nil
}

func (e *Engine) mustState(state string) {
	if e.state != state {
		panic(errors.Errorf("engine is not %s", state))
	}
}

func (e *Engine) RegisterWebMiddleware(mw web.MiddlewareFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mustState(StateIdle)
	e.cfg.Middleware.Web = append(defaultEngine.cfg.Middleware.Web, mw)
}

func (e *Engine) RegisterTaskMiddleware(mw task.MiddlewareFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mustState(StateIdle)
	e.cfg.Middleware.Task = append(defaultEngine.cfg.Middleware.Task, mw)
}

func (e *Engine) RegisterJobMiddleware(mw job.MiddlewareFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mustState(StateIdle)
	e.cfg.Middleware.Job = append(defaultEngine.cfg.Middleware.Job, mw)
}

func (e *Engine) RegisterNodeMiddleware(mw node.MiddlewareFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mustState(StateIdle)
	e.cfg.Middleware.Node = append(defaultEngine.cfg.Middleware.Node, mw)
}

func (e *Engine) RegisterEndpoint(method, path string, handler web.HandlerFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mustState(StateIdle)
	e.cfg.Endpoints[fmt.Sprintf("%s %s", method, path)] = handler
}

func (e *Engine) awaitTerm() {
	signal.Notify(e.quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-e.quit:
	case <-e.terminate:
	}
}
