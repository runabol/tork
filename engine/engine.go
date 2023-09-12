package engine

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/input"
	"github.com/runabol/tork/internal/coordinator"
	"github.com/runabol/tork/internal/worker"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/node"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/middleware/web"
	"github.com/runabol/tork/mq"
)

const (
	ModeCoordinator Mode = "coordinator"
	ModeWorker      Mode = "worker"
	ModeStandalone  Mode = "standalone"
)

type Mode string

const (
	StateIdle        = "IDLE"
	StateRunning     = "RUNNING"
	StateTerminating = "TERMINATING"
	StateTerminated  = "TERMINATED"
)

type Engine struct {
	quit        chan os.Signal
	terminate   chan any
	terminated  chan any
	cfg         Config
	state       string
	mu          sync.Mutex
	broker      mq.Broker
	ds          datastore.Datastore
	coordinator *coordinator.Coordinator
	worker      *worker.Worker
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
	if err := e.initBroker(); err != nil {
		return err
	}

	if err := e.initDatastore(); err != nil {
		return err
	}

	if err := e.initCoordinator(); err != nil {
		return err
	}

	go func() {
		e.awaitTerm()

		log.Debug().Msg("shutting down")
		if e.coordinator != nil {
			if err := e.coordinator.Stop(); err != nil {
				log.Error().Err(err).Msg("error stopping coordinator")
			}
		}
		close(e.terminated)
	}()

	return nil
}

func (e *Engine) runWorker() error {
	if err := e.initBroker(); err != nil {
		return err
	}

	if err := e.initWorker(); err != nil {
		return err
	}

	go func() {
		e.awaitTerm()

		log.Debug().Msg("shutting down")
		if e.worker != nil {
			if err := e.worker.Stop(); err != nil {
				log.Error().Err(err).Msg("error stopping worker")
			}
		}
		close(e.terminated)
	}()

	return nil
}

func (e *Engine) runStandalone() error {
	if err := e.initBroker(); err != nil {
		return err
	}

	if err := e.initDatastore(); err != nil {
		return err
	}

	if err := e.initWorker(); err != nil {
		return err
	}

	if err := e.initCoordinator(); err != nil {
		return err
	}

	go func() {
		e.awaitTerm()

		log.Debug().Msg("shutting down")
		if e.worker != nil {
			if err := e.worker.Stop(); err != nil {
				log.Error().Err(err).Msg("error stopping worker")
			}
		}
		if e.coordinator != nil {
			if err := e.coordinator.Stop(); err != nil {
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

func (e *Engine) SubmitJob(ctx context.Context, ij *input.Job, listeners ...web.JobListener) (*tork.Job, error) {
	e.mustState(StateRunning)
	if e.cfg.Mode != ModeStandalone && e.cfg.Mode != ModeCoordinator {
		panic(errors.Errorf("engine not in coordinator/standalone mode"))
	}
	if err := e.broker.SubscribeForEvents(ctx, mq.TOPIC_JOB, func(ev any) {
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
	job, err := e.coordinator.SubmitJob(ctx, ij)
	if err != nil {
		return nil, err
	}
	return job.Clone(), nil
}

func (e *Engine) awaitTerm() {
	signal.Notify(e.quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-e.quit:
	case <-e.terminate:
	}
}
