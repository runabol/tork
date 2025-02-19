package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/runtime"
)

type ServiceRuntime struct {
	backend      runtime.Runtime
	serviceTasks map[string]*task
	usedPorts    map[string]struct{}
	mu           sync.Mutex
}

type task struct {
	name          string
	port          string
	method        string
	body          string
	path          string
	lastRequestAt time.Time
	requests      int
}

func NewServiceRuntime(backend runtime.Runtime) *ServiceRuntime {
	return &ServiceRuntime{
		backend:      backend,
		serviceTasks: make(map[string]*task),
		usedPorts:    map[string]struct{}{},
	}
}

func (sr *ServiceRuntime) Run(ctx context.Context, t *tork.Task) error {
	if t.Service == nil {
		return errors.New("task service field is required")
	}
	if t.Service.Name == "" {
		return errors.New("task service name field is required")
	}
	// set default values
	if t.Service.Port == "" {
		t.Service.Port = "80"
	}
	if t.Service.Method == "" {
		t.Service.Method = "GET"
	}
	if t.Service.Path == "" {
		t.Service.Path = "/"
	}
	if t.Service.Headers == nil {
		t.Service.Headers = make(map[string]string)
	}
	if t.Service.ReadinessProbe == nil {
		t.Service.ReadinessProbe = &tork.Probe{Path: "/"}
	}

	// prepare the service task in the backend
	// if it's not already running
	st, err := sr.acquireServiceTask(ctx, t)
	if err != nil {
		return err
	}
	defer sr.releaseServiceTask(st)

	// call the service task
	result, err := sr.call(ctx, st)
	if err != nil {
		return err
	}
	t.Result = result
	return nil
}

func (sr *ServiceRuntime) Stop(ctx context.Context, t *tork.Task) error {
	return sr.backend.Stop(ctx, t)
}

func (sr *ServiceRuntime) HealthCheck(ctx context.Context) error {
	return sr.backend.HealthCheck(ctx)
}

func (sr *ServiceRuntime) Shutdown(ctx context.Context) error {
	return sr.backend.Shutdown(ctx)
}

func (sr *ServiceRuntime) call(ctx context.Context, st *task) (string, error) {
	req, err := http.NewRequestWithContext(ctx, st.method, fmt.Sprintf("http://localhost:%s%s", st.port, st.path), strings.NewReader(st.body))
	if err != nil {
		return "", err
	}
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("unexpected status code: %d, body: %s", res.StatusCode, string(body))
	}
	return string(body), nil
}

func (sr *ServiceRuntime) acquireServiceTask(ctx context.Context, t *tork.Task) (*task, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	for _, st := range sr.serviceTasks {
		if st.name == t.Service.Name {
			st.requests = st.requests + 1
			st.lastRequestAt = time.Now()
			return st, nil
		}
	}

	log.Debug().Msgf("starting service task %s", t.Service.Name)

	// reserve a port for the service task
	backendTaskPort, err := sr.reservePort()
	if err != nil {
		return nil, err
	}

	t.Service.HostPort = backendTaskPort

	// execute the service task in the backend
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})
	go func() {
		err := sr.backend.Run(context.Background(), t)
		if err != nil {
			errCh <- err
		} else {
			close(doneCh)
		}
		log.Debug().Msgf("service task %s stopped", t.Service.Name)
		sr.mu.Lock()
		delete(sr.serviceTasks, t.ID)
		sr.releasePort(backendTaskPort)
		sr.mu.Unlock()
	}()

	// wait for the task to be ready
	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	for {
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://localhost:%s%s", backendTaskPort, t.Service.ReadinessProbe.Path), nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if err != nil {
			log.Debug().Err(err).Msg("waiting for task to start")
		}
		if resp != nil {
			break
		}
		select {
		case err := <-errCh:
			return nil, err
		case <-doneCh:
			return nil, errors.New("task stopped before it was ready")
		case <-time.After(3 * time.Second):
		}
	}

	st := &task{
		name:          t.Service.Name,
		port:          backendTaskPort,
		lastRequestAt: time.Now(),
		requests:      1,
	}

	// set the task termination timeout
	go func() {
		// monitor the task requests count
		// if it's zero for more than 30 minutes
		// stop the task
		for {
			<-time.After(time.Minute)
			sr.mu.Lock()
			// check if last request was more than 30 minutes ago
			if st.requests == 0 && st.lastRequestAt.Add(30*time.Minute).Before(time.Now()) {
				break
			}
			sr.mu.Unlock()
		}
		defer sr.mu.Unlock()

		log.Debug().Msgf("stopping service task %s", t.Service.Name)

		delete(sr.serviceTasks, t.ID)
		sr.releasePort(backendTaskPort)
		if err := sr.backend.Stop(context.Background(), t); err != nil {
			log.Error().Err(err).Msg("failed to stop service task")
		}
	}()

	sr.serviceTasks[t.ID] = st

	return st, nil
}

func (sr *ServiceRuntime) releaseServiceTask(st *task) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	st.requests = st.requests - 1
}

func (sr *ServiceRuntime) reservePort() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer listener.Close()

	maxAttempts := 10

	var port int
	var attempt int
	for ; attempt < maxAttempts; attempt++ {
		port = listener.Addr().(*net.TCPAddr).Port
		if _, exists := sr.usedPorts[fmt.Sprintf("%d", port)]; !exists {
			break
		}
	}

	if attempt >= maxAttempts {
		return "", errors.New("could not find an available port to reserve")
	}

	sr.usedPorts[fmt.Sprintf("%d", port)] = struct{}{}

	return fmt.Sprintf("%d", port), nil
}

func (sr *ServiceRuntime) releasePort(port string) {
	delete(sr.usedPorts, port)
}
