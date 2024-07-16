package worker

import (
	"context"
	"fmt"
	"strings"
	"syscall"

	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/health"
	"github.com/runabol/tork/internal/httpx"
	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/runtime"
)

const (
	MIN_PORT = 8001
	MAX_PORT = 8100
)

type api struct {
	server  *http.Server
	broker  mq.Broker
	runtime runtime.Runtime
	tasks   *syncx.Map[string, runningTask]
	port    int
}

func newAPI(cfg Config, tasks *syncx.Map[string, runningTask]) *api {
	r := echo.New()
	s := &api{
		runtime: cfg.Runtime,
		broker:  cfg.Broker,
		tasks:   tasks,
		server: &http.Server{
			Addr:    cfg.Address,
			Handler: r,
		},
	}
	r.GET("/health", s.health)
	r.Any("/tasks/:id/:port", s.proxy)
	r.Any("/tasks/:id/:port/*", s.proxy)
	return s
}

func (s *api) health(c echo.Context) error {
	result := health.NewHealthCheck().
		WithIndicator(health.ServiceRuntime, s.runtime.HealthCheck).
		WithIndicator(health.ServiceBroker, s.broker.HealthCheck).
		Do(c.Request().Context())
	if result.Status == health.StatusDown {
		return c.JSON(http.StatusServiceUnavailable, result)
	} else {
		return c.JSON(http.StatusOK, result)
	}
}

func (s *api) proxy(c echo.Context) error {
	taskID := c.Param("id")
	port := c.Param("port")
	rt, ok := s.tasks.Get(taskID)
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "task not found")
	}
	if rt.task.Ports == nil {
		return echo.NewHTTPError(http.StatusNotFound, "task does not expose any ports")
	}
	binding, ok := rt.task.Port(port)
	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "port not found")
	}
	backendURL, err := url.Parse(fmt.Sprintf("http://localhost:%d", binding.HostPort))
	if err != nil {
		return err
	}
	proxy := httputil.NewSingleHostReverseProxy(backendURL)
	req := c.Request()
	path := strings.Join(strings.Split(req.URL.Path, "/")[4:], "/")
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	req.URL.Path = path
	proxy.ServeHTTP(c.Response(), req)
	return nil
}

func (s *api) start() error {
	if s.server.Addr != "" {
		if err := httpx.StartAsync(s.server); err != nil {
			return err
		}
	} else {
		// attempting to dynamically assign port
		for port := MIN_PORT; port < MAX_PORT; port++ {
			s.server.Addr = fmt.Sprintf(":%d", port)
			s.port = port
			if err := httpx.StartAsync(s.server); err != nil {
				if errors.Is(err, syscall.EADDRINUSE) {
					continue
				}
				log.Fatal().Err(err).Msgf("error starting up server")
			}
			break
		}
	}
	log.Info().Msgf("Worker listening on http://%s", s.server.Addr)
	return nil
}

func (s *api) shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
