package worker

import (
	"context"
	"fmt"
	"syscall"

	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/httpx"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/runtime"
	"github.com/runabol/tork/version"
)

const (
	MIN_PORT = 8001
	MAX_PORT = 8100
)

type api struct {
	server  *http.Server
	broker  mq.Broker
	runtime runtime.Runtime
}

func newAPI(cfg Config) *api {
	r := echo.New()

	s := &api{
		runtime: cfg.Runtime,
		broker:  cfg.Broker,
		server: &http.Server{
			Addr:    cfg.Address,
			Handler: r,
		},
	}
	r.GET("/health", s.health)
	return s
}

func (s *api) health(c echo.Context) error {
	var status = "UP"
	if err := s.runtime.HealthCheck(c.Request().Context()); err != nil {
		status = "DOWN"
	}
	return c.JSON(http.StatusOK, map[string]string{
		"status":  status,
		"version": fmt.Sprintf("%s (%s)", version.Version, version.GitCommit),
	})
}

func (s *api) start() error {
	if s.server.Addr != "" {
		if err := httpx.StartAsync(s.server); err != nil {
			return err
		}
	} else {
		// attempting to dynamically assign port
		for port := MIN_PORT; port <= MAX_PORT; port++ {
			s.server.Addr = fmt.Sprintf("localhost:%d", port)
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
