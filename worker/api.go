package worker

import (
	"context"
	"fmt"

	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/runtime"
	"github.com/runabol/tork/version"
)

type api struct {
	server  *http.Server
	broker  mq.Broker
	runtime runtime.Runtime
}

func newAPI(cfg Config) *api {
	if cfg.Address == "" {
		cfg.Address = ":8001"
	}
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
	go func() {
		log.Info().Msgf("worker listening on %s", s.server.Addr)
		// service connections
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msgf("error starting up server")
		}
	}()
	return nil
}

func (s *api) shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
