package worker

import (
	"context"

	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/runtime"
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
	if err := s.runtime.HealthCheck(c.Request().Context()); err != nil {
		return c.JSON(http.StatusOK, map[string]string{"status": "DOWN"})
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "UP"})
}

func (s *api) start() error {
	go func() {
		log.Info().Msgf("listening on %s", s.server.Addr)
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
