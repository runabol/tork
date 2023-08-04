package coordinator

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

type server struct {
	httpServer *http.Server
}

func newServer(cfg Config) *server {
	if cfg.Address == "" {
		cfg.Address = ":3000"
	}
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	s := &server{
		httpServer: &http.Server{
			Addr:    cfg.Address,
			Handler: r,
		},
	}
	r.GET("/status", s.status)
	return s
}

func (s *server) status(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "OK",
	})
}

func (s *server) start() error {
	go func() {
		// service connections
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msgf("error starting up server")
		}
	}()
	return nil
}

func (s *server) shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
