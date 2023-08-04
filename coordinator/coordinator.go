package coordinator

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	"github.com/tork/broker"
	"github.com/tork/uuid"
)

// Coordinator is the responsible for accepting tasks from
// clients, scheduling tasks for workers to execute and for
// exposing the cluster's state to the outside world.
type Coordinator struct {
	Name   string
	broker broker.Broker
	server *http.Server
}

type Config struct {
	Broker  broker.Broker
	Address string
}

func NewCoordinator(cfg Config) *Coordinator {
	name := fmt.Sprintf("coordinator-%s", uuid.NewUUID())
	if cfg.Address == "" {
		cfg.Address = ":3000"
	}
	r := gin.Default()
	r.GET("/status", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "OK",
		})
	})
	return &Coordinator{
		Name:   name,
		broker: cfg.Broker,
		server: &http.Server{
			Addr:    cfg.Address,
			Handler: r,
		},
	}
}

func (c *Coordinator) Start() error {
	log.Info().Msgf("starting %s", c.Name)
	go func() {
		// service connections
		if err := c.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msgf("error starting up server")
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Debug().Msgf("shutting down %s", c.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.server.Shutdown(ctx); err != nil {
		log.Fatal().Err(err).Msg("error shutting down server")
	}
	return nil
}
