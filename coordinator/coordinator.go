package coordinator

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	api    *api
}

type Config struct {
	Broker  broker.Broker
	Address string
}

func NewCoordinator(cfg Config) *Coordinator {
	name := fmt.Sprintf("coordinator-%s", uuid.NewUUID())
	return &Coordinator{
		Name:   name,
		broker: cfg.Broker,
		api:    newAPI(cfg),
	}
}

func (c *Coordinator) Start() error {
	log.Info().Msgf("starting %s", c.Name)
	if err := c.api.start(); err != nil {
		return err
	}
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Debug().Msgf("shutting down %s", c.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.api.shutdown(ctx); err != nil {
		return err
	}
	return nil
}
