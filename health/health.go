package health

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
)

const (
	StatusUp   = "UP"
	StatusDown = "DOWN"

	ServiceDatastore = "datastore"
	ServiceBroker    = "broker"
	ServiceRuntime   = "runtime"
)

type HealthIndicator func(ctx context.Context) error

type HealthCheckResult struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

type HealthCheck struct {
	indicators map[string]HealthIndicator
}

func (b *HealthCheck) WithIndicator(name string, ind HealthIndicator) *HealthCheck {
	name = strings.TrimSpace(name)
	if name == "" {
		panic("health indicator name must not be empty")
	}
	if _, ok := b.indicators[name]; ok {
		panic(fmt.Sprintf("health indicator with name %s already exists", name))
	}
	b.indicators[name] = ind
	return b
}

func NewHealthCheck() *HealthCheck {
	return &HealthCheck{
		indicators: make(map[string]HealthIndicator),
	}
}

func (b *HealthCheck) Do(ctx context.Context) HealthCheckResult {
	for name, ind := range b.indicators {
		if err := ind(ctx); err != nil {
			log.Error().Err(err).Msgf("failed %s healthcheck", name)
			return HealthCheckResult{
				Status:  StatusDown,
				Version: tork.FormattedVersion(),
			}
		}
	}
	return HealthCheckResult{
		Status:  StatusUp,
		Version: tork.FormattedVersion(),
	}
}
