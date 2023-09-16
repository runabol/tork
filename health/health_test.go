package health_test

import (
	"context"
	"errors"
	"testing"

	"github.com/runabol/tork/health"
	"github.com/stretchr/testify/assert"
)

func TestHealthCheckOK(t *testing.T) {
	ctx := context.Background()
	ind := func(ctx context.Context) error {
		return nil
	}
	res := health.NewHealthCheck().WithIndicator("test", ind).Do(ctx)
	assert.Equal(t, health.StatusUp, res.Status)
}

func TestHealthCheckFailed(t *testing.T) {
	ctx := context.Background()
	ind := func(ctx context.Context) error {
		return errors.New("something happened")
	}
	res := health.NewHealthCheck().WithIndicator("test", ind).Do(ctx)
	assert.Equal(t, health.StatusDown, res.Status)
}
