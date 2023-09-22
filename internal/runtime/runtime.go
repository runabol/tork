package runtime

import (
	"context"

	"github.com/runabol/tork"
)

type RuntimeType string

const (
	Docker RuntimeType = "docker"
)

// Runtime is the actual runtime environment that executes a task.
type Runtime interface {
	Run(ctx context.Context, t *tork.Task) error
	Stop(ctx context.Context, t *tork.Task) error
	HealthCheck(ctx context.Context) error
}
