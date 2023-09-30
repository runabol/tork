package runtime

import (
	"context"

	"github.com/runabol/tork"
)

const (
	Docker = "docker"
	Shell  = "shell"
)

// Runtime is the actual runtime environment that executes a task.
type Runtime interface {
	Run(ctx context.Context, t *tork.Task) error
	Stop(ctx context.Context, t *tork.Task) error
	HealthCheck(ctx context.Context) error
}
