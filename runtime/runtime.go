package runtime

import (
	"context"

	"github.com/runabol/tork"
)

const (
	Docker  = "docker"
	Podman  = "podman"
	Shell   = "shell"
	Service = "service"
)

// Runtime is the actual runtime environment that executes a task.
type Runtime interface {
	// Run executes a task.
	Run(ctx context.Context, t *tork.Task) error
	// Stop stops a running task.
	Stop(ctx context.Context, t *tork.Task) error
	// HealthCheck checks the health of the runtime.
	HealthCheck(ctx context.Context) error
	// Shutdown shuts down the runtime.
	Shutdown(ctx context.Context) error
}
