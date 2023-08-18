package runtime

import (
	"context"

	"github.com/runabol/tork/task"
)

type RuntimeType string

const (
	Docker RuntimeType = "docker"
)

// Runtime is the actual runtime environment that executes a task.
type Runtime interface {
	Run(ctx context.Context, t *task.Task) error
	Stop(ctx context.Context, t *task.Task) error
}
