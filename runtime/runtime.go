package runtime

import (
	"context"

	"github.com/tork/task"
)

type Runtime interface {
	Start(ctx context.Context, t task.Task) error
	Stop(ctx context.Context, t task.Task) error
}
