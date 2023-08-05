package scheduler

import (
	"context"

	"github.com/tork/task"
)

type Scheduler interface {
	Schedule(ctx context.Context, t *task.Task) error
}
