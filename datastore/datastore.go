package datastore

import (
	"context"

	"github.com/tork/task"
)

type TaskDatastore interface {
	Save(ctx context.Context, t *task.Task) error
	GetByID(ctx context.Context, id string) (*task.Task, error)
}
