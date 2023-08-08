package datastore

import (
	"context"

	"github.com/tork/task"
)

const (
	DATASTORE_INMEMORY = "inmemory"
	DATASTORE_POSTGRES = "postgres"
)

type Datastore interface {
	SaveTask(ctx context.Context, t *task.Task) error
	UpdateTask(ctx context.Context, id string, modify func(u *task.Task)) error
	GetTaskByID(ctx context.Context, id string) (*task.Task, error)
}
