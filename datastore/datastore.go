package datastore

import (
	"context"
	"time"

	"github.com/tork/node"
	"github.com/tork/task"
)

const (
	DATASTORE_INMEMORY = "inmemory"
	DATASTORE_POSTGRES = "postgres"
)

type Datastore interface {
	CreateTask(ctx context.Context, t task.Task) error
	UpdateTask(ctx context.Context, id string, modify func(u *task.Task) error) error
	GetTaskByID(ctx context.Context, id string) (task.Task, error)
	CreateNode(ctx context.Context, n node.Node) error
	UpdateNode(ctx context.Context, id string, modify func(u *node.Node) error) error
	GetNodeByID(ctx context.Context, id string) (node.Node, error)
	GetActiveNodes(ctx context.Context, lastHeartbeatAfter time.Time) ([]node.Node, error)
}
