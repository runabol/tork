package datastore

import (
	"context"
	"time"

	"github.com/tork/job"
	"github.com/tork/node"
	"github.com/tork/task"
)

const (
	DATASTORE_INMEMORY = "inmemory"
	DATASTORE_POSTGRES = "postgres"
)

type Datastore interface {
	CreateTask(ctx context.Context, t *task.Task) error
	UpdateTask(ctx context.Context, id string, modify func(u *task.Task) error) error
	GetTaskByID(ctx context.Context, id string) (*task.Task, error)
	GetActiveTasks(ctx context.Context, jobID string) ([]*task.Task, error)

	CreateNode(ctx context.Context, n node.Node) error
	UpdateNode(ctx context.Context, id string, modify func(u *node.Node) error) error
	GetNodeByID(ctx context.Context, id string) (node.Node, error)
	GetActiveNodes(ctx context.Context, lastHeartbeatAfter time.Time) ([]node.Node, error)

	CreateJob(ctx context.Context, j *job.Job) error
	UpdateJob(ctx context.Context, id string, modify func(u *job.Job) error) error
	GetJobByID(ctx context.Context, id string) (*job.Job, error)
}
