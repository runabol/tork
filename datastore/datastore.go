package datastore

import (
	"context"

	"github.com/runabol/tork/types/job"
	"github.com/runabol/tork/types/node"
	"github.com/runabol/tork/types/stats"
	"github.com/runabol/tork/types/task"
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
	GetActiveNodes(ctx context.Context) ([]node.Node, error)

	CreateJob(ctx context.Context, j *job.Job) error
	UpdateJob(ctx context.Context, id string, modify func(u *job.Job) error) error
	GetJobByID(ctx context.Context, id string) (*job.Job, error)
	GetJobs(ctx context.Context, q string, page, size int) (*Page[*job.Job], error)

	GetStats(ctx context.Context) (*stats.Stats, error)

	WithTx(ctx context.Context, f func(tx Datastore) error) error
}

type Page[T any] struct {
	Items      []T `json:"items"`
	Number     int `json:"number"`
	Size       int `json:"size"`
	TotalPages int `json:"totalPages"`
	TotalItems int `json:"totalItems"`
}
