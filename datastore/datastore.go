package datastore

import (
	"context"

	"github.com/runabol/tork"
)

const (
	DATASTORE_INMEMORY = "inmemory"
	DATASTORE_POSTGRES = "postgres"
)

type Datastore interface {
	CreateTask(ctx context.Context, t *tork.Task) error
	UpdateTask(ctx context.Context, id string, modify func(u *tork.Task) error) error
	GetTaskByID(ctx context.Context, id string) (*tork.Task, error)
	GetActiveTasks(ctx context.Context, jobID string) ([]*tork.Task, error)

	CreateNode(ctx context.Context, n *tork.Node) error
	UpdateNode(ctx context.Context, id string, modify func(u *tork.Node) error) error
	GetNodeByID(ctx context.Context, id string) (*tork.Node, error)
	GetActiveNodes(ctx context.Context) ([]*tork.Node, error)

	CreateJob(ctx context.Context, j *tork.Job) error
	UpdateJob(ctx context.Context, id string, modify func(u *tork.Job) error) error
	GetJobByID(ctx context.Context, id string) (*tork.Job, error)
	GetJobs(ctx context.Context, q string, page, size int) (*Page[*tork.Job], error)

	GetStats(ctx context.Context) (*tork.Stats, error)

	WithTx(ctx context.Context, f func(tx Datastore) error) error
}

type Page[T any] struct {
	Items      []T `json:"items"`
	Number     int `json:"number"`
	Size       int `json:"size"`
	TotalPages int `json:"totalPages"`
	TotalItems int `json:"totalItems"`
}
