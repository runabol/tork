package datastore

import (
	"context"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
)

type Provider func() (Datastore, error)

var (
	ErrTaskNotFound    = errors.New("task not found")
	ErrNodeNotFound    = errors.New("node not found")
	ErrJobNotFound     = errors.New("job not found")
	ErrContextNotFound = errors.New("context not found")
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
	CreateTaskLogPart(ctx context.Context, p *tork.TaskLogPart) error
	GetTaskLogParts(ctx context.Context, taskID string, page, size int) (*Page[*tork.TaskLogPart], error)

	CreateNode(ctx context.Context, n *tork.Node) error
	UpdateNode(ctx context.Context, id string, modify func(u *tork.Node) error) error
	GetNodeByID(ctx context.Context, id string) (*tork.Node, error)
	GetActiveNodes(ctx context.Context) ([]*tork.Node, error)

	CreateJob(ctx context.Context, j *tork.Job) error
	UpdateJob(ctx context.Context, id string, modify func(u *tork.Job) error) error
	GetJobByID(ctx context.Context, id string) (*tork.Job, error)
	GetJobLogParts(ctx context.Context, jobID string, page, size int) (*Page[*tork.TaskLogPart], error)
	GetJobs(ctx context.Context, q string, page, size int) (*Page[*tork.JobSummary], error)

	GetMetrics(ctx context.Context) (*tork.Metrics, error)

	WithTx(ctx context.Context, f func(tx Datastore) error) error

	HealthCheck(ctx context.Context) error
}

type Page[T any] struct {
	Items      []T `json:"items"`
	Number     int `json:"number"`
	Size       int `json:"size"`
	TotalPages int `json:"totalPages"`
	TotalItems int `json:"totalItems"`
}
