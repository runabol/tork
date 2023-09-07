package datastore

import (
	"context"
	"strings"

	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/runabol/tork/syncx"
	"github.com/runabol/tork/types/job"
	"github.com/runabol/tork/types/node"
	"github.com/runabol/tork/types/stats"
	"github.com/runabol/tork/types/task"
)

var ErrTaskNotFound = errors.New("task not found")
var ErrNodeNotFound = errors.New("node not found")
var ErrJobNotFound = errors.New("job not found")
var ErrContextNotFound = errors.New("context not found")

type InMemoryDatastore struct {
	tasks *syncx.Map[string, *task.Task]
	nodes *syncx.Map[string, node.Node]
	jobs  *syncx.Map[string, *job.Job]
}

func NewInMemoryDatastore() *InMemoryDatastore {
	return &InMemoryDatastore{
		tasks: new(syncx.Map[string, *task.Task]),
		nodes: new(syncx.Map[string, node.Node]),
		jobs:  new(syncx.Map[string, *job.Job]),
	}
}

func (ds *InMemoryDatastore) CreateTask(ctx context.Context, t *task.Task) error {
	if t.ID == "" {
		return errors.New("must provide ID")
	}
	ds.tasks.Set(t.ID, t.Clone())
	return nil
}

func (ds *InMemoryDatastore) GetTaskByID(ctx context.Context, id string) (*task.Task, error) {
	t, ok := ds.tasks.Get(id)
	if !ok {
		return nil, ErrTaskNotFound
	}
	return t.Clone(), nil
}

func (ds *InMemoryDatastore) UpdateTask(ctx context.Context, id string, modify func(u *task.Task) error) error {
	t, ok := ds.tasks.Get(id)
	if !ok {
		return ErrTaskNotFound
	}
	if err := modify(t); err != nil {
		return err
	}
	return nil
}

func (ds *InMemoryDatastore) CreateNode(ctx context.Context, n node.Node) error {
	_, ok := ds.nodes.Get(n.ID)
	if ok {
		return errors.Errorf("node %s already exists", n.ID)
	}
	ds.nodes.Set(n.ID, n)
	return nil
}

func (ds *InMemoryDatastore) UpdateNode(ctx context.Context, id string, modify func(u *node.Node) error) error {
	n, ok := ds.nodes.Get(id)
	if !ok {
		return ErrNodeNotFound
	}
	if err := modify(&n); err != nil {
		return err
	}
	ds.nodes.Set(n.ID, n)
	return nil
}

func (ds *InMemoryDatastore) GetNodeByID(ctx context.Context, id string) (node.Node, error) {
	n, ok := ds.nodes.Get(id)
	if !ok {
		return node.Node{}, ErrNodeNotFound
	}
	return n, nil
}

func (ds *InMemoryDatastore) GetActiveNodes(ctx context.Context) ([]node.Node, error) {
	nodes := make([]node.Node, 0)
	timeout := time.Now().UTC().Add(-node.LAST_HEARTBEAT_TIMEOUT)
	ds.nodes.Iterate(func(_ string, n node.Node) {
		if n.LastHeartbeatAt.After(timeout) {
			// if we hadn't seen an heartbeat for two or more
			// consecutive periods we consider the node as offline
			if n.LastHeartbeatAt.Before(time.Now().UTC().Add(-node.HEARTBEAT_RATE * 2)) {
				n.Status = node.Offline
			}
			nodes = append(nodes, n)
		}
	})
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].LastHeartbeatAt.After(nodes[j].LastHeartbeatAt)
	})
	return nodes, nil
}

func (ds *InMemoryDatastore) CreateJob(ctx context.Context, j *job.Job) error {
	ds.jobs.Set(j.ID, j.Clone())
	return nil
}

func (ds *InMemoryDatastore) UpdateJob(ctx context.Context, id string, modify func(u *job.Job) error) error {
	j, ok := ds.jobs.Get(id)
	if !ok {
		return ErrJobNotFound
	}
	if err := modify(j); err != nil {
		return err
	}
	ds.jobs.Set(j.ID, j)
	return nil
}

func (ds *InMemoryDatastore) getExecution(id string) []*task.Task {
	execution := make([]*task.Task, 0)
	ds.tasks.Iterate(func(_ string, t *task.Task) {
		if t.JobID == id {
			execution = append(execution, t.Clone())
		}
	})
	return execution
}

func (ds *InMemoryDatastore) GetJobByID(ctx context.Context, id string) (*job.Job, error) {
	j, ok := ds.jobs.Get(id)
	if !ok {
		return nil, ErrJobNotFound
	}
	execution := ds.getExecution(id)
	sort.Slice(execution, func(i, j int) bool {
		posi := execution[i].Position
		posj := execution[j].Position
		if posi != posj {
			return posi < posj
		}
		ci := execution[i].CreatedAt
		cj := execution[j].CreatedAt
		if ci == nil {
			return true
		}
		if cj == nil {
			return false
		}
		return ci.Before(*cj)
	})
	j.Execution = execution
	return j.Clone(), nil
}

func (ds *InMemoryDatastore) GetActiveTasks(ctx context.Context, jobID string) ([]*task.Task, error) {
	result := make([]*task.Task, 0)
	ds.tasks.Iterate(func(_ string, t *task.Task) {
		if t.JobID == jobID && t.State.IsActive() {
			result = append(result, t.Clone())
		}
	})
	return result, nil
}

func (ds *InMemoryDatastore) GetJobs(ctx context.Context, q string, page, size int) (*Page[*job.Job], error) {
	offset := (page - 1) * size
	filtered := make([]*job.Job, 0)
	ds.jobs.Iterate(func(_ string, j *job.Job) {
		if strings.Contains(strings.ToLower(j.Name), strings.ToLower(q)) ||
			strings.Contains(strings.ToLower(string(j.State)), strings.ToLower(q)) {
			filtered = append(filtered, j)
		}
	})
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
	})
	result := make([]*job.Job, 0)
	for i := offset; i < (offset+size) && i < len(filtered); i++ {
		j := filtered[i]
		jc := j.Clone()
		jc.Tasks = make([]*task.Task, 0)
		jc.Execution = make([]*task.Task, 0)
		result = append(result, jc)
	}
	totalPages := len(filtered) / size
	if len(filtered)%size != 0 {
		totalPages = totalPages + 1
	}
	return &Page[*job.Job]{
		Items:      result,
		Number:     page,
		Size:       len(result),
		TotalPages: totalPages,
		TotalItems: len(filtered),
	}, nil
}

func (ds *InMemoryDatastore) GetStats(ctx context.Context) (*stats.Stats, error) {
	s := &stats.Stats{}

	ds.jobs.Iterate(func(_ string, j *job.Job) {
		if j.State == job.Running {
			s.Jobs.Running = s.Jobs.Running + 1
		}
	})

	ds.tasks.Iterate(func(_ string, t *task.Task) {
		if t.State == task.Running {
			s.Tasks.Running = s.Tasks.Running + 1
		}
	})

	ds.nodes.Iterate(func(_ string, n node.Node) {
		if n.LastHeartbeatAt.After(time.Now().UTC().Add(-(time.Minute * 5))) {
			s.Nodes.Running = s.Nodes.Running + 1
			s.Nodes.CPUPercent = s.Nodes.CPUPercent + n.CPUPercent
		}
	})
	// calculate average
	if s.Nodes.Running > 0 {
		s.Nodes.CPUPercent = s.Nodes.CPUPercent / float64(s.Nodes.Running)
	}

	return s, nil
}

func (ds *InMemoryDatastore) WithTx(ctx context.Context, f func(tx Datastore) error) error {
	return f(ds)
}
