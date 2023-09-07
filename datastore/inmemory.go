package datastore

import (
	"context"
	"strings"

	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/runabol/tork"

	"github.com/runabol/tork/syncx"
)

var ErrTaskNotFound = errors.New("task not found")
var ErrNodeNotFound = errors.New("node not found")
var ErrJobNotFound = errors.New("job not found")
var ErrContextNotFound = errors.New("context not found")

type InMemoryDatastore struct {
	tasks *syncx.Map[string, *tork.Task]
	nodes *syncx.Map[string, tork.Node]
	jobs  *syncx.Map[string, *tork.Job]
}

func NewInMemoryDatastore() *InMemoryDatastore {
	return &InMemoryDatastore{
		tasks: new(syncx.Map[string, *tork.Task]),
		nodes: new(syncx.Map[string, tork.Node]),
		jobs:  new(syncx.Map[string, *tork.Job]),
	}
}

func (ds *InMemoryDatastore) CreateTask(ctx context.Context, t *tork.Task) error {
	if t.ID == "" {
		return errors.New("must provide ID")
	}
	ds.tasks.Set(t.ID, t.Clone())
	return nil
}

func (ds *InMemoryDatastore) GetTaskByID(ctx context.Context, id string) (*tork.Task, error) {
	t, ok := ds.tasks.Get(id)
	if !ok {
		return nil, ErrTaskNotFound
	}
	return t.Clone(), nil
}

func (ds *InMemoryDatastore) UpdateTask(ctx context.Context, id string, modify func(u *tork.Task) error) error {
	t, ok := ds.tasks.Get(id)
	if !ok {
		return ErrTaskNotFound
	}
	if err := modify(t); err != nil {
		return err
	}
	return nil
}

func (ds *InMemoryDatastore) CreateNode(ctx context.Context, n tork.Node) error {
	_, ok := ds.nodes.Get(n.ID)
	if ok {
		return errors.Errorf("node %s already exists", n.ID)
	}
	ds.nodes.Set(n.ID, n)
	return nil
}

func (ds *InMemoryDatastore) UpdateNode(ctx context.Context, id string, modify func(u *tork.Node) error) error {
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

func (ds *InMemoryDatastore) GetNodeByID(ctx context.Context, id string) (tork.Node, error) {
	n, ok := ds.nodes.Get(id)
	if !ok {
		return tork.Node{}, ErrNodeNotFound
	}
	return n, nil
}

func (ds *InMemoryDatastore) GetActiveNodes(ctx context.Context) ([]tork.Node, error) {
	nodes := make([]tork.Node, 0)
	timeout := time.Now().UTC().Add(-tork.LAST_HEARTBEAT_TIMEOUT)
	ds.nodes.Iterate(func(_ string, n tork.Node) {
		if n.LastHeartbeatAt.After(timeout) {
			// if we hadn't seen an heartbeat for two or more
			// consecutive periods we consider the node as offline
			if n.LastHeartbeatAt.Before(time.Now().UTC().Add(-tork.HEARTBEAT_RATE * 2)) {
				n.Status = tork.NodeStatusOffline
			}
			nodes = append(nodes, n)
		}
	})
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].LastHeartbeatAt.After(nodes[j].LastHeartbeatAt)
	})
	return nodes, nil
}

func (ds *InMemoryDatastore) CreateJob(ctx context.Context, j *tork.Job) error {
	ds.jobs.Set(j.ID, j.Clone())
	return nil
}

func (ds *InMemoryDatastore) UpdateJob(ctx context.Context, id string, modify func(u *tork.Job) error) error {
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

func (ds *InMemoryDatastore) getExecution(id string) []*tork.Task {
	execution := make([]*tork.Task, 0)
	ds.tasks.Iterate(func(_ string, t *tork.Task) {
		if t.JobID == id {
			execution = append(execution, t.Clone())
		}
	})
	return execution
}

func (ds *InMemoryDatastore) GetJobByID(ctx context.Context, id string) (*tork.Job, error) {
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

func (ds *InMemoryDatastore) GetActiveTasks(ctx context.Context, jobID string) ([]*tork.Task, error) {
	result := make([]*tork.Task, 0)
	ds.tasks.Iterate(func(_ string, t *tork.Task) {
		if t.JobID == jobID && t.State.IsActive() {
			result = append(result, t.Clone())
		}
	})
	return result, nil
}

func (ds *InMemoryDatastore) GetJobs(ctx context.Context, q string, page, size int) (*Page[*tork.Job], error) {
	offset := (page - 1) * size
	filtered := make([]*tork.Job, 0)
	ds.jobs.Iterate(func(_ string, j *tork.Job) {
		if strings.Contains(strings.ToLower(j.Name), strings.ToLower(q)) ||
			strings.Contains(strings.ToLower(string(j.State)), strings.ToLower(q)) {
			filtered = append(filtered, j)
		}
	})
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
	})
	result := make([]*tork.Job, 0)
	for i := offset; i < (offset+size) && i < len(filtered); i++ {
		j := filtered[i]
		jc := j.Clone()
		jc.Tasks = make([]*tork.Task, 0)
		jc.Execution = make([]*tork.Task, 0)
		result = append(result, jc)
	}
	totalPages := len(filtered) / size
	if len(filtered)%size != 0 {
		totalPages = totalPages + 1
	}
	return &Page[*tork.Job]{
		Items:      result,
		Number:     page,
		Size:       len(result),
		TotalPages: totalPages,
		TotalItems: len(filtered),
	}, nil
}

func (ds *InMemoryDatastore) GetStats(ctx context.Context) (*tork.Stats, error) {
	s := &tork.Stats{}

	ds.jobs.Iterate(func(_ string, j *tork.Job) {
		if j.State == tork.JobStateRunning {
			s.Jobs.Running = s.Jobs.Running + 1
		}
	})

	ds.tasks.Iterate(func(_ string, t *tork.Task) {
		if t.State == tork.TaskStateRunning {
			s.Tasks.Running = s.Tasks.Running + 1
		}
	})

	ds.nodes.Iterate(func(_ string, n tork.Node) {
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
