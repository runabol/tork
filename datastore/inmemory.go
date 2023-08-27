package datastore

import (
	"context"

	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/runabol/tork/job"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/stats"
	"github.com/runabol/tork/task"
)

var ErrTaskNotFound = errors.New("task not found")
var ErrNodeNotFound = errors.New("node not found")
var ErrJobNotFound = errors.New("job not found")
var ErrContextNotFound = errors.New("context not found")

type InMemoryDatastore struct {
	tasks  map[string]*task.Task
	nodes  map[string]node.Node
	jobs   map[string]*job.Job
	jobIDs []string
	tmu    sync.RWMutex
	nmu    sync.RWMutex
	jmu    sync.RWMutex
}

func NewInMemoryDatastore() *InMemoryDatastore {
	return &InMemoryDatastore{
		tasks: make(map[string]*task.Task),
		nodes: make(map[string]node.Node),
		jobs:  make(map[string]*job.Job),
	}
}

func (ds *InMemoryDatastore) CreateTask(ctx context.Context, t *task.Task) error {
	if t.ID == "" {
		return errors.New("must provide ID")
	}
	ds.tmu.Lock()
	defer ds.tmu.Unlock()
	ds.tasks[t.ID] = t.Clone()
	return nil
}

func (ds *InMemoryDatastore) GetTaskByID(ctx context.Context, id string) (*task.Task, error) {
	ds.tmu.RLock()
	defer ds.tmu.RUnlock()
	t, ok := ds.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return t.Clone(), nil
}

func (ds *InMemoryDatastore) UpdateTask(ctx context.Context, id string, modify func(u *task.Task) error) error {
	ds.tmu.Lock()
	defer ds.tmu.Unlock()
	t, ok := ds.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}
	if err := modify(t); err != nil {
		return err
	}
	return nil
}

func (ds *InMemoryDatastore) CreateNode(ctx context.Context, n node.Node) error {
	ds.nmu.Lock()
	defer ds.nmu.Unlock()
	_, ok := ds.nodes[n.ID]
	if ok {
		return errors.Errorf("node %s already exists", n.ID)
	}
	ds.nodes[n.ID] = n
	return nil
}

func (ds *InMemoryDatastore) UpdateNode(ctx context.Context, id string, modify func(u *node.Node) error) error {
	ds.nmu.Lock()
	defer ds.nmu.Unlock()
	n, ok := ds.nodes[id]
	if !ok {
		return ErrNodeNotFound
	}
	if err := modify(&n); err != nil {
		return err
	}
	ds.nodes[n.ID] = n
	return nil
}

func (ds *InMemoryDatastore) GetNodeByID(ctx context.Context, id string) (node.Node, error) {
	ds.nmu.RLock()
	defer ds.nmu.RUnlock()
	n, ok := ds.nodes[id]
	if !ok {
		return node.Node{}, ErrNodeNotFound
	}
	return n, nil
}

func (ds *InMemoryDatastore) GetActiveNodes(ctx context.Context) ([]node.Node, error) {
	ds.nmu.RLock()
	defer ds.nmu.RUnlock()
	nodes := make([]node.Node, 0)
	timeout := time.Now().UTC().Add(-node.LAST_HEARTBEAT_TIMEOUT)
	for _, n := range ds.nodes {
		if n.LastHeartbeatAt.After(timeout) {
			// if we hadn't seen an heartbeat for two or more
			// consecutive periods we consider the node as offline
			if n.LastHeartbeatAt.Before(time.Now().UTC().Add(-node.HEARTBEAT_RATE * 2)) {
				n.Status = node.Offline
			}
			nodes = append(nodes, n)
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].LastHeartbeatAt.After(nodes[j].LastHeartbeatAt)
	})
	return nodes, nil
}

func (ds *InMemoryDatastore) CreateJob(ctx context.Context, j *job.Job) error {
	ds.jmu.Lock()
	defer ds.jmu.Unlock()
	ds.jobs[j.ID] = j.Clone()
	ds.jobIDs = append([]string{j.ID}, ds.jobIDs...) // prepend
	return nil
}

func (ds *InMemoryDatastore) UpdateJob(ctx context.Context, id string, modify func(u *job.Job) error) error {
	ds.jmu.Lock()
	defer ds.jmu.Unlock()
	j, ok := ds.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if err := modify(j); err != nil {
		return err
	}
	ds.jobs[j.ID] = j
	return nil
}

func (ds *InMemoryDatastore) getExecution(id string) []*task.Task {
	ds.tmu.RLock()
	defer ds.tmu.RUnlock()
	execution := make([]*task.Task, 0)
	for _, t := range ds.tasks {
		if t.JobID == id {
			execution = append(execution, t.Clone())
		}
	}
	return execution
}

func (ds *InMemoryDatastore) GetJobByID(ctx context.Context, id string) (*job.Job, error) {
	ds.jmu.RLock()
	defer ds.jmu.RUnlock()
	j, ok := ds.jobs[id]
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
	ds.tmu.RLock()
	defer ds.tmu.RUnlock()
	result := make([]*task.Task, 0)
	for _, t := range ds.tasks {
		if t.JobID == jobID && t.State.IsActive() {
			result = append(result, t.Clone())
		}
	}
	return result, nil
}

func (ds *InMemoryDatastore) GetJobs(ctx context.Context, q string, page, size int) (*Page[*job.Job], error) {
	if q != "" {
		return nil, errors.New("full text-search is not supported in the inmem datastore")
	}
	ds.jmu.RLock()
	defer ds.jmu.RUnlock()
	offset := (page - 1) * size
	result := make([]*job.Job, 0)
	for i := offset; i < (offset+size) && i < len(ds.jobIDs); i++ {
		j, ok := ds.jobs[ds.jobIDs[i]]
		if !ok {
			return nil, ErrJobNotFound
		}
		jc := j.Clone()
		jc.Tasks = make([]*task.Task, 0)
		jc.Execution = make([]*task.Task, 0)
		result = append(result, jc)
	}
	totalPages := len(ds.jobIDs) / size
	if len(ds.jobIDs)%size != 0 {
		totalPages = totalPages + 1
	}
	return &Page[*job.Job]{
		Items:      result,
		Number:     page,
		Size:       len(result),
		TotalPages: totalPages,
		TotalItems: len(ds.jobIDs),
	}, nil
}

func (ds *InMemoryDatastore) GetStats(ctx context.Context) (*stats.Stats, error) {
	s := &stats.Stats{}

	ds.jmu.RLock()
	for _, j := range ds.jobs {
		if j.State == job.Running {
			s.Jobs.Running = s.Jobs.Running + 1
		}
	}
	ds.jmu.RUnlock()

	ds.tmu.RLock()
	for _, t := range ds.tasks {
		if t.State == task.Running {
			s.Tasks.Running = s.Tasks.Running + 1
		}
	}
	ds.tmu.RUnlock()

	ds.nmu.RLock()
	for _, n := range ds.nodes {
		if n.LastHeartbeatAt.After(time.Now().UTC().Add(-(time.Minute * 5))) {
			s.Nodes.Running = s.Nodes.Running + 1
			s.Nodes.CPUPercent = s.Nodes.CPUPercent + n.CPUPercent
		}
	}
	// calculate average
	if s.Nodes.Running > 0 {
		s.Nodes.CPUPercent = s.Nodes.CPUPercent / float64(s.Nodes.Running)
	}
	ds.nmu.RUnlock()

	return s, nil
}
