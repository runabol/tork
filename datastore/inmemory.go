package datastore

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/tork/node"
	"github.com/tork/task"
)

var ErrTaskNotFound = errors.New("task not found")
var ErrNodeNotFound = errors.New("node not found")

type InMemoryDatastore struct {
	tasks map[string]task.Task
	nodes map[string]node.Node
	mu    sync.RWMutex
}

func NewInMemoryDatastore() *InMemoryDatastore {
	return &InMemoryDatastore{
		tasks: make(map[string]task.Task),
		nodes: make(map[string]node.Node),
	}
}

func (ds *InMemoryDatastore) CreateTask(ctx context.Context, t *task.Task) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.tasks[t.ID] = *t
	return nil
}

func (ds *InMemoryDatastore) GetTaskByID(ctx context.Context, id string) (*task.Task, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	t, ok := ds.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return &t, nil
}

func (ds *InMemoryDatastore) UpdateTask(ctx context.Context, id string, modify func(u *task.Task) error) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	t, ok := ds.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}
	if err := modify(&t); err != nil {
		return err
	}
	ds.tasks[t.ID] = t
	return nil
}

func (ds *InMemoryDatastore) CreateNode(ctx context.Context, n *node.Node) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.nodes[n.ID] = *n
	return nil
}

func (ds *InMemoryDatastore) UpdateNode(ctx context.Context, id string, modify func(u *node.Node) error) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
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

func (ds *InMemoryDatastore) GetNodeByID(ctx context.Context, id string) (*node.Node, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	n, ok := ds.nodes[id]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return &n, nil
}

func (ds *InMemoryDatastore) GetActiveNodes(ctx context.Context, lastHeartbeatAfter time.Time) ([]*node.Node, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	nodes := make([]*node.Node, 0)
	for _, n := range ds.nodes {
		if n.LastHeartbeatAt.After(lastHeartbeatAfter) {
			nodes = append(nodes, &n)
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].LastHeartbeatAt.After(nodes[j].LastHeartbeatAt)
	})
	return nodes, nil
}
