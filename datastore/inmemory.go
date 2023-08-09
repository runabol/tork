package datastore

import (
	"context"
	"errors"
	"sync"

	"github.com/tork/task"
)

var ErrTaskNotFound = errors.New("task not found")

type InMemoryDatastore struct {
	data map[string]task.Task
	mu   sync.RWMutex
}

func NewInMemoryDatastore() *InMemoryDatastore {
	return &InMemoryDatastore{
		data: make(map[string]task.Task),
	}
}

func (ds *InMemoryDatastore) SaveTask(ctx context.Context, t *task.Task) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.data[t.ID] = *t
	return nil
}

func (ds *InMemoryDatastore) GetTaskByID(ctx context.Context, id string) (*task.Task, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	t, ok := ds.data[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return &t, nil
}

func (ds *InMemoryDatastore) UpdateTask(ctx context.Context, id string, modifier func(t *task.Task)) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	t, ok := ds.data[id]
	if !ok {
		return ErrTaskNotFound
	}
	modifier(&t)
	ds.data[t.ID] = t
	return nil
}
