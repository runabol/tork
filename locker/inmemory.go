package locker

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

type InMemoryLocker struct {
	mu    sync.Mutex
	locks map[string]struct{}
}

type inmemLock struct {
	key    string
	locker *InMemoryLocker
}

func (l *inmemLock) ReleaseLock(_ context.Context) error {
	return l.locker.releaseLock(l.key)
}

func NewInMemoryLocker() *InMemoryLocker {
	return &InMemoryLocker{
		locks: make(map[string]struct{}),
	}
}

func (m *InMemoryLocker) AcquireLock(ctx context.Context, key string) (Lock, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.locks[key]; exists {
		return nil, errors.Errorf("failed to acquire lock for key '%s'", key)
	}
	m.locks[key] = struct{}{}
	return &inmemLock{key: key, locker: m}, nil
}

func (m *InMemoryLocker) releaseLock(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.locks[key]; !exists {
		return errors.Errorf("failed to release lock for key '%s'", key)
	}
	delete(m.locks, key)
	return nil
}
