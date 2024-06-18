package runtime

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
)

type MultiMounter struct {
	mounters map[string]Mounter
	mapping  map[*tork.Mount]Mounter
	mu       sync.RWMutex
}

func NewMultiMounter() *MultiMounter {
	return &MultiMounter{
		mounters: map[string]Mounter{},
		mapping:  make(map[*tork.Mount]Mounter),
	}
}

func (m *MultiMounter) Mount(ctx context.Context, mnt *tork.Mount) error {
	m.mu.RLock()
	mounter, ok := m.mounters[mnt.Type]
	m.mu.RUnlock()
	if !ok {
		return errors.Errorf("unknown mount type: %s", mnt.Type)
	}
	m.mu.Lock()
	m.mapping[mnt] = mounter
	m.mu.Unlock()
	return mounter.Mount(ctx, mnt)
}

func (m *MultiMounter) Unmount(ctx context.Context, mnt *tork.Mount) error {
	m.mu.RLock()
	mounter, ok := m.mapping[mnt]
	m.mu.RUnlock()
	if !ok {
		return errors.Errorf("unmounter not found for: %+v", mnt)
	}
	m.mu.Lock()
	delete(m.mapping, mnt)
	m.mu.Unlock()
	return mounter.Unmount(ctx, mnt)
}

func (m *MultiMounter) RegisterMounter(mtype string, mr Mounter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.mounters[mtype]; ok {
		panic("mount: Register called twice for mounter")
	}
	m.mounters[mtype] = mr
}
