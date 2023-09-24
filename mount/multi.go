package mount

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

type MultiMounter struct {
	mounters map[string]Mounter
	mu       sync.RWMutex
}

func NewMultiMounter() *MultiMounter {
	return &MultiMounter{
		mounters: map[string]Mounter{},
	}
}

func (m *MultiMounter) Mount(ctx context.Context, mnt *Mount) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mounter, ok := m.mounters[mnt.Type]
	if !ok {
		return errors.Errorf("unknown mount type: %s", mnt.Type)
	}
	return mounter.Mount(ctx, mnt)
}

func (m *MultiMounter) Unmount(ctx context.Context, mnt *Mount) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mounter, ok := m.mounters[mnt.Type]
	if !ok {
		return errors.Errorf("unknown mount type: %s", mnt.Type)
	}
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
