package runtime

import (
	"context"
	"sync"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/uuid"

	"github.com/stretchr/testify/assert"
)

type fakeMounter struct {
	newType string
}

func (m *fakeMounter) Mount(ctx context.Context, mnt *tork.Mount) error {
	mnt.Type = m.newType
	return nil
}

func (m *fakeMounter) Unmount(ctx context.Context, mnt *tork.Mount) error {
	return nil
}

func TestMultiVolumeMount(t *testing.T) {
	m := NewMultiMounter()
	m.RegisterMounter(tork.MountTypeVolume, &fakeMounter{newType: tork.MountTypeVolume})
	ctx := context.Background()
	mnt := &tork.Mount{
		ID:     uuid.NewUUID(),
		Type:   tork.MountTypeVolume,
		Target: "/mnt",
	}
	err := m.Mount(ctx, mnt)
	defer func() {
		err := m.Unmount(ctx, mnt)
		assert.NoError(t, err)
	}()
	assert.NoError(t, err)
}

func TestMultiBadTypeMount(t *testing.T) {
	m := NewMultiMounter()
	ctx := context.Background()
	mnt := &tork.Mount{
		ID:     uuid.NewUUID(),
		Type:   "badone",
		Target: "/mnt",
	}
	err := m.Mount(ctx, mnt)
	assert.Error(t, err)
}

func TestMultiMountUnmount(t *testing.T) {
	m := NewMultiMounter()
	m.RegisterMounter(tork.MountTypeVolume, &fakeMounter{newType: "other-type"})
	ctx := context.Background()
	mnt := &tork.Mount{
		ID:     uuid.NewUUID(),
		Type:   tork.MountTypeVolume,
		Target: "/mnt",
	}
	err := m.Mount(ctx, mnt)
	defer func() {
		err := m.Unmount(ctx, mnt)
		assert.NoError(t, err)
	}()
	assert.NoError(t, err)
}

func TestMountConcurrency(t *testing.T) {
	ctx := context.Background()
	m := NewMultiMounter()
	m.RegisterMounter(tork.MountTypeVolume, &fakeMounter{newType: tork.MountTypeVolume})
	w := sync.WaitGroup{}
	w.Add(1_000)
	for i := 0; i < 1_000; i++ {
		go func() {
			defer w.Done()
			mnt := &tork.Mount{
				ID:     uuid.NewUUID(),
				Type:   tork.MountTypeVolume,
				Target: "/mnt",
			}
			err := m.Mount(ctx, mnt)
			assert.NoError(t, err)
			err = m.Unmount(ctx, mnt)
			assert.NoError(t, err)
		}()
	}
	w.Wait()
	assert.Len(t, m.mapping, 0)
}
