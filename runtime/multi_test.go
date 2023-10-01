package runtime

import (
	"context"
	"testing"

	"github.com/runabol/tork"

	"github.com/stretchr/testify/assert"
)

type fakeMounter struct{}

func (m *fakeMounter) Mount(ctx context.Context, mnt *tork.Mount) error {
	return nil
}

func (m *fakeMounter) Unmount(ctx context.Context, mnt *tork.Mount) error {
	return nil
}

func TestMultiVolumeMount(t *testing.T) {
	m := NewMultiMounter()
	m.RegisterMounter(tork.MountTypeVolume, &fakeMounter{})
	ctx := context.Background()
	mnt := &tork.Mount{
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
	mnt := &tork.Mount{Type: "badone", Target: "/mnt"}
	err := m.Mount(ctx, mnt)
	assert.Error(t, err)
}
