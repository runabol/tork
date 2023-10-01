package runtime

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestMultiVolumeMount(t *testing.T) {
	m := NewMultiMounter()
	vm, err := NewVolumeMounter()
	assert.NoError(t, err)
	m.RegisterMounter(tork.MountTypeVolume, vm)
	ctx := context.Background()
	mnt := &tork.Mount{Type: tork.MountTypeVolume, Target: "/mnt"}
	err = m.Mount(ctx, mnt)
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
