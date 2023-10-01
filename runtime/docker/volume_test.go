package docker

import (
	"context"
	"testing"

	"github.com/docker/docker/api/types/filters"
	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestCreateVolume(t *testing.T) {
	vm, err := NewVolumeMounter()
	assert.NoError(t, err)

	ctx := context.Background()
	mnt := &tork.Mount{}
	err = vm.Mount(ctx, mnt)
	assert.NoError(t, err)

	ls, err := vm.client.VolumeList(ctx, filters.Args{})
	assert.NoError(t, err)
	found := false
	for _, v := range ls.Volumes {
		if v.Name == mnt.Source {
			found = true
			break
		}
	}
	assert.True(t, found)

	err = vm.Unmount(ctx, mnt)
	assert.NoError(t, err)

	ls, err = vm.client.VolumeList(ctx, filters.Args{})
	assert.NoError(t, err)

	for _, v := range ls.Volumes {
		assert.NotEqual(t, "testvol", v.Name)
	}
}

func Test_createMountVolume(t *testing.T) {
	m, err := NewVolumeMounter()
	assert.NoError(t, err)

	mnt := &tork.Mount{
		Type:   tork.MountTypeVolume,
		Target: "/somevol",
	}

	err = m.Mount(context.Background(), mnt)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, m.Unmount(context.Background(), mnt))
	}()
	assert.Equal(t, "/somevol", mnt.Target)
	assert.NotEmpty(t, mnt.Source)
}
