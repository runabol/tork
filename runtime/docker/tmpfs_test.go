package docker

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestMountTmpfs(t *testing.T) {
	mounter := NewTmpfsMounter()
	ctx := context.Background()
	mnt := &tork.Mount{
		Type:   tork.MountTypeTmpfs,
		Target: "/target",
	}
	err := mounter.Mount(ctx, mnt)
	assert.NoError(t, err)
}

func TestMountTmpfsWithSource(t *testing.T) {
	mounter := NewTmpfsMounter()
	ctx := context.Background()
	mnt := &tork.Mount{
		Type:   tork.MountTypeTmpfs,
		Target: "/target",
		Source: "/source",
	}
	err := mounter.Mount(ctx, mnt)
	assert.Error(t, err)
}

func TestUnmountTmpfs(t *testing.T) {
	mounter := NewTmpfsMounter()
	ctx := context.Background()
	mnt := &tork.Mount{
		Type:   tork.MountTypeTmpfs,
		Target: "/target",
	}
	err := mounter.Mount(ctx, mnt)
	assert.NoError(t, err)
	err = mounter.Unmount(ctx, mnt)
	assert.NoError(t, err)
}
