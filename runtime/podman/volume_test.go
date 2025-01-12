package podman

import (
	"context"
	"os"
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestCreateVolume(t *testing.T) {
	vm := NewVolumeMounter()

	ctx := context.Background()
	mnt := &tork.Mount{}
	err := vm.Mount(ctx, mnt)
	assert.NoError(t, err)

	_, err = os.Stat(mnt.Source)
	assert.NoError(t, err)

	err = vm.Unmount(ctx, mnt)
	assert.NoError(t, err)

	_, err = os.Stat(mnt.Source)
	assert.Error(t, err)
}

func Test_createMountVolume(t *testing.T) {
	vm := NewVolumeMounter()

	mnt := &tork.Mount{
		Type:   tork.MountTypeVolume,
		Target: "/somevol",
	}

	err := vm.Mount(context.Background(), mnt)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, vm.Unmount(context.Background(), mnt))
	}()
	assert.Equal(t, "/somevol", mnt.Target)
	assert.NotEmpty(t, mnt.Source)
}
