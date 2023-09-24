package mount

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiVolumeMount(t *testing.T) {
	m := NewMultiMounter()
	vm, err := NewVolumeMounter()
	assert.NoError(t, err)
	m.RegisterMounter(TypeVolume, vm)
	ctx := context.Background()
	mnt := &Mount{Type: TypeVolume, Target: "/mnt"}
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
	mnt := &Mount{Type: "badone", Target: "/mnt"}
	err := m.Mount(ctx, mnt)
	assert.Error(t, err)
}
