package mount

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapperVolumeMount(t *testing.T) {
	m, err := NewMounter(Config{})
	assert.NoError(t, err)
	ctx := context.Background()
	mnt := &Mount{Type: TypeVolume, Target: "/mnt"}
	err = m.Mount(ctx, mnt)
	defer func() {
		err := m.Unmount(ctx, mnt)
		assert.NoError(t, err)
	}()
	assert.NoError(t, err)
}

func TestMapperBadTypeMount(t *testing.T) {
	m, err := NewMounter(Config{})
	assert.NoError(t, err)
	ctx := context.Background()
	mnt := &Mount{Type: "badone", Target: "/mnt"}
	err = m.Mount(ctx, mnt)
	assert.Error(t, err)
}
