package mount

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTempMount(t *testing.T) {
	m := &TempMounter{cfg: TempConfig{
		TempDir: "/tmp",
	}}
	mnt := &Mount{
		Type:   TypeBind,
		Target: "/somevol",
	}
	err := m.Mount(context.Background(), mnt)
	assert.NoError(t, err)

	_, err = os.Stat(mnt.Source)
	assert.NoError(t, err)
	assert.False(t, os.IsNotExist(err))

	err = m.Unmount(context.Background(), mnt)
	assert.NoError(t, err)
	_, err = os.Stat(mnt.Source)
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}
