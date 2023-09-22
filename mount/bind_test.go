package mount

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_createMountBindNotAllowed(t *testing.T) {
	m := &BindMounter{cfg: BindConfig{
		Allowed: false,
	}}

	err := m.Mount(context.Background(), &Mount{
		Type:   TypeBind,
		Source: "/tmp",
		Target: "/somevol",
	})
	assert.Error(t, err)
}

func Test_createMountBindDenylist(t *testing.T) {
	m := &BindMounter{cfg: BindConfig{
		Allowed:  true,
		Denylist: []string{"/tmp"},
	}}
	err := m.Mount(context.Background(), &Mount{
		Type:   TypeBind,
		Source: "/tmp",
		Target: "/somevol",
	})
	assert.Error(t, err)
}

func Test_createMountBindAllowlist(t *testing.T) {
	m := &BindMounter{cfg: BindConfig{
		Allowed:   true,
		Allowlist: []string{"/tmp"},
	}}
	mnt := Mount{
		Type:   TypeBind,
		Source: "/tmp",
		Target: "/somevol",
	}

	err := m.Mount(context.Background(), &mnt)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, m.Unmount(context.Background(), &mnt))
	}()
	assert.Equal(t, "/somevol", mnt.Target)
	assert.Equal(t, "/tmp", mnt.Source)
	assert.Equal(t, TypeBind, mnt.Type)
}
