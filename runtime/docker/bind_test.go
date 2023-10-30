package docker

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func Test_createMountBindNotAllowed(t *testing.T) {
	m := &BindMounter{cfg: BindConfig{
		Allowed: false,
	}}

	err := m.Mount(context.Background(), &tork.Mount{
		Type:   tork.MountTypeBind,
		Source: "/tmp",
		Target: "/somevol",
	})
	assert.Error(t, err)
}
