package docker

import (
	"context"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
)

type BindMounter struct {
	cfg BindConfig
}

type BindConfig struct {
	Allowed bool
}

func NewBindMounter(cfg BindConfig) *BindMounter {
	return &BindMounter{
		cfg: cfg,
	}
}

func (m *BindMounter) Mount(ctx context.Context, mnt *tork.Mount) error {
	if !m.cfg.Allowed {
		return errors.New("bind mounts are not allowed")
	}
	return nil
}

func (m *BindMounter) Unmount(ctx context.Context, mnt *tork.Mount) error {
	return nil
}
