package mount

import (
	"context"

	"github.com/pkg/errors"
	"github.com/runabol/tork/internal/wildcard"
)

type BindMounter struct {
	cfg BindConfig
}

func NewBindMounter(cfg BindConfig) *BindMounter {
	return &BindMounter{
		cfg: cfg,
	}
}

func (m *BindMounter) Mount(ctx context.Context, mnt *Mount) error {
	if !m.cfg.Allowed {
		return errors.New("bind mounts are not allowed")
	}
	for _, deny := range m.cfg.Denylist {
		if wildcard.Match(deny, mnt.Source) {
			return errors.Errorf("mount point not allowed: %s", mnt.Source)
		}
	}
	for _, allow := range m.cfg.Allowlist {
		if wildcard.Match(allow, mnt.Source) {
			return nil
		}
	}
	return errors.Errorf("mount point not allowed: %s", mnt.Source)
}

func (m *BindMounter) Unmount(ctx context.Context, mnt *Mount) error {
	return nil
}
