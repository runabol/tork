package mount

import (
	"context"

	"github.com/pkg/errors"
)

type MapperMounter struct {
	mounters map[string]Mounter
}

type Config struct {
	Bind BindConfig
	Temp TempConfig
}

type BindConfig struct {
	Allowed   bool
	Allowlist []string
	Denylist  []string
}

type TempConfig struct {
	TempDir string
}

func NewMounter(cfg Config) (*MapperMounter, error) {
	vol, err := NewVolumeMounter()
	if err != nil {
		return nil, err
	}
	return &MapperMounter{
		mounters: map[string]Mounter{
			TypeBind:   &BindMounter{cfg: cfg.Bind},
			TypeVolume: vol,
		},
	}, nil
}

func (m *MapperMounter) Mount(ctx context.Context, mnt *Mount) error {
	mounter, ok := m.mounters[mnt.Type]
	if !ok {
		return errors.Errorf("unknown mount type: %s", mnt.Type)
	}
	return mounter.Mount(ctx, mnt)
}

func (m *MapperMounter) Unmount(ctx context.Context, mnt *Mount) error {
	mounter, ok := m.mounters[mnt.Type]
	if !ok {
		return errors.Errorf("unknown mount type: %s", mnt.Type)
	}
	return mounter.Unmount(ctx, mnt)
}
