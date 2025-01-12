package podman

import (
	"context"
	"os"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
)

type VolumeMounter struct {
}

func NewVolumeMounter() *VolumeMounter {
	return &VolumeMounter{}
}

func (m *VolumeMounter) Mount(ctx context.Context, mn *tork.Mount) error {
	vol, err := os.MkdirTemp("", "tork-volume-*")
	if err != nil {
		return errors.Wrap(err, "failed to create temporary directory")
	}
	if err := os.Chmod(vol, 0777); err != nil {
		return errors.Wrap(err, "failed to chmod temporary directory")
	}
	mn.Source = vol
	return nil
}

func (m *VolumeMounter) Unmount(ctx context.Context, mn *tork.Mount) error {
	return os.RemoveAll(mn.Source)
}
