package mount

import (
	"context"

	"os"

	"github.com/pkg/errors"
)

type TempMounter struct {
	cfg TempConfig
}

func (m *TempMounter) Mount(ctx context.Context, mnt *Mount) error {
	workdir, err := os.MkdirTemp(m.cfg.TempDir, "tork-")
	if err != nil {
		return errors.Wrapf(err, "error creating temp dir")
	}
	mnt.Source = workdir
	return nil
}

func (m *TempMounter) Unmount(ctx context.Context, mnt *Mount) error {
	if err := os.RemoveAll(mnt.Source); err != nil {
		return errors.Wrapf(err, "error removing temp mount: %s", mnt.Source)
	}
	return nil
}
