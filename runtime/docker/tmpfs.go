package docker

import (
	"context"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
)

type TmpfsMounter struct {
}

func NewTmpfsMounter() *TmpfsMounter {
	return &TmpfsMounter{}
}

func (m *TmpfsMounter) Mount(ctx context.Context, mnt *tork.Mount) error {
	if mnt.Target == "" {
		return errors.Errorf("tmpfs target is required")
	}
	if mnt.Source != "" {
		return errors.Errorf("tmpfs source should be empty")
	}
	return nil
}

func (m *TmpfsMounter) Unmount(ctx context.Context, mnt *tork.Mount) error {
	return nil
}
