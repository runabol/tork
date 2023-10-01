package runtime

import (
	"context"

	"github.com/runabol/tork"
)

type Mounter interface {
	Mount(ctx context.Context, mnt *tork.Mount) error
	Unmount(ctx context.Context, mnt *tork.Mount) error
}
