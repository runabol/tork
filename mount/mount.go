package mount

import (
	"context"
)

const (
	TypeVolume string = "volume"
	TypeBind   string = "bind"
)

type Mounter interface {
	Mount(ctx context.Context, mnt *Mount) error
	Unmount(ctx context.Context, mnt *Mount) error
}

type Mount struct {
	Type   string `json:"type,omitempty"`
	Source string `json:"source,omitempty"`
	Target string `json:"target,omitempty"`
}
