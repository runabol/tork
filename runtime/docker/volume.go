package docker

import (
	"context"

	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/uuid"
)

type VolumeMounter struct {
	client      *client.Client
	allowOpts   bool
	allowDriver bool
}

type Opt = func(m *VolumeMounter)

func WithAllowOpts(allowOpts bool) Opt {
	return func(m *VolumeMounter) {
		m.allowOpts = allowOpts
	}
}

func WithAllowDriver(allowDriver bool) Opt {
	return func(m *VolumeMounter) {
		m.allowDriver = allowDriver
	}
}

func NewVolumeMounter(opts ...Opt) (*VolumeMounter, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	vm := &VolumeMounter{client: dc}
	for _, o := range opts {
		o(vm)
	}
	return vm, nil
}

func (m *VolumeMounter) Mount(ctx context.Context, mn *tork.Mount) error {
	name := uuid.NewUUID()
	if !m.allowDriver && mn.Driver != "" {
		return errors.Errorf("driver is not allowed for volume mounts")
	}
	if !m.allowOpts && len(mn.Opts) > 0 {
		return errors.Errorf("opts are not allowed for volume mounts")
	}
	mn.Source = name
	v, err := m.client.VolumeCreate(ctx, volume.CreateOptions{
		Name:       name,
		Driver:     mn.Driver,
		DriverOpts: mn.Opts,
	})
	if err != nil {
		return err
	}
	log.Debug().
		Str("mount-point", v.Mountpoint).Msgf("created volume %s", v.Name)
	return nil
}

func (m *VolumeMounter) Unmount(ctx context.Context, mn *tork.Mount) error {
	log.Debug().Msgf("removing volume %s", mn.Source)
	ls, err := m.client.VolumeList(ctx, volume.ListOptions{Filters: filters.NewArgs(filters.Arg("name", mn.Source))})
	if err != nil {
		return err
	}
	if len(ls.Volumes) == 0 {
		return errors.Errorf("unknown volume: %s", mn.Source)
	}
	if err := m.client.VolumeRemove(ctx, mn.Source, true); err != nil {
		return err
	}
	log.Debug().Msgf("removed volume %s", mn.Source)
	return nil
}
