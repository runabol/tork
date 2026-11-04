package docker

import (
	"context"

	"github.com/moby/moby/client"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/uuid"
)

type VolumeMounter struct {
	client *client.Client
}

func NewVolumeMounter() (*VolumeMounter, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	return &VolumeMounter{client: dc}, nil
}

func (m *VolumeMounter) Mount(ctx context.Context, mn *tork.Mount) error {
	name := uuid.NewUUID()
	mn.Source = name
	v, err := m.client.VolumeCreate(ctx, client.VolumeCreateOptions{Name: name, DriverOpts: mn.Opts})
	if err != nil {
		return err
	}
	log.Debug().
		Str("mount-point", v.Volume.Mountpoint).Msgf("created volume %s", v.Volume.Name)
	return nil
}

func (m *VolumeMounter) Unmount(ctx context.Context, mn *tork.Mount) error {
	log.Debug().Msgf("removing volume %s", mn.Source)
	ls, err := m.client.VolumeList(ctx, client.VolumeListOptions{Filters: client.Filters{}.Add("name", mn.Source)})
	if err != nil {
		return err
	}
	if len(ls.Items) == 0 {
		return errors.Errorf("unknown volume: %s", mn.Source)
	}
	if _, err := m.client.VolumeRemove(ctx, mn.Source, client.VolumeRemoveOptions{Force: true}); err != nil {
		return err
	}
	log.Debug().Msgf("removed volume %s", mn.Source)
	return nil
}
