package mount

import (
	"context"

	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
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

func (m *VolumeMounter) Mount(ctx context.Context, mn *Mount) error {
	name := uuid.NewUUID()
	mn.Source = name
	v, err := m.client.VolumeCreate(ctx, volume.CreateOptions{Name: name})
	if err != nil {
		return err
	}
	log.Debug().
		Str("mount-point", v.Mountpoint).Msgf("created volume %s", v.Name)
	return nil
}

func (m *VolumeMounter) Unmount(ctx context.Context, mn *Mount) error {
	ls, err := m.client.VolumeList(ctx, filters.NewArgs(filters.Arg("name", mn.Source)))
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
