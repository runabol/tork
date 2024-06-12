package docker

import (
	"context"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMountBindNotAllowed(t *testing.T) {
	m := &BindMounter{cfg: BindConfig{
		Allowed: false,
	}}

	err := m.Mount(context.Background(), &tork.Mount{
		Type:   tork.MountTypeBind,
		Source: "/tmp",
		Target: "/somevol",
	})
	assert.Error(t, err)
}

func TestMountCreate(t *testing.T) {
	m := NewBindMounter(BindConfig{
		Allowed: true,
	})
	dir := path.Join(os.TempDir(), uuid.NewUUID())
	wg := sync.WaitGroup{}
	c := 10
	wg.Add(c)
	for i := 0; i < c; i++ {
		go func() {
			defer wg.Done()
			err := m.Mount(context.Background(), &tork.Mount{
				Type:   tork.MountTypeBind,
				Source: dir,
				Target: "/somevol",
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func TestMountSources(t *testing.T) {

	t.Run("allowed source", func(t *testing.T) {
		m := NewBindMounter(BindConfig{
			Allowed: true,
			Sources: []string{"/tmp"},
		})
		mnt := tork.Mount{
			Type:   tork.MountTypeBind,
			Source: "/tmp",
			Target: "/somevol",
		}

		err := m.Mount(context.Background(), &mnt)
		assert.NoError(t, err)
		assert.Equal(t, "/somevol", mnt.Target)
		assert.Equal(t, "/tmp", mnt.Source)
		assert.Equal(t, tork.MountTypeBind, mnt.Type)
	})

	t.Run("non allowed source", func(t *testing.T) {
		m := NewBindMounter(BindConfig{
			Allowed: true,
			Sources: []string{"/tmp"},
		})
		mnt := tork.Mount{
			Type:   tork.MountTypeBind,
			Source: "/tmp/sub/path",
			Target: "/somevol",
		}

		err := m.Mount(context.Background(), &mnt)
		assert.Error(t, err)
	})

	t.Run("non allowed source", func(t *testing.T) {
		m := NewBindMounter(BindConfig{
			Allowed: true,
			Sources: []string{"/tmp"},
		})
		mnt := tork.Mount{
			Type:   tork.MountTypeBind,
			Source: "/other",
			Target: "/somevol",
		}

		err := m.Mount(context.Background(), &mnt)
		assert.Error(t, err)
	})

}
