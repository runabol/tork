package docker

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
)

type BindMounter struct {
	cfg    BindConfig
	mounts map[string]string
	mu     sync.RWMutex
}

type BindConfig struct {
	Allowed bool
	Sources []string
}

func NewBindMounter(cfg BindConfig) *BindMounter {
	return &BindMounter{
		cfg:    cfg,
		mounts: make(map[string]string),
	}
}

func (m *BindMounter) Mount(ctx context.Context, mnt *tork.Mount) error {
	if !m.cfg.Allowed {
		return errors.New("bind mounts are not allowed")
	}
	if !m.isSourceAllowed(mnt.Source) {
		return errors.New(fmt.Sprintf("src bind mount is not allowed: %s", mnt.Source))
	}
	m.mu.RLock()
	_, ok := m.mounts[mnt.Source]
	m.mu.RUnlock()
	if ok {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// check if the source dir exists
	if _, err := os.Stat(mnt.Source); os.IsNotExist(err) {
		if err := os.MkdirAll(mnt.Source, 0707); err != nil {
			return errors.Wrapf(err, "error creating mount directory: %s", mnt.Source)
		}
		log.Info().Msgf("Created bind mount: %s", mnt.Source)
	} else if err != nil {
		return errors.Wrapf(err, "error stat on directory: %s", mnt.Source)
	}
	m.mounts[mnt.Source] = mnt.Source
	return nil
}

func (m *BindMounter) isSourceAllowed(src string) bool {
	if len(m.cfg.Sources) == 0 {
		return true
	}
	for _, allow := range m.cfg.Sources {
		if strings.EqualFold(allow, src) {
			return true
		}
	}
	return false
}

func (m *BindMounter) Unmount(ctx context.Context, mnt *tork.Mount) error {
	return nil
}
