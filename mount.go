package tork

import (
	"fmt"

	"golang.org/x/exp/maps"
)

const (
	MountTypeVolume string = "volume"
	MountTypeBind   string = "bind"
	MountTypeTmpfs  string = "tmpfs"
)

var allowedPropagation = map[string]bool{
	"private":  true,
	"rprivate": true,
	"slave":    true,
	"rslave":   true,
}

var allowedBindOpts = map[string]bool{
	"propagation": true,
	"readonly":    true,
}

var allowedTmpfsOpts = map[string]bool{
	"readonly": true,
}

func ValidateMountOpts(m *Mount) error {
	var allowed map[string]bool
	switch m.Type {
	case MountTypeBind:
		allowed = allowedBindOpts
	case MountTypeTmpfs:
		allowed = allowedTmpfsOpts
	default:
		return nil
	}
	for key := range m.Opts {
		if !allowed[key] {
			return fmt.Errorf("unsupported option %q for %s mount", key, m.Type)
		}
	}
	if prop, ok := m.Opts["propagation"]; ok {
		if !allowedPropagation[prop] {
			return fmt.Errorf("unsupported mount propagation: %s", prop)
		}
	}
	return nil
}

type Mount struct {
	ID     string            `json:"-"`
	Type   string            `json:"type,omitempty"`
	Source string            `json:"source,omitempty"`
	Target string            `json:"target,omitempty"`
	Opts   map[string]string `json:"opts,omitempty"`
}

func (m *Mount) Clone() *Mount {
	return &Mount{
		ID:     m.ID,
		Type:   m.Type,
		Source: m.Source,
		Target: m.Target,
		Opts:   maps.Clone(m.Opts),
	}
}

func CloneMounts(mounts []*Mount) []*Mount {
	copy := make([]*Mount, len(mounts))
	for i, m := range mounts {
		copy[i] = m.Clone()
	}
	return copy
}
