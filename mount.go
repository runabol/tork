package tork

import "golang.org/x/exp/maps"

const (
	MountTypeVolume string = "volume"
	MountTypeBind   string = "bind"
	MountTypeTmpfs  string = "tmpfs"
)

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
