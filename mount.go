package tork

const (
	MountTypeVolume string = "volume"
	MountTypeBind   string = "bind"
	MountTypeTmpfs  string = "tmpfs"
)

type Mount struct {
	ID     string `json:"-"`
	Type   string `json:"type,omitempty"`
	Source string `json:"source,omitempty"`
	Target string `json:"target,omitempty"`
}
