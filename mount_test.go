package tork

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateMountOpts(t *testing.T) {
	tests := []struct {
		name    string
		mount   *Mount
		wantErr bool
	}{
		{
			name:    "bind mount with valid propagation",
			mount:   &Mount{Type: MountTypeBind, Opts: map[string]string{"propagation": "rslave"}},
			wantErr: false,
		},
		{
			name:    "bind mount with invalid propagation",
			mount:   &Mount{Type: MountTypeBind, Opts: map[string]string{"propagation": "rshared"}},
			wantErr: true,
		},
		{
			name:    "bind mount with readonly",
			mount:   &Mount{Type: MountTypeBind, Opts: map[string]string{"readonly": "true"}},
			wantErr: false,
		},
		{
			name:    "bind mount with unknown key",
			mount:   &Mount{Type: MountTypeBind, Opts: map[string]string{"propogation": "rslave"}},
			wantErr: true,
		},
		{
			name:    "tmpfs mount with readonly",
			mount:   &Mount{Type: MountTypeTmpfs, Opts: map[string]string{"readonly": "true"}},
			wantErr: false,
		},
		{
			name:    "tmpfs mount with unknown key",
			mount:   &Mount{Type: MountTypeTmpfs, Opts: map[string]string{"size": "64m"}},
			wantErr: true,
		},
		{
			name:    "volume mount allows arbitrary keys",
			mount:   &Mount{Type: MountTypeVolume, Opts: map[string]string{"driver_opt": "value"}},
			wantErr: false,
		},
		{
			name:    "bind mount with no opts",
			mount:   &Mount{Type: MountTypeBind, Opts: nil},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMountOpts(tt.mount)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
