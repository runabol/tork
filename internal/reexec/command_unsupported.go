//go:build !linux && !freebsd && !darwin

package reexec // import "github.com/docker/docker/pkg/reexec"

import (
	"os/exec"
)

func Self() string {
	return ""
}

func Command(args ...string) *exec.Cmd {
	return nil
}
