//go:build !freebsd && !darwin && !linux

package shell

import (
	"github.com/rs/zerolog/log"
)

func SetUID(uid string) {
	if uid != DEFAULT_UID {
		log.Fatal().Msgf("setting uid is only supported on unix/linux systems")
	}
}

func SetGID(gid string) {
	if gid != DEFAULT_GID {
		log.Fatal().Msgf("setting gid is only supported on unix/linux systems")
	}
}
