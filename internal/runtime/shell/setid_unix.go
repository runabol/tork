//go:build freebsd || darwin || linux

package shell

import (
	"strconv"
	"syscall"

	"github.com/rs/zerolog/log"
)

func SetUID(uid string) {
	if uid != DEFAULT_UID {
		uidi, err := strconv.Atoi(uid)
		if err != nil {
			log.Fatal().Err(err).Msgf("invalid uid: %s", uid)
		}
		if err := syscall.Setuid(uidi); err != nil {
			log.Fatal().Err(err).Msgf("error setting uid: %s", uid)
		}
	}
}

func SetGID(gid string) {
	if gid != DEFAULT_GID {
		gidi, err := strconv.Atoi(gid)
		if err != nil {
			log.Fatal().Err(err).Msgf("invalid gid: %s", gid)
		}
		if err := syscall.Setgid(gidi); err != nil {
			log.Fatal().Err(err).Msgf("error setting gid: %s", gid)
		}
	}
}
