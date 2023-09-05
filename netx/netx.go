package netx

import (
	"net"
	"time"

	"github.com/rs/zerolog/log"
)

func CanConnect(address string) bool {
	timeout := time.Second
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return false
	}
	if conn != nil {
		if err := conn.Close(); err != nil {
			log.Error().
				Err(err).
				Msgf("error closing connection to %s", address)
		}
		return true
	}
	return false
}
