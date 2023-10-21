package host

import (
	"github.com/rs/zerolog/log"
	"github.com/shirou/gopsutil/v3/cpu"
)

func GetCPUPercent() float64 {
	perc, err := cpu.Percent(0, false)
	if err != nil {
		log.Warn().
			Err(err).
			Msgf("error getting CPU usage")
		return 0
	}
	return perc[0]
}
