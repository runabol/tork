package worker

import (
	"github.com/shirou/gopsutil/v3/cpu"
)

type Stats struct {
	CPUPercent float64
}

func getStats() (*Stats, error) {
	perc, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}
	return &Stats{
		CPUPercent: perc[0],
	}, nil
}
