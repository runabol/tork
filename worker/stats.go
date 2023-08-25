package worker

import (
	"github.com/shirou/gopsutil/v3/cpu"
)

type HostStats struct {
	CPUPercent float64
}

func getStats() (*HostStats, error) {
	perc, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}
	return &HostStats{
		CPUPercent: perc[0],
	}, nil
}
