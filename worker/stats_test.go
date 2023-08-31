package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetStats(t *testing.T) {
	cpuPercent := getCPUPercent()
	assert.GreaterOrEqual(t, cpuPercent, float64(0))
}
