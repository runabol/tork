package host

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetStats(t *testing.T) {
	cpuPercent := GetCPUPercent()
	assert.GreaterOrEqual(t, cpuPercent, float64(0))
}
