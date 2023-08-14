package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetStats(t *testing.T) {
	stats, err := getStats()
	assert.NoError(t, err)
	assert.Greater(t, stats.CPUPercent, float64(0))
}
