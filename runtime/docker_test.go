package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tork/task"
)

func TestParseCPUs(t *testing.T) {
	parsed, err := parseCPUs(&task.Limits{CPUs: ".25"})
	assert.NoError(t, err)
	assert.Equal(t, int64(250000000), parsed)

	parsed, err = parseCPUs(&task.Limits{CPUs: "1"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1000000000), parsed)

	parsed, err = parseCPUs(&task.Limits{CPUs: "0.5"})
	assert.NoError(t, err)
	assert.Equal(t, int64(500000000), parsed)
}

func TestParseMemory(t *testing.T) {
	parsed, err := parseMemory(&task.Limits{Memory: "1MB"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1048576), parsed)

	parsed, err = parseMemory(&task.Limits{Memory: "10MB"})
	assert.NoError(t, err)
	assert.Equal(t, int64(10485760), parsed)

	parsed, err = parseMemory(&task.Limits{Memory: "500KB"})
	assert.NoError(t, err)
	assert.Equal(t, int64(512000), parsed)

	parsed, err = parseMemory(&task.Limits{Memory: "1B"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), parsed)
}
