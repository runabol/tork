package engine

import (
	"testing"

	"github.com/runabol/tork/pkg/conf"
	"github.com/stretchr/testify/assert"
)

func TestDefaultRunStandalone(t *testing.T) {
	err := conf.LoadConfig()
	assert.NoError(t, err)

	SetMode(ModeStandalone)

	assert.Equal(t, StateIdle, defaultEngine.state)
	err = Start()

	assert.NoError(t, err)
	assert.Equal(t, StateRunning, defaultEngine.state)

	err = Terminate()
	assert.NoError(t, err)
	assert.Equal(t, StateTerminated, defaultEngine.state)
}
