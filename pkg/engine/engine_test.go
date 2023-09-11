package engine_test

import (
	"testing"

	"github.com/runabol/tork/pkg/conf"
	"github.com/runabol/tork/pkg/engine"
	"github.com/stretchr/testify/assert"
)

func TestRunStandalone(t *testing.T) {
	err := conf.LoadConfig()
	assert.NoError(t, err)

	eng := engine.New(engine.Config{
		Mode: engine.ModeStandalone,
	})

	started := false
	eng.OnStarted(func() error {
		started = true
		eng.Terminate()
		return nil
	})

	err = eng.Start()
	assert.NoError(t, err)

	assert.True(t, started)
}

func TestRunCoordinator(t *testing.T) {
	err := conf.LoadConfig()
	assert.NoError(t, err)

	eng := engine.New(engine.Config{Mode: engine.ModeCoordinator})

	started := false
	eng.OnStarted(func() error {
		started = true
		eng.Terminate()
		return nil
	})

	err = eng.Start()
	assert.NoError(t, err)

	assert.True(t, started)
}

func TestRunWorker(t *testing.T) {
	err := conf.LoadConfig()
	assert.NoError(t, err)

	eng := engine.New(engine.Config{Mode: engine.ModeWorker})

	started := false
	eng.OnStarted(func() error {
		started = true
		eng.Terminate()
		return nil
	})

	err = eng.Start()
	assert.NoError(t, err)

	assert.True(t, started)
}
