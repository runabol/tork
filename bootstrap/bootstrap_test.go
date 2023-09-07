package bootstrap_test

import (
	"testing"

	"github.com/runabol/tork/bootstrap"
	"github.com/runabol/tork/conf"
	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	err := conf.LoadConfig()
	assert.NoError(t, err)

	started := false
	bootstrap.OnStarted(func() error {
		started = true
		bootstrap.Terminate()
		return nil
	})

	err = bootstrap.Start(bootstrap.ModeStandalone)
	assert.NoError(t, err)

	assert.True(t, started)
}
