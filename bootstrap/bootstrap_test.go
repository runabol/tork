package bootstrap_test

import (
	"os"
	"testing"

	"github.com/runabol/tork/bootstrap"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/mq"
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

func TestRegisterDatastoreProvider(t *testing.T) {
	assert.NoError(t, os.Setenv("TORK_DATASTORE_TYPE", "inmem2"))
	defer func() {
		assert.NoError(t, os.Unsetenv("TORK_DATASTORE_TYPE"))
	}()
	err := conf.LoadConfig()
	assert.NoError(t, err)
	calledProvider := false
	bootstrap.RegisterDatastoreProvider("inmem2", func() (datastore.Datastore, error) {
		calledProvider = true
		return datastore.NewInMemoryDatastore(), nil
	})
	started := false
	bootstrap.OnStarted(func() error {
		started = true
		bootstrap.Terminate()
		return nil
	})
	err = bootstrap.Start(bootstrap.ModeStandalone)
	assert.NoError(t, err)
	assert.True(t, started)
	assert.True(t, calledProvider)
}

func TestRegisterMQProvider(t *testing.T) {
	assert.NoError(t, os.Setenv("TORK_BROKER_TYPE", "inmem2"))
	defer func() {
		assert.NoError(t, os.Unsetenv("TORK_BROKER_TYPE"))
	}()
	err := conf.LoadConfig()
	assert.NoError(t, err)
	calledProvider := false
	bootstrap.RegisterBrokerProvider("inmem2", func() (mq.Broker, error) {
		calledProvider = true
		return mq.NewInMemoryBroker(), nil
	})
	started := false
	bootstrap.OnStarted(func() error {
		started = true
		bootstrap.Terminate()
		return nil
	})
	err = bootstrap.Start(bootstrap.ModeStandalone)
	assert.NoError(t, err)
	assert.True(t, started)
	assert.True(t, calledProvider)
}
