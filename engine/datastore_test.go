package engine

import (
	"testing"

	"github.com/runabol/tork/datastore"
	"github.com/stretchr/testify/assert"
)

func Test_createDatastore(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)
	ds, err := eng.createDatastore(datastore.DATASTORE_INMEMORY)
	assert.NoError(t, err)
	assert.IsType(t, &datastore.InMemoryDatastore{}, ds)
}

func Test_createDatastoreProvider(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)

	eng.RegisterDatastoreProvider("inmem2", func() (datastore.Datastore, error) {
		return datastore.NewInMemoryDatastore(), nil
	})

	ds, err := eng.createDatastore("inmem2")
	assert.NoError(t, err)
	assert.IsType(t, &datastore.InMemoryDatastore{}, ds)
}
