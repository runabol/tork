package engine

import (
	"testing"

	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/stretchr/testify/assert"
)

func Test_createDatastore(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)
	ds, err := eng.createDatastore(datastore.DATASTORE_POSTGRES)
	assert.NoError(t, err)
	assert.IsType(t, &postgres.PostgresDatastore{}, ds)
	dsp, ok := ds.(*postgres.PostgresDatastore)
	assert.True(t, ok)
	assert.NoError(t, dsp.Close())
}

func Test_createDatastoreProvider(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)

	eng.RegisterDatastoreProvider("inmem2", func() (datastore.Datastore, error) {
		return ds, nil
	})

	ds2, err := eng.createDatastore("inmem2")
	assert.NoError(t, err)
	assert.IsType(t, &postgres.PostgresDatastore{}, ds2)
	assert.NoError(t, ds.Close())
}
