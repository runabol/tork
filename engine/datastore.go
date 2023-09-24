package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/datastore"
)

func (e *Engine) initDatastore() error {
	dstype := conf.StringDefault("datastore.type", datastore.DATASTORE_INMEMORY)
	ds, err := e.createDatastore(dstype)
	if err != nil {
		return err
	}
	e.ds = ds
	return nil
}

func (e *Engine) createDatastore(dstype string) (datastore.Datastore, error) {
	dsp, ok := e.dsProviders[dstype]
	if ok {
		return dsp()
	}
	switch dstype {
	case datastore.DATASTORE_INMEMORY:
		return datastore.NewInMemoryDatastore(), nil
	case datastore.DATASTORE_POSTGRES:
		dsn := conf.StringDefault(
			"datastore.postgres.dsn",
			"host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable",
		)
		return datastore.NewPostgresDataStore(dsn)
	default:
		return nil, errors.Errorf("unknown datastore type: %s", dstype)
	}
}
