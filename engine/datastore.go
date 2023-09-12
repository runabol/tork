package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/datastore"
)

func (e *Engine) initDatastore() error {
	dstype := conf.StringDefault("datastore.type", datastore.DATASTORE_INMEMORY)
	var ds datastore.Datastore
	ds, err := datastore.NewFromProvider(dstype)
	if err != nil && !errors.Is(err, datastore.ErrProviderNotFound) {
		return err
	}
	if ds != nil {
		e.ds = ds
		return nil
	}
	switch dstype {
	case datastore.DATASTORE_INMEMORY:
		ds = datastore.NewInMemoryDatastore()
	case datastore.DATASTORE_POSTGRES:
		dsn := conf.StringDefault(
			"datastore.postgres.dsn",
			"host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable",
		)
		pg, err := datastore.NewPostgresDataStore(dsn)
		if err != nil {
			return err
		}
		ds = pg
	default:
		return errors.Errorf("unknown datastore type: %s", dstype)
	}
	e.ds = ds
	return nil
}
