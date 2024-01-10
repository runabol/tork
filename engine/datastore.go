package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/datastore/inmemory"
	"github.com/runabol/tork/datastore/postgres"
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
		return inmemory.NewInMemoryDatastore(), nil
	case datastore.DATASTORE_POSTGRES:
		dsn := conf.StringDefault(
			"datastore.postgres.dsn",
			"host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable",
		)
		return postgres.NewPostgresDataStore(dsn,
			postgres.WithTaskLogRetentionPeriod(conf.DurationDefault("datastore.postgres.task.logs.interval", postgres.DefaultTaskLogsRetentionPeriod)),
		)
	default:
		return nil, errors.Errorf("unknown datastore type: %s", dstype)
	}
}
