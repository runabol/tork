package engine

import (
	"time"

	"github.com/pkg/errors"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/locker"
)

func (e *Engine) initLocker() error {
	ltype := conf.StringDefault("locker.type", conf.StringDefault("datastore.type", locker.LOCKER_INMEMORY))
	locker, err := e.createLocker(ltype)
	if err != nil {
		return err
	}
	e.locker = locker
	return nil
}

func (e *Engine) createLocker(ltype string) (locker.Locker, error) {
	switch ltype {
	case locker.LOCKER_INMEMORY:
		return locker.NewInMemoryLocker(), nil
	case locker.LOCKER_POSTGRES:
		dsn := conf.StringDefault(
			"locker.postgres.dsn",
			conf.StringDefault("datastore.postgres.dsn", "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"),
		)
		return locker.NewPostgresLocker(dsn,
			locker.WithMaxOpenConns(conf.IntDefault("locker.postgres.max_open_conns", conf.IntDefault("datastore.postgres.max_open_conns", 25))),
			locker.WithMaxIdleConns(conf.IntDefault("locker.postgres.max_idle_conns", conf.IntDefault("datastore.postgres.max_idle_conns", 25))),
			locker.WithConnMaxLifetime(conf.DurationDefault("locker.postgres.conn_max_lifetime", conf.DurationDefault("datastore.postgres.conn_max_lifetime", time.Hour))),
			locker.WithConnMaxIdleTime(conf.DurationDefault("locker.postgres.conn_max_idle_time", conf.DurationDefault("datastore.postgres.conn_max_idle_time", time.Minute*5))),
		)
	default:
		return nil, errors.Errorf("unknown locker type: %s", ltype)
	}
}
