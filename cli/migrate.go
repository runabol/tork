package cli

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/datastore/postgres"
	schema "github.com/runabol/tork/db/postgres"
	ucli "github.com/urfave/cli/v2"
)

func (c *CLI) migrationCmd() *ucli.Command {
	return &ucli.Command{
		Name:   "migration",
		Usage:  "Run the db migration script",
		Action: migration,
	}
}

func migration(ctx *ucli.Context) error {
	dstype := conf.StringDefault("datastore.type", datastore.DATASTORE_INMEMORY)
	switch dstype {
	case datastore.DATASTORE_POSTGRES:
		dsn := conf.StringDefault(
			"datastore.postgres.dsn",
			"host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable",
		)
		pg, err := postgres.NewPostgresDataStore(dsn)
		if err != nil {
			return err
		}
		if err := pg.ExecScript(schema.SCHEMA); err != nil {
			return errors.Wrapf(err, "error when trying to create db schema")
		}
	default:
		return errors.Errorf("can't perform db migration on: %s", dstype)
	}
	log.Info().Msg("migration completed!")
	return nil
}
