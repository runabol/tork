package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/tork/task"
)

type PostgresDatastore struct {
	db *sqlx.DB
}

type taskRecord struct {
	ID         string    `db:"id"`
	CreatedAt  time.Time `db:"created_at"`
	State      string    `db:"state"`
	Serialized []byte    `db:"serialized"`
}

func NewPostgresDataStore(dsn string) (*PostgresDatastore, error) {
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to connect to postgres")
	}
	return &PostgresDatastore{db: db}, nil
}

func (ds *PostgresDatastore) CreateSchema() error {
	schema, err := os.ReadFile("db/postgres/schema.sql")
	if err != nil {
		return errors.Wrapf(err, "erroring reading postgres schema file")
	}
	_, err = ds.db.Exec(string(schema))
	return err
}

func (ds *PostgresDatastore) SaveTask(ctx context.Context, t *task.Task) error {
	bytez, err := json.Marshal(t)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize task")
	}
	q := `insert into tasks 
	       (id,created_at,state,serialized) 
	      values
	       ($1,$2,$3,$4)`
	_, err = ds.db.Exec(q, t.ID, t.CreatedAt, t.State, (bytez))
	if err != nil {
		return errors.Wrapf(err, "error inserting task to the db")
	}
	return nil
}

func (ds *PostgresDatastore) GetTaskByID(ctx context.Context, id string) (*task.Task, error) {
	tr := taskRecord{}
	if err := ds.db.Get(&tr, `SELECT * FROM tasks where id = $1`, id); err != nil {
		return nil, errors.Wrapf(err, "error fetching task from db")
	}
	t := &task.Task{}
	if err := json.Unmarshal(tr.Serialized, t); err != nil {
		return nil, errors.Wrapf(err, "error desiralizing task")
	}
	return t, nil
}

func (ds *PostgresDatastore) UpdateTask(ctx context.Context, id string, modify func(t *task.Task)) error {
	tx, err := ds.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to begin tx")
	}
	t, err := ds.GetTaskByID(ctx, id)
	if err != nil {
		return err
	}
	modify(t)
	bytez, err := json.Marshal(t)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize task")
	}
	q := `update tasks set 
	        state = $1,
			serialized = $2
		  where id = $3`
	_, err = ds.db.Exec(q, t.State, (bytez), t.ID)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return errors.Wrapf(err, "error rolling-back tx")
		}
		return errors.Wrapf(err, "error update task in db")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrapf(err, "error commiting tx")
	}
	return nil
}
