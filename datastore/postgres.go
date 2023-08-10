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
	"github.com/tork/node"
	"github.com/tork/task"
)

type PostgresDatastore struct {
	db *sqlx.DB
}

type taskRecord struct {
	ID          string     `db:"id"`
	CreatedAt   time.Time  `db:"created_at"`
	StartedAt   *time.Time `db:"started_at"`
	CompletedAt *time.Time `db:"completed_at"`
	State       string     `db:"state"`
	Serialized  []byte     `db:"serialized"`
}

type nodeRecord struct {
	ID              string    `db:"id"`
	StartedAt       time.Time `db:"started_at"`
	LastHeartbeatAt time.Time `db:"last_heartbeat_at"`
	CPUPercent      float64   `db:"cpu_percent"`
	Queue           string    `db:"queue"`
}

func (r nodeRecord) toNode() *node.Node {
	return &node.Node{
		ID:              r.ID,
		StartedAt:       r.StartedAt,
		CPUPercent:      r.CPUPercent,
		LastHeartbeatAt: r.LastHeartbeatAt,
		Queue:           r.Queue,
	}
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

func (ds *PostgresDatastore) CreateTask(ctx context.Context, t *task.Task) error {
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
		if err == sql.ErrNoRows {
			return nil, ErrNodeNotFound
		}
		return nil, errors.Wrapf(err, "error fetching task from db")
	}
	t := &task.Task{}
	if err := json.Unmarshal(tr.Serialized, t); err != nil {
		return nil, errors.Wrapf(err, "error desiralizing task")
	}
	return t, nil
}

func (ds *PostgresDatastore) UpdateTask(ctx context.Context, id string, modify func(t *task.Task) error) error {
	tx, err := ds.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to begin tx")
	}
	tr := taskRecord{}
	if err := ds.db.Get(&tr, `SELECT * FROM tasks where id = $1 for update`, id); err != nil {
		return errors.Wrapf(err, "error fetching task from db")
	}
	t := &task.Task{}
	if err := json.Unmarshal(tr.Serialized, t); err != nil {
		return errors.Wrapf(err, "error desiralizing task")
	}
	if err := modify(t); err != nil {
		return err
	}
	bytez, err := json.Marshal(t)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize task")
	}
	q := `update tasks set 
	        state = $1,
			serialized = $2,
			started_at = $3,
			completed_at = $4
		  where id = $5`
	_, err = ds.db.Exec(q, t.State, (bytez), t.StartedAt, t.CompletedAt, t.ID)
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

func (ds *PostgresDatastore) CreateNode(ctx context.Context, n *node.Node) error {
	q := `insert into nodes 
	       (id,started_at,last_heartbeat_at,cpu_percent,queue) 
	      values
	       ($1,$2,$3,$4,$5)`
	_, err := ds.db.Exec(q, n.ID, n.StartedAt, n.LastHeartbeatAt, n.CPUPercent, n.Queue)
	if err != nil {
		return errors.Wrapf(err, "error inserting node to the db")
	}
	return nil
}

func (ds *PostgresDatastore) UpdateNode(ctx context.Context, id string, modify func(u *node.Node) error) error {
	tx, err := ds.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to begin tx")
	}
	nr := nodeRecord{}
	if err := ds.db.Get(&nr, `SELECT * FROM nodes where id = $1 for update`, id); err != nil {
		return errors.Wrapf(err, "error fetching node from db")
	}
	n := nr.toNode()
	if err := modify(n); err != nil {
		return err
	}
	q := `update nodes set 
	        last_heartbeat_at = $1,
			cpu_percent = $2
		  where id = $3`
	_, err = ds.db.Exec(q, n.LastHeartbeatAt, n.CPUPercent, id)
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

func (ds *PostgresDatastore) GetNodeByID(ctx context.Context, id string) (*node.Node, error) {
	nr := nodeRecord{}
	if err := ds.db.Get(&nr, `SELECT * FROM nodes where id = $1`, id); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNodeNotFound
		}
		return nil, errors.Wrapf(err, "error fetching task from db")
	}
	return nr.toNode(), nil
}

func (ds *PostgresDatastore) GetActiveNodes(ctx context.Context, lastHeartbeatAfter time.Time) ([]*node.Node, error) {
	nrs := []nodeRecord{}
	q := `SELECT * 
	      FROM nodes 
		  where last_heartbeat_at > $1 
		  ORDER BY last_heartbeat_at DESC`
	if err := ds.db.Select(&nrs, q, lastHeartbeatAfter); err != nil {
		return nil, errors.Wrapf(err, "error getting active nodes from db")
	}
	ns := make([]*node.Node, len(nrs))
	for i, n := range nrs {
		ns[i] = n.toNode()
	}
	return ns, nil
}
