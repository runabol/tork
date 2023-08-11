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
	"github.com/tork/job"
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
	JobID       string     `db:"job_id"`
	Position    int        `db:"position"`
}

type jobRecord struct {
	ID          string     `db:"id"`
	Name        string     `db:"name"`
	State       string     `db:"state"`
	CreatedAt   time.Time  `db:"created_at"`
	StartedAt   *time.Time `db:"started_at"`
	CompletedAt *time.Time `db:"completed_at"`
	FailedAt    *time.Time `db:"failed_at"`
	Tasks       []byte     `db:"tasks"`
	Position    int        `db:"position"`
}

type nodeRecord struct {
	ID              string    `db:"id"`
	StartedAt       time.Time `db:"started_at"`
	LastHeartbeatAt time.Time `db:"last_heartbeat_at"`
	CPUPercent      float64   `db:"cpu_percent"`
	Queue           string    `db:"queue"`
}

func (r nodeRecord) toNode() node.Node {
	return node.Node{
		ID:              r.ID,
		StartedAt:       r.StartedAt,
		CPUPercent:      r.CPUPercent,
		LastHeartbeatAt: r.LastHeartbeatAt,
		Queue:           r.Queue,
	}
}

func (r jobRecord) toJob(tasks, execution []task.Task) job.Job {
	return job.Job{
		ID:          r.ID,
		Name:        r.Name,
		State:       job.State(r.State),
		CreatedAt:   r.CreatedAt,
		StartedAt:   r.StartedAt,
		CompletedAt: r.CompletedAt,
		FailedAt:    r.FailedAt,
		Tasks:       tasks,
		Execution:   execution,
		Position:    r.Position,
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

func (ds *PostgresDatastore) CreateTask(ctx context.Context, t task.Task) error {
	bytez, err := json.Marshal(t)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize task")
	}
	q := `insert into tasks 
	       (id,created_at,state,serialized,job_id,position) 
	      values
	       ($1,$2,$3,$4,$5,$6)`
	_, err = ds.db.Exec(q, t.ID, t.CreatedAt, t.State, (bytez), t.JobID, t.Position)
	if err != nil {
		return errors.Wrapf(err, "error inserting task to the db")
	}
	return nil
}

func (ds *PostgresDatastore) GetTaskByID(ctx context.Context, id string) (task.Task, error) {
	tr := taskRecord{}
	if err := ds.db.Get(&tr, `SELECT * FROM tasks where id = $1`, id); err != nil {
		if err == sql.ErrNoRows {
			return task.Task{}, ErrNodeNotFound
		}
		return task.Task{}, errors.Wrapf(err, "error fetching task from db")
	}
	t := task.Task{}
	if err := json.Unmarshal(tr.Serialized, &t); err != nil {
		return task.Task{}, errors.Wrapf(err, "error desiralizing task")
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

func (ds *PostgresDatastore) CreateNode(ctx context.Context, n node.Node) error {
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
	if err := modify(&n); err != nil {
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

func (ds *PostgresDatastore) GetNodeByID(ctx context.Context, id string) (node.Node, error) {
	nr := nodeRecord{}
	if err := ds.db.Get(&nr, `SELECT * FROM nodes where id = $1`, id); err != nil {
		if err == sql.ErrNoRows {
			return node.Node{}, ErrNodeNotFound
		}
		return node.Node{}, errors.Wrapf(err, "error fetching task from db")
	}
	return nr.toNode(), nil
}

func (ds *PostgresDatastore) GetActiveNodes(ctx context.Context, lastHeartbeatAfter time.Time) ([]node.Node, error) {
	nrs := []nodeRecord{}
	q := `SELECT * 
	      FROM nodes 
		  where last_heartbeat_at > $1 
		  ORDER BY last_heartbeat_at DESC`
	if err := ds.db.Select(&nrs, q, lastHeartbeatAfter); err != nil {
		return nil, errors.Wrapf(err, "error getting active nodes from db")
	}
	ns := make([]node.Node, len(nrs))
	for i, n := range nrs {
		ns[i] = n.toNode()
	}
	return ns, nil
}

func (ds *PostgresDatastore) CreateJob(ctx context.Context, j job.Job) error {
	if j.ID == "" {
		return errors.Errorf("job id must not be empty")
	}
	tasks, err := json.Marshal(j.Tasks)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize job.tasks")
	}
	q := `insert into jobs 
	       (id,name,state,created_at,started_at,tasks,position) 
	      values
	       ($1,$2,$3,$4,$5,$6,$7)`
	_, err = ds.db.Exec(q, j.ID, j.Name, j.State, j.CreatedAt, j.StartedAt, tasks, j.Position)
	if err != nil {
		return errors.Wrapf(err, "error inserting job to the db")
	}
	return nil
}
func (ds *PostgresDatastore) UpdateJob(ctx context.Context, id string, modify func(u *job.Job) error) error {
	tx, err := ds.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to begin tx")
	}
	r := jobRecord{}
	if err := ds.db.Get(&r, `SELECT * FROM jobs where id = $1 for update`, id); err != nil {
		return errors.Wrapf(err, "error fetching job from db")
	}
	tasks := make([]task.Task, 0)
	if err := json.Unmarshal(r.Tasks, &tasks); err != nil {
		return errors.Wrapf(err, "error desiralizing job.tasks")
	}
	j := r.toJob(tasks, []task.Task{})
	if err := modify(&j); err != nil {
		return err
	}
	q := `update jobs set 
	        state = $1,
			started_at = $2,
			completed_at = $3,
			failed_at = $4,
			position = $5
		  where id = $6`
	_, err = ds.db.Exec(q, j.State, j.StartedAt, j.CompletedAt, j.FailedAt, j.Position, j.ID)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return errors.Wrapf(err, "error rolling-back tx")
		}
		return errors.Wrapf(err, "error updating job in db")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrapf(err, "error commiting tx")
	}
	return nil
}

func (ds *PostgresDatastore) GetJobByID(ctx context.Context, id string) (job.Job, error) {
	r := jobRecord{}
	if err := ds.db.Get(&r, `SELECT * FROM jobs where id = $1`, id); err != nil {
		if err == sql.ErrNoRows {
			return job.Job{}, ErrJobNotFound
		}
		return job.Job{}, errors.Wrapf(err, "error fetching job from db")
	}
	tasks := make([]task.Task, 0)
	if err := json.Unmarshal(r.Tasks, &tasks); err != nil {
		return job.Job{}, errors.Wrapf(err, "error desiralizing job.tasks")
	}

	rs := make([]taskRecord, 0)
	q := `SELECT * 
	      FROM tasks 
		  where job_id = $1 
		  ORDER BY position,created_at ASC`
	if err := ds.db.Select(&rs, q, id); err != nil {
		return job.Job{}, errors.Wrapf(err, "error getting job execution from db")
	}
	exec := make([]task.Task, len(rs))
	for i, r := range rs {
		t := task.Task{}
		if err := json.Unmarshal(r.Serialized, &t); err != nil {
			return job.Job{}, errors.Wrapf(err, "error desiralizing job execution task")
		}
		exec[i] = t
	}

	return r.toJob(tasks, exec), nil
}
