package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/runabol/tork/job"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/task"
)

type PostgresDatastore struct {
	db *sqlx.DB
}

type taskRecord struct {
	ID          string         `db:"id"`
	JobID       string         `db:"job_id"`
	Position    int            `db:"position"`
	Name        string         `db:"name"`
	Description string         `db:"description"`
	State       string         `db:"state"`
	CreatedAt   time.Time      `db:"created_at"`
	ScheduledAt *time.Time     `db:"scheduled_at"`
	StartedAt   *time.Time     `db:"started_at"`
	CompletedAt *time.Time     `db:"completed_at"`
	FailedAt    *time.Time     `db:"failed_at"`
	CMD         pq.StringArray `db:"cmd"`
	Entrypoint  pq.StringArray `db:"entrypoint"`
	Run         string         `db:"run_script"`
	Image       string         `db:"image"`
	Env         []byte         `db:"env"`
	Queue       string         `db:"queue"`
	Error       string         `db:"error_msg"`
	Pre         []byte         `db:"pre_tasks"`
	Post        []byte         `db:"post_tasks"`
	Volumes     pq.StringArray `db:"volumes"`
	NodeID      string         `db:"node_id"`
	Retry       []byte         `db:"retry"`
	Limits      []byte         `db:"limits"`
	Timeout     string         `db:"timeout"`
	Var         string         `db:"var"`
	Result      string         `db:"result"`
	Parallel    []byte         `db:"parallel"`
	Completions int            `db:"completions"`
	ParentID    string         `db:"parent_id"`
	Each        []byte         `db:"each_"`
	SubJob      []byte         `db:"subjob"`
	SubJobID    string         `db:"subjob_id"`
}

type jobRecord struct {
	ID          string     `db:"id"`
	Name        string     `db:"name"`
	Description string     `db:"description"`
	State       string     `db:"state"`
	CreatedAt   time.Time  `db:"created_at"`
	StartedAt   *time.Time `db:"started_at"`
	CompletedAt *time.Time `db:"completed_at"`
	FailedAt    *time.Time `db:"failed_at"`
	Tasks       []byte     `db:"tasks"`
	Position    int        `db:"position"`
	Inputs      []byte     `db:"inputs"`
	Context     []byte     `db:"context"`
	ParentID    string     `db:"parent_id"`
	TaskCount   int        `db:"task_count"`
}

type nodeRecord struct {
	ID              string    `db:"id"`
	StartedAt       time.Time `db:"started_at"`
	LastHeartbeatAt time.Time `db:"last_heartbeat_at"`
	CPUPercent      float64   `db:"cpu_percent"`
	Queue           string    `db:"queue"`
}

func (r taskRecord) toTask() (*task.Task, error) {
	var env map[string]string
	if r.Env != nil {
		if err := json.Unmarshal(r.Env, &env); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.env")
		}
	}
	var pre []*task.Task
	if r.Pre != nil {
		if err := json.Unmarshal(r.Pre, &pre); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.pre")
		}
	}
	var post []*task.Task
	if r.Post != nil {
		if err := json.Unmarshal(r.Post, &post); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.post")
		}
	}
	var retry *task.Retry
	if r.Retry != nil {
		retry = &task.Retry{}
		if err := json.Unmarshal(r.Retry, retry); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.retry")
		}
	}
	var limits *task.Limits
	if r.Limits != nil {
		limits = &task.Limits{}
		if err := json.Unmarshal(r.Limits, limits); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.limits")
		}
	}
	var parallel []*task.Task
	if r.Parallel != nil {
		if err := json.Unmarshal(r.Parallel, &parallel); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.parallel")
		}
	}
	var each *task.Each
	if r.Each != nil {
		each = &task.Each{}
		if err := json.Unmarshal(r.Each, each); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.each")
		}
	}
	var subjob *task.SubJob
	if r.SubJob != nil {
		subjob = &task.SubJob{}
		if err := json.Unmarshal(r.SubJob, subjob); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.subjob")
		}
	}
	return &task.Task{
		ID:          r.ID,
		JobID:       r.JobID,
		Position:    r.Position,
		Name:        r.Name,
		State:       task.State(r.State),
		CreatedAt:   &r.CreatedAt,
		ScheduledAt: r.ScheduledAt,
		StartedAt:   r.StartedAt,
		CompletedAt: r.CompletedAt,
		FailedAt:    r.FailedAt,
		CMD:         r.CMD,
		Entrypoint:  r.Entrypoint,
		Run:         r.Run,
		Image:       r.Image,
		Env:         env,
		Queue:       r.Queue,
		Error:       r.Error,
		Pre:         pre,
		Post:        post,
		Volumes:     r.Volumes,
		NodeID:      r.NodeID,
		Retry:       retry,
		Limits:      limits,
		Timeout:     r.Timeout,
		Var:         r.Var,
		Result:      r.Result,
		Parallel:    parallel,
		Completions: r.Completions,
		ParentID:    r.ParentID,
		Each:        each,
		Description: r.Description,
		SubJob:      subjob,
		SubJobID:    r.SubJobID,
	}, nil
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

func (r jobRecord) toJob(tasks, execution []*task.Task) (*job.Job, error) {
	var c job.Context
	if err := json.Unmarshal(r.Context, &c); err != nil {
		return nil, errors.Wrapf(err, "error deserializing task.context")
	}
	var inputs map[string]string
	if err := json.Unmarshal(r.Inputs, &inputs); err != nil {
		return nil, errors.Wrapf(err, "error deserializing task.inputs")
	}
	return &job.Job{
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
		Context:     c,
		Inputs:      inputs,
		Description: r.Description,
		ParentID:    r.ParentID,
		TaskCount:   r.TaskCount,
	}, nil
}

func NewPostgresDataStore(dsn string) (*PostgresDatastore, error) {
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to connect to postgres")
	}
	return &PostgresDatastore{db: db}, nil
}

func (ds *PostgresDatastore) ExecScript(script string) error {
	schema, err := os.ReadFile(script)
	if err != nil {
		return errors.Wrapf(err, "erroring reading postgres schema file")
	}
	_, err = ds.db.Exec(string(schema))
	return err
}

func (ds *PostgresDatastore) CreateTask(ctx context.Context, t *task.Task) error {
	var env *string
	if t.Env != nil {
		b, err := json.Marshal(t.Env)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize task.env")
		}
		s := string(b)
		env = &s
	}
	pre, err := json.Marshal(t.Pre)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize task.pre")
	}
	post, err := json.Marshal(t.Post)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize task.post")
	}
	var retry *string
	if t.Retry != nil {
		b, err := json.Marshal(t.Retry)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize task.retry")
		}
		s := string(b)
		retry = &s
	}
	var limits *string
	if t.Limits != nil {
		b, err := json.Marshal(t.Limits)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize task.limits")
		}
		s := string(b)
		limits = &s
	}
	parallel, err := json.Marshal(t.Parallel)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize task.parallel")
	}
	var each *string
	if t.Each != nil {
		b, err := json.Marshal(t.Each)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize task.each")
		}
		s := string(b)
		each = &s
	}
	var subjob *string
	if t.SubJob != nil {
		b, err := json.Marshal(t.SubJob)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize task.subjob")
		}
		s := string(b)
		subjob = &s
	}
	q := `insert into tasks (
		    id, -- $1
			job_id, -- $2
			position, -- $3
			name, -- $4
			state, -- $5
			created_at, -- $6
			scheduled_at, -- $7
			started_at, -- $8
			completed_at, -- $9
			failed_at, -- $10
			cmd, -- $11
			entrypoint, -- $12
			run_script, -- $13
			image, -- $14
			env, -- $15
			queue, -- $16
			error_msg, -- $17
			pre_tasks, -- $18
			post_tasks, -- $19
			volumes, -- $20
			node_id, -- $21
			retry, -- $22
			limits, -- $23
			timeout, -- $24
			var, -- $25
			result, -- $26
			parallel, -- $27
			completions, -- $28
			parent_id, -- $29
			each_, -- $30
			description, -- $31
			subjob, -- $32
			subjob_id -- $33
		  ) 
	      values (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
		    $15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,
			$27,$28,$29,$30,$31,$32,$33)`
	_, err = ds.db.Exec(q,
		t.ID,                         // $1
		t.JobID,                      // $2
		t.Position,                   // $3
		t.Name,                       // $4
		t.State,                      // $5
		t.CreatedAt,                  // $6
		t.ScheduledAt,                // $7
		t.StartedAt,                  // $8
		t.CompletedAt,                // $9
		t.FailedAt,                   // $10
		pq.StringArray(t.CMD),        // $11
		pq.StringArray(t.Entrypoint), // $12
		t.Run,                        // $13
		t.Image,                      // $14
		env,                          // $15
		t.Queue,                      // $16
		t.Error,                      // $17
		pre,                          // $18
		post,                         // $19
		pq.StringArray(t.Volumes),    // $20
		t.NodeID,                     // $21
		retry,                        // $22
		limits,                       // $23
		t.Timeout,                    // $24
		t.Var,                        // $25
		t.Result,                     // $26
		parallel,                     // $27
		t.Completions,                // $28
		t.ParentID,                   // $29
		each,                         // $30
		t.Description,                // $31
		subjob,                       // $32
		t.SubJobID,                   // $33
	)
	if err != nil {
		return errors.Wrapf(err, "error inserting task to the db")
	}
	return nil
}

func (ds *PostgresDatastore) GetTaskByID(ctx context.Context, id string) (*task.Task, error) {
	r := taskRecord{}
	if err := ds.db.Get(&r, `SELECT * FROM tasks where id = $1`, id); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNodeNotFound
		}
		return nil, errors.Wrapf(err, "error fetching task from db")
	}
	return r.toTask()
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
	t, err := tr.toTask()
	if err != nil {
		return err
	}
	if err := modify(t); err != nil {
		return err
	}
	var each *string
	if t.Each != nil {
		b, err := json.Marshal(t.Each)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize task.each")
		}
		s := string(b)
		each = &s
	}
	q := `update tasks set 
	        position = $1,
	        state = $2,
			scheduled_at = $3,
			started_at = $4,
			completed_at = $5,
			failed_at = $6,
			error_msg = $7,
			node_id = $8,
			result = $9,
			completions = $10,
			each_ = $11,
			subjob_id = $12
		  where id = $13`
	_, err = ds.db.Exec(q,
		t.Position,    // $1
		t.State,       // $2
		t.ScheduledAt, // $3
		t.StartedAt,   // $4
		t.CompletedAt, // $5
		t.FailedAt,    // $6
		t.Error,       // $7
		t.NodeID,      // $8
		t.Result,      // $9
		t.Completions, // $10
		each,          // $11
		t.SubJobID,    // $12
		t.ID,          // $13
	)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return errors.Wrapf(err, "error rolling-back tx")
		}
		return errors.Wrapf(err, "error update task in db")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrapf(err, "error committing tx")
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
		return errors.Wrapf(err, "error committing tx")
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

func (ds *PostgresDatastore) CreateJob(ctx context.Context, j *job.Job) error {
	if j.ID == "" {
		return errors.Errorf("job id must not be empty")
	}
	tasks, err := json.Marshal(j.Tasks)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize job.tasks")
	}
	c, err := json.Marshal(j.Context)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize job.context")
	}
	inputs, err := json.Marshal(j.Inputs)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize job.inputs")
	}
	q := `insert into jobs 
	       (id,name,description,state,created_at,started_at,tasks,position,inputs,context,parent_id,task_count) 
	      values
	       ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`
	_, err = ds.db.Exec(q, j.ID, j.Name, j.Description, j.State, j.CreatedAt, j.StartedAt, tasks, j.Position, inputs, c, j.ParentID, j.TaskCount)
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
	tasks := make([]*task.Task, 0)
	if err := json.Unmarshal(r.Tasks, &tasks); err != nil {
		return errors.Wrapf(err, "error desiralizing job.tasks")
	}
	j, err := r.toJob(tasks, []*task.Task{})
	if err != nil {
		return errors.Wrapf(err, "failed to convert jobRecord")
	}
	if err := modify(j); err != nil {
		return err
	}
	c, err := json.Marshal(j.Context)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize job.context")
	}
	q := `update jobs set 
	        state = $1,
			started_at = $2,
			completed_at = $3,
			failed_at = $4,
			position = $5,
			context = $6
		  where id = $7`
	_, err = ds.db.Exec(q, j.State, j.StartedAt, j.CompletedAt, j.FailedAt, j.Position, c, j.ID)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return errors.Wrapf(err, "error rolling-back tx")
		}
		return errors.Wrapf(err, "error updating job in db")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrapf(err, "error committing tx")
	}
	return nil
}

func (ds *PostgresDatastore) GetJobByID(ctx context.Context, id string) (*job.Job, error) {
	r := jobRecord{}
	if err := ds.db.Get(&r, `SELECT * FROM jobs where id = $1`, id); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrJobNotFound
		}
		return nil, errors.Wrapf(err, "error fetching job from db")
	}
	tasks := make([]*task.Task, 0)
	if err := json.Unmarshal(r.Tasks, &tasks); err != nil {
		return nil, errors.Wrapf(err, "error desiralizing job.tasks")
	}
	rs := make([]taskRecord, 0)
	q := `SELECT * 
	      FROM tasks 
		  where job_id = $1 
		  ORDER BY position,created_at ASC`
	if err := ds.db.Select(&rs, q, id); err != nil {
		return nil, errors.Wrapf(err, "error getting job execution from db")
	}
	exec := make([]*task.Task, len(rs))
	for i, r := range rs {
		t, err := r.toTask()
		if err != nil {
			return nil, err
		}
		exec[i] = t
	}

	return r.toJob(tasks, exec)
}

func (ds *PostgresDatastore) GetActiveTasks(ctx context.Context, jobID string) ([]*task.Task, error) {
	rs := make([]taskRecord, 0)
	q := `SELECT * 
	      FROM tasks 
		  where job_id = $1 
		  AND 
		    (state = $2 OR state = $3 OR state = $4)
		  ORDER BY position,created_at ASC`
	if err := ds.db.Select(&rs, q, jobID, task.Pending, task.Scheduled, task.Running); err != nil {
		return nil, errors.Wrapf(err, "error getting job execution from db")
	}
	actives := make([]*task.Task, len(rs))
	for i, r := range rs {
		t, err := r.toTask()
		if err != nil {
			return nil, err
		}
		actives[i] = t
	}

	return actives, nil
}

func (ds *PostgresDatastore) GetJobs(ctx context.Context, page, size int) (*Page[*job.Job], error) {
	offset := (page - 1) * size
	rs := make([]jobRecord, 0)
	q := fmt.Sprintf(`SELECT * FROM jobs  ORDER BY created_at DESC OFFSET %d LIMIT %d`, offset, size)
	if err := ds.db.Select(&rs, q); err != nil {
		return nil, errors.Wrapf(err, "error getting a page of jobs")
	}
	result := make([]*job.Job, len(rs))
	for i, r := range rs {
		j, err := r.toJob([]*task.Task{}, []*task.Task{})
		if err != nil {
			return nil, err
		}
		result[i] = j
	}

	var count *int
	if err := ds.db.Get(&count, "select count(*) from jobs"); err != nil {
		return nil, errors.Wrapf(err, "error getting the jobs count")
	}

	totalPages := *count / size
	if *count%size != 0 {
		totalPages = totalPages + 1
	}

	return &Page[*job.Job]{
		Items:      result,
		Number:     page,
		Size:       len(result),
		TotalPages: totalPages,
	}, nil
}
