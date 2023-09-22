package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/mount"
)

type PostgresDatastore struct {
	db *sqlx.DB
	tx *sqlx.Tx
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
	Registry    []byte         `db:"registry"`
	Env         []byte         `db:"env"`
	Files       []byte         `db:"files_"`
	Queue       string         `db:"queue"`
	Error       string         `db:"error_"`
	Pre         []byte         `db:"pre_tasks"`
	Post        []byte         `db:"post_tasks"`
	Mounts      []byte         `db:"mounts"`
	Networks    pq.StringArray `db:"networks"`
	NodeID      string         `db:"node_id"`
	Retry       []byte         `db:"retry"`
	Limits      []byte         `db:"limits"`
	Timeout     string         `db:"timeout"`
	Var         string         `db:"var"`
	Result      string         `db:"result"`
	Parallel    []byte         `db:"parallel"`
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
	Output      string     `db:"output_"`
	Result      string     `db:"result"`
	Error       string     `db:"error_"`
	TS          string     `db:"ts"`
	Defaults    []byte     `db:"defaults"`
}

type nodeRecord struct {
	ID              string    `db:"id"`
	StartedAt       time.Time `db:"started_at"`
	LastHeartbeatAt time.Time `db:"last_heartbeat_at"`
	CPUPercent      float64   `db:"cpu_percent"`
	Queue           string    `db:"queue"`
	Status          string    `db:"status"`
	Hostname        string    `db:"hostname"`
	TaskCount       int       `db:"task_count"`
	Version         string    `db:"version_"`
}

func (r taskRecord) toTask() (*tork.Task, error) {
	var env map[string]string
	if r.Env != nil {
		if err := json.Unmarshal(r.Env, &env); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.env")
		}
	}
	var files map[string]string
	if r.Files != nil {
		if err := json.Unmarshal(r.Files, &files); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.files")
		}
	}
	var pre []*tork.Task
	if r.Pre != nil {
		if err := json.Unmarshal(r.Pre, &pre); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.pre")
		}
	}
	var post []*tork.Task
	if r.Post != nil {
		if err := json.Unmarshal(r.Post, &post); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.post")
		}
	}
	var retry *tork.TaskRetry
	if r.Retry != nil {
		retry = &tork.TaskRetry{}
		if err := json.Unmarshal(r.Retry, retry); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.retry")
		}
	}
	var limits *tork.TaskLimits
	if r.Limits != nil {
		limits = &tork.TaskLimits{}
		if err := json.Unmarshal(r.Limits, limits); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.limits")
		}
	}
	var parallel *tork.ParallelTask
	if r.Parallel != nil {
		parallel = &tork.ParallelTask{}
		if err := json.Unmarshal(r.Parallel, parallel); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.parallel")
		}
	}
	var each *tork.EachTask
	if r.Each != nil {
		each = &tork.EachTask{}
		if err := json.Unmarshal(r.Each, each); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.each")
		}
	}
	var subjob *tork.SubJobTask
	if r.SubJob != nil {
		subjob = &tork.SubJobTask{}
		if err := json.Unmarshal(r.SubJob, subjob); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.subjob")
		}
	}
	var registry *tork.Registry
	if r.Registry != nil {
		registry = &tork.Registry{}
		if err := json.Unmarshal(r.Registry, registry); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.registry")
		}
	}
	var mounts []mount.Mount
	if r.Mounts != nil {
		if err := json.Unmarshal(r.Mounts, &mounts); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.registry")
		}
	}
	return &tork.Task{
		ID:          r.ID,
		JobID:       r.JobID,
		Position:    r.Position,
		Name:        r.Name,
		State:       tork.TaskState(r.State),
		CreatedAt:   &r.CreatedAt,
		ScheduledAt: r.ScheduledAt,
		StartedAt:   r.StartedAt,
		CompletedAt: r.CompletedAt,
		FailedAt:    r.FailedAt,
		CMD:         r.CMD,
		Entrypoint:  r.Entrypoint,
		Run:         r.Run,
		Image:       r.Image,
		Registry:    registry,
		Env:         env,
		Files:       files,
		Queue:       r.Queue,
		Error:       r.Error,
		Pre:         pre,
		Post:        post,
		Mounts:      mounts,
		Networks:    r.Networks,
		NodeID:      r.NodeID,
		Retry:       retry,
		Limits:      limits,
		Timeout:     r.Timeout,
		Var:         r.Var,
		Result:      r.Result,
		Parallel:    parallel,
		ParentID:    r.ParentID,
		Each:        each,
		Description: r.Description,
		SubJob:      subjob,
	}, nil
}

func (r nodeRecord) toNode() *tork.Node {
	n := tork.Node{
		ID:              r.ID,
		StartedAt:       r.StartedAt,
		CPUPercent:      r.CPUPercent,
		LastHeartbeatAt: r.LastHeartbeatAt,
		Queue:           r.Queue,
		Status:          tork.NodeStatus(r.Status),
		Hostname:        r.Hostname,
		TaskCount:       r.TaskCount,
		Version:         r.Version,
	}
	// if we hadn't seen an heartbeat for two or more
	// consecutive periods we consider the node as offline
	if n.LastHeartbeatAt.Before(time.Now().UTC().Add(-tork.HEARTBEAT_RATE * 2)) {
		n.Status = tork.NodeStatusOffline
	}
	return &n
}

func (r jobRecord) toJob(tasks, execution []*tork.Task) (*tork.Job, error) {
	var c tork.JobContext
	if err := json.Unmarshal(r.Context, &c); err != nil {
		return nil, errors.Wrapf(err, "error deserializing job.context")
	}
	var inputs map[string]string
	if err := json.Unmarshal(r.Inputs, &inputs); err != nil {
		return nil, errors.Wrapf(err, "error deserializing job.inputs")
	}
	var defaults *tork.JobDefaults
	if r.Defaults != nil {
		defaults = &tork.JobDefaults{}
		if err := json.Unmarshal(r.Defaults, defaults); err != nil {
			return nil, errors.Wrapf(err, "error deserializing job.defaults")
		}
	}
	return &tork.Job{
		ID:          r.ID,
		Name:        r.Name,
		State:       tork.JobState(r.State),
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
		Output:      r.Output,
		Result:      r.Result,
		Error:       r.Error,
		Defaults:    defaults,
	}, nil
}

func NewPostgresDataStore(dsn string) (*PostgresDatastore, error) {
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to connect to postgres")
	}
	return &PostgresDatastore{
		db: db,
	}, nil
}

func (ds *PostgresDatastore) ExecScript(script string) error {
	_, err := ds.exec(string(script))
	return err
}

func (ds *PostgresDatastore) CreateTask(ctx context.Context, t *tork.Task) error {
	var env *string
	if t.Env != nil {
		b, err := json.Marshal(t.Env)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize task.env")
		}
		s := string(b)
		env = &s
	}
	var files *string
	if t.Files != nil {
		b, err := json.Marshal(t.Files)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize task.files")
		}
		s := string(b)
		files = &s
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
	var registry *string
	if t.Registry != nil {
		b, err := json.Marshal(t.Registry)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize task.registry")
		}
		s := string(b)
		registry = &s
	}
	var mounts *string
	if len(t.Mounts) > 0 {
		b, err := json.Marshal(t.Mounts)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize task.mounts")
		}
		s := string(b)
		mounts = &s
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
			error_, -- $17
			pre_tasks, -- $18
			post_tasks, -- $19
			mounts, -- $20
			node_id, -- $21
			retry, -- $22
			limits, -- $23
			timeout, -- $24
			var, -- $25
			result, -- $26
			parallel, -- $27
			parent_id, -- $28
			each_, -- $29
			description, -- $30
			subjob, -- $31
			networks, -- $32
			files_, -- $33
			registry -- $34
		  ) 
	      values (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
		    $15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,
			$27,$28,$29,$30,$31,$32,$33,$34)`
	_, err = ds.exec(q,
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
		sanitizeString(t.Error),      // $17
		pre,                          // $18
		post,                         // $19
		mounts,                       // $20
		t.NodeID,                     // $21
		retry,                        // $22
		limits,                       // $23
		t.Timeout,                    // $24
		t.Var,                        // $25
		sanitizeString(t.Result),     // $26
		parallel,                     // $27
		t.ParentID,                   // $28
		each,                         // $29
		t.Description,                // $30
		subjob,                       // $31
		pq.StringArray(t.Networks),   // $32
		files,                        // $33
		registry,                     // $34
	)
	if err != nil {
		return errors.Wrapf(err, "error inserting task to the db")
	}
	return nil
}

func sanitizeString(s string) string {
	return strings.ReplaceAll(s, "\u0000", "")
}

func (ds *PostgresDatastore) GetTaskByID(ctx context.Context, id string) (*tork.Task, error) {
	r := taskRecord{}
	if err := ds.get(&r, `SELECT * FROM tasks where id = $1`, id); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNodeNotFound
		}
		return nil, errors.Wrapf(err, "error fetching task from db")
	}
	return r.toTask()
}

func (ds *PostgresDatastore) UpdateTask(ctx context.Context, id string, modify func(t *tork.Task) error) error {
	return ds.WithTx(ctx, func(tx Datastore) error {
		ptx, ok := tx.(*PostgresDatastore)
		if !ok {
			return errors.New("unable to cast to a postgres datastore")
		}
		tr := taskRecord{}
		if err := ptx.get(&tr, `SELECT * FROM tasks where id = $1 for update`, id); err != nil {
			return errors.Wrapf(err, "error fetching task %s from db", id)
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
		var parallel *string
		if t.Parallel != nil {
			b, err := json.Marshal(t.Parallel)
			if err != nil {
				return errors.Wrapf(err, "failed to serialize task.parallel")
			}
			s := string(b)
			parallel = &s
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
		var limits *string
		if t.Limits != nil {
			b, err := json.Marshal(t.Limits)
			if err != nil {
				return errors.Wrapf(err, "failed to serialize task.limits")
			}
			s := string(b)
			limits = &s
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
		q := `update tasks set 
				position = $1,
				state = $2,
				scheduled_at = $3,
				started_at = $4,
				completed_at = $5,
				failed_at = $6,
				error_ = $7,
				node_id = $8,
				result = $9,
				each_ = $10,
				subjob = $11,
				parallel = $12,
				limits = $13,
				timeout = $14,
				retry = $15
			  where id = $16`
		_, err = ptx.exec(q,
			t.Position,               // $1
			t.State,                  // $2
			t.ScheduledAt,            // $3
			t.StartedAt,              // $4
			t.CompletedAt,            // $5
			t.FailedAt,               // $6
			sanitizeString(t.Error),  // $7
			t.NodeID,                 // $8
			sanitizeString(t.Result), // $9
			each,                     // $10
			subjob,                   // $11
			parallel,                 // $12
			limits,                   // $13
			t.Timeout,                // $14
			retry,                    // $15
			t.ID,                     // $16
		)
		if err != nil {
			return errors.Wrapf(err, "error updating task %s", t.ID)
		}
		return nil
	})
}

func (ds *PostgresDatastore) CreateNode(ctx context.Context, n *tork.Node) error {
	q := `insert into nodes 
	       (id,started_at,last_heartbeat_at,cpu_percent,queue,status,hostname,task_count,version_) 
	      values
	       ($1,$2,$3,$4,$5,$6,$7,$8,$9)`
	_, err := ds.exec(q, n.ID, n.StartedAt, n.LastHeartbeatAt, n.CPUPercent, n.Queue, n.Status, n.Hostname, n.TaskCount, n.Version)
	if err != nil {
		return errors.Wrapf(err, "error inserting node to the db")
	}
	return nil
}

func (ds *PostgresDatastore) UpdateNode(ctx context.Context, id string, modify func(u *tork.Node) error) error {
	return ds.WithTx(ctx, func(tx Datastore) error {
		ptx, ok := tx.(*PostgresDatastore)
		if !ok {
			return errors.New("unable to cast to a postgres datastore")
		}
		nr := nodeRecord{}
		if err := ptx.get(&nr, `SELECT * FROM nodes where id = $1 for update`, id); err != nil {
			return errors.Wrapf(err, "error fetching node from db")
		}
		n := nr.toNode()
		if err := modify(n); err != nil {
			return err
		}
		q := `update nodes set 
	        last_heartbeat_at = $1,
			cpu_percent = $2,
			status = $3,
			task_count = $4
		  where id = $5`
		_, err := ptx.exec(q, n.LastHeartbeatAt, n.CPUPercent, n.Status, n.TaskCount, id)
		if err != nil {
			return errors.Wrapf(err, "error update node in db")
		}
		return nil
	})
}

func (ds *PostgresDatastore) GetNodeByID(ctx context.Context, id string) (*tork.Node, error) {
	nr := nodeRecord{}
	if err := ds.get(&nr, `SELECT * FROM nodes where id = $1`, id); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNodeNotFound
		}
		return nil, errors.Wrapf(err, "error fetching task from db")
	}
	return nr.toNode(), nil
}

func (ds *PostgresDatastore) GetActiveNodes(ctx context.Context) ([]*tork.Node, error) {
	nrs := []nodeRecord{}
	q := `SELECT * 
	      FROM nodes 
		  where last_heartbeat_at > $1 
		  ORDER BY last_heartbeat_at DESC`
	timeout := time.Now().UTC().Add(-tork.LAST_HEARTBEAT_TIMEOUT)
	if err := ds.select_(&nrs, q, timeout); err != nil {
		return nil, errors.Wrapf(err, "error getting active nodes from db")
	}
	ns := make([]*tork.Node, len(nrs))
	for i, n := range nrs {

		ns[i] = n.toNode()
	}
	return ns, nil
}

func (ds *PostgresDatastore) CreateJob(ctx context.Context, j *tork.Job) error {
	if j.ID == "" {
		return errors.Errorf("job id must not be empty")
	}
	tasks, err := json.Marshal(j.Tasks)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize job.tasks")
	}
	c, err := json.Marshal(j.Context)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize tork.Context")
	}
	inputs, err := json.Marshal(j.Inputs)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize job.inputs")
	}
	var defaults *string
	if j.Defaults != nil {
		b, err := json.Marshal(j.Defaults)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize job.defaults")
		}
		s := string(b)
		defaults = &s
	}
	q := `insert into jobs 
	       (id,name,description,state,created_at,started_at,tasks,position,
			inputs,context,parent_id,task_count,output_,result,error_,defaults) 
	      values
	       ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`
	_, err = ds.exec(q, j.ID, j.Name, j.Description, j.State, j.CreatedAt, j.StartedAt, tasks, j.Position,
		inputs, c, j.ParentID, j.TaskCount, j.Output, j.Result, j.Error, defaults)
	if err != nil {
		return errors.Wrapf(err, "error inserting job to the db")
	}
	return nil
}
func (ds *PostgresDatastore) UpdateJob(ctx context.Context, id string, modify func(u *tork.Job) error) error {
	return ds.WithTx(ctx, func(tx Datastore) error {
		ptx, ok := tx.(*PostgresDatastore)
		if !ok {
			return errors.New("unable to cast to a postgres datastore")
		}
		r := jobRecord{}
		if err := ptx.get(&r, `SELECT * FROM jobs where id = $1 for update`, id); err != nil {
			return errors.Wrapf(err, "error fetching job from db")
		}
		tasks := make([]*tork.Task, 0)
		if err := json.Unmarshal(r.Tasks, &tasks); err != nil {
			return errors.Wrapf(err, "error desiralizing job.tasks")
		}
		j, err := r.toJob(tasks, []*tork.Task{})
		if err != nil {
			return errors.Wrapf(err, "failed to convert jobRecord")
		}
		if err := modify(j); err != nil {
			return err
		}
		c, err := json.Marshal(j.Context)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize tork.Context")
		}
		q := `update jobs set 
				state = $1,
				started_at = $2,
				completed_at = $3,
				failed_at = $4,
				position = $5,
				context = $6,
				result = $7,
				error_ = $8
			  where id = $9`
		_, err = ptx.exec(q, j.State, j.StartedAt, j.CompletedAt, j.FailedAt, j.Position, c, j.Result, j.Error, j.ID)
		return err
	})
}

func (ds *PostgresDatastore) GetJobByID(ctx context.Context, id string) (*tork.Job, error) {
	r := jobRecord{}
	if err := ds.get(&r, `SELECT * FROM jobs where id = $1`, id); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrJobNotFound
		}
		return nil, errors.Wrapf(err, "error fetching job from db")
	}
	tasks := make([]*tork.Task, 0)
	if err := json.Unmarshal(r.Tasks, &tasks); err != nil {
		return nil, errors.Wrapf(err, "error desiralizing job.tasks")
	}
	rs := make([]taskRecord, 0)
	q := `SELECT * 
	      FROM tasks 
		  where job_id = $1 
		  ORDER BY position asc,created_at asc`
	if err := ds.select_(&rs, q, id); err != nil {
		return nil, errors.Wrapf(err, "error getting job execution from db")
	}
	exec := make([]*tork.Task, len(rs))
	for i, r := range rs {
		t, err := r.toTask()
		if err != nil {
			return nil, err
		}
		exec[i] = t
	}

	return r.toJob(tasks, exec)
}

func (ds *PostgresDatastore) GetActiveTasks(ctx context.Context, jobID string) ([]*tork.Task, error) {
	rs := make([]taskRecord, 0)
	q := `SELECT * 
	      FROM tasks 
		  where job_id = $1 
		  AND 
		    (state = $2 OR state = $3 OR state = $4)
		  ORDER BY position,created_at ASC`
	if err := ds.select_(&rs, q, jobID, tork.TaskStatePending, tork.TaskStateScheduled, tork.TaskStateRunning); err != nil {
		return nil, errors.Wrapf(err, "error getting job execution from db")
	}
	actives := make([]*tork.Task, len(rs))
	for i, r := range rs {
		t, err := r.toTask()
		if err != nil {
			return nil, err
		}
		actives[i] = t
	}

	return actives, nil
}

func (ds *PostgresDatastore) GetJobs(ctx context.Context, q string, page, size int) (*Page[*tork.JobSummary], error) {
	offset := (page - 1) * size
	rs := make([]jobRecord, 0)
	qry := fmt.Sprintf(`
	  SELECT * 
	  FROM jobs 
	  where 
	    case when $1 != '' then ts @@ plainto_tsquery('english', $1) else true end
	  ORDER BY created_at DESC 
	  OFFSET %d LIMIT %d`, offset, size)
	if err := ds.select_(&rs, qry, q); err != nil {
		return nil, errors.Wrapf(err, "error getting a page of jobs")
	}
	result := make([]*tork.JobSummary, len(rs))
	for i, r := range rs {
		j, err := r.toJob([]*tork.Task{}, []*tork.Task{})
		if err != nil {
			return nil, err
		}
		result[i] = tork.NewJobSummary(j)
	}

	var count *int
	if err := ds.get(&count, `
	  select count(*) 
	  from jobs
	  where case when $1 != '' 
	    then ts @@ plainto_tsquery('english', $1) 
		else true 
	  end`, q); err != nil {
		return nil, errors.Wrapf(err, "error getting the jobs count")
	}

	totalPages := *count / size
	if *count%size != 0 {
		totalPages = totalPages + 1
	}

	return &Page[*tork.JobSummary]{
		Items:      result,
		Number:     page,
		Size:       len(result),
		TotalPages: totalPages,
		TotalItems: *count,
	}, nil
}

func (ds *PostgresDatastore) GetMetrics(ctx context.Context) (*tork.Metrics, error) {
	s := &tork.Metrics{}

	if err := ds.get(&s.Jobs.Running, "select count(*) from jobs where state = 'RUNNING'"); err != nil {
		return nil, errors.Wrapf(err, "error getting the running jobs count")
	}

	if err := ds.get(&s.Tasks.Running, "select count(*) from tasks where state = 'RUNNING'"); err != nil {
		return nil, errors.Wrapf(err, "error getting the running tasks count")
	}

	if err := ds.get(&s.Nodes.Running, "select count(*) from nodes where last_heartbeat_at > current_timestamp - interval '5 minutes'"); err != nil {
		return nil, errors.Wrapf(err, "error getting the running tasks count")
	}

	if err := ds.get(&s.Nodes.CPUPercent, "select coalesce(avg(cpu_percent),0) from nodes where last_heartbeat_at > current_timestamp - interval '5 minutes'"); err != nil {
		return nil, errors.Wrapf(err, "error getting the running tasks count")
	}

	return s, nil
}

func (ds *PostgresDatastore) get(dest interface{}, query string, args ...interface{}) error {
	if ds.tx != nil {
		return ds.tx.Get(dest, query, args...)
	} else {
		return ds.db.Get(dest, query, args...)
	}
}

func (ds *PostgresDatastore) select_(dest interface{}, query string, args ...interface{}) error {
	if ds.tx != nil {
		return ds.tx.Select(dest, query, args...)
	} else {
		return ds.db.Select(dest, query, args...)
	}
}

func (ds *PostgresDatastore) exec(query string, args ...any) (sql.Result, error) {
	if ds.tx != nil {
		return ds.tx.Exec(query, args...)
	} else {
		return ds.db.Exec(query, args...)
	}
}

func (ds *PostgresDatastore) WithTx(ctx context.Context, f func(tx Datastore) error) error {
	var tx *sqlx.Tx
	var err error
	var owner bool
	if ds.tx != nil {
		tx = ds.tx
	} else {
		owner = true
		tx, err = ds.db.BeginTxx(ctx, &sql.TxOptions{})
		if err != nil {
			return errors.Wrapf(err, "unable to begin tx")
		}
	}
	dsx := &PostgresDatastore{
		tx: tx,
	}
	if err := f(dsx); err != nil {
		if owner {
			if err := tx.Rollback(); err != nil {
				log.Error().
					Err(err).
					Msgf("error rolling back tx")
			}
		}
		return err
	}
	if owner {
		if err := tx.Commit(); err != nil {
			return errors.Wrapf(err, "error committing transaction")
		}
	}
	return nil
}

func (ds *PostgresDatastore) HealthCheck(ctx context.Context) error {
	if _, err := ds.db.ExecContext(ctx, "select 1 from jobs limit 1"); err != nil {
		return errors.Wrapf(err, "error reading from jobs table")
	}
	if _, err := ds.db.ExecContext(ctx, "select 1 from tasks limit 1"); err != nil {
		return errors.Wrapf(err, "error reading from tasks table")
	}
	if _, err := ds.db.ExecContext(ctx, "select 1 from nodes limit 1"); err != nil {
		return errors.Wrapf(err, "error reading from nodes table")
	}
	return nil
}
