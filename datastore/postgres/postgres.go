package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/uuid"
)

type PostgresDatastore struct {
	db                      *sqlx.DB
	tx                      *sqlx.Tx
	taskLogsRetentionPeriod *time.Duration
	cleanupInterval         *time.Duration
	rand                    *rand.Rand
	disableCleanup          bool
}

var (
	initialCleanupInterval         = minCleanupInterval
	minCleanupInterval             = time.Minute
	maxCleanupInterval             = time.Hour
	DefaultTaskLogsRetentionPeriod = time.Hour * 24 * 7
)

type Option = func(ds *PostgresDatastore)

func WithTaskLogRetentionPeriod(dur time.Duration) Option {
	return func(ds *PostgresDatastore) {
		ds.taskLogsRetentionPeriod = &dur
	}
}

func WithDisableCleanup(val bool) Option {
	return func(ds *PostgresDatastore) {
		ds.disableCleanup = val
	}
}

func NewPostgresDataStore(dsn string, opts ...Option) (*PostgresDatastore, error) {
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to connect to postgres")
	}
	ds := &PostgresDatastore{
		db:   db,
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, opt := range opts {
		opt(ds)
	}
	ds.cleanupInterval = &initialCleanupInterval
	if ds.taskLogsRetentionPeriod == nil {
		ds.taskLogsRetentionPeriod = &DefaultTaskLogsRetentionPeriod
	}
	if *ds.cleanupInterval < time.Minute {
		return nil, errors.Errorf("cleanup interval can not be under 1 minute")
	}
	if *ds.taskLogsRetentionPeriod < time.Minute {
		return nil, errors.Errorf("task logs retention period can not be under 1 minute")
	}
	if !ds.disableCleanup {
		go ds.cleanupProcess()
	}
	return ds, nil
}

func (ds *PostgresDatastore) cleanupProcess() {
	for {
		jitter := time.Second * (time.Duration(ds.rand.Intn(60) + 1))
		time.Sleep(*ds.cleanupInterval + jitter)
		if err := ds.cleanup(); err != nil {
			log.Error().Err(err).Msg("error expunging task logs")
		}
	}
}

func (ds *PostgresDatastore) cleanup() error {
	n, err := ds.expungeExpiredTaskLogPart()
	if err != nil {
		return err
	}
	if n > 0 {
		log.Debug().Msgf("Expunged %d expired task log parts from the DB", n)
		newCleanupInterval := (*ds.cleanupInterval) / 2
		if newCleanupInterval < minCleanupInterval {
			newCleanupInterval = minCleanupInterval
		}
		ds.cleanupInterval = &newCleanupInterval
	} else {
		newCleanupInterval := (*ds.cleanupInterval) * 2
		if newCleanupInterval > maxCleanupInterval {
			newCleanupInterval = maxCleanupInterval
		}
		ds.cleanupInterval = &newCleanupInterval
	}
	return nil
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
	var parallel *string
	if t.Parallel != nil {
		b, err := json.Marshal(t.Parallel)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize task.parallel")
		}
		s := string(b)
		parallel = &s
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
			registry, -- $34
			gpus, -- $35
			if_, -- $36
			tags -- $37
		  ) 
	      values (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
		    $15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,
			$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37)`
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
		t.GPUs,                       // $35
		t.If,                         // $36
		pq.StringArray(t.Tags),       // $37
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
			return nil, datastore.ErrTaskNotFound
		}
		return nil, errors.Wrapf(err, "error fetching task from db")
	}
	return r.toTask()
}

func (ds *PostgresDatastore) UpdateTask(ctx context.Context, id string, modify func(t *tork.Task) error) error {
	return ds.WithTx(ctx, func(tx datastore.Datastore) error {
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
	       (id,name,started_at,last_heartbeat_at,cpu_percent,queue,status,hostname,task_count,version_) 
	      values
	       ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
	_, err := ds.exec(q, n.ID, n.Name, n.StartedAt, n.LastHeartbeatAt, n.CPUPercent, n.Queue, n.Status, n.Hostname, n.TaskCount, n.Version)
	if err != nil {
		return errors.Wrapf(err, "error inserting node to the db")
	}
	return nil
}

func (ds *PostgresDatastore) UpdateNode(ctx context.Context, id string, modify func(u *tork.Node) error) error {
	return ds.WithTx(ctx, func(tx datastore.Datastore) error {
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
			return nil, datastore.ErrNodeNotFound
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
		  ORDER BY name ASC`
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
	webhooks, err := json.Marshal(j.Webhooks)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize job.webhooks")
	}
	q := `insert into jobs 
	       (id,name,description,state,created_at,started_at,tasks,position,
			inputs,context,parent_id,task_count,output_,result,error_,defaults,webhooks) 
	      values
	       ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`
	_, err = ds.exec(q, j.ID, j.Name, j.Description, j.State, j.CreatedAt, j.StartedAt, tasks, j.Position,
		inputs, c, j.ParentID, j.TaskCount, j.Output, j.Result, j.Error, defaults, webhooks)
	if err != nil {
		return errors.Wrapf(err, "error inserting job to the db")
	}
	return nil
}
func (ds *PostgresDatastore) UpdateJob(ctx context.Context, id string, modify func(u *tork.Job) error) error {
	return ds.WithTx(ctx, func(tx datastore.Datastore) error {
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
			return nil, datastore.ErrJobNotFound
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
		  ORDER BY position asc,started_at asc`
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

func (ds *PostgresDatastore) CreateTaskLogPart(ctx context.Context, p *tork.TaskLogPart) error {
	if p.TaskID == "" {
		return errors.Errorf("must provide task id")
	}
	if p.Number < 1 {
		return errors.Errorf("part number must be > 0")
	}
	q := `insert into tasks_log_parts 
	       (id,number_,task_id,created_at,contents) 
	      values
	       ($1,$2,$3,$4,$5)`
	_, err := ds.exec(q, uuid.NewUUID(), p.Number, p.TaskID, time.Now().UTC(), p.Contents)
	if err != nil {
		return errors.Wrapf(err, "error inserting task log part to the db")
	}
	return nil
}

func (ds *PostgresDatastore) expungeExpiredTaskLogPart() (int, error) {
	q := `delete from tasks_log_parts where id in ( 
	        select id 
		    from   tasks_log_parts 
		    where  created_at < $1 
		    limit  1000
	      )`
	res, err := ds.exec(q, time.Now().UTC().Add(-*ds.taskLogsRetentionPeriod))
	if err != nil {
		return 0, errors.Wrapf(err, "error deleting expired task log parts from the db")
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, errors.Wrapf(err, "error getting the number of deleted log parts")
	}
	return int(rows), nil
}

func (ds *PostgresDatastore) GetTaskLogParts(ctx context.Context, taskID string, page, size int) (*datastore.Page[*tork.TaskLogPart], error) {
	offset := (page - 1) * size
	rs := []taskLogPartRecord{}
	q := fmt.Sprintf(`SELECT * 
	      FROM tasks_log_parts 
		  where task_id = $1
		  ORDER BY number_ DESC
		  OFFSET %d LIMIT %d`, offset, size)

	if err := ds.select_(&rs, q, taskID); err != nil {
		return nil, errors.Wrapf(err, "error task log parts from db")
	}
	items := make([]*tork.TaskLogPart, len(rs))
	for i, r := range rs {
		items[i] = r.toTaskLogPart()
	}
	var count *int
	if err := ds.get(&count, `select count(*) from tasks_log_parts where task_id = $1`, taskID); err != nil {
		return nil, errors.Wrapf(err, "error getting the task log parts count")
	}
	totalPages := *count / size
	if *count%size != 0 {
		totalPages = totalPages + 1
	}
	return &datastore.Page[*tork.TaskLogPart]{
		Items:      items,
		Number:     page,
		Size:       len(items),
		TotalPages: totalPages,
		TotalItems: *count,
	}, nil
}

func (ds *PostgresDatastore) GetJobs(ctx context.Context, q string, page, size int) (*datastore.Page[*tork.JobSummary], error) {
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

	return &datastore.Page[*tork.JobSummary]{
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

func (ds *PostgresDatastore) WithTx(ctx context.Context, f func(tx datastore.Datastore) error) error {
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
