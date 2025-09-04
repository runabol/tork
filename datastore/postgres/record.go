package postgres

import (
	"encoding/json"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/runabol/tork"
)

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
	Sidecars    []byte         `db:"sidecars"`
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
	GPUs        string         `db:"gpus"`
	IF          string         `db:"if_"`
	Tags        pq.StringArray `db:"tags"`
	Priority    int            `db:"priority"`
	Workdir     string         `db:"workdir"`
	Progress    float64        `db:"progress"`
}

type jobRecord struct {
	ID             string         `db:"id"`
	Name           string         `db:"name"`
	Description    string         `db:"description"`
	Tags           pq.StringArray `db:"tags"`
	State          string         `db:"state"`
	CreatedAt      time.Time      `db:"created_at"`
	CreatedBy      string         `db:"created_by"`
	StartedAt      *time.Time     `db:"started_at"`
	CompletedAt    *time.Time     `db:"completed_at"`
	FailedAt       *time.Time     `db:"failed_at"`
	DeleteAt       *time.Time     `db:"delete_at"`
	Tasks          []byte         `db:"tasks"`
	Position       int            `db:"position"`
	Inputs         []byte         `db:"inputs"`
	Context        []byte         `db:"context"`
	ParentID       string         `db:"parent_id"`
	TaskCount      int            `db:"task_count"`
	Output         string         `db:"output_"`
	Result         string         `db:"result"`
	Error          string         `db:"error_"`
	TS             string         `db:"ts"`
	Defaults       []byte         `db:"defaults"`
	Webhooks       []byte         `db:"webhooks"`
	AutoDelete     []byte         `db:"auto_delete"`
	Secrets        []byte         `db:"secrets"`
	Progress       float64        `db:"progress"`
	ScheduledJobID *string        `db:"scheduled_job_id"`
}

type scheduledJobRecord struct {
	ID          string         `db:"id"`
	Cron        string         `db:"cron_expr"`
	Name        string         `db:"name"`
	Description string         `db:"description"`
	Tags        pq.StringArray `db:"tags"`
	State       string         `db:"state"`
	CreatedAt   time.Time      `db:"created_at"`
	CreatedBy   string         `db:"created_by"`
	Tasks       []byte         `db:"tasks"`
	Inputs      []byte         `db:"inputs"`
	Output      string         `db:"output_"`
	Defaults    []byte         `db:"defaults"`
	Webhooks    []byte         `db:"webhooks"`
	AutoDelete  []byte         `db:"auto_delete"`
	Secrets     []byte         `db:"secrets"`
}

type jobPermRecord struct {
	ID        string    `db:"id"`
	JobID     string    `db:"job_id"`
	UserID    *string   `db:"user_id"`
	RoleID    *string   `db:"role_id"`
	CreatedAt time.Time `db:"created_at"`
}

type scheduledPermRecord struct {
	ID             string    `db:"id"`
	ScheduledJobID string    `db:"scheduled_job_id"`
	UserID         *string   `db:"user_id"`
	RoleID         *string   `db:"role_id"`
	CreatedAt      time.Time `db:"created_at"`
}

type nodeRecord struct {
	ID              string    `db:"id"`
	Name            string    `db:"name"`
	StartedAt       time.Time `db:"started_at"`
	LastHeartbeatAt time.Time `db:"last_heartbeat_at"`
	CPUPercent      float64   `db:"cpu_percent"`
	Queue           string    `db:"queue"`
	Status          string    `db:"status"`
	Hostname        string    `db:"hostname"`
	Port            int       `db:"port"`
	TaskCount       int       `db:"task_count"`
	Version         string    `db:"version_"`
}

type taskLogPartRecord struct {
	ID       string    `db:"id"`
	Number   int       `db:"number_"`
	TaskID   string    `db:"task_id"`
	CreateAt time.Time `db:"created_at"`
	Contents string    `db:"contents"`
	TS       string    `db:"ts"`
}

type userRecord struct {
	ID        string    `db:"id"`
	Name      string    `db:"name"`
	Username  string    `db:"username_"`
	Password  string    `db:"password_"`
	CreatedAt time.Time `db:"created_at"`
	Disabled  bool      `db:"is_disabled"`
}

type roleRecord struct {
	ID        string    `db:"id"`
	Slug      string    `db:"slug"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
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
	var sidecars []*tork.Task
	if r.Sidecars != nil {
		if err := json.Unmarshal(r.Sidecars, &sidecars); err != nil {
			return nil, errors.Wrapf(err, "error deserializing task.sidecars")
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
	var mounts []tork.Mount
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
		Sidecars:    sidecars,
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
		GPUs:        r.GPUs,
		If:          r.IF,
		Tags:        r.Tags,
		Priority:    r.Priority,
		Workdir:     r.Workdir,
		Progress:    r.Progress,
	}, nil
}

func (r nodeRecord) toNode() *tork.Node {
	n := tork.Node{
		ID:              r.ID,
		Name:            r.Name,
		StartedAt:       r.StartedAt,
		CPUPercent:      r.CPUPercent,
		LastHeartbeatAt: r.LastHeartbeatAt,
		Queue:           r.Queue,
		Status:          tork.NodeStatus(r.Status),
		Hostname:        r.Hostname,
		Port:            r.Port,
		TaskCount:       r.TaskCount,
		Version:         r.Version,
	}
	// if we hadn't seen an heartbeat for two or more
	// consecutive periods we consider the node as offline
	if n.LastHeartbeatAt.Before(time.Now().UTC().Add(-tork.HEARTBEAT_RATE*2)) && n.Status == tork.NodeStatusUP {
		n.Status = tork.NodeStatusOffline
	}
	return &n
}

func (r taskLogPartRecord) toTaskLogPart() *tork.TaskLogPart {
	return &tork.TaskLogPart{
		ID:        r.ID,
		Number:    r.Number,
		TaskID:    r.TaskID,
		Contents:  r.Contents,
		CreatedAt: &r.CreateAt,
	}
}

func (r jobRecord) toJob(
	tasks,
	execution []*tork.Task,
	createdBy *tork.User,
	perms []*tork.Permission,
	encryptionKey *string,
) (*tork.Job, error) {
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
	var autoDelete *tork.AutoDelete
	if r.AutoDelete != nil {
		autoDelete = &tork.AutoDelete{}
		if err := json.Unmarshal(r.AutoDelete, autoDelete); err != nil {
			return nil, errors.Wrapf(err, "error deserializing job.autoDelete")
		}
	}
	var webhooks []*tork.Webhook
	if err := json.Unmarshal(r.Webhooks, &webhooks); err != nil {
		return nil, errors.Wrapf(err, "error deserializing job.webhook")
	}
	var secrets map[string]string
	if r.Secrets != nil {
		if err := json.Unmarshal(r.Secrets, &secrets); err != nil {
			return nil, errors.Wrapf(err, "error deserializing job.secrets")
		}
		decryptedSecrets, err := decryptSecrets(secrets, encryptionKey)
		if err != nil {
			return nil, errors.Wrapf(err, "error decrypting job.secrets")
		}
		secrets = decryptedSecrets
	}
	var schedule *tork.JobSchedule
	if r.ScheduledJobID != nil {
		schedule = &tork.JobSchedule{
			ID: *r.ScheduledJobID,
		}
	}
	return &tork.Job{
		ID:          r.ID,
		Name:        r.Name,
		Tags:        r.Tags,
		State:       tork.JobState(r.State),
		CreatedAt:   r.CreatedAt,
		CreatedBy:   createdBy,
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
		Webhooks:    webhooks,
		Permissions: perms,
		AutoDelete:  autoDelete,
		DeleteAt:    r.DeleteAt,
		Secrets:     secrets,
		Progress:    r.Progress,
		Schedule:    schedule,
	}, nil
}

func (r scheduledJobRecord) toScheduledJob(tasks []*tork.Task, createdBy *tork.User, perms []*tork.Permission, encryptionKey *string) (*tork.ScheduledJob, error) {
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
	var autoDelete *tork.AutoDelete
	if r.AutoDelete != nil {
		autoDelete = &tork.AutoDelete{}
		if err := json.Unmarshal(r.AutoDelete, autoDelete); err != nil {
			return nil, errors.Wrapf(err, "error deserializing job.autoDelete")
		}
	}
	var webhooks []*tork.Webhook
	if err := json.Unmarshal(r.Webhooks, &webhooks); err != nil {
		return nil, errors.Wrapf(err, "error deserializing job.webhook")
	}
	var secrets map[string]string
	if r.Secrets != nil {
		if err := json.Unmarshal(r.Secrets, &secrets); err != nil {
			return nil, errors.Wrapf(err, "error deserializing job.secrets")
		}
		decryptedSecrets, err := decryptSecrets(secrets, encryptionKey)
		if err != nil {
			return nil, errors.Wrapf(err, "error decrypting job.secrets")
		}
		secrets = decryptedSecrets
	}
	return &tork.ScheduledJob{
		ID:          r.ID,
		Cron:        r.Cron,
		Name:        r.Name,
		Tags:        r.Tags,
		State:       tork.ScheduledJobState(r.State),
		CreatedAt:   r.CreatedAt,
		CreatedBy:   createdBy,
		Tasks:       tasks,
		Inputs:      inputs,
		Description: r.Description,
		Output:      r.Output,
		Defaults:    defaults,
		Webhooks:    webhooks,
		Permissions: perms,
		AutoDelete:  autoDelete,
		Secrets:     secrets,
	}, nil
}

func (r userRecord) toUser() *tork.User {
	n := tork.User{
		ID:           r.ID,
		Name:         r.Name,
		Username:     r.Username,
		PasswordHash: r.Password,
		CreatedAt:    &r.CreatedAt,
		Disabled:     r.Disabled,
	}
	return &n
}

func (r roleRecord) toRole() *tork.Role {
	n := tork.Role{
		ID:        r.ID,
		Slug:      r.Slug,
		Name:      r.Name,
		CreatedAt: &r.CreatedAt,
	}
	return &n
}
