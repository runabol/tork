package tork

import (
	"slices"
	"time"

	"golang.org/x/exp/maps"
)

// State defines the list of states that a
// task can be in, at any given moment.
type TaskState = string

const (
	TaskStateCreated   TaskState = "CREATED"
	TaskStatePending   TaskState = "PENDING"
	TaskStateScheduled TaskState = "SCHEDULED"
	TaskStateRunning   TaskState = "RUNNING"
	TaskStateCancelled TaskState = "CANCELLED"
	TaskStateStopped   TaskState = "STOPPED"
	TaskStateCompleted TaskState = "COMPLETED"
	TaskStateFailed    TaskState = "FAILED"
	TaskStateSkipped   TaskState = "SKIPPED"
)

var TaskStateActive = []TaskState{
	TaskStateCreated,
	TaskStatePending,
	TaskStateScheduled,
	TaskStateRunning,
}

// Task is the basic unit of work that a Worker can handle.
type Task struct {
	ID          string            `json:"id,omitempty"`
	JobID       string            `json:"jobId,omitempty"`
	ParentID    string            `json:"parentId,omitempty"`
	Position    int               `json:"position,omitempty"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	State       TaskState         `json:"state,omitempty"`
	CreatedAt   *time.Time        `json:"createdAt,omitempty"`
	ScheduledAt *time.Time        `json:"scheduledAt,omitempty"`
	StartedAt   *time.Time        `json:"startedAt,omitempty"`
	CompletedAt *time.Time        `json:"completedAt,omitempty"`
	FailedAt    *time.Time        `json:"failedAt,omitempty"`
	CMD         []string          `json:"cmd,omitempty"`
	Entrypoint  []string          `json:"entrypoint,omitempty"`
	Run         string            `json:"run,omitempty"`
	Image       string            `json:"image,omitempty"`
	Registry    *Registry         `json:"registry,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	Files       map[string]string `json:"files,omitempty"`
	Queue       string            `json:"queue,omitempty"`
	Error       string            `json:"error,omitempty"`
	Pre         []*Task           `json:"pre,omitempty"`
	Post        []*Task           `json:"post,omitempty"`
	Sidecars    []*Task           `json:"sidecars,omitempty"`
	Mounts      []Mount           `json:"mounts,omitempty"`
	Networks    []string          `json:"networks,omitempty"`
	NodeID      string            `json:"nodeId,omitempty"`
	Retry       *TaskRetry        `json:"retry,omitempty"`
	Limits      *TaskLimits       `json:"limits,omitempty"`
	Timeout     string            `json:"timeout,omitempty"`
	Result      string            `json:"result,omitempty"`
	Var         string            `json:"var,omitempty"`
	If          string            `json:"if,omitempty"`
	Parallel    *ParallelTask     `json:"parallel,omitempty"`
	Each        *EachTask         `json:"each,omitempty"`
	SubJob      *SubJobTask       `json:"subjob,omitempty"`
	GPUs        string            `json:"gpus,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Workdir     string            `json:"workdir,omitempty"`
	Priority    int               `json:"priority,omitempty"`
	Progress    float64           `json:"progress,omitempty"`
	Probe       *Probe            `json:"probe,omitempty"`
}

type TaskSummary struct {
	ID          string     `json:"id,omitempty"`
	JobID       string     `json:"jobId,omitempty"`
	Position    int        `json:"position,omitempty"`
	Progress    float64    `json:"progress,omitempty"`
	Name        string     `json:"name,omitempty"`
	Description string     `json:"description,omitempty"`
	State       TaskState  `json:"state,omitempty"`
	CreatedAt   *time.Time `json:"createdAt,omitempty"`
	ScheduledAt *time.Time `json:"scheduledAt,omitempty"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	Error       string     `json:"error,omitempty"`
	Result      string     `json:"result,omitempty"`
	Var         string     `json:"var,omitempty"`
	Tags        []string   `json:"tags,omitempty"`
}

type TaskLogPart struct {
	ID        string     `json:"id,omitempty"`
	Number    int        `json:"number,omitempty"`
	TaskID    string     `json:"taskId,omitempty"`
	Contents  string     `json:"contents,omitempty"`
	CreatedAt *time.Time `json:"createdAt,omitempty"`
}

type SubJobTask struct {
	ID          string            `json:"id,omitempty"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Tasks       []*Task           `json:"tasks,omitempty"`
	Inputs      map[string]string `json:"inputs,omitempty"`
	Secrets     map[string]string `json:"secrets,omitempty"`
	AutoDelete  *AutoDelete       `json:"autoDelete,omitempty"`
	Output      string            `json:"output,omitempty"`
	Detached    bool              `json:"detached,omitempty"`
	Webhooks    []*Webhook        `json:"webhooks,omitempty"`
}

type ParallelTask struct {
	Tasks       []*Task `json:"tasks,omitempty"`
	Completions int     `json:"completions,omitempty"`
}

type EachTask struct {
	Var         string `json:"var,omitempty"`
	List        string `json:"list,omitempty"`
	Task        *Task  `json:"task,omitempty"`
	Size        int    `json:"size,omitempty"`
	Completions int    `json:"completions,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
	Index       int    `json:"index,omitempty"`
}

type TaskRetry struct {
	Limit    int `json:"limit,omitempty"`
	Attempts int `json:"attempts,omitempty"`
}

type TaskLimits struct {
	CPUs   string `json:"cpus,omitempty"`
	Memory string `json:"memory,omitempty"`
}

type Registry struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type Probe struct {
	Path    string `json:"path,omitempty"`
	Port    int    `json:"port,omitempty"`
	Timeout string `json:"timeout,omitempty"`
}

func (t *Task) IsActive() bool {
	return slices.Contains(TaskStateActive, t.State)
}

func (t *Task) Clone() *Task {
	var retry *TaskRetry
	if t.Retry != nil {
		retry = t.Retry.Clone()
	}
	var limits *TaskLimits
	if t.Limits != nil {
		limits = t.Limits.Clone()
	}
	var each *EachTask
	if t.Each != nil {
		each = t.Each.Clone()
	}
	var subjob *SubJobTask
	if t.SubJob != nil {
		subjob = t.SubJob.Clone()
	}
	var parallel *ParallelTask
	if t.Parallel != nil {
		parallel = t.Parallel.Clone()
	}
	var registry *Registry
	if t.Registry != nil {
		registry = t.Registry.Clone()
	}
	var probe *Probe
	if t.Probe != nil {
		probe = t.Probe.Clone()
	}
	return &Task{
		ID:          t.ID,
		JobID:       t.JobID,
		ParentID:    t.ParentID,
		Position:    t.Position,
		Name:        t.Name,
		State:       t.State,
		CreatedAt:   t.CreatedAt,
		ScheduledAt: t.ScheduledAt,
		StartedAt:   t.StartedAt,
		CompletedAt: t.CompletedAt,
		FailedAt:    t.FailedAt,
		CMD:         t.CMD,
		Entrypoint:  t.Entrypoint,
		Run:         t.Run,
		Image:       t.Image,
		Registry:    registry,
		Env:         maps.Clone(t.Env),
		Files:       maps.Clone(t.Files),
		Queue:       t.Queue,
		Error:       t.Error,
		Pre:         CloneTasks(t.Pre),
		Post:        CloneTasks(t.Post),
		Sidecars:    CloneTasks(t.Sidecars),
		Mounts:      slices.Clone(t.Mounts),
		Networks:    t.Networks,
		NodeID:      t.NodeID,
		Retry:       retry,
		Limits:      limits,
		Timeout:     t.Timeout,
		Result:      t.Result,
		Var:         t.Var,
		If:          t.If,
		Parallel:    parallel,
		Each:        each,
		Description: t.Description,
		SubJob:      subjob,
		GPUs:        t.GPUs,
		Tags:        t.Tags,
		Workdir:     t.Workdir,
		Priority:    t.Priority,
		Progress:    t.Progress,
		Probe:       probe,
	}
}

func CloneTasks(tasks []*Task) []*Task {
	copy := make([]*Task, len(tasks))
	for i, t := range tasks {
		copy[i] = t.Clone()
	}
	return copy
}

func (r *TaskRetry) Clone() *TaskRetry {
	return &TaskRetry{
		Limit:    r.Limit,
		Attempts: r.Attempts,
	}
}

func (l *TaskLimits) Clone() *TaskLimits {
	return &TaskLimits{
		CPUs:   l.CPUs,
		Memory: l.Memory,
	}
}

func (e *EachTask) Clone() *EachTask {
	return &EachTask{
		Var:         e.Var,
		List:        e.List,
		Task:        e.Task.Clone(),
		Size:        e.Size,
		Completions: e.Completions,
		Concurrency: e.Concurrency,
		Index:       e.Index,
	}
}

func (s *SubJobTask) Clone() *SubJobTask {
	var autoDelete *AutoDelete
	if s.AutoDelete != nil {
		autoDelete = s.AutoDelete.Clone()
	}
	return &SubJobTask{
		ID:          s.ID,
		Name:        s.Name,
		Description: s.Description,
		Inputs:      maps.Clone(s.Inputs),
		Secrets:     maps.Clone(s.Secrets),
		AutoDelete:  autoDelete,
		Tasks:       CloneTasks(s.Tasks),
		Output:      s.Output,
		Detached:    s.Detached,
		Webhooks:    CloneWebhooks(s.Webhooks),
	}
}

func (p *ParallelTask) Clone() *ParallelTask {
	return &ParallelTask{
		Tasks:       CloneTasks(p.Tasks),
		Completions: p.Completions,
	}
}

func (r *Registry) Clone() *Registry {
	return &Registry{
		Username: r.Username,
		Password: r.Password,
	}
}

func (p *Probe) Clone() *Probe {
	return &Probe{
		Path:    p.Path,
		Port:    p.Port,
		Timeout: p.Timeout,
	}
}

func NewTaskSummary(t *Task) *TaskSummary {
	return &TaskSummary{
		ID:          t.ID,
		JobID:       t.JobID,
		Position:    t.Position,
		Progress:    t.Progress,
		Name:        t.Name,
		Description: t.Description,
		State:       t.State,
		CreatedAt:   t.CreatedAt,
		ScheduledAt: t.ScheduledAt,
		StartedAt:   t.StartedAt,
		CompletedAt: t.CompletedAt,
		Error:       t.Error,
		Result:      t.Result,
		Var:         t.Var,
		Tags:        t.Tags,
	}
}
