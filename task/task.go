package task

import (
	"time"

	"github.com/runabol/tork/clone"
)

// State defines the list of states that a
// task can be in, at any given moment.
type State string

const (
	Pending   State = "PENDING"
	Scheduled State = "SCHEDULED"
	Running   State = "RUNNING"
	Cancelled State = "CANCELLED"
	Stopped   State = "STOPPED"
	Completed State = "COMPLETED"
	Failed    State = "FAILED"
)

// Task is the basic unit of work that a Worker can handle.
type Task struct {
	ID          string            `json:"id,omitempty"`
	JobID       string            `json:"jobId,omitempty"`
	ParentID    string            `json:"parentId,omitempty"`
	Position    int               `json:"position,omitempty"`
	Name        string            `json:"name,omitempty" yaml:"name,omitempty"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	State       State             `json:"state,omitempty"`
	CreatedAt   *time.Time        `json:"createdAt,omitempty"`
	ScheduledAt *time.Time        `json:"scheduledAt,omitempty"`
	StartedAt   *time.Time        `json:"startedAt,omitempty"`
	CompletedAt *time.Time        `json:"completedAt,omitempty"`
	FailedAt    *time.Time        `json:"failedAt,omitempty"`
	CMD         []string          `json:"cmd,omitempty" yaml:"cmd,omitempty"`
	Entrypoint  []string          `json:"entrypoint,omitempty" yaml:"entrypoint,omitempty"`
	Run         string            `json:"run,omitempty" yaml:"run,omitempty"`
	Image       string            `json:"image,omitempty" yaml:"image,omitempty"`
	Env         map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	Queue       string            `json:"queue,omitempty" yaml:"queue,omitempty"`
	Error       string            `json:"error,omitempty"`
	Pre         []*Task           `json:"pre,omitempty" yaml:"pre,omitempty"`
	Post        []*Task           `json:"post,omitempty" yaml:"post,omitempty"`
	Volumes     []string          `json:"volumes,omitempty" yaml:"volumes,omitempty"`
	Node        string            `json:"node,omitempty"`
	Retry       *Retry            `json:"retry,omitempty" yaml:"retry,omitempty"`
	Limits      *Limits           `json:"limits,omitempty" yaml:"limits,omitempty"`
	Timeout     string            `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Result      string            `json:"result,omitempty"`
	Var         string            `json:"var,omitempty" yaml:"var,omitempty"`
	If          string            `json:"if,omitempty" yaml:"if,omitempty"`
	Parallel    []*Task           `json:"parallel,omitempty" yaml:"parallel,omitempty"`
	Completions int               `json:"completions,omitempty"`
	Each        *Each             `json:"each,omitempty" yaml:"each,omitempty"`
}

type Each struct {
	List string `json:"list,omitempty" yaml:"list,omitempty"`
	Task *Task  `json:"task,omitempty" yaml:"task,omitempty"`
	Size int    `json:"size,omitempty"`
}

type Retry struct {
	Limit    int `json:"limit,omitempty" yaml:"limit,omitempty"`
	Attempts int `json:"attempts,omitempty" yaml:"attempts,omitempty"`
}

type Limits struct {
	CPUs   string `json:"cpus,omitempty" yaml:"cpus,omitempty"`
	Memory string `json:"memory,omitempty" yaml:"memory,omitempty"`
}

func (s State) IsActive() bool {
	return s == Pending ||
		s == Scheduled ||
		s == Running
}

func (t *Task) Clone() *Task {
	var retry *Retry
	if t.Retry != nil {
		retry = t.Retry.Clone()
	}
	var limits *Limits
	if t.Limits != nil {
		limits = t.Limits.Clone()
	}
	var each *Each
	if t.Each != nil {
		each = t.Each.Clone()
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
		Env:         clone.CloneStringMap(t.Env),
		Queue:       t.Queue,
		Error:       t.Error,
		Pre:         CloneTasks(t.Pre),
		Post:        CloneTasks(t.Post),
		Volumes:     t.Volumes,
		Node:        t.Node,
		Retry:       retry,
		Limits:      limits,
		Timeout:     t.Timeout,
		Result:      t.Result,
		Var:         t.Var,
		If:          t.If,
		Parallel:    CloneTasks(t.Parallel),
		Completions: t.Completions,
		Each:        each,
		Description: t.Description,
	}
}

func CloneTasks(tasks []*Task) []*Task {
	copy := make([]*Task, len(tasks))
	for i, t := range tasks {
		copy[i] = t.Clone()
	}
	return copy
}

func (r *Retry) Clone() *Retry {
	return &Retry{
		Limit:    r.Limit,
		Attempts: r.Attempts,
	}
}

func (l *Limits) Clone() *Limits {
	return &Limits{
		CPUs:   l.CPUs,
		Memory: l.Memory,
	}
}

func (l *Each) Clone() *Each {
	return &Each{
		List: l.List,
		Task: l.Task.Clone(),
	}
}
