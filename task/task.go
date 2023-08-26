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
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	State       State             `json:"state,omitempty"`
	CreatedAt   *time.Time        `json:"createdAt,omitempty"`
	ScheduledAt *time.Time        `json:"scheduledAt,omitempty"`
	StartedAt   *time.Time        `json:"startedAt,omitempty"`
	CompletedAt *time.Time        `json:"completedAt,omitempty"`
	FailedAt    *time.Time        `json:"failedAt,omitempty"`
	CMD         []string          `json:"cmd,omitempty"`
	Entrypoint  []string          `json:"entrypoint,omitempty"`
	Run         string            `json:"run,omitempty"`
	Image       string            `json:"image,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	Queue       string            `json:"queue,omitempty"`
	Error       string            `json:"error,omitempty"`
	Pre         []*Task           `json:"pre,omitempty"`
	Post        []*Task           `json:"post,omitempty"`
	Volumes     []string          `json:"volumes,omitempty"`
	Networks    []string          `json:"networks,omitempty"`
	NodeID      string            `json:"nodeId,omitempty"`
	Retry       *Retry            `json:"retry,omitempty"`
	Limits      *Limits           `json:"limits,omitempty"`
	Timeout     string            `json:"timeout,omitempty"`
	Result      string            `json:"result,omitempty"`
	Var         string            `json:"var,omitempty"`
	If          string            `json:"if,omitempty"`
	Parallel    *Parallel         `json:"parallel,omitempty"`
	Each        *Each             `json:"each,omitempty"`
	SubJob      *SubJob           `json:"subjob,omitempty"`
}

type SubJob struct {
	ID          string            `json:"id,omitempty"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Tasks       []*Task           `json:"tasks,omitempty"`
	Inputs      map[string]string `json:"inputs,omitempty"`
	Output      string            `json:"output,omitempty"`
}

type Parallel struct {
	Tasks       []*Task `json:"tasks,omitempty"`
	Completions int     `json:"completions,omitempty"`
}

type Each struct {
	List        string `json:"list,omitempty"`
	Task        *Task  `json:"task,omitempty"`
	Size        int    `json:"size,omitempty"`
	Completions int    `json:"completions,omitempty"`
}

type Retry struct {
	Limit    int `json:"limit,omitempty"`
	Attempts int `json:"attempts,omitempty"`
}

type Limits struct {
	CPUs   string `json:"cpus,omitempty"`
	Memory string `json:"memory,omitempty"`
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
	var subjob *SubJob
	if t.SubJob != nil {
		subjob = t.SubJob.Clone()
	}
	var parallel *Parallel
	if t.Parallel != nil {
		parallel = t.Parallel.Clone()
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

func (e *Each) Clone() *Each {
	return &Each{
		List: e.List,
		Task: e.Task.Clone(),
	}
}

func (s *SubJob) Clone() *SubJob {
	return &SubJob{
		ID:          s.ID,
		Name:        s.Name,
		Description: s.Description,
		Inputs:      clone.CloneStringMap(s.Inputs),
		Tasks:       CloneTasks(s.Tasks),
		Output:      s.Output,
	}
}

func (p *Parallel) Clone() *Parallel {
	return &Parallel{
		Tasks:       CloneTasks(p.Tasks),
		Completions: p.Completions,
	}
}
