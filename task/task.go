package task

import (
	"time"
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
	Position    int               `json:"position,omitempty"`
	Name        string            `json:"name,omitempty" yaml:"name,omitempty"`
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
	Pre         []Task            `json:"pre,omitempty" yaml:"pre,omitempty"`
	Post        []Task            `json:"post,omitempty" yaml:"post,omitempty"`
	Volumes     []string          `json:"volumes,omitempty" yaml:"volumes,omitempty"`
	Node        string            `json:"node,omitempty"`
	Retry       *Retry            `json:"retry,omitempty" yaml:"retry,omitempty"`
	Limits      *Limits           `json:"limits,omitempty" yaml:"limits,omitempty"`
	Timeout     string            `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Outputs     map[string]string `json:"outputs,omitempty"`
	Var         string            `json:"var,omitempty" yaml:"var,omitempty"`
}

const (
	RETRY_DEFAULT_INITIAL_DELAY  = "1s"
	RETRY_DEFAULT_SCALING_FACTOR = 2
)

// Retry allows to specify a retry policy for a given
// task using the exponential backoff formula:
//
// initialDelay*scalingFactor^attempt
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
