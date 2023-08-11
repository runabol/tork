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
	Name        string            `json:"name,omitempty"`
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
	Result      string            `json:"result,omitempty"`
	Error       string            `json:"error,omitempty"`
	Pre         []Task            `json:"pre,omitempty"`
	Post        []Task            `json:"post,omitempty"`
	Volumes     []string          `json:"volumes,omitempty"`
	Node        string            `json:"node,omitempty"`
	Retry       *Retry            `json:"retry,omitempty"`
	Limits      Limits            `json:"limits,omitempty"`
}

const (
	RETRY_DEFAULT_INITIAL_DELAY  = "1s"
	RETRY_DEFAULT_SCALING_FACTOR = 2
)

// Retry allows to specify a retry policy for a given
// task using the exponential backoff formula:
//
// initalDelay*scalingFactor^attempt
type Retry struct {
	Limit         int    `json:"limit,omitempty"`
	InitialDelay  string `json:"initialDelay,omitempty"`
	ScalingFactor int    `json:"scalingFactor,omitempty"`
	Attempts      int    `json:"attempts,omitempty"`
}

type Limits struct {
	CPUs   string `json:"cpus,omitempty"`
	Memory string `json:"memory,omitempty"`
}
