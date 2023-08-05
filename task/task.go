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
	ID            string     `json:"id"`
	Name          string     `json:"name,omitempty"`
	State         State      `json:"state"`
	ScheduledAt   *time.Time `json:"scheduledAt,omitempty"`
	StartedAt     *time.Time `json:"startedAt,omitempty"`
	CompletedAt   *time.Time `json:"completedAt,omitempty"`
	CMD           []string   `json:"cmd,omitempty"`
	Image         string     `json:"image"`
	Memory        int64      `json:"memory,omitempty"`
	Disk          int64      `json:"disk,omitempty"`
	Env           []string   `json:"env,omitempty"`
	RestartPolicy string     `json:"restartPolicy,omitempty"`
	Queue         string     `json:"queue,omitempty"`
}
