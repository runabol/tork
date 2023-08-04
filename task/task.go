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
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	State          State     `json:"state"`
	StartTime      time.Time `json:"startTime"`
	CompletionTime time.Time `json:"completionTime"`
	CMD            []string  `json:"cmd"`
	Image          string    `json:"image"`
	Memory         int64     `json:"memory"`
	Disk           int64     `json:"disk"`
	Env            []string  `json:"env"`
	RestartPolicy  string    `json:"restartPolicy"`
}

// CancelRequest is used by the Coordinator to singal a Worker to
// cancel a running task.
type CancelRequest struct {
	ID   string
	Task Task
}
