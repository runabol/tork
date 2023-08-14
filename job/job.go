package job

import (
	"time"

	"github.com/tork/task"
)

type State string

const (
	Pending   State = "PENDING"
	Running   State = "RUNNING"
	Cancelled State = "CANCELLED"
	Completed State = "COMPLETED"
	Failed    State = "FAILED"
)

type Job struct {
	ID          string            `json:"id,omitempty"`
	Name        string            `json:"name,omitempty"`
	State       State             `json:"state,omitempty"`
	CreatedAt   time.Time         `json:"createdAt,omitempty"`
	StartedAt   *time.Time        `json:"startedAt,omitempty"`
	CompletedAt *time.Time        `json:"completedAt,omitempty"`
	FailedAt    *time.Time        `json:"failedAt,omitempty"`
	Tasks       []task.Task       `json:"tasks,omitempty"`
	Execution   []task.Task       `json:"execution,omitempty"`
	Position    int               `json:"position,omitempty"`
	Inputs      map[string]string `json:"inputs,omitempty"`
	Context     Context           `json:"context,omitempty"`
}

type Context struct {
	Inputs map[string]string            `json:"inputs,omitempty"`
	Tasks  map[string]map[string]string `json:"tasks,omitempty"`
}
