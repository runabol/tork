package job

import (
	"time"

	"github.com/runabol/tork/clone"
	"github.com/runabol/tork/task"
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
	Name        string            `json:"name,omitempty" yaml:"name,omitempty"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	State       State             `json:"state,omitempty"`
	CreatedAt   time.Time         `json:"createdAt,omitempty"`
	StartedAt   *time.Time        `json:"startedAt,omitempty"`
	CompletedAt *time.Time        `json:"completedAt,omitempty"`
	FailedAt    *time.Time        `json:"failedAt,omitempty"`
	Tasks       []*task.Task      `json:"tasks,omitempty" yaml:"tasks,omitempty"`
	Execution   []*task.Task      `json:"execution,omitempty"`
	Position    int               `json:"position,omitempty"`
	Inputs      map[string]string `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Context     Context           `json:"context,omitempty"`
}

type Context struct {
	Inputs map[string]string `json:"inputs,omitempty"`
	Tasks  map[string]string `json:"tasks,omitempty"`
}

func (j *Job) Clone() *Job {
	return &Job{
		ID:          j.ID,
		Name:        j.Name,
		State:       j.State,
		CreatedAt:   j.CreatedAt,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
		FailedAt:    j.FailedAt,
		Tasks:       task.CloneTasks(j.Tasks),
		Execution:   task.CloneTasks(j.Execution),
		Position:    j.Position,
		Inputs:      j.Inputs,
		Context:     j.Context.Clone(),
	}
}

func (c Context) Clone() Context {
	return Context{
		Inputs: clone.CloneStringMap(c.Inputs),
		Tasks:  clone.CloneStringMap(c.Tasks),
	}
}

func (c Context) AsMap() map[string]any {
	return map[string]any{
		"inputs": c.Inputs,
		"tasks":  c.Tasks,
	}
}
