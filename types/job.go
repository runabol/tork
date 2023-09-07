package types

import (
	"time"

	"github.com/runabol/tork/clone"
	"github.com/runabol/tork/types/task"
)

type State string

const (
	StatusPending   State = "PENDING"
	StatusRunning   State = "RUNNING"
	StatusCancelled State = "CANCELLED"
	StatusCompleted State = "COMPLETED"
	StatusFailed    State = "FAILED"
	StatusRestart   State = "RESTART"
)

type Job struct {
	ID          string            `json:"id,omitempty"`
	ParentID    string            `json:"parentId,omitempty"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	State       State             `json:"state,omitempty"`
	CreatedAt   time.Time         `json:"createdAt,omitempty"`
	StartedAt   *time.Time        `json:"startedAt,omitempty"`
	CompletedAt *time.Time        `json:"completedAt,omitempty"`
	FailedAt    *time.Time        `json:"failedAt,omitempty"`
	Tasks       []*task.Task      `json:"tasks"`
	Execution   []*task.Task      `json:"execution"`
	Position    int               `json:"position"`
	Inputs      map[string]string `json:"inputs,omitempty"`
	Context     Context           `json:"context,omitempty"`
	TaskCount   int               `json:"taskCount,omitempty"`
	Output      string            `json:"output,omitempty"`
	Result      string            `json:"result,omitempty"`
	Error       string            `json:"error,omitempty"`
}

type Context struct {
	Inputs map[string]string `json:"inputs,omitempty"`
	Tasks  map[string]string `json:"tasks,omitempty"`
}

func (j *Job) Clone() *Job {
	return &Job{
		ID:          j.ID,
		Name:        j.Name,
		Description: j.Description,
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
		ParentID:    j.ParentID,
		TaskCount:   j.TaskCount,
		Output:      j.Output,
		Result:      j.Result,
		Error:       j.Error,
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
