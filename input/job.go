package input

import (
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/uuid"
)

type Job struct {
	Name        string            `json:"name,omitempty" yaml:"name,omitempty" validate:"required"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	Tasks       []Task            `json:"tasks,omitempty" yaml:"tasks,omitempty" validate:"required,min=1,dive"`
	Inputs      map[string]string `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Output      string            `json:"output,omitempty" yaml:"output,omitempty" validate:"expr"`
}

func (ji Job) ToJob() *tork.Job {
	n := time.Now().UTC()
	j := &tork.Job{}
	j.ID = uuid.NewUUID()
	j.Description = ji.Description
	j.Inputs = ji.Inputs
	j.Name = ji.Name
	tasks := make([]*tork.Task, len(ji.Tasks))
	for i, ti := range ji.Tasks {
		tasks[i] = ti.toTask()
	}
	j.Tasks = tasks
	j.State = tork.JobStatePending
	j.CreatedAt = n
	j.Context = tork.JobContext{}
	j.Context.Inputs = ji.Inputs
	j.TaskCount = len(tasks)
	j.Output = ji.Output
	return j
}
